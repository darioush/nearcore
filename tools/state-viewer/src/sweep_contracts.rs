//! Sweep all deployed contracts from flat storage into a directory of wasm files.
//!
//! The sweep has two phases:
//! 1. Scan the `Account` records of every shard and write the account to
//!    contract mapping into `accounts.<shard_uid>.tsv`. Scan the
//!    `GlobalContractCode` records and write the global contracts.
//! 2. Write one wasm file per unique local contract code hash.
//!
//! Both phases save progress in the output directory. A rerun with the same
//! output directory resumes the sweep. A read-only database opened next to a
//! running node can fail when the node compacts and deletes SST files. Run the
//! command in a loop until it exits with success:
//!
//! ```text
//! until neard view-state sweep-contracts --output-dir OUT; do sleep 5; done
//! ```

use anyhow::Context;
use borsh::BorshDeserialize;
use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};
use near_epoch_manager::{EpochManager, EpochManagerAdapter};
use near_primitives::account::{Account, AccountContract};
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::{ShardLayout, ShardUId};
use near_primitives::state::FlatStateValue;
use near_primitives::trie_key::GlobalContractCodeIdentifier;
use near_primitives::trie_key::col;
use near_primitives::trie_key::trie_key_parsers::parse_account_id_from_account_key;
use near_primitives::types::AccountId;
use near_store::adapter::StoreAdapter;
use near_store::flat::FlatStorageStatus;
use near_store::{DBCol, Store};
use nearcore::NearConfig;
use parking_lot::Mutex;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

const STATE_FILE: &str = "state.json";
const CONTRACTS_DIR: &str = "contracts";
const GLOBAL_BY_HASH_DIR: &str = "global_contracts/by_hash";
const GLOBAL_BY_ACCOUNT_DIR: &str = "global_contracts/by_account";
const SHARED_CODE_SUFFIX: &str = "also_deployed_elsewhere";
const PROGRESS_STEPS: u64 = 10_000;

#[derive(clap::Parser)]
pub struct SweepContractsCmd {
    /// Directory for the results and the saved progress. Rerun with the same
    /// directory to resume.
    #[clap(long)]
    output_dir: PathBuf,
    /// Seconds between two saves of the scan progress.
    #[clap(long, default_value = "5")]
    checkpoint_interval_secs: u64,
    /// Stop the scan of each shard after this many accounts. For test runs:
    /// the shard counts as done, so use a separate output directory.
    #[clap(long)]
    max_accounts_per_shard: Option<u64>,
}

#[derive(Serialize, Deserialize, Default)]
struct SweepState {
    /// Progress of phase 1, keyed by the `ShardUId` string.
    shards: BTreeMap<String, ShardProgress>,
}

#[derive(Serialize, Deserialize, Clone, Default)]
struct ShardProgress {
    /// Flat head height when the scan of this shard started.
    flat_head_height: u64,
    /// Hex of the last trie key written to the accounts file.
    cursor: Option<String>,
    /// Length of the accounts file at `cursor`.
    accounts_file_len: u64,
    accounts_scanned: u64,
    contract_accounts: u64,
    accounts_done: bool,
    globals_done: bool,
}

impl SweepContractsCmd {
    pub fn run(self, home_dir: &Path, near_config: NearConfig, store: Store) {
        let dir = self.output_dir;
        fs::create_dir_all(dir.join(CONTRACTS_DIR)).unwrap();
        fs::create_dir_all(dir.join(GLOBAL_BY_HASH_DIR)).unwrap();
        fs::create_dir_all(dir.join(GLOBAL_BY_ACCOUNT_DIR)).unwrap();
        let interval = Duration::from_secs(self.checkpoint_interval_secs);

        let state = load_state(&dir).unwrap();
        let state = Mutex::new(state);
        let shard_layout = head_shard_layout(home_dir, &near_config, &store);
        let max_accounts = self.max_accounts_per_shard;
        scan_shards(&store, &dir, &state, shard_layout.as_ref(), interval, max_accounts).unwrap();
        write_local_contracts(&store, &dir).unwrap();
        write_global_index(&dir).unwrap();
        eprintln!("sweep complete, results in {}", dir.display());
    }
}

fn load_state(dir: &Path) -> anyhow::Result<SweepState> {
    let path = dir.join(STATE_FILE);
    if !path.exists() {
        return Ok(SweepState::default());
    }
    let bytes = fs::read(&path)?;
    serde_json::from_slice(&bytes).with_context(|| format!("parse {}", path.display()))
}

/// Writes the state to a temporary file and renames it, so a crash leaves
/// either the old or the new state on disk.
fn save_state(dir: &Path, state: &SweepState) -> anyhow::Result<()> {
    write_atomic(&dir.join(STATE_FILE), &serde_json::to_vec_pretty(state)?)
}

/// Two threads can write the same path, so each write uses its own temporary file.
fn write_atomic(path: &Path, bytes: &[u8]) -> anyhow::Result<()> {
    static NEXT_TMP_ID: AtomicU64 = AtomicU64::new(0);
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(format!(".tmp{}", NEXT_TMP_ID.fetch_add(1, Ordering::Relaxed)));
    let mut file = File::create(&tmp)?;
    file.write_all(bytes)?;
    file.sync_data()?;
    fs::rename(&tmp, path)?;
    Ok(())
}

fn head_shard_layout(
    home_dir: &Path,
    near_config: &NearConfig,
    store: &Store,
) -> Option<ShardLayout> {
    let epoch_manager =
        EpochManager::new_arc_handle(store.clone(), &near_config.genesis.config, Some(home_dir));
    let head = store.chain_store().head().ok()?;
    epoch_manager.get_shard_layout(&head.epoch_id).ok()
}

/// Returns the shards with ready flat storage and their flat head heights.
fn ready_shards(store: &Store) -> anyhow::Result<Vec<(ShardUId, u64)>> {
    let mut shards = vec![];
    for (key, value) in store.iter(DBCol::FlatStorageStatus) {
        let shard_uid = ShardUId::try_from(key.as_ref()).map_err(|err| anyhow::anyhow!("{err}"))?;
        match FlatStorageStatus::try_from_slice(&value)? {
            FlatStorageStatus::Ready(ready) => shards.push((shard_uid, ready.flat_head.height)),
            status => eprintln!("skip shard {shard_uid}: flat storage status {status:?}"),
        }
    }
    Ok(shards)
}

fn scan_shards(
    store: &Store,
    dir: &Path,
    state: &Mutex<SweepState>,
    shard_layout: Option<&ShardLayout>,
    interval: Duration,
    max_accounts: Option<u64>,
) -> anyhow::Result<()> {
    let shards = ready_shards(store)?;
    anyhow::ensure!(!shards.is_empty(), "no shard has ready flat storage");
    {
        let mut state = state.lock();
        for (shard_uid, height) in &shards {
            state.shards.entry(shard_uid.to_string()).or_insert_with(|| ShardProgress {
                flat_head_height: *height,
                ..Default::default()
            });
        }
        save_state(dir, &state)?;
    }

    let multi = MultiProgress::with_draw_target(ProgressDrawTarget::stderr_with_hz(2));
    let style =
        ProgressStyle::with_template("{prefix:>8} [{bar:30}] {percent:>3}% eta {eta:>6} {msg}")?
            .progress_chars("=> ");
    let multi = &multi;
    std::thread::scope(|scope| {
        let handles: Vec<_> = shards
            .iter()
            .map(|(shard_uid, _)| {
                let bar = multi.add(ProgressBar::new(PROGRESS_STEPS));
                bar.set_style(style.clone());
                bar.set_prefix(shard_uid.to_string());
                let range = shard_key_range(shard_layout, *shard_uid);
                scope.spawn(move || {
                    scan_shard(
                        store,
                        dir,
                        state,
                        *shard_uid,
                        range,
                        bar,
                        multi,
                        interval,
                        max_accounts,
                    )
                })
            })
            .collect();
        for handle in handles {
            handle.join().expect("scan thread panicked")?;
        }
        anyhow::Ok(())
    })
}

/// Returns the account key range of the shard as positions on a `u64` line.
/// The progress bar uses it to estimate how much of the shard is scanned.
fn shard_key_range(shard_layout: Option<&ShardLayout>, shard_uid: ShardUId) -> (u64, u64) {
    let full = (key_position(b"0"), key_position(b"{"));
    let Some(layout) = shard_layout else { return full };
    let Ok(index) = layout.get_shard_index(shard_uid.shard_id()) else { return full };
    let boundaries = layout.boundary_accounts();
    let lower = match index {
        0 => full.0,
        _ => boundaries.get(index - 1).map_or(full.0, |b| key_position(b.as_bytes())),
    };
    let upper = boundaries.get(index).map_or(full.1, |b| key_position(b.as_bytes()));
    (lower, upper)
}

/// Maps the first 8 bytes of an account ID to a number with the same order.
fn key_position(account: &[u8]) -> u64 {
    let mut bytes = [0u8; 8];
    let len = account.len().min(8);
    bytes[..len].copy_from_slice(&account[..len]);
    u64::from_be_bytes(bytes)
}

fn progress_step(range: (u64, u64), account: &[u8]) -> u64 {
    let (lower, upper) = range;
    let position = key_position(account).clamp(lower, upper);
    let span = (upper - lower).max(1) as f64;
    ((position - lower) as f64 / span * PROGRESS_STEPS as f64) as u64
}

fn read_value(
    store: &Store,
    shard_uid: ShardUId,
    value: FlatStateValue,
) -> anyhow::Result<Vec<u8>> {
    match value {
        FlatStateValue::Inlined(bytes) => Ok(bytes),
        FlatStateValue::Ref(value_ref) => {
            Ok(store.trie_store().get(shard_uid, &value_ref.hash)?.to_vec())
        }
    }
}

/// Formats one line of the accounts file: account, contract kind, contract ID.
fn mapping_line(account_id: &AccountId, contract: &AccountContract) -> Option<String> {
    match contract {
        AccountContract::None => None,
        AccountContract::Local(hash) => Some(format!("{account_id}\tlocal\t{hash}\n")),
        AccountContract::Global(hash) => Some(format!("{account_id}\tglobal_hash\t{hash}\n")),
        AccountContract::GlobalByAccount(owner) => {
            Some(format!("{account_id}\tglobal_account\t{owner}\n"))
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn scan_shard(
    store: &Store,
    dir: &Path,
    state: &Mutex<SweepState>,
    shard_uid: ShardUId,
    range: (u64, u64),
    bar: ProgressBar,
    multi: &MultiProgress,
    interval: Duration,
    max_accounts: Option<u64>,
) -> anyhow::Result<()> {
    let shard_key = shard_uid.to_string();
    let mut progress = state.lock().shards[&shard_key].clone();

    if !progress.accounts_done {
        let path = dir.join(format!("accounts.{shard_key}.tsv"));
        let mut file = OpenOptions::new().create(true).truncate(false).write(true).open(&path)?;
        // Drop the lines written after the last saved checkpoint.
        file.set_len(progress.accounts_file_len)?;
        file.seek(SeekFrom::End(0))?;
        let mut writer = BufWriter::new(file);

        let from = match &progress.cursor {
            Some(cursor) => {
                // The smallest key after the cursor.
                let mut key = hex::decode(cursor)?;
                key.push(0);
                key
            }
            None => vec![col::ACCOUNT],
        };
        let to = [col::ACCOUNT + 1];
        if let Some(cursor) = &progress.cursor {
            bar.set_position(progress_step(range, &hex::decode(cursor)?[1..]));
            bar.reset_eta();
        }

        let started = Instant::now();
        let scanned_at_start = progress.accounts_scanned;
        let mut last_checkpoint = Instant::now();
        let flat_store = store.flat_store();
        for (key, value) in flat_store.iter_range(shard_uid, Some(&from), Some(&to)) {
            if max_accounts.is_some_and(|max| progress.accounts_scanned >= max) {
                break;
            }
            progress.accounts_scanned += 1;
            match parse_account(store, shard_uid, &key, value) {
                Ok((account_id, account)) => {
                    if let Some(line) = mapping_line(&account_id, account.contract().as_ref()) {
                        writer.write_all(line.as_bytes())?;
                        progress.accounts_file_len += line.len() as u64;
                        progress.contract_accounts += 1;
                    }
                }
                Err(err) => multi
                    .println(format!("{shard_key}: skip key {}: {err:#}", hex::encode(&key)))?,
            }
            progress.cursor = Some(hex::encode(&key));

            if progress.accounts_scanned.is_multiple_of(4096) {
                let rate = (progress.accounts_scanned - scanned_at_start) as f64
                    / started.elapsed().as_secs_f64().max(1.0);
                bar.set_position(progress_step(range, &key[1..]));
                bar.set_message(format!(
                    "{} accounts, {} with contract, {rate:.0}/s, at {}",
                    progress.accounts_scanned,
                    progress.contract_accounts,
                    String::from_utf8_lossy(&key[1..key.len().min(40)]),
                ));
            }
            if last_checkpoint.elapsed() >= interval {
                checkpoint(dir, state, &shard_key, &progress, &mut writer)?;
                last_checkpoint = Instant::now();
            }
        }
        progress.accounts_done = true;
        checkpoint(dir, state, &shard_key, &progress, &mut writer)?;
    }

    if !progress.globals_done {
        bar.set_message("scanning global contracts");
        let count = scan_global_contracts(store, dir, shard_uid)?;
        progress.globals_done = true;
        let mut state = state.lock();
        state.shards.insert(shard_key, progress.clone());
        save_state(dir, &state)?;
        drop(state);
        bar.set_message(format!(
            "done: {} accounts, {} with contract, {count} global contracts",
            progress.accounts_scanned, progress.contract_accounts
        ));
    }
    bar.finish();
    Ok(())
}

fn parse_account(
    store: &Store,
    shard_uid: ShardUId,
    key: &[u8],
    value: FlatStateValue,
) -> anyhow::Result<(AccountId, Account)> {
    let account_id = parse_account_id_from_account_key(key)?;
    let bytes = read_value(store, shard_uid, value)?;
    let account = Account::try_from_slice(&bytes)?;
    Ok((account_id, account))
}

/// Flushes the accounts file to disk, then saves the cursor. The cursor never
/// points past data that is not on disk.
fn checkpoint(
    dir: &Path,
    state: &Mutex<SweepState>,
    shard_key: &str,
    progress: &ShardProgress,
    writer: &mut BufWriter<File>,
) -> anyhow::Result<()> {
    writer.flush()?;
    writer.get_ref().sync_data()?;
    let mut state = state.lock();
    state.shards.insert(shard_key.to_string(), progress.clone());
    save_state(dir, &state)
}

/// Writes every global contract stored in the shard. All shards store the
/// same global contracts, so a file that exists already is skipped.
fn scan_global_contracts(store: &Store, dir: &Path, shard_uid: ShardUId) -> anyhow::Result<u64> {
    let from = [col::GLOBAL_CONTRACT_CODE];
    let to = [col::GLOBAL_CONTRACT_CODE + 1];
    let mut count = 0;
    for (key, value) in store.flat_store().iter_range(shard_uid, Some(&from), Some(&to)) {
        let identifier = GlobalContractCodeIdentifier::try_from_slice(&key[1..])
            .with_context(|| format!("parse global contract key {}", hex::encode(&key)))?;
        let path = match &identifier {
            GlobalContractCodeIdentifier::CodeHash(hash) => {
                dir.join(GLOBAL_BY_HASH_DIR).join(format!("{hash}.wasm"))
            }
            GlobalContractCodeIdentifier::AccountId(owner) => {
                dir.join(GLOBAL_BY_ACCOUNT_DIR).join(format!("{owner}.wasm"))
            }
        };
        count += 1;
        if path.exists() {
            continue;
        }
        let code = read_value(store, shard_uid, value)?;
        if let GlobalContractCodeIdentifier::CodeHash(hash) = &identifier {
            anyhow::ensure!(
                CryptoHash::hash_bytes(&code) == *hash,
                "global code hash mismatch {hash}"
            );
        }
        write_atomic(&path, &code)?;
    }
    Ok(count)
}

/// Accounts that use one local contract code hash.
struct LocalCode {
    /// The smallest account ID that uses the code.
    first_account: AccountId,
    shard_uid: ShardUId,
    num_accounts: u64,
}

impl LocalCode {
    fn file_name(&self) -> String {
        match self.num_accounts {
            1 => format!("{}.wasm", self.first_account),
            _ => format!("{}.{SHARED_CODE_SUFFIX}.wasm", self.first_account),
        }
    }
}

/// Yields the parsed lines of all accounts files with their shard.
fn read_mapping(dir: &Path) -> anyhow::Result<Vec<(ShardUId, AccountId, String, String)>> {
    let mut rows = vec![];
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        let name = path.file_name().unwrap().to_string_lossy().to_string();
        let Some(shard) = name.strip_prefix("accounts.").and_then(|n| n.strip_suffix(".tsv"))
        else {
            continue;
        };
        let shard_uid = ShardUId::from_str(shard).map_err(|err| anyhow::anyhow!("{err}"))?;
        for line in BufReader::new(File::open(&path)?).lines() {
            let line = line?;
            let mut parts = line.split('\t');
            let (Some(account), Some(kind), Some(id)) = (parts.next(), parts.next(), parts.next())
            else {
                anyhow::bail!("bad line in {}: {line}", path.display());
            };
            rows.push((shard_uid, account.parse()?, kind.to_string(), id.to_string()));
        }
    }
    Ok(rows)
}

fn write_local_contracts(store: &Store, dir: &Path) -> anyhow::Result<()> {
    let mut codes: BTreeMap<CryptoHash, LocalCode> = BTreeMap::new();
    for (shard_uid, account_id, kind, id) in read_mapping(dir)? {
        if kind != "local" {
            continue;
        }
        let hash = CryptoHash::from_str(&id).map_err(|err| anyhow::anyhow!("{err}"))?;
        let code = codes.entry(hash).or_insert_with(|| LocalCode {
            first_account: account_id.clone(),
            shard_uid,
            num_accounts: 0,
        });
        code.num_accounts += 1;
        if account_id < code.first_account {
            code.first_account = account_id;
            code.shard_uid = shard_uid;
        }
    }

    let contracts_dir = dir.join(CONTRACTS_DIR);
    let total = codes.len() as u64;
    let bar = ProgressBar::with_draw_target(Some(total), ProgressDrawTarget::stderr_with_hz(2));
    bar.set_style(ProgressStyle::with_template(
        "contracts [{bar:30}] {pos}/{len} eta {eta} {msg}",
    )?);
    let bytes_written = AtomicU64::new(0);
    let failures = Mutex::new(vec![]);
    let entries: Vec<_> = codes.iter().collect();
    entries.par_iter().for_each(|(hash, code)| {
        let path = contracts_dir.join(code.file_name());
        if !path.exists() {
            match fetch_local_code(store, code.shard_uid, hash) {
                Ok(bytes) => {
                    bytes_written.fetch_add(bytes.len() as u64, Ordering::Relaxed);
                    if let Err(err) = write_atomic(&path, &bytes) {
                        failures.lock().push(format!("{hash}\t{}\t{err:#}", code.first_account));
                    }
                }
                Err(err) => {
                    failures.lock().push(format!("{hash}\t{}\t{err:#}", code.first_account))
                }
            }
        }
        bar.inc(1);
        bar.set_message(format!(
            "{} written this run",
            bytesize::ByteSize::b(bytes_written.load(Ordering::Relaxed))
        ));
    });
    bar.finish();

    let failures = failures.into_inner();
    let mut index = String::from("code_hash\tsize\tnum_accounts\tfile\n");
    for (hash, code) in &codes {
        let name = code.file_name();
        let Ok(meta) = fs::metadata(contracts_dir.join(&name)) else { continue };
        index.push_str(&format!("{hash}\t{}\t{}\t{name}\n", meta.len(), code.num_accounts));
    }
    write_atomic(&contracts_dir.join("index.tsv"), index.as_bytes())?;
    write_atomic(&dir.join("errors.tsv"), failures.join("\n").as_bytes())?;
    anyhow::ensure!(
        failures.is_empty(),
        "{} contracts failed, see {}; rerun to retry",
        failures.len(),
        dir.join("errors.tsv").display()
    );
    Ok(())
}

/// Reads the code from the `State` column. The trie stores every value under
/// the hash of the value, so the code hash is the key of the code.
fn fetch_local_code(
    store: &Store,
    shard_uid: ShardUId,
    hash: &CryptoHash,
) -> anyhow::Result<Vec<u8>> {
    let code = store.trie_store().get(shard_uid, hash)?.to_vec();
    anyhow::ensure!(CryptoHash::hash_bytes(&code) == *hash, "code hash mismatch");
    Ok(code)
}

/// Writes `global_contracts/index.tsv` with the number of accounts that use
/// each global contract.
fn write_global_index(dir: &Path) -> anyhow::Result<()> {
    let mut users: BTreeMap<(String, String), u64> = BTreeMap::new();
    for (_, _, kind, id) in read_mapping(dir)? {
        if kind == "global_hash" || kind == "global_account" {
            *users.entry((kind, id)).or_default() += 1;
        }
    }
    let mut index = String::from("kind\tidentifier\tsize\tnum_accounts\tfile\n");
    for (kind, sub_dir) in
        [("global_hash", GLOBAL_BY_HASH_DIR), ("global_account", GLOBAL_BY_ACCOUNT_DIR)]
    {
        let mut names: Vec<_> = fs::read_dir(dir.join(sub_dir))?
            .map(|entry| entry.map(|e| e.file_name().to_string_lossy().to_string()))
            .collect::<Result<_, _>>()?;
        names.sort();
        for name in names {
            let Some(id) = name.strip_suffix(".wasm") else { continue };
            let size = fs::metadata(dir.join(sub_dir).join(&name))?.len();
            let num = users.get(&(kind.to_string(), id.to_string())).copied().unwrap_or(0);
            index.push_str(&format!("{kind}\t{id}\t{size}\t{num}\t{sub_dir}/{name}\n"));
        }
    }
    write_atomic(&dir.join("global_contracts/index.tsv"), index.as_bytes())
}
