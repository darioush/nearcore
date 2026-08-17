mod checkpoint;
mod db;
mod progress;
mod report;
mod rpc;
mod source;
mod stats;

use anyhow::{Context, bail};
use clap::{Parser, Subcommand};
use db::StoreSource;
use near_primitives::types::{AccountId, BlockHeight};
use progress::ProgressReporter;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use rpc::RpcSource;
use source::{AccountSource, BlockSource, HistoricalAccountSource};
use stats::{HistoricalContractStatus, ScanState};
use std::path::PathBuf;
use std::time::{Duration, Instant};

#[derive(Parser)]
#[command(about = "Scan blocks for DelegateV2 usage and for repeat transactions of one account")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Scan a block range, then read contract state at the chain head.
    Scan(ScanArgs),
    /// Compare the database contract lookup against a node's own answer.
    VerifyContracts(VerifyContractsArgs),
    /// Read contract state at the heights where each account was concurrent.
    HistoricalContracts(HistoricalContractsArgs),
    /// Print one account's contract state at given heights.
    ContractAt(ContractAtArgs),
}

#[derive(clap::Args)]
struct WorkerArgs {
    #[arg(long, default_value_t = 32)]
    concurrency: usize,

    #[arg(long, default_value_t = 15)]
    save_every_secs: u64,

    /// How often to print a progress line when standard error is not a
    /// terminal, as over ssh or into a log file.
    #[arg(long, default_value_t = 30)]
    progress_every_secs: u64,
}

impl WorkerArgs {
    fn pool(&self) -> anyhow::Result<rayon::ThreadPool> {
        rayon::ThreadPoolBuilder::new()
            .num_threads(self.concurrency.max(1))
            .build()
            .context("building worker pool")
    }

    fn batch_size(&self) -> usize {
        (self.concurrency * 4).max(1)
    }

    fn save_interval(&self) -> Duration {
        Duration::from_secs(self.save_every_secs)
    }

    fn progress_interval(&self) -> Duration {
        Duration::from_secs(self.progress_every_secs)
    }
}

#[derive(clap::Args)]
struct RpcArgs {
    #[arg(long, default_value_t = 30)]
    request_timeout_secs: u64,

    #[arg(long, default_value_t = 5)]
    max_attempts: u32,
}

impl RpcArgs {
    fn source(&self, url: &str) -> anyhow::Result<RpcSource> {
        RpcSource::new(
            url.to_string(),
            Duration::from_secs(self.request_timeout_secs),
            self.max_attempts,
        )
    }
}

#[derive(clap::Args)]
struct ScanArgs {
    /// Node home directory. Blocks are read from its RocksDB, opened read only.
    #[arg(long, group = "blocks_from")]
    home_dir: Option<PathBuf>,

    /// JSON RPC endpoint to read blocks from, as an alternative to `home_dir`.
    #[arg(long, group = "blocks_from")]
    rpc_url: Option<String>,

    #[arg(long)]
    start_height: BlockHeight,

    /// Last height to scan, inclusive. Defaults to `start_height + blocks - 1`,
    /// or to the chain head when `blocks` is also absent.
    #[arg(long)]
    end_height: Option<BlockHeight>,

    #[arg(long, conflicts_with = "end_height")]
    blocks: Option<u64>,

    /// Scan state, written while the scan runs and read back to resume.
    #[arg(long, default_value = "tx-scan-checkpoint.json")]
    checkpoint: PathBuf,

    #[arg(long, default_value = "tx-scan-report.json")]
    report: PathBuf,

    /// Skip the contract lookup for accounts that sent repeat transactions.
    #[arg(long)]
    skip_contract_lookup: bool,

    #[command(flatten)]
    worker: WorkerArgs,

    #[command(flatten)]
    rpc: RpcArgs,
}

#[derive(clap::Args)]
struct VerifyContractsArgs {
    #[arg(long)]
    home_dir: PathBuf,

    #[arg(long)]
    rpc_url: String,

    #[arg(long, default_value = "tx-scan-checkpoint.json")]
    checkpoint: PathBuf,

    /// Check at most this many accounts. Zero checks every one.
    #[arg(long, default_value_t = 0)]
    limit: usize,

    #[command(flatten)]
    worker: WorkerArgs,

    #[command(flatten)]
    rpc: RpcArgs,
}

#[derive(clap::Args)]
struct ContractAtArgs {
    #[arg(long)]
    home_dir: PathBuf,

    #[arg(long)]
    account_id: AccountId,

    /// Heights to read at. The chain head is always printed too.
    #[arg(long, num_args = 1.., required = true)]
    height: Vec<BlockHeight>,
}

#[derive(clap::Args)]
struct HistoricalContractsArgs {
    #[arg(long)]
    home_dir: PathBuf,

    #[arg(long, default_value = "tx-scan-checkpoint.json")]
    checkpoint: PathBuf,

    #[arg(long, default_value = "tx-scan-report.json")]
    report: PathBuf,

    #[command(flatten)]
    worker: WorkerArgs,
}

fn main() -> anyhow::Result<()> {
    match Cli::parse().command {
        Command::Scan(args) => run_scan(args),
        Command::VerifyContracts(args) => run_verify_contracts(args),
        Command::HistoricalContracts(args) => run_historical_contracts(args),
        Command::ContractAt(args) => run_contract_at(args),
    }
}

fn run_contract_at(args: ContractAtArgs) -> anyhow::Result<()> {
    let store_source = StoreSource::open(&args.home_dir)?;
    println!("head    {:?}", store_source.contract_status(&args.account_id)?);
    for height in &args.height {
        let status = store_source.contract_status_at_height(&args.account_id, *height)?;
        println!("{height:<7} {status:?}");
    }
    Ok(())
}

fn run_scan(args: ScanArgs) -> anyhow::Result<()> {
    let store_source = match &args.home_dir {
        Some(home_dir) => Some(StoreSource::open(home_dir)?),
        None => None,
    };
    let rpc_source = match &args.rpc_url {
        Some(url) => Some(args.rpc.source(url)?),
        None => None,
    };
    let (block_source, chain_id): (&dyn BlockSource, String) = match (&store_source, &rpc_source) {
        (Some(source), _) => (source, source.chain_id().to_string()),
        (None, Some(source)) => (source, source.chain_id().context("reading node status")?),
        (None, None) => bail!("pass either --home-dir or --rpc-url"),
    };

    let end_height = match (args.end_height, args.blocks) {
        (Some(end_height), _) => end_height,
        (None, Some(blocks)) if blocks > 0 => args.start_height + blocks - 1,
        (None, Some(_)) => bail!("--blocks must be positive"),
        (None, None) => block_source.head_height().context("reading head height")?,
    };
    if end_height < args.start_height {
        bail!("end height {end_height} is below start height {}", args.start_height);
    }

    let mut state = checkpoint::load(&args.checkpoint, &chain_id, args.start_height, end_height)?
        .unwrap_or_else(|| ScanState::new(chain_id, args.start_height, end_height));
    let pool = args.worker.pool()?;

    scan_blocks(&args, &pool, block_source, &mut state, end_height)?;

    if !args.skip_contract_lookup {
        // The database read needs no network, so it is preferred when both are
        // available.
        let account_source: Option<&dyn AccountSource> = match (&store_source, &rpc_source) {
            (Some(source), _) => Some(source),
            (None, Some(source)) => Some(source),
            (None, None) => None,
        };
        if let Some(account_source) = account_source {
            look_up_contracts(&args.worker, &pool, account_source, &mut state, &args.checkpoint)?;
        }
    }
    checkpoint::save(&args.checkpoint, &state)?;
    write_report(&state, &args.report)
}

fn write_report(state: &ScanState, path: &PathBuf) -> anyhow::Result<()> {
    let report = report::build(state);
    std::fs::write(path, serde_json::to_vec_pretty(&report)?)
        .with_context(|| format!("writing {}", path.display()))?;
    println!("{}", report::render_text(state, &report));
    println!("report written to {}", path.display());
    Ok(())
}

fn scan_blocks(
    args: &ScanArgs,
    pool: &rayon::ThreadPool,
    source: &dyn BlockSource,
    state: &mut ScanState,
    end_height: BlockHeight,
) -> anyhow::Result<()> {
    let total = end_height - args.start_height + 1;
    let mut bar = ProgressReporter::new(total, "blocks", args.worker.progress_interval())?;
    bar.start_at(state.next_height.saturating_sub(args.start_height));

    let batch_size = args.worker.batch_size() as u64;
    let save_interval = args.worker.save_interval();
    let mut last_save = Instant::now();

    while state.next_height <= end_height {
        let batch_end = (state.next_height + batch_size - 1).min(end_height);
        let heights: Vec<BlockHeight> = (state.next_height..=batch_end).collect();
        let blocks = pool.install(|| {
            heights.par_iter().map(|height| source.block_at_height(*height)).collect::<Vec<_>>()
        });
        for (height, block) in heights.iter().zip(blocks) {
            match block.with_context(|| format!("reading block {height}"))? {
                Some(block) => state.record_block(&block),
                None => state.record_missing_height(*height),
            }
        }
        bar.set_position(state.next_height - args.start_height);
        if last_save.elapsed() >= save_interval {
            checkpoint::save(&args.checkpoint, state)?;
            last_save = Instant::now();
        }
    }
    bar.finish();
    Ok(())
}

fn look_up_contracts(
    worker: &WorkerArgs,
    pool: &rayon::ThreadPool,
    source: &dyn AccountSource,
    state: &mut ScanState,
    checkpoint_path: &PathBuf,
) -> anyhow::Result<()> {
    let pending: Vec<AccountId> = state
        .concurrent_accounts
        .keys()
        .filter(|account_id| !state.contract_status.contains_key(*account_id))
        .cloned()
        .collect();
    if pending.is_empty() {
        return Ok(());
    }
    let mut bar =
        ProgressReporter::new(pending.len() as u64, "accounts", worker.progress_interval())?;
    let save_interval = worker.save_interval();
    let mut last_save = Instant::now();

    for batch in pending.chunks(worker.batch_size()) {
        let statuses = pool.install(|| {
            batch
                .par_iter()
                .map(|account_id| source.contract_status(account_id))
                .collect::<Vec<_>>()
        });
        for (account_id, status) in batch.iter().zip(statuses) {
            let status = status.with_context(|| format!("looking up {account_id}"))?;
            state.contract_status.insert(account_id.clone(), status);
        }
        bar.advance(batch.len() as u64);
        if last_save.elapsed() >= save_interval {
            checkpoint::save(checkpoint_path, state)?;
            last_save = Instant::now();
        }
    }
    bar.finish();
    Ok(())
}

fn run_verify_contracts(args: VerifyContractsArgs) -> anyhow::Result<()> {
    let store_source = StoreSource::open(&args.home_dir)?;
    let rpc_source = args.rpc.source(&args.rpc_url)?;
    let state = checkpoint::read_any(&args.checkpoint)?;
    let mut accounts: Vec<AccountId> = state.concurrent_accounts.keys().cloned().collect();
    if args.limit > 0 {
        accounts.truncate(args.limit);
    }
    if accounts.is_empty() {
        bail!("checkpoint holds no accounts to check");
    }

    let pool = args.worker.pool()?;
    let mut bar =
        ProgressReporter::new(accounts.len() as u64, "accounts", args.worker.progress_interval())?;
    let mut agreed = 0;
    let mut disagreements = Vec::new();

    for batch in accounts.chunks(args.worker.batch_size()) {
        let pairs = pool.install(|| {
            batch
                .par_iter()
                .map(|account_id| {
                    let from_store = store_source.contract_status(account_id);
                    let from_rpc = rpc_source.contract_status(account_id);
                    (from_store, from_rpc)
                })
                .collect::<Vec<_>>()
        });
        for (account_id, (from_store, from_rpc)) in batch.iter().zip(pairs) {
            let from_store =
                from_store.with_context(|| format!("database read of {account_id}"))?;
            let from_rpc = from_rpc.with_context(|| format!("rpc read of {account_id}"))?;
            if from_store == from_rpc {
                agreed += 1;
            } else {
                disagreements.push((account_id.clone(), from_store, from_rpc));
            }
        }
        bar.advance(batch.len() as u64);
    }
    bar.finish();

    println!("{agreed} of {} accounts agree between database and rpc", accounts.len());
    if disagreements.is_empty() {
        return Ok(());
    }
    println!("\n{} disagree:", disagreements.len());
    for (account_id, from_store, from_rpc) in disagreements.iter().take(50) {
        println!("  {account_id}: database {from_store:?}, rpc {from_rpc:?}");
    }
    bail!("database and rpc contract lookups disagree")
}

fn run_historical_contracts(args: HistoricalContractsArgs) -> anyhow::Result<()> {
    let store_source = StoreSource::open(&args.home_dir)?;
    let mut state = checkpoint::read_any(&args.checkpoint)?;
    let pending: Vec<AccountId> = state
        .concurrent_accounts
        .keys()
        .filter(|account_id| !state.historical_contract_status.contains_key(*account_id))
        .cloned()
        .collect();
    if pending.is_empty() {
        println!("every account already has a historical result");
        return write_report(&state, &args.report);
    }

    let pool = args.worker.pool()?;
    let mut bar =
        ProgressReporter::new(pending.len() as u64, "accounts", args.worker.progress_interval())?;
    let save_interval = args.worker.save_interval();
    let mut last_save = Instant::now();

    for batch in pending.chunks(args.worker.batch_size()) {
        let results = pool.install(|| {
            batch
                .par_iter()
                .map(|account_id| {
                    let concurrency = &state.concurrent_accounts[account_id];
                    store_source.check_recorded_shard(
                        account_id,
                        concurrency.first_height,
                        concurrency.shard_id,
                    )?;
                    let at_first_height = store_source
                        .contract_status_at_height(account_id, concurrency.first_height)?;
                    let at_last_height = store_source
                        .contract_status_at_height(account_id, concurrency.last_height)?;
                    anyhow::Ok(HistoricalContractStatus { at_first_height, at_last_height })
                })
                .collect::<Vec<_>>()
        });
        let mut resolved = Vec::with_capacity(batch.len());
        for (account_id, result) in batch.iter().zip(results) {
            let status = result.with_context(|| format!("historical read of {account_id}"))?;
            resolved.push((account_id.clone(), status));
        }
        for (account_id, status) in resolved {
            state.historical_contract_status.insert(account_id, status);
        }
        bar.advance(batch.len() as u64);
        if last_save.elapsed() >= save_interval {
            checkpoint::save(&args.checkpoint, &state)?;
            last_save = Instant::now();
        }
    }
    bar.finish();
    checkpoint::save(&args.checkpoint, &state)?;
    write_report(&state, &args.report)
}
