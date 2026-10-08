use crate::evaluate::{Analysis, InheritedLoss, evaluate};
use crate::extract::extract_range;
use crate::frame::{FrameReader, FrameWriter};
use crate::row::ProducerRow;
use anyhow::Context;
use near_chain::ChainStore;
use near_chain_configs::GenesisValidationMode;
use near_parameters::RuntimeConfigStore;
use near_primitives::types::BlockHeight;
use near_store::{Mode, NodeStorage};
use std::io::{BufReader, BufWriter};
use std::path::{Path, PathBuf};

#[derive(clap::Parser)]
pub struct ReceiptGasHeadroomCommand {
    #[clap(subcommand)]
    subcmd: SubCommand,
}

#[derive(clap::Parser)]
enum SubCommand {
    /// Read an archival database and write one row per producer, by window.
    Extract(ExtractCmd),
    /// Apply one candidate fee change to extracted rows and report what fails.
    Evaluate(EvaluateCmd),
}

#[derive(clap::Parser)]
pub struct EvaluateCmd {
    /// Directory `extract` wrote its windows into. Every `.rows` file in it is
    /// read, so a range split across workers evaluates as one population.
    #[clap(long)]
    out_dir: PathBuf,
    #[clap(long, value_enum)]
    analysis: Analysis,
    /// Run with both settings and compare: matching failures pin the answer
    /// exactly, so the proportional split never has to be written.
    #[clap(long, value_enum, default_value = "none")]
    inherited_loss: InheritedLoss,
}

/// Rows a frame holds before it is compressed and written. Wide enough for
/// zstd to notice the repeated account ids, small enough that a run cut short
/// loses little.
const ROWS_PER_FRAME: usize = 4096;

/// Blocks one window covers. Each window indexes the receipts it sent and
/// resolves its own producers, so it can be rerun, skipped or handed to another
/// worker on its own. A receipt whose producer ran before the window starts
/// stays unclaimed in it, which the offset histogram counts, so the window
/// wants to be wide against the couple of hundred blocks a yield or a congested
/// buffer can delay a send by.
const DEFAULT_WINDOW_BLOCKS: u64 = 20_000;

#[derive(clap::Parser)]
pub struct ExtractCmd {
    #[clap(long)]
    start_height: BlockHeight,
    #[clap(long)]
    end_height: BlockHeight,
    /// Directory to write windows into. Each one lands as its own pair of
    /// files, so a run that stops leaves finished windows behind.
    #[clap(long)]
    out_dir: PathBuf,
    #[clap(long, default_value_t = DEFAULT_WINDOW_BLOCKS)]
    window_blocks: u64,
    /// Skip windows already written, rather than doing them again.
    #[clap(long)]
    resume: bool,
}

impl ReceiptGasHeadroomCommand {
    pub fn run(
        self,
        home_dir: &Path,
        genesis_validation: GenesisValidationMode,
    ) -> anyhow::Result<()> {
        match self.subcmd {
            SubCommand::Extract(cmd) => cmd.run(home_dir, genesis_validation),
            SubCommand::Evaluate(cmd) => cmd.run(),
        }
    }
}

impl EvaluateCmd {
    fn run(self) -> anyhow::Result<()> {
        let mut window_files: Vec<PathBuf> = std::fs::read_dir(&self.out_dir)
            .with_context(|| format!("failed to read {}", self.out_dir.display()))?
            .filter_map(|entry| entry.ok().map(|entry| entry.path()))
            .filter(|path| path.extension().is_some_and(|ext| ext == "rows"))
            // A window killed midway still leaves readable frames, so without
            // this a partial population would evaluate as if it were whole.
            .filter(|path| path.with_extension("done").exists())
            .collect();
        // Height order, so a failure reads in the order the chain ran.
        window_files.sort();
        anyhow::ensure!(!window_files.is_empty(), "no window files in {}", self.out_dir.display());
        tracing::info!(
            target: "receipt-gas-headroom",
            windows = window_files.len(),
            "evaluating"
        );
        let rows = window_files
            .into_iter()
            .map(|path| -> anyhow::Result<_> {
                let file = std::fs::File::open(&path)
                    .with_context(|| format!("failed to open {}", path.display()))?;
                Ok(FrameReader::<_, ProducerRow>::new(BufReader::new(file)))
            })
            .collect::<anyhow::Result<Vec<_>>>()?
            .into_iter()
            .flatten()
            .map(Ok);
        // `for_chain_id` only differs from this for benchmarknet, which has no
        // archival data to evaluate.
        let configs = RuntimeConfigStore::new();
        let report = evaluate(rows, self.analysis, self.inherited_loss, &configs)?;
        println!("{}", serde_json::to_string_pretty(&report)?);
        Ok(())
    }
}

/// Written once a window's files are complete, so a resumed run can tell a
/// finished window from one that was cut off midway.
fn window_marker(out_dir: &Path, start: BlockHeight, end: BlockHeight) -> PathBuf {
    out_dir.join(format!("{start}-{end}.done"))
}

impl ExtractCmd {
    fn run(self, home_dir: &Path, genesis_validation: GenesisValidationMode) -> anyhow::Result<()> {
        let near_config = nearcore::config::load_config(home_dir, genesis_validation)
            .context("failed to load config")?;
        let opener = NodeStorage::opener(
            home_dir,
            &near_config.config.store,
            near_config.config.cold_store.as_ref(),
            near_config.cloud_storage_context(),
        );
        let storage = opener.open_in_mode(Mode::ReadOnly).context("failed to open storage")?;
        let store = storage.get_split_store().unwrap_or_else(|| storage.get_hot_store());
        let chain_store = ChainStore::new(
            store,
            near_config.client_config.save_trie_changes,
            near_config.genesis.config.transaction_validity_period,
        );

        std::fs::create_dir_all(&self.out_dir)?;
        let mut windows_done = 0;
        let mut windows_skipped = 0;
        let mut height = self.start_height;
        while height <= self.end_height {
            let window_end = (height + self.window_blocks - 1).min(self.end_height);
            let marker = window_marker(&self.out_dir, height, window_end);
            if self.resume && marker.exists() {
                windows_skipped += 1;
                height = window_end + 1;
                continue;
            }

            // Rows are framed and compressed, so stdout is not an option.
            let rows_path = self.out_dir.join(format!("{height}-{window_end}.rows"));
            let chunks_path = self.out_dir.join(format!("{height}-{window_end}.chunks"));
            let mut out = FrameWriter::new(
                BufWriter::new(std::fs::File::create(&rows_path)?),
                ROWS_PER_FRAME,
            );
            let mut chunk_out = FrameWriter::new(
                BufWriter::new(std::fs::File::create(&chunks_path)?),
                ROWS_PER_FRAME,
            );
            let (rows, checks, census) =
                extract_range(&chain_store, height, window_end, &mut out, &mut chunk_out)?;
            out.finish()?;
            chunk_out.finish()?;

            // Summaries land beside the rows, so a window stands alone and a
            // run that stops keeps what it already paid for.
            std::fs::write(
                self.out_dir.join(format!("{height}-{window_end}.checks.json")),
                serde_json::to_string_pretty(&checks)?,
            )?;
            std::fs::write(
                self.out_dir.join(format!("{height}-{window_end}.census.json")),
                serde_json::to_string_pretty(&census)?,
            )?;
            // Written last, so a window is only skipped once its files are whole.
            std::fs::write(&marker, format!("{rows}\n"))?;
            tracing::info!(
                target: "receipt-gas-headroom",
                window = format!("{height}-{window_end}"),
                rows,
                "window finished"
            );
            windows_done += 1;
            height = window_end + 1;
        }
        tracing::info!(
            target: "receipt-gas-headroom",
            windows_done,
            windows_skipped,
            "extract finished"
        );
        Ok(())
    }
}
