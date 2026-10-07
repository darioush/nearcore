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
    /// Read an archival database and write one row per producer as JSON lines.
    Extract(ExtractCmd),
    /// Apply one candidate fee change to extracted rows and report what fails.
    Evaluate(EvaluateCmd),
}

#[derive(clap::Parser)]
pub struct EvaluateCmd {
    /// Rows written by `extract`.
    #[clap(long)]
    rows: PathBuf,
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

#[derive(clap::Parser)]
pub struct ExtractCmd {
    #[clap(long)]
    start_height: BlockHeight,
    #[clap(long)]
    end_height: BlockHeight,
    /// Where to write the producer rows, as zstd compressed borsh frames.
    #[clap(long)]
    out: PathBuf,
    /// Where to write the per chunk totals, in the same format.
    #[clap(long)]
    chunk_out: PathBuf,
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
        let file = std::fs::File::open(&self.rows)
            .with_context(|| format!("failed to open {}", self.rows.display()))?;
        let rows = FrameReader::<_, ProducerRow>::new(BufReader::new(file)).map(Ok);
        // `for_chain_id` only differs from this for benchmarknet, which has no
        // archival data to evaluate.
        let configs = RuntimeConfigStore::new();
        let report = evaluate(rows, self.analysis, self.inherited_loss, &configs)?;
        println!("{}", serde_json::to_string_pretty(&report)?);
        Ok(())
    }
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

        // Rows are framed and compressed, so stdout is not an option.
        let mut out =
            FrameWriter::new(BufWriter::new(std::fs::File::create(&self.out)?), ROWS_PER_FRAME);
        let mut chunk_out = FrameWriter::new(
            BufWriter::new(std::fs::File::create(&self.chunk_out)?),
            ROWS_PER_FRAME,
        );
        let (rows, checks, census) = extract_range(
            &chain_store,
            self.start_height,
            self.end_height,
            &mut out,
            &mut chunk_out,
        )?;
        tracing::info!(target: "receipt-gas-headroom", rows, "extract finished");
        eprintln!("{}", serde_json::to_string_pretty(&checks)?);
        eprintln!("{}", serde_json::to_string_pretty(&census)?);
        out.finish()?;
        chunk_out.finish()?;
        Ok(())
    }
}
