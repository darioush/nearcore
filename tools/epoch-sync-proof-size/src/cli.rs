use anyhow::Context;
use near_network::types::PeerInfo;
use near_ping::cli::CHAIN_INFO;
use near_primitives::epoch_sync::CompressedEpochSyncProof;
use near_primitives::hash::CryptoHash;
use std::path::PathBuf;
use std::str::FromStr;

#[derive(clap::Parser)]
pub struct EpochSyncProofSizeCommand {
    #[clap(subcommand)]
    subcmd: EpochSyncProofSizeSubCommand,
}

#[derive(clap::Subcommand)]
enum EpochSyncProofSizeSubCommand {
    /// Requests an epoch sync proof from a peer and reports how big the response is.
    Fetch(FetchCommand),
    /// Reports the size of a proof saved by `fetch --save-proof`.
    Analyze(AnalyzeCommand),
}

#[derive(clap::Parser)]
struct FetchCommand {
    /// Chain to connect to. The genesis hash is known for "mainnet" and "testnet".
    #[clap(long)]
    chain_id: String,
    /// Genesis hash to send in the handshake. Required for chains other than mainnet and testnet.
    #[clap(long)]
    genesis_hash: Option<String>,
    /// Head height to send in the handshake.
    #[clap(long, default_value = "0")]
    head_height: u64,
    /// Protocol version to advertise in the handshake.
    #[clap(long)]
    protocol_version: Option<u32>,
    /// Peer to request the proof from, as {public key}@{socket addr}. e.g.:
    /// ed25519:7PGseFbWxvYVgZ89K1uTJKYoKetWs7BJtbyXDzfbAcqX@127.0.0.1:24567
    #[clap(long)]
    peer: String,
    /// Seconds to wait for incoming data before giving up.
    #[clap(long, default_value = "120")]
    recv_timeout_seconds: u32,
    /// Writes the compressed proof, exactly as it came over the wire, to this file.
    #[clap(long)]
    save_proof: Option<PathBuf>,
    #[clap(flatten)]
    report: ReportOptions,
}

#[derive(clap::Parser)]
struct AnalyzeCommand {
    /// File written by `fetch --save-proof`.
    proof_file: PathBuf,
    #[clap(flatten)]
    report: ReportOptions,
}

#[derive(clap::Parser)]
struct ReportOptions {
    /// How many of the newest epochs to use for the growth-per-epoch measurement.
    #[clap(long, default_value = "10")]
    marginal_epochs: usize,
    /// Writes one row per epoch entry to this file.
    #[clap(long)]
    csv: Option<PathBuf>,
}

impl ReportOptions {
    fn run(&self, proof: &CompressedEpochSyncProof) -> anyhow::Result<()> {
        let measurement = crate::measure_proof(proof, self.marginal_epochs)?;
        println!("{}", measurement.report());
        if let Some(csv) = &self.csv {
            measurement.write_csv(csv)?;
            println!("per-epoch rows written to {}", csv.display());
        }
        Ok(())
    }
}

fn genesis_hash_for_chain(chain_id: &str, given: &Option<String>) -> anyhow::Result<CryptoHash> {
    if let Some(hash) = given {
        return CryptoHash::from_str(hash)
            .map_err(|err| anyhow::anyhow!("could not parse --genesis-hash {}: {:?}", hash, err));
    }
    let known = CHAIN_INFO.iter().find(|info| info.chain_id == chain_id);
    let Some(known) = known else {
        anyhow::bail!("--genesis-hash not given, and it is not known for --chain-id {}", chain_id)
    };
    Ok(known.genesis_hash)
}

impl FetchCommand {
    fn run(&self) -> anyhow::Result<()> {
        let genesis_hash = genesis_hash_for_chain(&self.chain_id, &self.genesis_hash)?;
        let peer = PeerInfo::from_str(&self.peer)
            .map_err(|err| anyhow::anyhow!("could not parse --peer {}: {:?}", &self.peer, err))?;
        let Some(peer_addr) = peer.addr else {
            anyhow::bail!("--peer should be in the form [public key]@[socket addr]")
        };

        let runtime = tokio::runtime::Runtime::new().unwrap();
        let proof = runtime.block_on(crate::fetch_proof_from_peer(
            &self.chain_id,
            genesis_hash,
            self.head_height,
            self.protocol_version,
            peer.id.clone(),
            peer_addr,
            self.recv_timeout_seconds,
        ))?;

        if let Some(path) = &self.save_proof {
            std::fs::write(path, proof.as_ref())
                .with_context(|| format!("failed writing {}", path.display()))?;
            println!("compressed proof written to {}", path.display());
        }
        println!("chain: {}  peer: {}@{}", self.chain_id, peer.id, peer_addr);
        self.report.run(&proof)
    }
}

impl AnalyzeCommand {
    fn run(&self) -> anyhow::Result<()> {
        let bytes = std::fs::read(&self.proof_file)
            .with_context(|| format!("failed reading {}", self.proof_file.display()))?;
        let proof = CompressedEpochSyncProof::from(bytes.into_boxed_slice());
        println!("file: {}", self.proof_file.display());
        self.report.run(&proof)
    }
}

impl EpochSyncProofSizeCommand {
    pub fn run(&self) -> anyhow::Result<()> {
        match &self.subcmd {
            EpochSyncProofSizeSubCommand::Fetch(cmd) => cmd.run(),
            EpochSyncProofSizeSubCommand::Analyze(cmd) => cmd.run(),
        }
    }
}
