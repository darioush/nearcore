use anyhow::Context;
use borsh::object_length;
use near_network::raw::{ConnectError, Connection, DirectMessage, Message};
use near_network::types::HandshakeFailureReason;
use near_primitives::epoch_sync::{
    CompressedEpochSyncProof, EpochSyncProof, EpochSyncProofEpochData, EpochSyncProofV1,
};
use near_primitives::hash::CryptoHash;
use near_primitives::network::PeerId;
use near_primitives::types::{BlockHeight, ShardId};
use near_primitives::utils::compression::CompressedData;
use near_primitives::version::ProtocolVersion;
use std::fmt::Write as _;
use std::net::SocketAddr;
use std::path::Path;

pub mod cli;

/// Everything we measured about one `EpochSyncProof` response.
pub struct ProofMeasurement {
    pub compressed_bytes: usize,
    pub uncompressed_bytes: usize,
    pub all_epochs_bytes: usize,
    pub last_epoch_bytes: usize,
    pub current_epoch_bytes: usize,
    pub epochs: Vec<EpochEntryMeasurement>,
    /// Compressed size of the same proof with the newest `marginal_epochs` entries dropped.
    /// `None` when the proof has too few epochs to truncate.
    pub truncated_compressed_bytes: Option<usize>,
    pub marginal_epochs: usize,
}

pub struct EpochEntryMeasurement {
    pub last_final_block_height: BlockHeight,
    pub last_final_block_timestamp_nanos: u64,
    pub num_block_producers: usize,
    pub num_endorsements: usize,
    pub total_bytes: usize,
    pub block_producers_bytes: usize,
    pub endorsements_bytes: usize,
    pub block_header_bytes: usize,
}

fn measure_epoch_entry(epoch: &EpochSyncProofEpochData) -> EpochEntryMeasurement {
    EpochEntryMeasurement {
        last_final_block_height: epoch.last_final_block_header.height(),
        last_final_block_timestamp_nanos: epoch.last_final_block_header.raw_timestamp(),
        num_block_producers: epoch.block_producers.len(),
        num_endorsements: epoch
            .this_epoch_endorsements_for_last_final_block
            .iter()
            .filter(|signature| signature.is_some())
            .count(),
        total_bytes: object_length(epoch).unwrap(),
        block_producers_bytes: object_length(&epoch.block_producers).unwrap(),
        endorsements_bytes: object_length(&epoch.this_epoch_endorsements_for_last_final_block)
            .unwrap(),
        block_header_bytes: object_length(&epoch.last_final_block_header).unwrap(),
    }
}

/// Recompresses the proof with the newest `marginal_epochs` epochs removed, so that the
/// difference against the full proof gives the compressed cost of those epochs.
fn compress_without_newest_epochs(
    proof: &EpochSyncProofV1,
    marginal_epochs: usize,
) -> anyhow::Result<Option<usize>> {
    if proof.all_epochs.len() <= marginal_epochs {
        return Ok(None);
    }
    let mut truncated = proof.clone();
    truncated.all_epochs.truncate(proof.all_epochs.len() - marginal_epochs);
    let (compressed, _) = CompressedEpochSyncProof::encode(&EpochSyncProof::V1(truncated))
        .context("failed compressing the truncated proof")?;
    Ok(Some(compressed.size_bytes()))
}

pub fn measure_proof(
    compressed: &CompressedEpochSyncProof,
    marginal_epochs: usize,
) -> anyhow::Result<ProofMeasurement> {
    let (proof, uncompressed_bytes) =
        compressed.decode().context("failed decoding the epoch sync proof")?;
    let proof = proof.into_v1();
    let epochs = proof.all_epochs.iter().map(measure_epoch_entry).collect();
    let truncated_compressed_bytes = compress_without_newest_epochs(&proof, marginal_epochs)?;
    Ok(ProofMeasurement {
        compressed_bytes: compressed.size_bytes(),
        uncompressed_bytes,
        all_epochs_bytes: object_length(&proof.all_epochs).unwrap(),
        last_epoch_bytes: object_length(&proof.last_epoch).unwrap(),
        current_epoch_bytes: object_length(&proof.current_epoch).unwrap(),
        epochs,
        truncated_compressed_bytes,
        marginal_epochs,
    })
}

fn format_bytes(bytes: u64) -> String {
    const UNITS: [(&str, f64); 4] =
        [("GiB", 1024.0 * 1024.0 * 1024.0), ("MiB", 1024.0 * 1024.0), ("KiB", 1024.0), ("B", 1.0)];
    for (unit, scale) in UNITS {
        if bytes as f64 >= scale {
            return format!("{:.2} {}", bytes as f64 / scale, unit);
        }
    }
    format!("{} B", bytes)
}

fn mean(values: impl Iterator<Item = usize>) -> f64 {
    let (sum, count) = values.fold((0u64, 0u64), |(sum, count), v| (sum + v as u64, count + 1));
    if count == 0 { 0.0 } else { sum as f64 / count as f64 }
}

/// Seconds between the last final block of the first and the last epoch entry, divided by
/// the number of epochs in between. `None` if there are fewer than two entries or the
/// timestamps are not increasing.
fn seconds_per_epoch(epochs: &[EpochEntryMeasurement]) -> Option<f64> {
    let (first, last) = (epochs.first()?, epochs.last()?);
    let elapsed_nanos = last
        .last_final_block_timestamp_nanos
        .checked_sub(first.last_final_block_timestamp_nanos)?;
    if elapsed_nanos == 0 {
        return None;
    }
    Some(elapsed_nanos as f64 / 1e9 / (epochs.len() - 1) as f64)
}

impl ProofMeasurement {
    /// Mean uncompressed size of the newest `marginal_epochs` epoch entries.
    pub fn recent_bytes_per_epoch(&self) -> f64 {
        let start = self.epochs.len().saturating_sub(self.marginal_epochs);
        mean(self.epochs[start..].iter().map(|epoch| epoch.total_bytes))
    }

    pub fn compressed_bytes_per_epoch(&self) -> Option<f64> {
        let truncated = self.truncated_compressed_bytes?;
        Some(
            (self.compressed_bytes.saturating_sub(truncated)) as f64
                / self.marginal_epochs.min(self.epochs.len()) as f64,
        )
    }

    pub fn report(&self) -> String {
        let mut out = String::new();
        let ratio = self.uncompressed_bytes as f64 / self.compressed_bytes as f64;
        writeln!(out, "epochs in proof:          {}", self.epochs.len()).unwrap();
        writeln!(
            out,
            "on the wire (compressed): {:>12} ({} bytes)",
            format_bytes(self.compressed_bytes as u64),
            self.compressed_bytes
        )
        .unwrap();
        writeln!(
            out,
            "uncompressed (borsh):     {:>12} ({} bytes)",
            format_bytes(self.uncompressed_bytes as u64),
            self.uncompressed_bytes
        )
        .unwrap();
        writeln!(out, "compression ratio:        {:>12.2}x", ratio).unwrap();
        writeln!(out).unwrap();

        writeln!(out, "sections (uncompressed):").unwrap();
        for (name, bytes) in [
            ("all_epochs", self.all_epochs_bytes),
            ("last_epoch", self.last_epoch_bytes),
            ("current_epoch", self.current_epoch_bytes),
        ] {
            writeln!(
                out,
                "  {:<16}{:>12}  {:>5.1}%",
                name,
                format_bytes(bytes as u64),
                100.0 * bytes as f64 / self.uncompressed_bytes as f64
            )
            .unwrap();
        }
        writeln!(out).unwrap();

        if let Some(newest) = self.epochs.last() {
            writeln!(out, "newest epoch entry (uncompressed):").unwrap();
            writeln!(out, "  block producers:  {}", newest.num_block_producers).unwrap();
            writeln!(out, "  endorsements:     {}", newest.num_endorsements).unwrap();
            for (name, bytes) in [
                ("total", newest.total_bytes),
                ("block_producers", newest.block_producers_bytes),
                ("endorsements", newest.endorsements_bytes),
                ("block_header", newest.block_header_bytes),
            ] {
                writeln!(out, "  {:<16}{:>12}", name, format_bytes(bytes as u64)).unwrap();
            }
            writeln!(out).unwrap();
        }

        let uncompressed_per_epoch = self.recent_bytes_per_epoch();
        writeln!(out, "growth per epoch (mean of newest {} entries):", self.marginal_epochs)
            .unwrap();
        writeln!(out, "  uncompressed:   {:>12}", format_bytes(uncompressed_per_epoch as u64))
            .unwrap();
        let compressed_per_epoch = self.compressed_bytes_per_epoch();
        match compressed_per_epoch {
            Some(bytes) => writeln!(
                out,
                "  compressed:     {:>12}  (full proof minus its newest {} epochs)",
                format_bytes(bytes as u64),
                self.marginal_epochs
            )
            .unwrap(),
            None => writeln!(out, "  compressed:     not enough epochs to measure").unwrap(),
        }

        let Some(seconds_per_epoch) = seconds_per_epoch(&self.epochs) else {
            return out;
        };
        let epochs_per_year = 365.25 * 24.0 * 3600.0 / seconds_per_epoch;
        writeln!(
            out,
            "  epoch duration: {:>12.1}h  (mean over the whole proof), {:.0} epochs/year",
            seconds_per_epoch / 3600.0,
            epochs_per_year
        )
        .unwrap();
        writeln!(out).unwrap();
        writeln!(out, "projection at the current validator count:").unwrap();
        for (name, per_epoch, size_today) in [
            ("uncompressed", Some(uncompressed_per_epoch), self.uncompressed_bytes),
            ("compressed", compressed_per_epoch, self.compressed_bytes),
        ] {
            let Some(per_epoch) = per_epoch else { continue };
            let per_year = per_epoch * epochs_per_year;
            writeln!(
                out,
                "  {:<14}+{:>10}/year, {:>10} in 1 year, {:>10} in 5 years",
                name,
                format_bytes(per_year as u64),
                format_bytes((size_today as f64 + per_year) as u64),
                format_bytes((size_today as f64 + per_year * 5.0) as u64),
            )
            .unwrap();
        }
        out
    }

    pub fn write_csv(&self, path: &Path) -> anyhow::Result<()> {
        let mut out = String::from(
            "index,last_final_block_height,timestamp_nanos,num_block_producers,num_endorsements,\
             total_bytes,block_producers_bytes,endorsements_bytes,block_header_bytes\n",
        );
        for (index, epoch) in self.epochs.iter().enumerate() {
            writeln!(
                out,
                "{},{},{},{},{},{},{},{},{}",
                index,
                epoch.last_final_block_height,
                epoch.last_final_block_timestamp_nanos,
                epoch.num_block_producers,
                epoch.num_endorsements,
                epoch.total_bytes,
                epoch.block_producers_bytes,
                epoch.endorsements_bytes,
                epoch.block_header_bytes,
            )
            .unwrap();
        }
        std::fs::write(path, out).with_context(|| format!("failed writing {}", path.display()))
    }
}

async fn connect_to_peer(
    chain_id: &str,
    genesis_hash: CryptoHash,
    head_height: BlockHeight,
    protocol_version: Option<ProtocolVersion>,
    peer_id: PeerId,
    peer_addr: SocketAddr,
    recv_timeout_seconds: u32,
) -> Result<Connection, ConnectError> {
    Connection::connect(
        &near_time::Clock::real(),
        peer_addr,
        peer_id,
        protocol_version,
        chain_id,
        genesis_hash,
        head_height,
        vec![ShardId::new(0)],
        Some(near_time::Duration::seconds(recv_timeout_seconds.into())),
    )
    .await
}

pub async fn fetch_proof_from_peer(
    chain_id: &str,
    genesis_hash: CryptoHash,
    head_height: BlockHeight,
    protocol_version: Option<ProtocolVersion>,
    peer_id: PeerId,
    peer_addr: SocketAddr,
    recv_timeout_seconds: u32,
) -> anyhow::Result<CompressedEpochSyncProof> {
    let mut connect_result = connect_to_peer(
        chain_id,
        genesis_hash,
        head_height,
        protocol_version,
        peer_id.clone(),
        peer_addr,
        recv_timeout_seconds,
    )
    .await;

    // Our build is often ahead of the version the network runs, so retry once with the
    // version the peer asked for, unless the caller chose one.
    if protocol_version.is_none()
        && let Err(ConnectError::HandshakeFailure(
            HandshakeFailureReason::ProtocolVersionMismatch { version, .. },
        )) = &connect_result
    {
        tracing::info!(target: "epoch-sync-proof-size", peer_protocol_version = version, "retrying the handshake with the version the peer supports");
        connect_result = connect_to_peer(
            chain_id,
            genesis_hash,
            head_height,
            Some(*version),
            peer_id.clone(),
            peer_addr,
            recv_timeout_seconds,
        )
        .await;
    }

    let mut peer = match connect_result {
        Ok(peer) => peer,
        Err(ConnectError::HandshakeFailure(reason)) => match reason {
            HandshakeFailureReason::ProtocolVersionMismatch {
                version,
                oldest_supported_version,
            } => anyhow::bail!(
                "handshake failure: {:?}. try again with --protocol-version between {} and {}",
                reason,
                oldest_supported_version,
                version
            ),
            HandshakeFailureReason::GenesisMismatch(_) => anyhow::bail!(
                "handshake failure: {:?}. try again with --chain-id and --genesis-hash set to these values",
                reason,
            ),
            HandshakeFailureReason::InvalidTarget => anyhow::bail!(
                "handshake failure: {:?}. is the public key given with --peer correct?",
                reason,
            ),
        },
        Err(err) => anyhow::bail!("error connecting to {:?}: {}", peer_addr, err),
    };
    tracing::info!(target: "epoch-sync-proof-size", ?peer_addr, ?peer_id, "connected to peer");

    peer.send_message(DirectMessage::EpochSyncRequest)
        .await
        .context("failed sending the epoch sync request")?;
    tracing::info!(target: "epoch-sync-proof-size", "sent epoch sync request, waiting for the response");

    loop {
        let (message, _received_at) = peer.recv().await.context(
            "failed receiving the epoch sync response; the peer may have no proof stored",
        )?;
        if let Message::Direct(DirectMessage::EpochSyncResponse(proof)) = message {
            return Ok(proof);
        }
        tracing::debug!(target: "epoch-sync-proof-size", %message, "ignoring message");
    }
}
