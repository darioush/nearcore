use crate::stats::ScanState;
use anyhow::{Context, bail};
use near_primitives::types::BlockHeight;
use std::path::Path;

pub fn save(path: &Path, state: &ScanState) -> anyhow::Result<()> {
    let temporary = path.with_extension("tmp");
    std::fs::write(&temporary, serde_json::to_vec(state)?)
        .with_context(|| format!("writing {}", temporary.display()))?;
    std::fs::rename(&temporary, path)
        .with_context(|| format!("renaming {} to {}", temporary.display(), path.display()))
}

/// Reads a checkpoint if one exists and describes the same scan. A checkpoint
/// of a different range is an error rather than a silent restart, because the
/// counters it holds would be meaningless for the new range.
pub fn load(
    path: &Path,
    chain_id: &str,
    start_height: BlockHeight,
    end_height: BlockHeight,
) -> anyhow::Result<Option<ScanState>> {
    if !path.exists() {
        return Ok(None);
    }
    let bytes = std::fs::read(path).with_context(|| format!("reading {}", path.display()))?;
    let state: ScanState =
        serde_json::from_slice(&bytes).with_context(|| format!("decoding {}", path.display()))?;
    if state.chain_id != chain_id {
        bail!("checkpoint is for chain {}, node reports {chain_id}", state.chain_id);
    }
    if state.start_height != start_height || state.end_height != end_height {
        bail!(
            "checkpoint covers {}..={}, requested {start_height}..={end_height}",
            state.start_height,
            state.end_height
        );
    }
    eprintln!(
        "resuming from {}, {} of {} blocks already scanned",
        path.display(),
        state.next_height.saturating_sub(start_height),
        end_height - start_height + 1
    );
    Ok(Some(state))
}

/// Reads a checkpoint without checking which range it covers, for the passes
/// that run after a scan has finished.
pub fn read_any(path: &Path) -> anyhow::Result<ScanState> {
    let bytes = std::fs::read(path).with_context(|| format!("reading {}", path.display()))?;
    serde_json::from_slice(&bytes).with_context(|| format!("decoding {}", path.display()))
}
