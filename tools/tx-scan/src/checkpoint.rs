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

/// Reads a checkpoint if one exists and describes the same scan.
///
/// A later `end_height` extends the range and keeps every counter, because the
/// scan only ever moves forward and the per-shard windows carry over. A
/// different `start_height`, or an earlier `end_height`, would leave the
/// counters describing blocks outside the requested range, so both are errors
/// rather than a silent restart.
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
    let mut state: ScanState =
        serde_json::from_slice(&bytes).with_context(|| format!("decoding {}", path.display()))?;
    if state.chain_id != chain_id {
        bail!("checkpoint is for chain {}, node reports {chain_id}", state.chain_id);
    }
    if state.start_height != start_height {
        bail!("checkpoint starts at {}, requested {start_height}", state.start_height);
    }
    if end_height < state.end_height {
        bail!(
            "checkpoint already covers {}..={}, and a shorter range ending {end_height} would \
             leave its counters describing blocks outside it",
            state.start_height,
            state.end_height
        );
    }
    if end_height > state.end_height {
        eprintln!("extending the range from {} to {end_height}", state.end_height);
        state.end_height = end_height;
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
