use crate::source::{
    AccountSource, BlockSource, ContractStatus, ScannedBlock, ScannedChunk, ScannedTransaction,
};
use anyhow::{Context, anyhow, bail};
use near_primitives::hash::CryptoHash;
use near_primitives::types::{AccountId, BlockHeight};
use near_primitives::views::{AccountView, BlockView, SignedTransactionView};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use std::time::Duration;

#[derive(Deserialize)]
struct RpcEnvelope<T> {
    result: Option<T>,
    error: Option<serde_json::Value>,
}

#[derive(Deserialize)]
struct ChunkTransactions {
    transactions: Vec<SignedTransactionView>,
}

pub struct RpcSource {
    client: reqwest::blocking::Client,
    url: String,
    max_attempts: u32,
}

impl RpcSource {
    pub fn new(url: String, request_timeout: Duration, max_attempts: u32) -> anyhow::Result<Self> {
        let client = reqwest::blocking::Client::builder()
            .timeout(request_timeout)
            .pool_max_idle_per_host(64)
            .build()
            .context("building http client")?;
        Ok(Self { client, url, max_attempts })
    }

    /// `Ok(None)` means the node answered with a "does not exist" error, which
    /// is an expected answer for skipped heights and untracked shards.
    fn call<T: DeserializeOwned>(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> anyhow::Result<Option<T>> {
        let body = serde_json::json!({"jsonrpc": "2.0", "id": "tx-scan", "method": method, "params": params});
        let mut last_error = None;
        for attempt in 0..self.max_attempts {
            if attempt > 0 {
                std::thread::sleep(Duration::from_millis(200 * (1 << attempt.min(6))));
            }
            let response = match self.client.post(&self.url).json(&body).send() {
                Ok(response) => response,
                Err(error) => {
                    last_error = Some(anyhow!("transport error: {error}"));
                    continue;
                }
            };
            let bytes = match response.bytes() {
                Ok(bytes) => bytes,
                Err(error) => {
                    last_error = Some(anyhow!("reading body: {error}"));
                    continue;
                }
            };
            let envelope: RpcEnvelope<T> = match serde_json::from_slice(&bytes) {
                Ok(envelope) => envelope,
                Err(error) => {
                    let head = String::from_utf8_lossy(&bytes[..bytes.len().min(400)]).into_owned();
                    last_error = Some(anyhow!("decoding {method} response: {error}: {head}"));
                    continue;
                }
            };
            if let Some(result) = envelope.result {
                return Ok(Some(result));
            }
            let error = envelope.error.unwrap_or(serde_json::Value::Null);
            if is_not_found(&error) {
                return Ok(None);
            }
            last_error = Some(anyhow!("{method} failed: {error}"));
        }
        bail!(last_error.unwrap_or_else(|| anyhow!("{method} failed with no attempts")))
    }
}

fn is_not_found(error: &serde_json::Value) -> bool {
    let text = error.to_string();
    ["UNKNOWN_BLOCK", "UNKNOWN_CHUNK", "UNKNOWN_ACCOUNT", "UNAVAILABLE_SHARD", "DB Not Found"]
        .iter()
        .any(|marker| text.contains(marker))
}

impl BlockSource for RpcSource {
    fn head_height(&self) -> anyhow::Result<BlockHeight> {
        let block: BlockView = self
            .call("block", serde_json::json!({"finality": "final"}))?
            .ok_or_else(|| anyhow!("node returned no final block"))?;
        Ok(block.header.height)
    }

    fn block_at_height(&self, height: BlockHeight) -> anyhow::Result<Option<ScannedBlock>> {
        let Some(block) =
            self.call::<BlockView>("block", serde_json::json!({"block_id": height}))?
        else {
            return Ok(None);
        };
        let mut chunks = Vec::new();
        for header in &block.chunks {
            if header.height_included != height {
                continue;
            }
            // An empty transaction list merkleizes to the default hash, so the
            // header alone rules the chunk out and its body never gets fetched.
            if header.tx_root == CryptoHash::default() {
                chunks.push(ScannedChunk { shard_id: header.shard_id, transactions: vec![] });
                continue;
            }
            let params = serde_json::json!({"block_id": height, "shard_id": header.shard_id});
            let Some(chunk) = self.call::<ChunkTransactions>("chunk", params)? else {
                continue;
            };
            let transactions = chunk
                .transactions
                .into_iter()
                .map(|tx| ScannedTransaction {
                    hash: tx.hash,
                    signer_id: tx.signer_id,
                    nonce_index: tx.nonce_index,
                    actions: tx.actions,
                })
                .collect();
            chunks.push(ScannedChunk { shard_id: header.shard_id, transactions });
        }
        chunks.sort_by_key(|chunk| chunk.shard_id);
        Ok(Some(ScannedBlock { height, chunks }))
    }
}

impl AccountSource for RpcSource {
    fn contract_status(&self, account_id: &AccountId) -> anyhow::Result<ContractStatus> {
        let params = serde_json::json!({
            "request_type": "view_account",
            "finality": "final",
            "account_id": account_id,
        });
        let Some(account) = self.call::<AccountView>("query", params)? else {
            return Ok(ContractStatus::AccountNotFound);
        };
        if account.global_contract_hash.is_some() || account.global_contract_account_id.is_some() {
            return Ok(ContractStatus::GlobalContract);
        }
        if account.code_hash == CryptoHash::default() {
            return Ok(ContractStatus::NoContract);
        }
        Ok(ContractStatus::LocalContract)
    }
}

#[derive(Deserialize)]
struct StatusResponse {
    chain_id: String,
}

impl RpcSource {
    pub fn chain_id(&self) -> anyhow::Result<String> {
        let status: StatusResponse = self
            .call("status", serde_json::json!([]))?
            .ok_or_else(|| anyhow!("node returned no status"))?;
        Ok(status.chain_id)
    }
}
