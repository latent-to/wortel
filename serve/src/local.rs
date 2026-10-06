//! The locally served methods, for one connection mode.
//!
//! Results are returned already serialized: header and block JSON come from
//! the same `sp_runtime` types the node serializes, so they match the node
//! byte for byte, and a 47k-pair map is written once, not built as a tree.

use std::sync::Arc;

use codec::Decode;
use serde::Serialize;
use sp_core::storage::{StorageChangeSet, StorageData, StorageKey};
use sp_core::{Bytes, H256, U256};
use sp_rpc::list::ListOrValue;
use sp_rpc::number::NumberOrHex;
use sp_runtime::OpaqueExtrinsic;
use sp_runtime::generic::{Block, SignedBlock};
use wortel::{Header, Wortel};

use crate::readers::{ReadError, Readers};
use crate::rpc::{Fail, INVALID_COUNT, Params, STORAGE_KEYS_PAGED_MAX_COUNT};

/// Every local method that takes an explicit block: what router mode can
/// serve. `--router-methods all` advertises them all, for benchmarking.
pub const ROUTER_CANDIDATES: &[&str] = &[
    "state_getStorage",
    "state_getStorageAt",
    "state_queryStorageAt",
    "state_getKeysPaged",
    "state_getKeys",
    "state_getPairs",
    "chain_getBlockHash",
    "chain_getHeader",
    "chain_getBlock",
];

/// Methods served in router mode by default, advertised there by
/// `rpc_methods`. A method joins only after `examples/bench_router.py` shows
/// it beats the node (mainnet, 2026-10-06): the storage reads and
/// `chain_getHeader` win cold, which is how router traffic arrives.
/// `chain_getBlock`, `chain_getBlockHash` and the paged walk stay with the
/// node; `state_getPairs` is unbenchmarked but the node refuses it (unsafe).
/// Re-measure with the Rust server before changing the set.
pub const ROUTER_METHODS: &[&str] = &[
    "state_getStorage",
    "state_getStorageAt",
    "state_queryStorageAt",
    "state_getPairs",
    "chain_getHeader",
];

const DIRECT_METHODS: &[&str] = &[
    "state_getStorage",
    "state_getStorageAt",
    "state_queryStorageAt",
    "state_getKeysPaged",
    "state_getKeys",
    "state_getPairs",
    "chain_getBlockHash",
    "chain_getHead",
    "chain_getFinalizedHead",
    "chain_getHeader",
    "chain_getBlock",
];

pub struct Local {
    readers: Arc<Readers>,
    max_keys_page: u32,
    router: bool,
    methods: Vec<String>,
}

fn json<T: Serialize>(value: &T) -> Result<String, Fail> {
    serde_json::to_string(value).map_err(|e| Fail::Internal(format!("serialize: {e}")))
}

fn block_number(value: NumberOrHex) -> Result<u32, Fail> {
    let n = value.into_u256();
    if n > U256::from(u32::MAX) {
        return Err(Fail::InvalidParams(format!(
            "block number {n} out of range"
        )));
    }
    Ok(n.low_u32())
}

impl Local {
    /// `router_methods` is the router-mode set; ignored in direct mode.
    pub fn new(
        readers: Arc<Readers>,
        max_keys_page: u32,
        router: bool,
        router_methods: &[String],
    ) -> Self {
        let methods = if router {
            let mut methods: Vec<String> = router_methods.to_vec();
            methods.push("rpc_methods".into());
            methods.sort();
            methods
        } else {
            DIRECT_METHODS.iter().map(|m| m.to_string()).collect()
        };
        Local {
            readers,
            max_keys_page,
            router,
            methods,
        }
    }

    pub fn serves(&self, method: &str) -> bool {
        self.methods.iter().any(|m| m == method)
    }

    pub async fn call(&self, method: &str, params: &Params<'_>) -> Result<String, Fail> {
        match method {
            "state_getStorage" | "state_getStorageAt" => self.state_get_storage(params).await,
            "state_queryStorageAt" => self.state_query_storage_at(params).await,
            "state_getKeysPaged" => self.state_get_keys_paged(params).await,
            "state_getKeys" => self.state_get_keys(params).await,
            "state_getPairs" => self.state_get_pairs(params).await,
            "chain_getBlockHash" => self.chain_get_block_hash(params).await,
            "chain_getHead" => json(&self.best_hash().await?),
            "chain_getFinalizedHead" => self.chain_get_finalized_head().await,
            "chain_getHeader" => self.chain_get_header(params).await,
            "chain_getBlock" => self.chain_get_block(params).await,
            "rpc_methods" => json(&serde_json::json!({ "methods": self.methods })),
            _ => Err(Fail::Internal(format!("{method}: no handler"))),
        }
    }

    // -- shared helpers ---------------------------------------------------

    async fn read<T, F>(&self, f: F) -> Result<T, Fail>
    where
        T: Send + 'static,
        F: Fn(&Wortel) -> wortel::Result<T> + Send + Sync + 'static,
    {
        // Router mode never waits for a reopen; the router asks the node.
        match self.readers.read(!self.router, f).await {
            Ok(value) => Ok(value),
            Err(ReadError::NoReader) => Err(Fail::NotInSnapshot("no snapshot open".into())),
            Err(ReadError::Stale) => Err(Fail::NotInSnapshot("stale snapshot".into())),
            Err(ReadError::Lookup(e)) => Err(Fail::NotInSnapshot(e.to_string())),
            Err(ReadError::Refresh(m)) => Err(Fail::Internal(m)),
            Err(ReadError::Other(e)) => Err(Fail::Internal(e.to_string())),
        }
    }

    async fn best_hash(&self) -> Result<H256, Fail> {
        if self.router {
            // The snapshot's head is minutes old, not the latest state. The
            // router never sends these; refusing guards against a routing bug.
            return Err(Fail::InvalidParams(
                "router mode: a block is required".into(),
            ));
        }
        let best = self.read(|r| r.best_block()).await?;
        best.map(|(_, hash)| hash)
            .ok_or_else(|| Fail::Internal("database has no best block".into()))
    }

    /// Resolve the optional trailing block-hash param at `index`.
    async fn at(&self, params: &Params<'_>, index: usize) -> Result<H256, Fail> {
        match params.opt::<H256>(index, "block hash")? {
            Some(hash) => Ok(hash),
            None => self.best_hash().await,
        }
    }

    // -- state_* -----------------------------------------------------------

    async fn state_get_storage(&self, params: &Params<'_>) -> Result<String, Fail> {
        let key: Bytes = params.get(0, "storage key")?;
        let at = self.at(params, 1).await?;
        let value = self.read(move |r| r.get_storage(&at, &key.0)).await?;
        json(&value.map(StorageData))
    }

    async fn state_query_storage_at(&self, params: &Params<'_>) -> Result<String, Fail> {
        let keys: Vec<StorageKey> = params.get(0, "keys")?;
        let at = self.at(params, 1).await?;
        let raw: Vec<Vec<u8>> = keys.iter().map(|k| k.0.clone()).collect();
        let values = self.read(move |r| r.get_storage_many(&at, &raw)).await?;
        let changes = keys
            .into_iter()
            .zip(values)
            .map(|(key, value)| (key, value.map(StorageData)))
            .collect();
        json(&[StorageChangeSet { block: at, changes }])
    }

    async fn state_get_keys_paged(&self, params: &Params<'_>) -> Result<String, Fail> {
        if params.len() < 2 {
            return Err(Fail::InvalidParams(
                "state_getKeysPaged: expected (prefix, count, ...)".into(),
            ));
        }
        let prefix: Option<StorageKey> = params.opt(0, "prefix")?;
        let count: u32 = params.get(1, "count")?;
        if self.router && count > STORAGE_KEYS_PAGED_MAX_COUNT {
            // The node rejects this; truncating would give a different answer.
            return Err(Fail::Code(
                INVALID_COUNT,
                format!(
                    "count exceeds maximum value. value: {count}, max: {STORAGE_KEYS_PAGED_MAX_COUNT}"
                ),
            ));
        }
        let count = count.min(self.max_keys_page) as usize;
        let start_after: Option<StorageKey> = params.opt(2, "start key")?;
        let at = self.at(params, 3).await?;
        let keys = self
            .read(move |r| {
                r.storage_keys(
                    &at,
                    prefix.as_ref().map_or(&[][..], |p| &p.0),
                    start_after.as_ref().map(|s| &s.0[..]),
                    Some(count),
                )
            })
            .await?;
        json(&keys.into_iter().map(StorageKey).collect::<Vec<_>>())
    }

    async fn state_get_keys(&self, params: &Params<'_>) -> Result<String, Fail> {
        if params.is_empty() {
            return Err(Fail::InvalidParams("state_getKeys: missing prefix".into()));
        }
        let prefix: Option<StorageKey> = params.opt(0, "prefix")?;
        let at = self.at(params, 1).await?;
        let keys = self
            .read(move |r| {
                r.storage_keys(&at, prefix.as_ref().map_or(&[][..], |p| &p.0), None, None)
            })
            .await?;
        json(&keys.into_iter().map(StorageKey).collect::<Vec<_>>())
    }

    /// Full `(key, value)` map under a prefix in ONE response — the method
    /// nodes keep behind `--rpc-methods unsafe`: one round trip and one trie
    /// traversal instead of ~N/1000 getKeysPaged + queryStorageAt exchanges.
    async fn state_get_pairs(&self, params: &Params<'_>) -> Result<String, Fail> {
        if params.is_empty() {
            return Err(Fail::InvalidParams("state_getPairs: missing prefix".into()));
        }
        let prefix: Option<StorageKey> = params.opt(0, "prefix")?;
        let at = self.at(params, 1).await?;
        let pairs = self
            .read(move |r| {
                r.storage_pairs(&at, prefix.as_ref().map_or(&[][..], |p| &p.0), None, None)
            })
            .await?;
        json(
            &pairs
                .into_iter()
                .map(|(k, v)| (StorageKey(k), StorageData(v)))
                .collect::<Vec<_>>(),
        )
    }

    // -- chain_* -----------------------------------------------------------

    async fn chain_get_block_hash(&self, params: &Params<'_>) -> Result<String, Fail> {
        let arg: Option<ListOrValue<NumberOrHex>> = params.opt(0, "block number")?;
        let numbers: Vec<u32> = match &arg {
            None => return json(&self.best_hash().await?),
            Some(ListOrValue::Value(n)) => vec![block_number(*n)?],
            Some(ListOrValue::List(list)) => list
                .iter()
                .map(|n| block_number(*n))
                .collect::<Result<_, _>>()?,
        };
        if self.router {
            // A number names one block for good only once it is finalized.
            let finalized = self.readers.snapshot().and_then(|s| s.finalized);
            for &n in &numbers {
                if finalized.is_none_or(|(f, _)| n > f) {
                    return Err(Fail::NotInSnapshot(format!(
                        "block {n} above snapshot finalized block"
                    )));
                }
            }
        }
        let wanted = numbers.clone();
        // One blocking-pool hop for the whole list, not one per number.
        let hashes = self
            .read(move |r| {
                wanted
                    .iter()
                    .map(|&n| r.block_hash(n))
                    .collect::<Result<Vec<_>, _>>()
            })
            .await?;
        if !self.router
            && hashes.iter().any(Option::is_none)
            && let Some((best, _)) = self.read(|r| r.best_block()).await?
        {
            for (&n, hash) in numbers.iter().zip(&hashes) {
                if hash.is_none() && n > best {
                    // Beyond our snapshot but possibly known to the node — let
                    // the upstream fallback answer instead of asserting null.
                    return Err(Fail::NotInSnapshot(format!(
                        "block {n} beyond snapshot head {best}"
                    )));
                }
            }
        }
        match arg {
            Some(ListOrValue::List(_)) => json(&hashes),
            _ => json(&hashes[0]),
        }
    }

    async fn chain_get_finalized_head(&self) -> Result<String, Fail> {
        let finalized = self.read(|r| r.finalized_block()).await?;
        let (_, hash) =
            finalized.ok_or_else(|| Fail::Internal("database has no finalized block".into()))?;
        json(&hash)
    }

    async fn chain_get_header(&self, params: &Params<'_>) -> Result<String, Fail> {
        let at = self.at(params, 0).await?;
        let header: Option<Header> = self.read(move |r| r.header(&at)).await?;
        match header {
            Some(header) => json(&header),
            // The router's resolver reads null as "not a block".
            None if self.router => Ok("null".into()),
            // Unknown to the snapshot ≠ nonexistent (we lag the node):
            // refresh/upstream-fallback rather than answering null.
            None => Err(Fail::NotInSnapshot(format!(
                "no header for {at:?} in snapshot"
            ))),
        }
    }

    async fn chain_get_block(&self, params: &Params<'_>) -> Result<String, Fail> {
        let at = self.at(params, 0).await?;
        let block = self
            .read(move |r| Ok((r.header(&at)?, r.body_raw(&at)?)))
            .await?;
        let (header, body) = match block {
            (Some(header), Some(body)) => (header, body),
            (None, _) => {
                return Err(Fail::NotInSnapshot(format!(
                    "no header for {at:?} in snapshot"
                )));
            }
            // Header without body should not happen on an archive node; let
            // the node answer rather than invent an empty block.
            (Some(_), None) => {
                return Err(Fail::NotInSnapshot(format!("no body for block {at:?}")));
            }
        };
        let extrinsics = Vec::<OpaqueExtrinsic>::decode(&mut &body[..])
            .map_err(|e| Fail::Internal(format!("decode block body: {e}")))?;
        let signed = SignedBlock {
            block: Block { header, extrinsics },
            justifications: None,
        };
        json(&signed)
    }
}
