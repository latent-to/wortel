//! Python bindings (feature `python`, built with maturin).

use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use pyo3::exceptions::{PyLookupError, PyOSError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;

use sp_core::H256;

use crate::error::Error;
use crate::{OpenMode, Wortel, keys};

fn to_py_err(e: Error) -> PyErr {
    if e.is_stale_snapshot() {
        // A readonly Reader raced the node's compaction: recreate the Reader
        // and retry (for iteration, resume via start_after / iter_prefix).
        return PyOSError::new_err(format!("stale snapshot: {e}"));
    }
    match e {
        Error::UnknownBlock(_) | Error::MissingState(_, _) => {
            PyLookupError::new_err(e.to_string())
        }
        Error::InvalidInput(_) => PyValueError::new_err(e.to_string()),
        other => PyRuntimeError::new_err(other.to_string()),
    }
}

fn h256(bytes: &[u8]) -> PyResult<H256> {
    if bytes.len() != 32 {
        return Err(PyValueError::new_err(format!(
            "expected 32-byte block hash, got {} bytes",
            bytes.len()
        )));
    }
    Ok(H256::from_slice(bytes))
}

static SECONDARY_SEQ: AtomicU64 = AtomicU64::new(0);

/// Reader over a subtensor archive node's RocksDB.
#[pyclass(frozen)]
struct Reader {
    inner: Wortel,
}

#[pymethods]
impl Reader {
    /// Open the node database at `path` (the `.../chains/<chain>/db/full` dir).
    ///
    /// `mode` is one of:
    /// - `"readonly"` (default) — safe next to a live node, cheap, but frozen
    ///   at the on-disk snapshot taken at open time (the head lags the node by
    ///   the un-flushed memtable). Reopen to refresh. Right choice for
    ///   backfills over historical blocks.
    /// - `"secondary"` — safe next to a live node and can follow it via
    ///   `catch_up()`. Opening replays the node's whole MANIFEST edit log,
    ///   which on a long-running node can be huge (restarting the node rolls
    ///   it); `secondary_path` is a scratch dir for this reader (auto-created
    ///   under the system temp dir if not given).
    /// - `"primary"` — read-write lock; only when no node uses the DB.
    #[new]
    #[pyo3(signature = (path, mode="readonly", secondary_path=None, cache_mib=512))]
    fn new(
        py: Python<'_>,
        path: String,
        mode: &str,
        secondary_path: Option<String>,
        cache_mib: usize,
    ) -> PyResult<Self> {
        let mode = match mode {
            "readonly" => OpenMode::ReadOnly,
            "primary" => OpenMode::Primary,
            "secondary" => {
                let secondary_path = match secondary_path {
                    Some(p) => PathBuf::from(p),
                    None => std::env::temp_dir().join(format!(
                        "wortel-secondary-{}-{}",
                        std::process::id(),
                        SECONDARY_SEQ.fetch_add(1, Ordering::Relaxed),
                    )),
                };
                OpenMode::Secondary { secondary_path }
            }
            other => {
                return Err(PyValueError::new_err(format!(
                    "mode must be \"readonly\", \"secondary\" or \"primary\", got {other:?}"
                )));
            }
        };
        // Opening scans >100k SSTs and can take a minute — release the GIL so
        // a server refreshing its Reader in a worker thread stays responsive.
        let inner = py
            .detach(|| Wortel::open_with_cache(path, mode, cache_mib))
            .map_err(to_py_err)?;
        Ok(Reader { inner })
    }

    /// Catch up with blocks the node wrote since open / the last catch-up.
    /// Secondary mode only; errors in readonly/primary mode.
    fn catch_up(&self, py: Python<'_>) -> PyResult<()> {
        py.detach(|| self.inner.try_catch_up_with_primary())
            .map_err(to_py_err)
    }

    fn genesis_hash(&self) -> PyResult<Option<Vec<u8>>> {
        Ok(self
            .inner
            .genesis_hash()
            .map_err(to_py_err)?
            .map(|h| h.as_bytes().to_vec()))
    }

    /// `(number, hash)` of the best block, or None on an empty db.
    fn best_block(&self) -> PyResult<Option<(u32, Vec<u8>)>> {
        Ok(self
            .inner
            .best_block()
            .map_err(to_py_err)?
            .map(|(n, h)| (n, h.as_bytes().to_vec())))
    }

    /// `(number, hash)` of the last finalized block.
    fn finalized_block(&self) -> PyResult<Option<(u32, Vec<u8>)>> {
        Ok(self
            .inner
            .finalized_block()
            .map_err(to_py_err)?
            .map(|(n, h)| (n, h.as_bytes().to_vec())))
    }

    /// Hash of the canonical block at `number` (chain_getBlockHash).
    fn block_hash(&self, number: u32) -> PyResult<Option<Vec<u8>>> {
        Ok(self
            .inner
            .block_hash(number)
            .map_err(to_py_err)?
            .map(|h| h.as_bytes().to_vec()))
    }

    fn block_number(&self, block_hash: &[u8]) -> PyResult<Option<u32>> {
        self.inner.block_number(&h256(block_hash)?).map_err(to_py_err)
    }

    /// SCALE-encoded header bytes (chain_getHeader, undecoded).
    fn header(&self, block_hash: &[u8]) -> PyResult<Option<Vec<u8>>> {
        self.inner.header_raw(&h256(block_hash)?).map_err(to_py_err)
    }

    fn state_root(&self, block_hash: &[u8]) -> PyResult<Option<Vec<u8>>> {
        Ok(self
            .inner
            .state_root(&h256(block_hash)?)
            .map_err(to_py_err)?
            .map(|h| h.as_bytes().to_vec()))
    }

    /// SCALE-encoded body (`Vec<OpaqueExtrinsic>`), undecoded.
    fn body(&self, block_hash: &[u8]) -> PyResult<Option<Vec<u8>>> {
        self.inner.body_raw(&h256(block_hash)?).map_err(to_py_err)
    }

    /// The block's extrinsics, one `bytes` per extrinsic — the same bytes the
    /// RPC returns (compact length prefix included).
    fn extrinsics(&self, block_hash: &[u8]) -> PyResult<Option<Vec<Vec<u8>>>> {
        self.inner.extrinsics(&h256(block_hash)?).map_err(to_py_err)
    }

    /// state_getStorage at `block_hash`.
    fn get_storage(
        &self,
        py: Python<'_>,
        block_hash: &[u8],
        key: Vec<u8>,
    ) -> PyResult<Option<Vec<u8>>> {
        let hash = h256(block_hash)?;
        py.detach(|| self.inner.get_storage(&hash, &key))
            .map_err(to_py_err)
    }

    /// Batched get_storage over one trie context; result aligns with `keys`.
    fn get_storage_many(
        &self,
        py: Python<'_>,
        block_hash: &[u8],
        keys: Vec<Vec<u8>>,
    ) -> PyResult<Vec<Option<Vec<u8>>>> {
        let hash = h256(block_hash)?;
        py.detach(|| self.inner.get_storage_many(&hash, &keys))
            .map_err(to_py_err)
    }

    /// All `(key, value)` pairs under `prefix` at `block_hash`, in one trie
    /// traversal (the fused getKeysPaged + getStorage). `start_after` resumes
    /// strictly after that key; `limit` caps the page size.
    #[pyo3(signature = (block_hash, prefix, start_after=None, limit=None))]
    fn storage_pairs(
        &self,
        py: Python<'_>,
        block_hash: &[u8],
        prefix: Vec<u8>,
        start_after: Option<Vec<u8>>,
        limit: Option<usize>,
    ) -> PyResult<Vec<(Vec<u8>, Vec<u8>)>> {
        let hash = h256(block_hash)?;
        py.detach(|| {
            self.inner
                .storage_pairs(&hash, &prefix, start_after.as_deref(), limit)
        })
        .map_err(to_py_err)
    }

    /// Lazy iterator over `(key, value)` pairs under `prefix`, fetching
    /// `page_size` pairs per underlying traversal. Use this instead of
    /// `storage_pairs` when the map is too large to hold at once.
    #[pyo3(signature = (block_hash, prefix, page_size=10_000))]
    fn iter_prefix(
        slf: Py<Reader>,
        block_hash: &[u8],
        prefix: Vec<u8>,
        page_size: usize,
    ) -> PyResult<PrefixIter> {
        Ok(PrefixIter {
            reader: slf,
            block_hash: h256(block_hash)?,
            prefix,
            page_size: page_size.max(1),
            last_key: None,
            buffer: VecDeque::new(),
            exhausted: false,
        })
    }
}

#[pyclass]
struct PrefixIter {
    reader: Py<Reader>,
    block_hash: H256,
    prefix: Vec<u8>,
    page_size: usize,
    last_key: Option<Vec<u8>>,
    buffer: VecDeque<(Vec<u8>, Vec<u8>)>,
    exhausted: bool,
}

#[pymethods]
impl PrefixIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self, py: Python<'_>) -> PyResult<Option<(Vec<u8>, Vec<u8>)>> {
        if self.buffer.is_empty() && !self.exhausted {
            let reader = self.reader.get();
            let (hash, prefix, start, limit) = (
                self.block_hash,
                self.prefix.clone(),
                self.last_key.clone(),
                self.page_size,
            );
            let page = py
                .detach(|| {
                    reader
                        .inner
                        .storage_pairs(&hash, &prefix, start.as_deref(), Some(limit))
                })
                .map_err(to_py_err)?;
            if page.len() < self.page_size {
                self.exhausted = true;
            }
            if let Some((k, _)) = page.last() {
                self.last_key = Some(k.clone());
            }
            self.buffer.extend(page);
        }
        Ok(self.buffer.pop_front())
    }
}

/// `twox128(pallet) ++ twox128(item)` storage prefix.
#[pyfunction]
fn storage_prefix(pallet: &str, item: &str) -> Vec<u8> {
    keys::storage_prefix(pallet, item).to_vec()
}

/// FRAME Twox64Concat hasher: `twox64(data) ++ data`.
#[pyfunction]
fn twox_64_concat(data: &[u8]) -> Vec<u8> {
    keys::twox_64_concat(data)
}

/// FRAME Blake2_128Concat hasher: `blake2_128(data) ++ data`.
#[pyfunction]
fn blake2_128_concat(data: &[u8]) -> Vec<u8> {
    keys::blake2_128_concat(data)
}

#[pymodule]
#[pyo3(name = "_wortel")]
fn wortel(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Reader>()?;
    m.add_class::<PrefixIter>()?;
    m.add_function(wrap_pyfunction!(storage_prefix, m)?)?;
    m.add_function(wrap_pyfunction!(twox_64_concat, m)?)?;
    m.add_function(wrap_pyfunction!(blake2_128_concat, m)?)?;
    Ok(())
}
