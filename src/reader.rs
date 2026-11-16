use std::path::{Path, PathBuf};

use codec::Decode;
use hash_db::{HashDBRef, Prefix};
use rocksdb::{BlockBasedOptions, Cache, ColumnFamilyDescriptor, Options};
use sp_core::{Blake2Hasher, H256};
use sp_runtime::{OpaqueExtrinsic, generic, traits::BlakeTwo256};
use sp_trie::{DBValue, Trie, TrieDBIterator, TrieDBKeyIterator, trie_types::TrieDBBuilder};

use crate::columns::{self, meta_keys};
use crate::error::{Error, Result};

type Db = rocksdb::DBWithThreadMode<rocksdb::MultiThreaded>;

/// The header type of the subtensor chain (`generic::Header<u32, BlakeTwo256>`).
pub type Header = generic::Header<u32, BlakeTwo256>;

/// How to open the RocksDB database.
#[derive(Clone, Debug)]
pub enum OpenMode {
    /// Read-only open against the current on-disk snapshot. Safe next to a
    /// live node, cheap (bounded open files, MBs of memory), but frozen at
    /// open time: it sees neither later blocks nor the un-flushed memtable
    /// (WAL) contents, so the visible head lags the node's. Reopen to refresh.
    /// This is the right mode for backfills over historical blocks.
    ReadOnly,
    /// RocksDB *secondary* instance: also safe next to a live node, and can
    /// follow it live — [`Wortel::try_catch_up_with_primary`] replays the
    /// node's WAL, so new blocks and the memtable contents become visible.
    /// Caveats: RocksDB requires `max_open_files = -1` here (every SST is
    /// opened — raise the fd limit), and opening replays the primary's whole
    /// MANIFEST edit log. On a node that has been up for weeks the MANIFEST
    /// can reach hundreds of MB and the replay takes tens of GB of memory —
    /// restarting the node rolls the MANIFEST and fixes this. Prefer ReadOnly
    /// unless live-following is actually needed. `secondary_path` is a
    /// writable scratch dir owned by this reader instance.
    Secondary { secondary_path: PathBuf },
    /// Open as the primary instance (takes the write lock). Only valid when no
    /// node is running against the database — e.g. a snapshot or stopped node.
    Primary,
}

/// A direct reader over a subtensor (substrate) archive node's RocksDB.
///
/// Mirrors the RPC surface `taoapp/etl` consumes: `state_getStorage`,
/// `getKeysPaged`+`getStorage` (fused as [`Wortel::storage_pairs`]), headers,
/// and block bodies. All inputs and outputs are raw SCALE bytes, so existing
/// key construction and decoding on the caller's side works unchanged.
pub struct Wortel {
    db: Db,
}

impl Wortel {
    /// Open the database at `path` (the node's `.../db/full` directory) with a
    /// 512 MiB block cache. See [`Wortel::open_with_cache`].
    pub fn open(path: impl AsRef<Path>, mode: OpenMode) -> Result<Self> {
        Self::open_with_cache(path, mode, 512)
    }

    /// Open with an explicit block-cache budget (MiB).
    ///
    /// The budget is for data blocks only. Index and filter blocks stay on
    /// the heap, outside it, for each SST file a read has opened: see
    /// [`Wortel::open_with_block_cache`].
    pub fn open_with_cache(
        path: impl AsRef<Path>,
        mode: OpenMode,
        cache_mib: usize,
    ) -> Result<Self> {
        Self::open_with_block_cache(path, mode, &block_cache(cache_mib)?)
    }

    /// Open on a block cache the caller owns (see [`block_cache`]). A
    /// process that reopens its reader to refresh passes the same cache each
    /// time: RocksDB keys cached blocks by SST file, so the new reader starts
    /// warm instead of re-reading every data block.
    ///
    /// The cache holds data blocks only. Index and filter blocks stay on the
    /// heap (`cache_index_and_filter_blocks(false)`), so memory grows with
    /// the number of SST files that reads have opened and is not limited by
    /// the cache size. With them in the cache, a 2 GiB cache on the mainnet
    /// archive DB reloaded them continuously: a warm 47k-entry map walk took
    /// 15 s, against 0.5 s with them on the heap.
    pub fn open_with_block_cache(
        path: impl AsRef<Path>,
        mode: OpenMode,
        cache: &BlockCache,
    ) -> Result<Self> {
        let mut opts = Options::default();
        opts.create_if_missing(false);
        let mut bopts = BlockBasedOptions::default();
        bopts.set_block_cache(cache);
        bopts.set_cache_index_and_filter_blocks(false);
        opts.set_block_based_table_factory(&bopts);

        // "default" always exists; the data lives in kvdb-rocksdb's col0..col12.
        // Each column family gets the table options explicitly: the
        // name-only open functions give a named column family
        // `Options::default()`, which has its own small block cache.
        let cf_descriptors = || {
            std::iter::once("default".to_string())
                .chain((0..columns::NUM_COLUMNS).map(columns::cf_name))
                .map(|name| {
                    let mut cf_opts = Options::default();
                    cf_opts.set_block_based_table_factory(&bopts);
                    ColumnFamilyDescriptor::new(name, cf_opts)
                })
        };

        let db = match mode {
            OpenMode::ReadOnly => {
                // Bounded, but generous: a small table cache thrashes on trie
                // traversals (every miss re-reads an SST footer + index).
                // Unlike -1 this does not eagerly open every file at startup.
                raise_fd_limit();
                opts.set_max_open_files(262_144);
                // A live primary compacts continuously; between our manifest
                // read and file open an SST can vanish. Each retry re-reads
                // the (new) manifest, so a handful of attempts suffices.
                let mut attempt = 0;
                loop {
                    match Db::open_cf_descriptors_read_only(
                        &opts,
                        path.as_ref(),
                        cf_descriptors(),
                        false,
                    ) {
                        Ok(db) => break db,
                        Err(e) if is_stale_file_error(&e) && attempt < 5 => {
                            attempt += 1;
                            std::thread::sleep(std::time::Duration::from_millis(300 * attempt));
                        }
                        Err(e) => return Err(e.into()),
                    }
                }
            }
            OpenMode::Secondary { secondary_path } => {
                // RocksDB constraint: secondary instances must keep every SST
                // open, so lift the fd soft limit as far as allowed.
                raise_fd_limit();
                opts.set_max_open_files(-1);
                Db::open_cf_descriptors_as_secondary(
                    &opts,
                    path.as_ref(),
                    &secondary_path,
                    cf_descriptors(),
                )?
            }
            OpenMode::Primary => {
                opts.set_max_open_files(512);
                Db::open_cf_descriptors(&opts, path.as_ref(), cf_descriptors())?
            }
        };
        Ok(Wortel { db })
    }

    /// Replay the primary's recent MANIFEST/WAL so this secondary instance
    /// sees new blocks. Call periodically when following a live node.
    /// Errors in the other open modes.
    pub fn try_catch_up_with_primary(&self) -> Result<()> {
        self.db.try_catch_up_with_primary()?;
        Ok(())
    }

    fn get(&self, col: u32, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let cf = self
            .db
            .cf_handle(&columns::cf_name(col))
            .ok_or_else(|| Error::InvalidInput(format!("missing column family col{col}")))?;
        Ok(self.db.get_cf(&cf, key)?)
    }

    // ---- chain metadata -------------------------------------------------

    pub fn genesis_hash(&self) -> Result<Option<H256>> {
        match self.get(columns::META, meta_keys::GENESIS_HASH)? {
            Some(raw) => H256::decode(&mut &raw[..])
                .map(Some)
                .map_err(|e| Error::codec("genesis hash", e)),
            None => Ok(None),
        }
    }

    /// Best (head) block as `(number, hash)`.
    pub fn best_block(&self) -> Result<Option<(u32, H256)>> {
        self.meta_block(meta_keys::BEST_BLOCK)
    }

    /// Last finalized block as `(number, hash)`.
    pub fn finalized_block(&self) -> Result<Option<(u32, H256)>> {
        self.meta_block(meta_keys::FINALIZED_BLOCK)
    }

    fn meta_block(&self, key: &[u8]) -> Result<Option<(u32, H256)>> {
        match self.get(columns::META, key)? {
            Some(lookup) => Ok(Some(split_lookup_key(&lookup)?)),
            None => Ok(None),
        }
    }

    // ---- block lookup ---------------------------------------------------

    /// Hash of the canonical block at `number` (chain_getBlockHash).
    pub fn block_hash(&self, number: u32) -> Result<Option<H256>> {
        match self.get(columns::KEY_LOOKUP, &number.to_be_bytes())? {
            Some(lookup) => Ok(Some(split_lookup_key(&lookup)?.1)),
            None => Ok(None),
        }
    }

    /// Number of the block with `hash`, if known.
    pub fn block_number(&self, hash: &H256) -> Result<Option<u32>> {
        match self.lookup_key(hash)? {
            Some(lookup) => Ok(Some(split_lookup_key(&lookup)?.0)),
            None => Ok(None),
        }
    }

    fn lookup_key(&self, hash: &H256) -> Result<Option<Vec<u8>>> {
        self.get(columns::KEY_LOOKUP, hash.as_bytes())
    }

    // ---- headers and bodies (flat columns, no trie) ---------------------

    /// SCALE-encoded header (chain_getHeader, undecoded).
    pub fn header_raw(&self, hash: &H256) -> Result<Option<Vec<u8>>> {
        match self.lookup_key(hash)? {
            Some(lookup) => self.get(columns::HEADER, &lookup),
            None => Ok(None),
        }
    }

    pub fn header(&self, hash: &H256) -> Result<Option<Header>> {
        match self.header_raw(hash)? {
            Some(raw) => Header::decode(&mut &raw[..])
                .map(Some)
                .map_err(|e| Error::codec("header", e)),
            None => Ok(None),
        }
    }

    pub fn state_root(&self, hash: &H256) -> Result<Option<H256>> {
        Ok(self.header(hash)?.map(|h| h.state_root))
    }

    /// SCALE-encoded block body, i.e. `Vec<OpaqueExtrinsic>` (undecoded).
    pub fn body_raw(&self, hash: &H256) -> Result<Option<Vec<u8>>> {
        match self.lookup_key(hash)? {
            Some(lookup) => self.get(columns::BODY, &lookup),
            None => Ok(None),
        }
    }

    /// The block's extrinsics, each as its own SCALE bytes — the same bytes
    /// the RPC returns per extrinsic (compact length prefix included).
    pub fn extrinsics(&self, hash: &H256) -> Result<Option<Vec<Vec<u8>>>> {
        match self.body_raw(hash)? {
            Some(raw) => {
                let exts = Vec::<OpaqueExtrinsic>::decode(&mut &raw[..])
                    .map_err(|e| Error::codec("block body", e))?;
                Ok(Some(exts.iter().map(|e| codec::Encode::encode(e)).collect()))
            }
            None => Ok(None),
        }
    }

    // ---- state (trie) ---------------------------------------------------

    /// state_getStorage: value under `key` at `block_hash`.
    pub fn get_storage(&self, block_hash: &H256, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.with_trie(block_hash, |trie| {
            trie.get(key).map_err(|e| Error::trie(*block_hash, e))
        })
    }

    /// Batched `get_storage` over one trie descent context.
    pub fn get_storage_many(
        &self,
        block_hash: &H256,
        keys: &[Vec<u8>],
    ) -> Result<Vec<Option<Vec<u8>>>> {
        self.with_trie(block_hash, |trie| {
            keys.iter()
                .map(|k| trie.get(k).map_err(|e| Error::trie(*block_hash, e)))
                .collect()
        })
    }

    /// Fused getKeysPaged + state_getStorage: stream `(key, value)` pairs under
    /// `prefix` in a single trie traversal.
    ///
    /// `start_after` resumes iteration strictly after that key (pagination);
    /// `limit` caps the number of returned pairs. `None` limit returns the
    /// whole prefix subtree.
    pub fn storage_pairs(
        &self,
        block_hash: &H256,
        prefix: &[u8],
        start_after: Option<&[u8]>,
        limit: Option<usize>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.with_trie(block_hash, |trie| {
            let iter = match start_after {
                Some(start) => TrieDBIterator::new_prefixed_then_seek(trie, prefix, start),
                None => TrieDBIterator::new_prefixed(trie, prefix),
            }
            .map_err(|e| Error::trie(*block_hash, e))?;

            let limit = limit.unwrap_or(usize::MAX);
            let mut out = Vec::new();
            for item in iter {
                let (key, value) = item.map_err(|e| Error::trie(*block_hash, e))?;
                // The seek is inclusive; pagination wants strictly-after.
                if let Some(start) = start_after {
                    if key.as_slice() <= start {
                        continue;
                    }
                }
                out.push((key, value));
                if out.len() >= limit {
                    break;
                }
            }
            Ok(out)
        })
    }

    /// state_getKeysPaged: keys under `prefix`, values left unread. Same
    /// pagination contract as [`Self::storage_pairs`].
    pub fn storage_keys(
        &self,
        block_hash: &H256,
        prefix: &[u8],
        start_after: Option<&[u8]>,
        limit: Option<usize>,
    ) -> Result<Vec<Vec<u8>>> {
        self.with_trie(block_hash, |trie| {
            let iter = match start_after {
                Some(start) => TrieDBKeyIterator::new_prefixed_then_seek(trie, prefix, start),
                None => TrieDBKeyIterator::new_prefixed(trie, prefix),
            }
            .map_err(|e| Error::trie(*block_hash, e))?;

            let limit = limit.unwrap_or(usize::MAX);
            let mut out = Vec::new();
            for item in iter {
                let key = item.map_err(|e| Error::trie(*block_hash, e))?;
                if let Some(start) = start_after {
                    if key.as_slice() <= start {
                        continue;
                    }
                }
                out.push(key);
                if out.len() >= limit {
                    break;
                }
            }
            Ok(out)
        })
    }

    fn with_trie<T>(
        &self,
        block_hash: &H256,
        f: impl FnOnce(&sp_trie::trie_types::TrieDB<Blake2Hasher>) -> Result<T>,
    ) -> Result<T> {
        let root = self
            .state_root(block_hash)?
            .ok_or(Error::UnknownBlock(*block_hash))?;
        let state = StateHashDb { db: self };
        // A missing root node means the state for this block was pruned (or
        // the node isn't a full archive) — distinguish that from trie errors.
        if !HashDBRef::<Blake2Hasher, DBValue>::contains(&state, &root, hash_db::EMPTY_PREFIX) {
            return Err(Error::MissingState(*block_hash, root));
        }
        let trie = TrieDBBuilder::<Blake2Hasher>::new(&state, &root).build();
        f(&trie)
    }
}

/// A RocksDB block cache, shareable between readers (cloning shares it).
pub type BlockCache = Cache;

/// A block cache of `cache_mib` MiB for data blocks. Index and filter blocks
/// are not in it: see [`Wortel::open_with_block_cache`].
pub fn block_cache(cache_mib: usize) -> Result<BlockCache> {
    if cache_mib == 0 {
        return Err(Error::InvalidInput(
            "cache_mib must be > 0 (with no block cache, each read of a data \
             block goes to the file again)"
                .into(),
        ));
    }
    Ok(Cache::new_lru_cache(cache_mib * 1024 * 1024))
}

/// Adapter exposing the STATE column as the `HashDBRef` the trie reader wants.
/// Keys on disk are `prefixed_key(node_hash, prefix)` — the scheme sc-client-db
/// uses for RocksDB (`prefix_keys == !supports_ref_counting()`).
struct StateHashDb<'a> {
    db: &'a Wortel,
}

impl HashDBRef<Blake2Hasher, DBValue> for StateHashDb<'_> {
    fn get(&self, key: &H256, prefix: Prefix) -> Option<DBValue> {
        let db_key = sp_trie::prefixed_key::<Blake2Hasher>(key, prefix);
        self.db.get(columns::STATE, &db_key).ok().flatten()
    }

    fn contains(&self, key: &H256, prefix: Prefix) -> bool {
        self.get(key, prefix).is_some()
    }
}

/// A read-only instance racing the live node's compaction: the file was
/// deleted after our manifest snapshot. Fixed by reopening (fresh manifest).
pub(crate) fn is_stale_file_error(e: &rocksdb::Error) -> bool {
    let msg = e.to_string();
    msg.contains("No such file or directory") || msg.contains("Stale file")
}

/// Raise the fd soft limit to the hard limit. Secondary mode forces
/// `max_open_files = -1`, and an archive DB holds >100k SST files.
fn raise_fd_limit() {
    #[cfg(unix)]
    unsafe {
        let mut lim = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut lim) == 0 && lim.rlim_cur < lim.rlim_max {
            lim.rlim_cur = lim.rlim_max;
            libc::setrlimit(libc::RLIMIT_NOFILE, &lim);
        }
    }
}

fn split_lookup_key(lookup: &[u8]) -> Result<(u32, H256)> {
    if lookup.len() != 36 {
        return Err(Error::InvalidInput(format!(
            "bad lookup key length {} (expected 36)",
            lookup.len()
        )));
    }
    let number = u32::from_be_bytes(lookup[..4].try_into().expect("length checked"));
    Ok((number, H256::from_slice(&lookup[4..])))
}
