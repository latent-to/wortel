//! Column indices of the substrate client database.
//!
//! Verified against `substrate/client/db/src/utils.rs` at polkadot-sdk revision
//! `a08c7b0f0b6429910a953cbbaec4b4138294fa38` (the revision pinned by
//! `~/Git/subtensor/Cargo.lock`). If subtensor bumps its polkadot-sdk revision,
//! re-check these against `sc-client-db`.

pub const NUM_COLUMNS: u32 = 13;

/// Column-family names as kvdb-rocksdb creates them: `col<index>`.
pub fn cf_name(col: u32) -> String {
    format!("col{col}")
}

pub const META: u32 = 0;
pub const STATE: u32 = 1;
/// Maps block hashes to lookup keys, and 4-byte big-endian block numbers to
/// the lookup keys of canonical blocks. A lookup key is `be32(number) ++ hash`.
pub const KEY_LOOKUP: u32 = 3;
pub const HEADER: u32 = 4;
pub const BODY: u32 = 5;

/// Keys of the META column (`sc-client-db`'s `meta_keys`).
pub mod meta_keys {
    /// Value is the lookup key of the best block; its header lives in HEADER.
    pub const BEST_BLOCK: &[u8] = b"best";
    /// Value is the lookup key of the last finalized block.
    pub const FINALIZED_BLOCK: &[u8] = b"final";
    /// Value is the SCALE-encoded genesis hash (raw 32 bytes for H256).
    pub const GENESIS_HASH: &[u8] = b"gen";
}
