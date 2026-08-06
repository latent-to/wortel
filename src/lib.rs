//! wortel — direct RocksDB reader for subtensor archive-node state.
//!
//! Reads the substrate client database of a subtensor node without going
//! through the websocket RPC, mirroring the RPC surface `taoapp/etl` uses:
//!
//! - [`Wortel::get_storage`] / [`Wortel::get_storage_many`] — `state_getStorage`
//! - [`Wortel::storage_pairs`] — `state_getKeysPaged` + `state_getStorage`,
//!   fused into one trie traversal
//! - [`Wortel::header_raw`] / [`Wortel::extrinsics`] — headers and bodies
//!   (flat columns, no trie involved)
//! - [`Wortel::block_hash`] / [`Wortel::best_block`] / [`Wortel::finalized_block`]
//!
//! Everything is raw SCALE bytes in and out, so callers keep their existing
//! key construction and decoding (`scalecodec` on the Python side).
//!
//! Pinned to polkadot-sdk revision `a08c7b0f0b6429910a953cbbaec4b4138294fa38`
//! (what `~/Git/subtensor` builds against). Requirements on the node side:
//! `--database rocksdb` (the default) and `--state-pruning archive`. When the
//! node is live, open in [`OpenMode::Secondary`] and call
//! [`Wortel::try_catch_up_with_primary`] periodically.

mod columns;
mod error;
pub mod keys;
mod reader;

#[cfg(feature = "python")]
mod py;

pub use error::{Error, Result};
pub use reader::{Header, OpenMode, Wortel};
