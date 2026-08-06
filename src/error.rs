use sp_core::H256;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("database error: {0}")]
    Db(#[from] rocksdb::Error),

    #[error("unknown block {0:?}")]
    UnknownBlock(H256),

    #[error(
        "state for block {0:?} not in database (root {1:?}); \
         is the node running with --state-pruning archive?"
    )]
    MissingState(H256, H256),

    #[error("failed to decode {what}: {source}")]
    Codec {
        what: &'static str,
        source: codec::Error,
    },

    #[error("trie error at block {block:?}: {message}")]
    Trie { block: H256, message: String },

    #[error("{0}")]
    InvalidInput(String),
}

impl Error {
    /// True when a read-only reader hit an SST file the live node's compaction
    /// deleted after the reader's open-time snapshot. Recover by reopening the
    /// reader and retrying (for prefix iteration, resume via `start_after`).
    pub fn is_stale_snapshot(&self) -> bool {
        matches!(self, Error::Db(e) if crate::reader::is_stale_file_error(e))
    }

    pub(crate) fn codec(what: &'static str, source: codec::Error) -> Self {
        Error::Codec { what, source }
    }

    pub(crate) fn trie<E: std::fmt::Debug>(block: H256, e: E) -> Self {
        Error::Trie {
            block,
            message: format!("{e:?}"),
        }
    }
}
