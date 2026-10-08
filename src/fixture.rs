//! Builder for a synthetic substrate client database (`fixture` feature,
//! for tests): a real LayoutV1 trie (via the same sp-trie the node uses),
//! header, lookup keys, meta entries and body, written into a RocksDB with
//! the sc-client-db column layout. One block, number 42.

use codec::{Decode, Encode};
use kvdb_rocksdb::{Database, DatabaseConfig};
use sp_core::{Blake2Hasher, H256};
use sp_runtime::{OpaqueExtrinsic, generic::Digest};
use sp_trie::{PrefixedMemoryDB, TrieMut, trie_types::TrieDBMutBuilderV1};
use crate::Header;

pub const NUM_COLUMNS: u32 = 13;
pub const COL_META: u32 = 0;
pub const COL_STATE: u32 = 1;
pub const COL_KEY_LOOKUP: u32 = 3;
pub const COL_HEADER: u32 = 4;
pub const COL_BODY: u32 = 5;

pub fn storage_entries() -> Vec<(Vec<u8>, Vec<u8>)> {
    let prefix = crate::keys::storage_prefix("SubtensorModule", "Tempo");
    let mut entries: Vec<(Vec<u8>, Vec<u8>)> = (0u16..50)
        .map(|netuid| {
            let mut key = prefix.to_vec();
            key.extend(crate::keys::twox_64_concat(&netuid.encode()));
            (key, (100u16 + netuid).encode())
        })
        .collect();
    // A value > 32 bytes so the V1 hashed-value node path is exercised too.
    entries.push((
        crate::keys::storage_prefix("System", "Code").to_vec(),
        vec![0xAB; 100],
    ));
    entries
}

/// Move everything out of the WAL into SST files, as a node's compaction
/// would have: a readonly reader then holds a snapshot of files that can be
/// deleted under it (the stale-snapshot case tests want to provoke).
fn flush_to_sst(path: &std::path::Path) {
    let names: Vec<String> = (0..NUM_COLUMNS).map(|i| format!("col{i}")).collect();
    let db = rocksdb::DB::open_cf(&rocksdb::Options::default(), path, &names).unwrap();
    for name in &names {
        db.flush_cf(db.cf_handle(name).unwrap()).unwrap();
    }
}

/// Returns the block hash and the per-extrinsic SCALE bytes.
pub const BLOCK_NUMBER: u32 = 42;

pub fn build_db(path: &std::path::Path) -> (H256, Vec<Vec<u8>>) {
    let mut mdb = PrefixedMemoryDB::<Blake2Hasher>::default();
    let mut root = H256::default();
    {
        let mut trie = TrieDBMutBuilderV1::new(&mut mdb, &mut root).build();
        for (k, v) in storage_entries() {
            trie.insert(&k, &v).unwrap();
        }
    }

    let extrinsics: Vec<OpaqueExtrinsic> = [vec![1u8, 2, 3], vec![9u8; 40]]
        .into_iter()
        .map(|payload| {
            let encoded = payload.encode();
            OpaqueExtrinsic::decode(&mut &encoded[..]).unwrap()
        })
        .collect();

    let header = Header {
        parent_hash: H256::repeat_byte(7),
        number: BLOCK_NUMBER,
        state_root: root,
        extrinsics_root: H256::repeat_byte(8),
        digest: Digest::default(),
    };
    let hash = header.hash();
    let lookup_key: Vec<u8> = [&BLOCK_NUMBER.to_be_bytes()[..], hash.as_bytes()].concat();

    let mut config = DatabaseConfig::with_columns(NUM_COLUMNS);
    config.create_if_missing = true;
    let db = Database::open(&config, path).unwrap();
    let mut tx = db.transaction();
    for (key, (value, rc)) in mdb.drain() {
        assert!(rc > 0);
        tx.put(COL_STATE, key.as_ref(), &value);
    }
    tx.put(COL_KEY_LOOKUP, hash.as_bytes(), &lookup_key);
    tx.put(COL_KEY_LOOKUP, &BLOCK_NUMBER.to_be_bytes(), &lookup_key);
    tx.put(COL_HEADER, &lookup_key, &header.encode());
    tx.put(COL_BODY, &lookup_key, &extrinsics.encode());
    tx.put(COL_META, b"best", &lookup_key);
    tx.put(COL_META, b"final", &lookup_key);
    tx.put(COL_META, b"gen", &H256::repeat_byte(1).encode());
    db.write(tx).unwrap();
    drop(db);
    flush_to_sst(path);

    (hash, extrinsics.iter().map(|e| e.encode()).collect())
}
