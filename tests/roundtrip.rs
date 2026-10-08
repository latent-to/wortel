//! End-to-end test against a synthetic substrate client database: writes a
//! real LayoutV1 trie (via the same sp-trie the node uses), header, lookup
//! keys, meta entries and body into a temp RocksDB with the sc-client-db
//! column layout, then reads everything back through `Wortel`.

use codec::Encode;
use sp_core::H256;
use wortel::{OpenMode, Wortel};

use wortel::fixture::build_db;

#[test]
fn reads_synthetic_substrate_db() {
    let dir = tempfile::tempdir().unwrap();
    let (hash, expected_exts) = build_db(dir.path());

    let reader = Wortel::open(dir.path(), OpenMode::Primary).unwrap();

    // Meta and lookups.
    assert_eq!(reader.genesis_hash().unwrap(), Some(H256::repeat_byte(1)));
    assert_eq!(reader.best_block().unwrap(), Some((42, hash)));
    assert_eq!(reader.finalized_block().unwrap(), Some((42, hash)));
    assert_eq!(reader.block_hash(42).unwrap(), Some(hash));
    assert_eq!(reader.block_hash(41).unwrap(), None);
    assert_eq!(reader.block_number(&hash).unwrap(), Some(42));

    // Header and body.
    let header = reader.header(&hash).unwrap().unwrap();
    assert_eq!(header.number, 42);
    assert_eq!(reader.extrinsics(&hash).unwrap().unwrap(), expected_exts);
    assert_eq!(reader.header(&H256::repeat_byte(9)).unwrap(), None);

    // Point reads through the trie.
    let tempo_prefix = wortel::keys::storage_prefix("SubtensorModule", "Tempo");
    let mut key3 = tempo_prefix.to_vec();
    key3.extend(wortel::keys::twox_64_concat(&3u16.encode()));
    assert_eq!(
        reader.get_storage(&hash, &key3).unwrap(),
        Some(103u16.encode())
    );
    assert_eq!(reader.get_storage(&hash, b"missing").unwrap(), None);

    // Large (V1 hashed) value.
    let code_key = wortel::keys::storage_prefix("System", "Code");
    assert_eq!(
        reader.get_storage(&hash, &code_key).unwrap(),
        Some(vec![0xAB; 100])
    );

    // Batched reads.
    let many = reader
        .get_storage_many(&hash, &[key3.clone(), b"missing".to_vec()])
        .unwrap();
    assert_eq!(many, vec![Some(103u16.encode()), None]);

    // Prefix iteration: full map in one traversal.
    let pairs = reader
        .storage_pairs(&hash, &tempo_prefix, None, None)
        .unwrap();
    assert_eq!(pairs.len(), 50);
    assert!(pairs.iter().all(|(k, _)| k.starts_with(&tempo_prefix)));
    let mut sorted = pairs.clone();
    sorted.sort();
    assert_eq!(pairs, sorted, "iteration must be key-ordered");

    // Pagination: pages concatenate to the full set, no dupes or gaps.
    let mut paged = Vec::new();
    let mut start_after: Option<Vec<u8>> = None;
    loop {
        let page = reader
            .storage_pairs(&hash, &tempo_prefix, start_after.as_deref(), Some(7))
            .unwrap();
        if page.is_empty() {
            break;
        }
        start_after = Some(page.last().unwrap().0.clone());
        paged.extend(page);
    }
    assert_eq!(paged, pairs);

    // Unknown block and value-level errors.
    assert!(reader.get_storage(&H256::repeat_byte(9), &key3).is_err());

    // ReadOnly mode sees the same data (the production mode for backfills).
    drop(reader);
    let ro = Wortel::open(dir.path(), OpenMode::ReadOnly).unwrap();
    assert_eq!(ro.best_block().unwrap(), Some((42, hash)));
    assert_eq!(
        ro.get_storage(&hash, &key3).unwrap(),
        Some(103u16.encode())
    );
    assert_eq!(
        ro.storage_pairs(&hash, &tempo_prefix, None, None).unwrap(),
        pairs
    );
}
