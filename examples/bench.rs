//! Benchmark against a real node database.
//!
//! Usage: bench <db-path> [block-number] [mode]
//!
//! Times every read path wortel offers: flat-column lookups (headers, bodies,
//! block hashes), trie point reads (cold and warm), batched reads, and full
//! storage-map traversals including the adjacent-block case that dominates
//! incremental ETL work. Mode defaults to readonly.

use codec::Decode;
use std::time::Instant;
use wortel::{OpenMode, Wortel, keys};

fn main() {
    let mut args = std::env::args().skip(1);
    let db_path = args.next().expect("usage: bench <db-path> [block-number] [mode]");
    let block_number: Option<u32> = args
        .next()
        .filter(|s| s != "-")
        .map(|s| s.parse().expect("bad block number"));
    let mode = match args.next().as_deref() {
        None | Some("readonly") => OpenMode::ReadOnly,
        Some("secondary") => OpenMode::Secondary {
            secondary_path: std::env::temp_dir()
                .join(format!("wortel-bench-{}", std::process::id())),
        },
        Some("primary") => OpenMode::Primary,
        Some(other) => panic!("unknown mode {other}"),
    };

    let t = Instant::now();
    let r = Wortel::open(&db_path, mode).expect("open failed");
    println!("open                        {:>10.1?}", t.elapsed());

    let (best_n, best_h) = r.best_block().unwrap().expect("no best block");
    let (number, hash) = match block_number {
        Some(n) => (n, r.block_hash(n).unwrap().expect("block not found")),
        None => (best_n, best_h),
    };
    println!("block                       {number} (best {best_n})");

    // Flat columns: no trie involved.
    let t = Instant::now();
    let _ = r.block_hash(number).unwrap().unwrap();
    println!("block_hash(n)               {:>10.1?}", t.elapsed());

    let t = Instant::now();
    let header = r.header(&hash).unwrap().unwrap();
    println!("header                      {:>10.1?}", t.elapsed());

    let t = Instant::now();
    let exts = r.extrinsics(&hash).unwrap().unwrap();
    println!("extrinsics ({:>3})           {:>10.1?}", exts.len(), t.elapsed());

    // Trie point reads. First one pays the root-descent and cache misses.
    let ts_key = keys::value_key("Timestamp", "Now");
    let t = Instant::now();
    let now_raw = r.get_storage(&hash, &ts_key).unwrap().expect("Timestamp.Now");
    println!("point read, cold            {:>10.1?}", t.elapsed());
    let t = Instant::now();
    for _ in 0..1000 {
        r.get_storage(&hash, &ts_key).unwrap();
    }
    println!("point read, warm (avg/1000) {:>10.1?}", t.elapsed() / 1000);
    let now = u64::decode(&mut &now_raw[..]).unwrap();
    println!("  (Timestamp.Now = {now})");

    // Small map: Tempo, one entry per subnet.
    let tempo_prefix = keys::storage_prefix("SubtensorModule", "Tempo");
    let t = Instant::now();
    let tempo = r.storage_pairs(&hash, &tempo_prefix, None, None).unwrap();
    println!("Tempo map ({:>3} pairs)      {:>10.1?}", tempo.len(), t.elapsed());

    // Batched point reads over the keys we just learned.
    let tempo_keys: Vec<Vec<u8>> = tempo.iter().map(|(k, _)| k.clone()).collect();
    let t = Instant::now();
    let many = r.get_storage_many(&hash, &tempo_keys).unwrap();
    assert!(many.iter().all(Option::is_some));
    println!("get_storage_many ({:>3})     {:>10.1?}", tempo_keys.len(), t.elapsed());

    // Large map: TotalHotkeyAlpha (tens of thousands of entries).
    let big_prefix = keys::storage_prefix("SubtensorModule", "TotalHotkeyAlpha");
    let t = Instant::now();
    let big = r.storage_pairs(&hash, &big_prefix, None, None).unwrap();
    let cold = t.elapsed();
    println!("TotalHotkeyAlpha, cold      {:>10.1?} ({} pairs)", cold, big.len());

    let t = Instant::now();
    let again = r.storage_pairs(&hash, &big_prefix, None, None).unwrap();
    assert_eq!(again.len(), big.len());
    println!("TotalHotkeyAlpha, warm      {:>10.1?}", t.elapsed());

    // Adjacent block: the incremental-ETL steady state. Consecutive blocks
    // share almost every trie node, so this runs nearly all-warm.
    if let Some(prev_hash) = r.block_hash(number.wrapping_sub(1)).unwrap() {
        let t = Instant::now();
        let prev = r.storage_pairs(&prev_hash, &big_prefix, None, None).unwrap();
        println!("TotalHotkeyAlpha, block n-1 {:>10.1?} ({} pairs)", t.elapsed(), prev.len());
    }

    // Paged iteration (how iter_prefix consumes big maps from Python).
    let t = Instant::now();
    let mut fetched = 0usize;
    let mut start_after: Option<Vec<u8>> = None;
    loop {
        let page = r
            .storage_pairs(&hash, &big_prefix, start_after.as_deref(), Some(10_000))
            .unwrap();
        if page.is_empty() {
            break;
        }
        fetched += page.len();
        start_after = Some(page.last().unwrap().0.clone());
    }
    assert_eq!(fetched, big.len());
    println!("same, paged x10k            {:>10.1?}", t.elapsed());

    let _ = header;
}
