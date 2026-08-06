//! Benchmark against a real node database.
//!
//! Usage: bench <db-path> [block-number] [mode]
//!
//! Times every read path wortel offers: flat-column lookups (headers, bodies,
//! block hashes), trie point reads (cold and warm), batched reads, and full
//! storage-map traversals including the adjacent-block case that dominates
//! incremental ETL work. Mode defaults to readonly.

use codec::Decode;
use std::time::{Duration, Instant};
use wortel::{OpenMode, Wortel, keys};

fn fmt(d: Duration) -> String {
    let us = d.as_micros();
    if us < 1_000 {
        format!("{:.1} µs", us as f64)
    } else if us < 1_000_000 {
        format!("{:.1} ms", us as f64 / 1_000.0)
    } else {
        format!("{:.1} s", us as f64 / 1_000_000.0)
    }
}

fn row(label: &str, d: Duration, note: &str) {
    println!("{label:<30}{:>10}  {note}", fmt(d));
}

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
    row("open", t.elapsed(), "");

    let (best_n, best_h) = r.best_block().unwrap().expect("no best block");
    let (number, hash) = match block_number {
        Some(n) => (n, r.block_hash(n).unwrap().expect("block not found")),
        None => (best_n, best_h),
    };
    println!("{:<30}{:>10}  (best {best_n})", "block", number);

    // Flat columns: no trie involved.
    let t = Instant::now();
    let _ = r.block_hash(number).unwrap().unwrap();
    row("block_hash(n)", t.elapsed(), "");

    let t = Instant::now();
    let _header = r.header(&hash).unwrap().unwrap();
    row("header", t.elapsed(), "");

    let t = Instant::now();
    let exts = r.extrinsics(&hash).unwrap().unwrap();
    row("extrinsics", t.elapsed(), &format!("{} extrinsics", exts.len()));

    // Trie point reads. First one pays the root-descent and cache misses.
    let ts_key = keys::value_key("Timestamp", "Now");
    let t = Instant::now();
    let now_raw = r.get_storage(&hash, &ts_key).unwrap().expect("Timestamp.Now");
    row("point read, cold", t.elapsed(), "");
    let t = Instant::now();
    for _ in 0..1000 {
        r.get_storage(&hash, &ts_key).unwrap();
    }
    let now = u64::decode(&mut &now_raw[..]).unwrap();
    row(
        "point read, warm",
        t.elapsed() / 1000,
        &format!("avg of 1000; Timestamp.Now = {now}"),
    );

    // Small map: Tempo, one entry per subnet.
    let tempo_prefix = keys::storage_prefix("SubtensorModule", "Tempo");
    let t = Instant::now();
    let tempo = r.storage_pairs(&hash, &tempo_prefix, None, None).unwrap();
    row("Tempo map", t.elapsed(), &format!("{} pairs", tempo.len()));

    // Batched point reads over the keys we just learned.
    let tempo_keys: Vec<Vec<u8>> = tempo.iter().map(|(k, _)| k.clone()).collect();
    let t = Instant::now();
    let many = r.get_storage_many(&hash, &tempo_keys).unwrap();
    assert!(many.iter().all(Option::is_some));
    row(
        "get_storage_many",
        t.elapsed(),
        &format!("{} keys", tempo_keys.len()),
    );

    // Large map: TotalHotkeyAlpha (tens of thousands of entries).
    let big_prefix = keys::storage_prefix("SubtensorModule", "TotalHotkeyAlpha");
    let t = Instant::now();
    let big = r.storage_pairs(&hash, &big_prefix, None, None).unwrap();
    row(
        "TotalHotkeyAlpha, cold",
        t.elapsed(),
        &format!("{} pairs", big.len()),
    );

    let t = Instant::now();
    let again = r.storage_pairs(&hash, &big_prefix, None, None).unwrap();
    assert_eq!(again.len(), big.len());
    row("TotalHotkeyAlpha, warm", t.elapsed(), "");

    // Adjacent block: the incremental-ETL steady state. Consecutive blocks
    // share almost every trie node, so this runs nearly all-warm.
    if let Some(prev_hash) = r.block_hash(number.wrapping_sub(1)).unwrap() {
        let t = Instant::now();
        let prev = r.storage_pairs(&prev_hash, &big_prefix, None, None).unwrap();
        row(
            "TotalHotkeyAlpha, block n-1",
            t.elapsed(),
            &format!("{} pairs", prev.len()),
        );
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
    row("same, paged x10k", t.elapsed(), "");
}
