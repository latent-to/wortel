//! Live smoke test against a real node database.
//!
//! Usage: smoke <db-path> [block-number] [mode]
//!
//! Opens the DB (readonly by default), prints chain meta, then reads a few
//! well-known subtensor storage items at the chosen block (best by default).
//! Output is `name=value` lines so it can be diffed against RPC responses.

use codec::Decode;
use wortel::{OpenMode, Wortel, keys};

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn main() {
    let mut args = std::env::args().skip(1);
    let db_path = args
        .next()
        .expect("usage: smoke <db-path> [block-number] [mode]");
    let block_number: Option<u32> = args
        .next()
        .filter(|s| s != "-")
        .map(|s| s.parse().expect("bad block number"));
    let mode = match args.next().as_deref() {
        None | Some("readonly") => OpenMode::ReadOnly,
        Some("secondary") => OpenMode::Secondary {
            secondary_path: std::env::temp_dir()
                .join(format!("wortel-smoke-{}", std::process::id())),
        },
        Some("primary") => OpenMode::Primary,
        Some(other) => panic!("unknown mode {other}"),
    };
    let secondary = matches!(mode, OpenMode::Secondary { .. });

    let t0 = std::time::Instant::now();
    eprintln!("opening db (pid {})...", std::process::id());
    let r = Wortel::open(&db_path, mode).expect("open failed");
    eprintln!("open done in {} ms", t0.elapsed().as_millis());
    println!("open_ms={}", t0.elapsed().as_millis());

    let genesis = r.genesis_hash().unwrap().expect("no genesis in meta");
    println!("genesis=0x{}", hex(genesis.as_bytes()));
    let (best_n, best_h) = r.best_block().unwrap().expect("no best block");
    println!("best_number={best_n}");
    println!("best_hash=0x{}", hex(best_h.as_bytes()));
    let (fin_n, _) = r.finalized_block().unwrap().expect("no finalized block");
    println!("finalized_number={fin_n}");

    let (number, hash) = match block_number {
        Some(n) => (n, r.block_hash(n).unwrap().expect("block not found")),
        None => (best_n, best_h),
    };
    println!("block_number={number}");
    println!("block_hash=0x{}", hex(hash.as_bytes()));

    let header = r.header(&hash).unwrap().expect("no header");
    println!("state_root=0x{}", hex(header.state_root.as_bytes()));
    let exts = r.extrinsics(&hash).unwrap().expect("no body");
    println!("extrinsic_count={}", exts.len());
    println!("extrinsic0=0x{}", hex(&exts[0]));

    // Timestamp.Now — set in every block, decodes as u64 milliseconds.
    let t = std::time::Instant::now();
    let now_raw = r
        .get_storage(&hash, &keys::value_key("Timestamp", "Now"))
        .unwrap()
        .expect("Timestamp.Now missing");
    let now = u64::decode(&mut &now_raw[..]).unwrap();
    println!("timestamp_now={now}");
    println!("timestamp_now_hex=0x{}", hex(&now_raw));
    println!("timestamp_now_key=0x{}", hex(&keys::value_key("Timestamp", "Now")));
    println!("point_read_us={}", t.elapsed().as_micros());

    // SubtensorModule.TotalNetworks — plain value.
    let tn_raw = r
        .get_storage(
            &hash,
            &keys::value_key("SubtensorModule", "TotalNetworks"),
        )
        .unwrap()
        .expect("TotalNetworks missing");
    println!("total_networks_hex=0x{}", hex(&tn_raw));

    // SubtensorModule.Tempo — full map in one traversal (what query_map does
    // with N+1 RPC round-trips).
    let t = std::time::Instant::now();
    let tempo_prefix = keys::storage_prefix("SubtensorModule", "Tempo");
    let pairs = r.storage_pairs(&hash, &tempo_prefix, None, None).unwrap();
    println!("tempo_map_len={}", pairs.len());
    println!("tempo_map_us={}", t.elapsed().as_micros());
    for (k, v) in pairs.iter().take(3) {
        // Subtensor keys netuid maps with the Identity hasher:
        // prefix(32) ++ netuid_le(2).
        let netuid = u16::from_le_bytes([k[k.len() - 2], k[k.len() - 1]]);
        let tempo = u16::decode(&mut &v[..]).unwrap();
        println!("tempo[{netuid}]={tempo}");
        println!("tempo_kv=0x{}:0x{}", hex(k), hex(v));
    }

    // A bigger map, timed: SubnetworkN.
    let t = std::time::Instant::now();
    let n_prefix = keys::storage_prefix("SubtensorModule", "SubnetworkN");
    let n_pairs = r.storage_pairs(&hash, &n_prefix, None, None).unwrap();
    println!("subnetwork_n_len={}", n_pairs.len());
    println!("subnetwork_n_us={}", t.elapsed().as_micros());

    // catch_up smoke (secondary mode only): best should be monotonic.
    if secondary {
        r.try_catch_up_with_primary().unwrap();
        let (best2, _) = r.best_block().unwrap().unwrap();
        println!("best_after_catchup={best2}");
        assert!(best2 >= best_n);
    }
}
