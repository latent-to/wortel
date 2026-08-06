# wortel

Direct RocksDB reader for a subtensor (substrate) **archive node's** database.
Replaces the websocket round-trips substrate WS makes for storage queries with
local reads of the node's own DB — same keys in, same SCALE bytes out, so the
existing key construction and `scalecodec` decoding on the Python side work
unchanged.

Pinned to the exact polkadot-sdk revision subtensor builds against
(`RaoFoundation/polkadot-sdk @ a08c7b0f0b64`), so the trie codec, column
layout, and key schemes are correct by construction. When subtensor bumps its
revision, bump the `rev` in `Cargo.toml` and re-check `src/columns.rs` against
that revision's `sc-client-db`.

## What it maps to

| RPC used by substrate                                 | wortel                                               |
|-------------------------------------------------------|------------------------------------------------------|
| `state_getStorage`                                    | `get_storage(block_hash, key)`                       |
| batched storage queries (`query_multi`)               | `get_storage_many(block_hash, keys)`                 |
| `state_getKeysPaged` + N × `getStorage` (`query_map`) | `storage_pairs` / `iter_prefix` — one trie traversal |
| `chain_getHeader`                                     | `header(hash)` (raw SCALE)                           |
| `chain_getBlock` extrinsics                           | `extrinsics(hash)` / `body(hash)`                    |
| `chain_getBlockHash(n)`                               | `block_hash(n)`                                      |
| best / finalized head                                 | `best_block()` / `finalized_block()`                 |

Not covered: `runtime_call` (e.g. `StakeInfoRuntimeApi.get_stake_info_for_coldkeys`)
— those execute wasm and stay on the RPC path.

## Node requirements

- `--database rocksdb` (the default). ParityDB has no safe concurrent-reader story.
- `--state-pruning archive` (or the legacy `--pruning=archive`) — plain
  `archive`, not `archive-canonical`, or you'll have headers without
  historical state. A block whose state is missing raises a distinct
  `MissingState` error rather than "not found".
- DB path is the node's `.../chains/<chain-id>/db/full` directory.

## Open modes (tested against a live finney archive node, 4.2 TB, 143k SSTs)

**`readonly` (default).** Read-only open of the current on-disk snapshot.
Safe next to the running node; opens in ~20 s (~70 s with a cold OS page
cache) at a few hundred MB RSS. Frozen at open
time: later blocks and the un-flushed memtable are invisible, so the visible
head lags the node (make a new Reader to refresh). If the node's compaction
deletes an SST under you, calls raise a stale-snapshot error (`OSError` in
Python) — recreate the Reader and resume (open itself retries this
automatically). This is the right mode for backfills and historical reads.

**`secondary`.** A RocksDB secondary instance can follow the live node via
`catch_up()` (replays the node's WAL, so it also sees the memtable). Two
caveats, both hit in testing: RocksDB requires `max_open_files=-1` here, and
open replays the primary's entire MANIFEST edit log — after 5 weeks of node
uptime that log was 667 MB and the replay needed >20 GB of RAM (OOM).
Restarting the node rolls the MANIFEST to a compact snapshot, after which
secondary mode is viable until it grows again. Prefer `readonly` unless you
truly need sub-minute freshness.

**`primary`.** Read-write open; only for a stopped node or a snapshot copy.

wortel raises the process fd soft limit to the hard limit on open — an archive
DB holds >100k SST files.

## Benchmarks

Two runnable benchmarks live in `examples/`:

- `bench.rs` — times every read path against a node DB:
  `cargo run --release --example bench -- <db-path> [block] [mode]`
- `bench_vs_rpc.py` — races wortel against the node's own RPC on a large map
  and verifies a random sample of values byte-for-byte (needs the wheel
  installed and the RPC reachable).

Output of `bench` on the live finney archive node (4.2 TB, 143k SSTs, host
NVMe), historical block 8,700,000 with a **fully cold OS page cache** — the
worst case:

```
open                             72.2s
block                       8700000 (best 8785885)
block_hash(n)                    7.6µs
header                           1.7ms
extrinsics ( 15)                2.3ms
point read, cold                 9.7ms
point read, warm (avg/1000)     23.3µs
  (Timestamp.Now = 1784996196001)
Tempo map (129 pairs)          12.2ms
get_storage_many (129)          3.9ms
TotalHotkeyAlpha, cold           69.1s (47110 pairs)
TotalHotkeyAlpha, warm            1.4s
TotalHotkeyAlpha, block n-1       1.4s (47110 pairs)
same, paged x10k                  1.3s
```

The two ~70 s lines are disk-bound: a first-ever traversal random-reads
across the whole state column. Every subsequent traversal — including at a
*different, adjacent block*, which is the incremental-ETL steady state — runs
from cache in ~1.4 s. An ETL walking consecutive blocks pays the cold cost
once per process, not per block.

Output of `bench_vs_rpc.py` on the same block (page cache warm, node RPC
local):

```
open_s=26.1
block=8700000 hash=0x2c9ff31bb0d78a38dd35966193a74a192aeeaad2499b74fe25e90944f4b019e8
wortel_pairs=47110 wortel_cold_s=1.98
wortel_warm_s=1.42
wortel_adjacent_block_s=1.40 pairs=47110
rpc_keys=47110 rpc_keys_s=2.27
sample=200 mismatches=0 rpc_per_get_ms=1.4
rpc_projected_full_s=68 (keys paging + one getStorage per key)
speedup_vs_rpc=35x
bench: all values byte-identical
```

`rpc_projected_full_s` is what `query_map` amounts to (keys paged + one
`getStorage` per key) against the node's *local* RPC; over a remote websocket
the gap widens further. wortel returned the identical 47,110 values ~35×
faster, in one trie traversal.

## Python

```bash
pip install maturin
maturin develop --release        # or: maturin build --release → wheel in target/wheels
```

```python
import wortel

r = wortel.Reader("/data/subtensor/chains/bittensor/db/full")  # readonly
num, head = r.best_block()

# Point read: SubtensorModule.Tempo[3]
key = wortel.storage_prefix("SubtensorModule", "Tempo") + wortel.twox_64_concat((3).to_bytes(2, "little"))
raw = r.get_storage(head, key)            # SCALE bytes, decode with scalecodec as today

# Full map, one traversal (what query_map does with N+1 RPCs):
for key, value in r.iter_prefix(head, wortel.storage_prefix("SubtensorModule", "Tempo")):
    ...

# Refresh view of the chain: readonly mode → new Reader;
# secondary mode → r.catch_up()
r = wortel.Reader(db_path)
```

Keys can equally come from async-substrate-interface's metadata-driven
`create_storage_key` — wortel only sees bytes.

## Rust

```rust
use wortel::{Wortel, OpenMode};

let r = Wortel::open("/data/.../db/full", OpenMode::ReadOnly)?;
let (n, head) = r.best_block()?.unwrap();
let pairs = r.storage_pairs(&head, &wortel::keys::storage_prefix("SubtensorModule", "Tempo"), None, None)?;
```

## Tests

`cargo test` builds a synthetic client DB (real LayoutV1 trie via the same
sp-trie the node uses, plus headers/lookup/meta/body columns) in a temp
RocksDB and exercises every read path against it.
