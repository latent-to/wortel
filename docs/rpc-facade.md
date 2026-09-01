# wortel RPC facade — design

> **Status**: implemented as `wortel.serve` (`python/wortel/serve/`), tests in
> `tests/python/`, conformance script in `examples/facade_conformance.py`.
> Tested 2026-08-27 against the live mainnet archive node — see **Measured
> results** below. Not yet deployed for ETL.

## Measured results (mainnet archive node, 4.2 TB DB)

Conformance: 20 blocks sampled across the whole chain (1 → 8.49M), every
locally-served method byte-compared against the node RPC — **0 diffs**. A real
`async-substrate-interface` client (the taoapp fork) ran the ETL operation mix
(`query`, `query_map`, `runtime_call`, `get_events`, `get_block`) against both
endpoints — identical results, including metadata fetch through pass-through.
A Reader reopen (~75 s) overlapped the test load with no stalls or errors
(requires the `py.detach` fix in `Reader::new`; without it the open holds the
GIL and freezes every connection).

Big-map benchmark, `TotalHotkeyAlpha` (48,093 entries) via asi `query_map`
(`fully_exhaust=True`), all on localhost — representative for the planned
same-kubernetes-cluster deployment, where RTT is negligible:

| path                                        | cold  | warm   |
|---------------------------------------------|-------|--------|
| node RPC (asi's batched protocol)           | 8.2 s | 7.4 s  |
| facade, same paged protocol                 | 63 s  | 11 s   |
| **facade via `state_getPairs`** (asi patch) | ~49 s | **2.1 s** |

Identical decoded values (checksummed) on every path.

**How to read this.** The README's 68 s RPC figure describes the *old* N+1
`query_map`; the taoapp asi fork already batches values through
`state_queryStorageAt`, so speaking that same paged protocol the facade is
slightly slower than the node (Python/JSON overhead). The win comes from
changing the protocol: the facade serves `state_getPairs` — the whole map in
one request and one trie traversal — which nodes refuse (`error 4003, RPC
call is unsafe`). With the asi-fork patch (uncommitted, in
`~/Git/async-substrate-interface`: `query_map(fully_exhaust=True)` tries
`state_getPairs` first, caches endpoint support, falls back to the paged
path on plain nodes), big maps run **3.6× faster than the node RPC**. The
cold column is the once-per-facade-process first traversal of that map;
block-walking ETL then stays warm.

Secondary benefits unchanged: node isolation (heavy reads bypass the node's
RPC workers), consistent-snapshot reads, explicit staleness.

**Tuning**: on a 4.2 TB DB a reopen takes ~40–75 s, so the 60 s default
refresh interval reopens nearly back-to-back; run mainnet with
`--refresh-interval 300` (head lag up to ~6 min) or add on-demand refresh.

A websocket JSON-RPC server that runs sidecar to the subtensor archive node.
It answers storage/chain reads from a local wortel `Reader` and forwards every
other method to the node's own RPC. ETL cron jobs keep
`async-substrate-interface` (asi) unchanged and switch only their endpoint URL
(`ARCHIVE_ENDPOINT` in `etl/python/constants.py`).

## Goals / non-goals

**Goals**

- Serve the storage-read hot path (`query`, `query_map`, `query_multi`,
  block hashes, headers, bodies) from RocksDB directly.
- Full asi compatibility: a cron job must not know it talks to the facade.
- Read-only. `OpenMode::ReadOnly` only; never `secondary`, never `primary`.

**Non-goals**

- `websocket_insertion.py` and other head-subscription services. They stay on
  the real node RPC. (Subscriptions still *work* through pass-through, but the
  facade adds nothing for them.)
- Transaction submission guarantees. `author_*` passes through untouched.
- Decoding. The facade moves SCALE bytes; all decoding stays client-side.

## Placement: inside the wortel repo

The facade ships in this repo as a Python module (`wortel.serve`) in the same
wheel, via maturin's mixed Rust/Python layout (`python-source`), with an
optional dependency extra:

```
pip install 'wortel[serve]'          # adds: websockets
python -m wortel.serve --db /data/subtensor/chains/bittensor/db/full \
    --upstream ws://127.0.0.1:9944 --listen 0.0.0.0:9955
```

Why here and not elsewhere:

- **Not in asi**: asi is a client library. Client-side routing (wortel
  transport for storage, ws for the rest) only helps when the client runs on
  the node host — the ETL does not. A server does not belong in a client
  package.
- **Not in taoapp**: the facade is subtensor-generic, not taoapp-specific,
  and belongs next to the reader whose polkadot-sdk pin it depends on. One
  repo, one wheel, one sidecar artifact.
- **Python, not a Rust binary**: all reader methods release the GIL
  (`py.detach`), so an asyncio server scales; the JSON-RPC layer is ~500
  lines of Python vs a new Rust server dependency tree. Revisit only if the
  facade itself becomes a bottleneck.

## Architecture

```
 ETL cron job (asi, unchanged)
        │  wss
        ▼
 ┌─────────────────────────────┐        ┌──────────────────┐
 │ facade (wortel.serve)       │  ws    │ subtensor node   │
 │  ├─ local: storage/chain    │───────▶│ RPC :9944        │
 │  │   reads via Reader       │ pass-  │                  │
 │  └─ Reader(readonly) ◀──────│ through│ RocksDB db/full  │
 │       ▲ periodic reopen     │        └────────┬─────────┘
 │       └────────── same host, ro mount ────────┘
 └─────────────────────────────┘
```

## Method routing

Methods asi actually calls (surveyed from the asi source):

| method                                        | route | wortel call |
|-----------------------------------------------|-------|-------------|
| `state_getStorage` / `state_getStorageAt`     | local | `get_storage(at, key)` |
| `state_queryStorageAt`                        | local | `get_storage_many(at, keys)` → `[{block, changes}]` |
| `state_getKeys` / `state_getKeysPaged`        | local | `storage_pairs(at, prefix, start_after, limit)`, keys only |
| `chain_getBlockHash` / `chain_getHead`        | local | `block_hash(n)` / `best_block()` |
| `chain_getFinalizedHead`                      | local | `finalized_block()` |
| `chain_getHeader`                             | local | `header(hash)` + SCALE→JSON (see below) |
| `chain_getBlock`                              | local | `header` + `extrinsics` composed |
| `state_call` (runtime calls)                  | pass  | — (wasm execution; wortel does not cover it) |
| `state_getMetadata`, `state_getRuntimeVersion`| pass  | — |
| `system_*`, `rpc_methods`                     | pass  | — |
| `author_*`                                    | pass  | — |
| all `*_subscribe*` / `*_unsubscribe*`         | pass  | — |
| anything unrecognized                         | pass  | — |

Default-pass-through means an asi upgrade that adds a method cannot break the
facade — new methods are slower, not wrong.

**Header JSON.** `chain_getHeader` returns JSON, not bytes. The substrate
header layout is fixed: `parent_hash(32) ‖ compact(number) ‖ state_root(32) ‖
extrinsics_root(32) ‖ digest(Vec<log-bytes>)`. The facade hand-decodes exactly
this (digest logs stay as opaque hex), matching node output byte-for-byte in
tests. `chain_getBlock` composes the same header JSON with
`extrinsics(hash)` as hex. If this ever drifts from the node's shape, the
escape hatch is one line: move both methods to the pass column.

**Parameter handling.** Hex `0x…` string params decode to bytes; results
encode back to hex. `chain_getBlockHash` accepts int, hex-int, or null.
A null/omitted `at` resolves to the **snapshot's** best block (see
consistency, below).

## Snapshot consistency and reader lifecycle

A `readonly` Reader is frozen at open time. Rules:

1. **One consistent view.** Head methods (`chain_getHead`,
   `chain_getFinalizedHead`, `chain_getHeader` with no hash) answer from the
   *snapshot*, not the node. A client that asks "head?" then reads storage at
   that head always succeeds. The visible head lags the node by up to one
   refresh interval — acceptable for cron jobs, and the reason
   `websocket_insertion` is out of scope.
2. **Background refresh.** A task reopens a new `Reader` every
   `--refresh-interval` (default 60 s; open is ~20 s warm) and swaps it
   atomically. In-flight calls keep the old object; Python GC drops it.
3. **Stale snapshot.** Node compaction can delete an SST under the snapshot →
   `OSError`. Handler: trigger refresh, retry the call once on the new
   Reader, then error.
4. **Unknown/newer block hash.** A hash the snapshot lacks (client learned it
   out-of-band): trigger refresh and retry once; if still missing, forward
   that one call upstream. Correctness is preserved either way.
5. **`MissingState`** (header exists, state pruned) means the node is not a
   plain archive node → surface loudly; do not fall back silently. Startup
   runs a self-check: read `Timestamp.Now` at an old block and fail fast with
   a clear message if pruning is wrong.

## Upstream pass-through

One upstream websocket per client connection (1:1). Request ids and
subscription ids then need no translation table — frames forward verbatim in
both directions. Cron-job connection counts are single-digit; pooling is a
non-problem. Upstream disconnect → close the client connection; asi's own
reconnect logic takes over (and re-fetches metadata itself).

## Concurrency

- `asyncio` + `websockets`. Local reads run via `asyncio.to_thread` — the
  bindings release the GIL, so traversals run truly parallel.
- Requests within one client connection execute concurrently (asi pipelines
  ids); responses return as they finish, per JSON-RPC.
- A single shared `Reader` (`#[pyclass(frozen)]`, thread-safe) serves all
  connections. First traversal of a big map per *process* is the one cold
  cost (~70 s worst case); the long-lived facade pays it once.

## Configuration

| flag / env                  | default | meaning |
|-----------------------------|---------|---------|
| `--db` / `WORTEL_DB`        | —       | node's `.../db/full` path |
| `--upstream` / `WORTEL_UPSTREAM` | `ws://127.0.0.1:9944` | node RPC |
| `--listen`                  | `0.0.0.0:9955` | bind address |
| `--refresh-interval`        | `60`    | seconds between Reader reopens |
| `--max-keys-page`           | `1000`  | cap for `state_getKeysPaged` count |
| `--log-level` / `LOG_LEVEL` | `INFO` | logging level; `DEBUG` also shows websockets probe traffic |

Node must run `--database rocksdb` and `--state-pruning archive` (plain, not
`archive-canonical`) — verify before rollout. See **Deployment** below.

## Deployment

**Image.** `Dockerfile` at the repo root: builder stage compiles the wheel
from source (rust + maturin, `CARGO_BUILD_JOBS` capped at 4 so building on
the node host leaves CPU for the node), runtime stage is `python:3.12-slim`
with the wheel + `websockets`. Self-contained — **no PyPI or crates.io
release is required**; push the image to the taoapp registry like any other
service. Publishing wortel publicly is optional and orthogonal.

**docker-compose** (the current archive-server layout):

```yaml
  wortel-facade:
    image: wortel-serve
    command: ["--db", "/db", "--upstream", "ws://subtensor-mainnet-archive:9944",
              "--listen", "0.0.0.0:9955", "--refresh-interval", "300"]
    volumes:
      - /mnt/HC_Volume_103568853/blockchain/chains/bittensor/db/full:/db:ro
    ports: ["9955:9955"]
    ulimits:
      nofile: { soft: 1048576, hard: 1048576 }
    restart: unless-stopped
```

**Kubernetes** — a true sidecar: a second container in the node's pod, sharing
the blockchain volume read-only (RocksDB readonly open needs the same
filesystem, so it must be the same pod — or a hostPath deployment pinned to
the node's k8s node):

```yaml
      - name: wortel-facade
        image: <registry>/wortel-serve:<tag>
        args: ["--db", "/data/blockchain/chains/bittensor/db/full",
               "--upstream", "ws://127.0.0.1:9944",
               "--listen", "0.0.0.0:9955", "--refresh-interval", "300"]
        ports: [{ containerPort: 9955 }]
        volumeMounts:
          - { name: blockchain-data, mountPath: /data/blockchain, readOnly: true }
        startupProbe:            # open takes ~40–75 s before it listens
          httpGet: { path: /livez, port: 9955 }
          periodSeconds: 5
          failureThreshold: 40
        readinessProbe:
          httpGet: { path: /readyz, port: 9955 }
          periodSeconds: 10
        livenessProbe:
          httpGet: { path: /livez, port: 9955 }
          periodSeconds: 15
          failureThreshold: 4
        resources:
          requests: { memory: 3Gi, cpu: "1" }
          limits: { memory: 4Gi, cpu: "2" }
```

Plus a `Service` (`wortel-facade`, port 9955); cron jobs then set
`ARCHIVE_ENDPOINT=ws://wortel-facade.<namespace>.svc:9955`. File-limit note:
modern containerd defaults `nofile` to 1048576 — confirm on the cluster, the
DB holds >100k SSTs.

**Probes.** Plain-HTTP on the websocket port. `GET /livez` is 200 whenever
the event loop answered — it does not touch RocksDB, so a stale snapshot
cannot restart the pod. `GET /readyz` is 200 when a Reader is open and can
resolve the best block (body e.g. `ok best_block=8936761`), 503 otherwise.
The listen socket only opens after the first Reader open, so startup is
waiting on `/livez` becoming reachable. Upstream node health is deliberately
NOT part of readiness — local reads keep working when the node RPC is down,
and an unready facade would not help pass-through callers either. The facade
keeps serving during its reader reopen, so `/readyz` does not flap on
refresh unless `best_block` itself fails.

**Sizing (measured on the mainnet DB, 143k SSTs).** Steady-state RSS ~2.4 GB
— RocksDB table metadata for >100k SSTs dominates, on top of the 512 MiB
block cache; the two-reader overlap during a refresh peaked at 2.6 GB.
Requests 3Gi / limit 4Gi gives headroom for chain growth (RSS scales with
SST count) and concurrent big-map responses (~15 MB each, transient). CPU:
map traversals release the GIL but hex/JSON encoding holds it, so the
process cannot use much more than 2 cores — request 1, limit 2; more buys
nothing without a second replica. Testnet (~2 TB) needs proportionally less;
keep the same limits unless memory is tight.

**Release sequencing** — what has to ship, in order:

1. **Nothing published**: build + push the sidecar image from this repo,
   deploy next to the node, run `examples/facade_conformance.py` against it.
2. **Endpoint flip**: change `ARCHIVE_ENDPOINT` for one cron job, then all.
   Works with the asi version ETL runs today — correctness needs no asi
   change.
3. **asi release** (the performance step): land the `state_getPairs` patch in
   the asi fork, release it (e.g. aio-substrate-interface 3.2.x), bump the
   ETL image's requirements. The speedup activates by itself — the patch
   probes the endpoint once per connection and falls back on plain nodes.

## Failure modes

| failure | behavior |
|---------|----------|
| upstream node RPC down | pass-through methods error; local reads keep working |
| DB path wrong / not rocksdb | fail fast at startup |
| stale snapshot (compaction) | auto refresh + one retry |
| wrong pruning mode | fail fast at startup (self-check) |
| facade down | cron job fails visibly; flip `ARCHIVE_ENDPOINT` back to the node/public WS — full rollback is an env change |

## Testing

1. **Unit**: the repo's fixture DB (`tests/make_fixture.rs`) exercises every
   local method; golden JSON for header/block shapes.
2. **Conformance**: against a live node, replay each local-route method and
   byte-compare facade vs node RPC (the `bench_vs_rpc.py` pattern) — headers,
   blocks, keys pages, `queryStorageAt` batches, `null`-param edge cases.
3. **Row diff**: run a job against the facade and against the plain WS
   endpoint for the same block; diff the produced rows. On testnet the WS
   baseline is `testnet-archive.sub.latent.to` itself, so this check rides
   along with step 4 for a sample of blocks/jobs.
4. **Testnet full ingestion** (the primary end-to-end validation): run the
   complete mainnet ETL job set — everything we currently pull for mainnet
   (extrinsics/events pipeline, metagraphs, stake info, emissions, subnets,
   etc.) — against a facade sidecar next to a testnet archive node (the node
   behind `testnet-archive.sub.latent.to`), from block 1 to head. What this
   exercises:
   - Every routing path the facade has: point reads and block bodies
     (extrinsics/events), the big-map traversals behind
     `query_map`/`query_multi` (`state_getKeysPaged` +
     `state_queryStorageAt` batches), and `state_call` pass-through
     (`runtime_call`-based jobs) — the full production request mix, across
     every historical digest shape and runtime version since testnet genesis.
   - Every runtime upgrade forces asi to re-fetch metadata via pass-through —
     validates the pass-through path under a long-running connection.
   - A block-1-to-head walk is sequential blocks — wortel's incremental
     steady state; also soaks the refresh/stale-snapshot handling for hours,
     with several jobs hitting the shared Reader concurrently.
   - At ~2 TB (vs ~5 TB mainnet) the testnet DB is the same order of
     magnitude — open times and read costs are reasonably representative.
   - Housekeeping: the db path uses the testnet chain id
     (`.../chains/<testnet-chain-id>/db/full`), and the node binary's
     polkadot-sdk rev is believed identical to wortel's pin — confirm once
     at deploy (`cargo tree`-vs-node-binary check, one minute).

## Rollout

0. Deploy the facade sidecar on the **testnet** archive node host
   (`testnet-archive.sub.latent.to`); run the full testnet ingestion
   (testing step 4) to completion; byte-compare a sample against the node's
   own RPC.
1. Land `wortel.serve`; deploy sidecar on the mainnet archive node host.
2. Point one heavy cron job at it via `ARCHIVE_ENDPOINT`; run smoke diff.
3. Migrate remaining cron jobs (env change only). `runtime_call`-heavy jobs
   (`inserter_metagraphs`, `inserter_all_subnets`, `stake_utils` users) still
   benefit: their storage reads go local, `state_call` passes through.
4. Leave `websocket_insertion.py` untouched.

## Open questions

- Refresh cadence vs cron freshness: is a 60–90 s head lag acceptable for
  every cron job? (Any job that must read the true chain head within seconds
  is a `websocket_insertion`-shaped job and out of scope.)
- Should `state_getKeysPaged` results be cached per (prefix, at) to serve
  asi's follow-up `queryStorageAt` from the same traversal? Measure first —
  `get_storage_many` at 4 ms/129 keys suggests no.
- Metrics endpoint (Prometheus) now or later.
