"""Race wortel against the node's own RPC on a large storage map, and verify
a random sample of values byte-for-byte.

Needs the wortel wheel installed (maturin build --release --features python)
and the node's RPC reachable (default http://127.0.0.1:9944).

Usage: python bench_vs_rpc.py <db-path> <block-number> [rpc-url]
"""

import json
import random
import sys
import time
import urllib.request

import wortel

DB = sys.argv[1]
BLOCK = int(sys.argv[2])
RPC = sys.argv[3] if len(sys.argv) > 3 else "http://127.0.0.1:9944"


def rpc(method, params):
    req = urllib.request.Request(
        RPC,
        data=json.dumps(
            {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
        ).encode(),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req) as resp:
        out = json.load(resp)
    if "error" in out:
        raise RuntimeError(out["error"])
    return out["result"]


t0 = time.monotonic()
r = wortel.Reader(DB)  # readonly
print(f"open_s={time.monotonic() - t0:.1f}")

head = r.block_hash(BLOCK)
assert head is not None, "block not in db"
hexhash = "0x" + head.hex()
print(f"block={BLOCK} hash={hexhash}")

prefix = wortel.storage_prefix("SubtensorModule", "TotalHotkeyAlpha")

# --- wortel: full map, single trie traversal ---------------------------
t0 = time.monotonic()
pairs = r.storage_pairs(head, prefix)
wortel_s = time.monotonic() - t0
print(f"wortel_pairs={len(pairs)} wortel_cold_s={wortel_s:.2f}")

# Same map again (cache warm — the steady state when an ETL walks
# consecutive blocks, whose tries share almost all nodes).
t0 = time.monotonic()
pairs2 = r.storage_pairs(head, prefix)
print(f"wortel_warm_s={time.monotonic() - t0:.2f}")
assert pairs2 == pairs

# Adjacent block (the realistic incremental case).
prev = r.block_hash(BLOCK - 1)
t0 = time.monotonic()
pairs_prev = r.storage_pairs(prev, prefix)
print(f"wortel_adjacent_block_s={time.monotonic() - t0:.2f} pairs={len(pairs_prev)}")

# --- RPC: keys only, paged (what query_map does before fetching values) -
t0 = time.monotonic()
rpc_keys = []
start_key = None
while True:
    page = rpc("state_getKeysPaged", ["0x" + prefix.hex(), 1000, start_key, hexhash])
    rpc_keys.extend(page)
    if len(page) < 1000:
        break
    start_key = page[-1]
rpc_keys_s = time.monotonic() - t0
print(f"rpc_keys={len(rpc_keys)} rpc_keys_s={rpc_keys_s:.2f}")

assert len(pairs) == len(rpc_keys), (len(pairs), len(rpc_keys))

# --- RPC: sample values, check byte equality, project the full cost -----
by_key = {k: v for k, v in pairs}
sample = random.sample(rpc_keys, min(200, len(rpc_keys)))
t0 = time.monotonic()
mismatches = 0
for k in sample:
    v = rpc("state_getStorage", [k, hexhash])
    if by_key.get(bytes.fromhex(k[2:])) != bytes.fromhex(v[2:]):
        mismatches += 1
per_call = (time.monotonic() - t0) / len(sample)
print(f"sample={len(sample)} mismatches={mismatches} rpc_per_get_ms={per_call * 1000:.1f}")
projected = rpc_keys_s + per_call * len(rpc_keys)
print(f"rpc_projected_full_s={projected:.0f} (keys paging + one getStorage per key)")
print(f"speedup_vs_rpc={projected / wortel_s:.0f}x")
assert mismatches == 0
print("bench: all values byte-identical")
