"""Replay router-shaped archive traffic against the facade's router mode and
against the node's own RPC, and report p50/p99 per method.

The subtensor-router sends wortel a method only if wortel beats the node on
it; these numbers decide ROUTER_METHODS in wortel/serve/server.py. Unlike
bench.rs and bench_vs_rpc.py (ETL patterns: one big map, block after block),
this is many concurrent small reads at random historical blocks.

    python examples/bench_router.py \
        --facade ws://127.0.0.1:9955 --node ws://127.0.0.1:9944 \
        [--requests 2000] [--concurrency 32] [--connections 4] \
        [--blocks 1000] [--max-block N] [--seed 0] \
        [--map SubtensorModule.TotalHotkeyAlpha] [--only state_getStorage,...]

Each target gets its own random block sample from the same range, so neither
is measured on pages the other just pulled into the OS cache. Blocks stop at
the facade's finalized block (GET /status), as the router's would.

Needs: pip install 'wortel[serve]' (websockets + the wortel key helpers).
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import itertools
import json
import random
import time

from websockets.asyncio.client import connect

import wortel
from wortel.serve.server import MODE_HEADER

from facade_conformance import facade_status  # sibling script


class Conn:
    """One websocket with pipelined requests, as the router uses them."""

    def __init__(self, ws):
        self._ws = ws
        self._ids = itertools.count(1)
        self._pending: dict[int, asyncio.Future] = {}
        self._pump = asyncio.create_task(self._pump_loop())

    async def _pump_loop(self) -> None:
        try:
            async for raw in self._ws:
                reply = json.loads(raw)
                fut = self._pending.pop(reply.get("id"), None)
                if fut is not None and not fut.done():
                    fut.set_result(reply)
        finally:
            for fut in self._pending.values():
                if not fut.done():
                    fut.set_exception(ConnectionError("connection closed"))

    async def request(self, method: str, params: list) -> dict:
        id_ = next(self._ids)
        fut = asyncio.get_running_loop().create_future()
        self._pending[id_] = fut
        await self._ws.send(
            json.dumps(
                {"jsonrpc": "2.0", "id": id_, "method": method, "params": params}
            )
        )
        return await fut


class Target:
    def __init__(self, name: str, conns: list[Conn], blocks: list[tuple[int, str]]):
        self.name = name
        self.blocks = blocks
        self._conns = itertools.cycle(conns)

    async def request(self, method: str, params: list) -> dict:
        return await next(self._conns).request(method, params)


def hx(data: bytes) -> str:
    return "0x" + bytes(data).hex()


def tempo_key(netuid: int) -> str:
    return hx(
        wortel.storage_prefix("SubtensorModule", "Tempo")
        + wortel.twox_64_concat(netuid.to_bytes(2, "little"))
    )


PLAIN_KEYS = [
    hx(wortel.storage_prefix("Timestamp", "Now")),
    hx(wortel.storage_prefix("System", "Number")),
    hx(wortel.storage_prefix("SubtensorModule", "TotalIssuance")),
]
TEMPO_KEYS = [tempo_key(netuid) for netuid in range(64)]


def workloads(map_prefix: str):
    """name → (method, params builder); a builder takes the rng and one
    ``(number, hash)`` block of the target's sample."""
    return {
        "state_getStorage": (
            "state_getStorage",
            lambda rng, b: [rng.choice(PLAIN_KEYS + TEMPO_KEYS), b[1]],
        ),
        "state_queryStorageAt (64 keys)": (
            "state_queryStorageAt",
            lambda rng, b: [TEMPO_KEYS, b[1]],
        ),
        "state_getKeysPaged (1000)": (
            "state_getKeysPaged",
            lambda rng, b: [map_prefix, 1000, None, b[1]],
        ),
        "chain_getHeader": ("chain_getHeader", lambda rng, b: [b[1]]),
        # The router's resolver: a 32-byte value that is no block → null.
        "chain_getHeader (not a block)": (
            "chain_getHeader",
            lambda rng, b: [hx(rng.randbytes(32))],
        ),
        "chain_getBlock": ("chain_getBlock", lambda rng, b: [b[1]]),
        "chain_getBlockHash": ("chain_getBlockHash", lambda rng, b: [b[0]]),
    }


async def run_workload(
    target: Target, method: str, build, requests: int, concurrency: int, seed: int
) -> dict:
    rng = random.Random(seed)
    calls = [build(rng, rng.choice(target.blocks)) for _ in range(requests)]
    queue = iter(calls)
    latencies: list[float] = []
    errors: list[str] = []

    async def worker() -> None:
        for params in queue:
            t0 = time.perf_counter()
            reply = await target.request(method, params)
            elapsed = time.perf_counter() - t0
            if "error" in reply:
                errors.append(
                    f"{reply['error'].get('code')} {reply['error']['message']}"
                )
            else:
                latencies.append(elapsed)

    t0 = time.perf_counter()
    await asyncio.gather(*(worker() for _ in range(concurrency)))
    wall = time.perf_counter() - t0
    latencies.sort()

    def pct(q: float) -> float:
        if not latencies:
            return float("nan")
        return latencies[min(len(latencies) - 1, int(q * len(latencies)))] * 1000

    return {
        "ok": len(latencies),
        "errors": errors,
        "p50": pct(0.50),
        "p99": pct(0.99),
        "rps": requests / wall,
    }


async def sample_blocks(
    node: Conn, rng: random.Random, max_block: int, count: int
) -> list[tuple[int, str]]:
    numbers = [rng.randint(1, max_block) for _ in range(count)]
    blocks = []
    for i in range(0, count, 500):
        chunk = numbers[i : i + 500]
        reply = await node.request("chain_getBlockHash", [chunk])
        if "error" in reply:
            raise SystemExit(f"node chain_getBlockHash failed: {reply['error']}")
        blocks.extend(zip(chunk, reply["result"]))
    return [(n, h) for n, h in blocks if h is not None]


async def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--facade", required=True)
    p.add_argument("--node", required=True)
    p.add_argument("--requests", type=int, default=2000, help="per method and target")
    p.add_argument("--concurrency", type=int, default=32, help="requests in flight")
    p.add_argument(
        "--connections", type=int, default=4, help="websockets per target (router K)"
    )
    p.add_argument("--blocks", type=int, default=1000, help="block sample per target")
    p.add_argument("--max-block", type=int, help="default: facade's finalized block")
    p.add_argument("--seed", type=int, default=0)
    p.add_argument(
        "--map",
        default="SubtensorModule.TotalHotkeyAlpha",
        help="Pallet.Item paged by the state_getKeysPaged workload",
    )
    p.add_argument("--only", help="comma-separated methods to run")
    args = p.parse_args()

    status = await asyncio.to_thread(facade_status, args.facade)
    if not status["healthy"] or status["finalized"] is None:
        raise SystemExit(f"facade not healthy: {status}")
    finalized = status["finalized"]["number"]
    max_block = min(args.max_block or finalized, finalized)
    map_prefix = hx(wortel.storage_prefix(*args.map.split(".", 1)))

    async with contextlib.AsyncExitStack() as stack:
        facade_conns = [
            Conn(
                await stack.enter_async_context(
                    connect(
                        args.facade,
                        max_size=None,
                        additional_headers={MODE_HEADER: "router"},
                    )
                )
            )
            for _ in range(args.connections)
        ]
        node_conns = [
            Conn(await stack.enter_async_context(connect(args.node, max_size=None)))
            for _ in range(args.connections)
        ]

        rng = random.Random(args.seed)
        targets = [
            Target(
                name,
                conns,
                await sample_blocks(node_conns[0], rng, max_block, args.blocks),
            )
            for name, conns in (("wortel", facade_conns), ("node", node_conns))
        ]
        served = (await facade_conns[0].request("rpc_methods", []))["result"]["methods"]
        only = set(args.only.split(",")) if args.only else None

        print(
            f"blocks 1..{max_block}, {args.requests} requests per cell, "
            f"{args.concurrency} in flight over {args.connections} connections\n"
        )
        print(
            f"{'workload':<32}{'target':<8}{'ok':>7}{'err':>6}"
            f"{'p50 ms':>10}{'p99 ms':>10}{'req/s':>9}"
        )
        for name, (method, build) in workloads(map_prefix).items():
            if only is not None and method not in only:
                continue
            results = {}
            for target in targets:
                r = results[target.name] = await run_workload(
                    target, method, build, args.requests, args.concurrency, args.seed
                )
                print(
                    f"{name if target is targets[0] else '':<32}{target.name:<8}"
                    f"{r['ok']:>7}{len(r['errors']):>6}"
                    f"{r['p50']:>10.2f}{r['p99']:>10.2f}{r['rps']:>9.0f}"
                )
                if r["errors"]:
                    print(f"{'':<32}first error: {r['errors'][0]}")
            w, n = results["wortel"], results["node"]
            if w["errors"] or not w["ok"]:
                verdict = "wortel fails"
            elif n["errors"] or not n["ok"]:
                verdict = "node fails, no comparison"
            elif w["p50"] < n["p50"] and w["p99"] < n["p99"]:
                verdict = "wortel beats the node"
            else:
                verdict = "node is not beaten"
            note = "" if method in served else " (not in rpc_methods)"
            print(f"{'':<32}→ {verdict}{note}\n")


if __name__ == "__main__":
    asyncio.run(main())
