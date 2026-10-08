"""Byte-compare the facade against the node's own RPC.

For a sample of blocks, issues the same locally-served methods to both
endpoints and diffs the JSON results. Run this against the testnet facade
before trusting an ingestion (see docs/rpc-facade.md, testing step 2).

    python examples/facade_conformance.py \
        --facade ws://sidecar:9955 --node wss://testnet-archive.sub.latent.to \
        [--blocks 1,1000,2500000] [--count 20] [--router-mode]

With no --blocks, samples --count blocks evenly from 1..facade head.
Needs: pip install wortel websockets (the wortel key helpers).

--router-mode connects to the facade as the subtensor-router does
(X-Wortel-Mode: router). Successful answers are still byte-compared with the
node's; on top, the mode's null, error and refusal rules are checked, and the
block sample stops at the facade's finalized block (from GET /status).
"""

from __future__ import annotations

import argparse
import asyncio
import itertools
import json
import os
import urllib.request
from urllib.parse import urlsplit

from websockets.asyncio.client import connect

import wortel

# The facade's router-mode contract (serve/src/rpc.rs, serve/src/server.rs).
MODE_HEADER = "X-Wortel-Mode"
NOT_IN_SNAPSHOT = -32090


class Rpc:
    def __init__(self, ws):
        self._ws = ws
        self._ids = itertools.count(1)

    async def request(self, method: str, params: list) -> dict:
        """The whole reply, error object included."""
        id_ = next(self._ids)
        await self._ws.send(
            json.dumps(
                {"jsonrpc": "2.0", "id": id_, "method": method, "params": params}
            )
        )
        while True:
            reply = json.loads(await self._ws.recv())
            if reply.get("id") == id_:
                return reply

    async def call(self, method: str, params: list):
        reply = await self.request(method, params)
        if "error" in reply:
            return {"__error__": reply["error"]["message"]}
        return reply["result"]


def facade_status(facade_url: str) -> dict:
    parts = urlsplit(facade_url)
    scheme = "https" if parts.scheme == "wss" else "http"
    url = f"{scheme}://{parts.netloc}/status"
    with urllib.request.urlopen(url, timeout=10) as resp:
        return json.load(resp)


async def compare_block(
    facade: Rpc, node: Rpc, n: int, served: set[str] | None = None
) -> list[str]:
    """``served`` is the facade's router-mode method set; a method outside it
    is the node's alone and is not compared."""
    diffs = []

    async def check(method: str, params: list) -> object:
        if served is not None and method not in served:
            return await node.call(method, params)
        got, want = await asyncio.gather(
            facade.call(method, params), node.call(method, params)
        )
        if got != want:
            diffs.append(
                f"block {n} {method}{params!r}:\n  facade: {got!r}\n  node:   {want!r}"
            )
        return want

    block_hash = await check("chain_getBlockHash", [n])
    if not isinstance(block_hash, str):
        return diffs
    await check("chain_getHeader", [block_hash])
    await check("chain_getBlock", [block_hash])

    ts_key = "0x" + wortel.storage_prefix("Timestamp", "Now").hex()
    await check("state_getStorage", [ts_key, block_hash])

    tempo_prefix = "0x" + wortel.storage_prefix("SubtensorModule", "Tempo").hex()
    keys = await check("state_getKeysPaged", [tempo_prefix, 1000, None, block_hash])
    if isinstance(keys, list) and keys:
        await check("state_queryStorageAt", [keys[:100], block_hash])
    return diffs


async def check_router_rules(
    facade: Rpc, node: Rpc, served: set[str], finalized: dict
) -> list[str]:
    """Router mode's own contract: what it answers where direct mode would
    refresh, fall back to the node, or answer from the snapshot's head."""
    diffs = []
    at = finalized["hash"]
    unknown = "0x" + os.urandom(32).hex()
    ts_key = "0x" + wortel.storage_prefix("Timestamp", "Now").hex()
    tempo_prefix = "0x" + wortel.storage_prefix("SubtensorModule", "Tempo").hex()

    async def expect_code(method: str, params: list, code: int) -> None:
        if method not in served and code != -32601:
            return
        reply = await facade.request(method, params)
        got = reply.get("error", {}).get("code")
        if got != code:
            diffs.append(f"router {method}{params!r}: want error {code}, got {reply!r}")

    # No pass-through: what is not advertised does not exist.
    for method in ("system_chain", "state_call", "chain_getHead"):
        if method in served:
            diffs.append(f"router rpc_methods advertises {method}")
        await expect_code(method, [], -32601)

    async def same_as_node(method: str, params: list) -> None:
        if method not in served:
            return
        got, want = await asyncio.gather(
            facade.request(method, params), node.request(method, params)
        )
        got, want = ({k: r.get(k) for k in ("result", "error")} for r in (got, want))
        if got != want:
            diffs.append(
                f"router {method}{params!r}:\n  facade: {got!r}\n  node:   {want!r}"
            )

    # A hash that is no block: null header, as on the node; any other read
    # is "not in snapshot", for the router to retry on the node.
    await same_as_node("chain_getHeader", [unknown])
    await expect_code("state_getStorage", [ts_key, unknown], NOT_IN_SNAPSHOT)
    await expect_code("chain_getBlock", [unknown], NOT_IN_SNAPSHOT)
    await expect_code("chain_getBlockHash", [finalized["number"] + 1], NOT_IN_SNAPSHOT)

    # The node's own error for an oversized page, code and message.
    await same_as_node("state_getKeysPaged", [tempo_prefix, 1001, None, at])

    # No block argument: refused, never answered from the snapshot's head.
    await expect_code("state_getStorage", [ts_key], -32602)
    await expect_code("state_queryStorageAt", [[ts_key]], -32602)
    await expect_code("state_getKeysPaged", [tempo_prefix, 10], -32602)
    await expect_code("chain_getHeader", [], -32602)
    await expect_code("chain_getBlock", [], -32602)
    await expect_code("chain_getBlockHash", [], -32602)
    return diffs


async def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--facade", required=True)
    p.add_argument("--node", required=True)
    p.add_argument("--blocks", help="comma-separated block numbers")
    p.add_argument("--count", type=int, default=20)
    p.add_argument(
        "--router-mode",
        action="store_true",
        help="talk to the facade in router mode and check that mode's rules",
    )
    args = p.parse_args()

    headers = {MODE_HEADER: "router"} if args.router_mode else None
    async with (
        connect(args.facade, max_size=None, additional_headers=headers) as fws,
        connect(args.node, max_size=None) as nws,
    ):
        facade, node = Rpc(fws), Rpc(nws)
        served = None
        failures = 0
        if args.router_mode:
            status = await asyncio.to_thread(facade_status, args.facade)
            if not status["healthy"] or status["finalized"] is None:
                raise SystemExit(f"facade not healthy: {status}")
            head = status["finalized"]["number"]
            served = set((await facade.call("rpc_methods", []))["methods"])
            diffs = await check_router_rules(facade, node, served, status["finalized"])
            for d in diffs:
                print(d)
            failures += len(diffs)
            print(f"router rules: {'ok' if not diffs else f'{len(diffs)} diffs'}")
        else:
            head_hash = await facade.call("chain_getHead", [])
            header = await facade.call("chain_getHeader", [head_hash])
            head = int(header["number"], 16)
        if args.blocks:
            blocks = [int(b) for b in args.blocks.split(",")]
        else:
            step = max(1, head // args.count)
            blocks = list(range(1, head + 1, step))[: args.count]

        for n in blocks:
            diffs = await compare_block(facade, node, n, served)
            for d in diffs:
                print(d)
            failures += len(diffs)
            print(f"block {n}: {'ok' if not diffs else f'{len(diffs)} diffs'}")
        print(f"\n{len(blocks)} blocks checked, {failures} diffs")
        raise SystemExit(1 if failures else 0)


if __name__ == "__main__":
    asyncio.run(main())
