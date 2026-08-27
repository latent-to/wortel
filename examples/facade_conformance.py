"""Byte-compare the facade against the node's own RPC.

For a sample of blocks, issues the same locally-served methods to both
endpoints and diffs the JSON results. Run this against the testnet facade
before trusting an ingestion (see docs/rpc-facade.md, testing step 2).

    python examples/facade_conformance.py \
        --facade ws://sidecar:9955 --node wss://testnet-archive.sub.latent.to \
        [--blocks 1,1000,2500000] [--count 20]

With no --blocks, samples --count blocks evenly from 1..facade head.
Needs: pip install 'wortel[serve]' (websockets + the wortel key helpers).
"""

from __future__ import annotations

import argparse
import asyncio
import itertools
import json

from websockets.asyncio.client import connect

import wortel


class Rpc:
    def __init__(self, ws):
        self._ws = ws
        self._ids = itertools.count(1)

    async def call(self, method: str, params: list):
        id_ = next(self._ids)
        await self._ws.send(
            json.dumps(
                {"jsonrpc": "2.0", "id": id_, "method": method, "params": params}
            )
        )
        while True:
            reply = json.loads(await self._ws.recv())
            if reply.get("id") == id_:
                if "error" in reply:
                    return {"__error__": reply["error"]["message"]}
                return reply["result"]


async def compare_block(facade: Rpc, node: Rpc, n: int) -> list[str]:
    diffs = []

    async def check(method: str, params: list) -> object:
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


async def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--facade", required=True)
    p.add_argument("--node", required=True)
    p.add_argument("--blocks", help="comma-separated block numbers")
    p.add_argument("--count", type=int, default=20)
    args = p.parse_args()

    async with (
        connect(args.facade, max_size=None) as fws,
        connect(args.node, max_size=None) as nws,
    ):
        facade, node = Rpc(fws), Rpc(nws)
        if args.blocks:
            blocks = [int(b) for b in args.blocks.split(",")]
        else:
            head_hash = await facade.call("chain_getHead", [])
            header = await facade.call("chain_getHeader", [head_hash])
            head = int(header["number"], 16)
            step = max(1, head // args.count)
            blocks = list(range(1, head + 1, step))[: args.count]

        failures = 0
        for n in blocks:
            diffs = await compare_block(facade, node, n)
            for d in diffs:
                print(d)
            failures += len(diffs)
            print(f"block {n}: {'ok' if not diffs else f'{len(diffs)} diffs'}")
        print(f"\n{len(blocks)} blocks checked, {failures} diffs")
        raise SystemExit(1 if failures else 0)


if __name__ == "__main__":
    asyncio.run(main())
