"""JSON-RPC websocket facade over a wortel Reader.

Storage/chain reads are answered from the local RocksDB snapshot; every other
method — ``state_call``, metadata, ``system_*``, ``author_*``, subscriptions,
batches, anything unrecognized — forwards verbatim to the node's own RPC over
a per-client upstream connection (1:1, so request ids and subscription ids
need no translation).

Consistency rule: head-returning methods (``chain_getHead``,
``chain_getFinalizedHead``, ``chain_getHeader``/``chain_getBlock`` with no
hash) answer from the *snapshot*, so "ask head → read storage at head" always
succeeds against one consistent view. A hash the snapshot does not know
triggers one refresh + retry, then falls back to upstream for that call.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Optional

from websockets.asyncio.client import connect
from websockets.asyncio.server import ServerConnection

from .header import HeaderDecodeError, decode_header
from .readers import ReaderManager

logger = logging.getLogger(__name__)

_GETKEYS_PAGE = 10_000


def _err(id_: Any, code: int, message: str) -> dict:
    return {"jsonrpc": "2.0", "id": id_, "error": {"code": code, "message": message}}


def _ok(id_: Any, result: Any) -> dict:
    return {"jsonrpc": "2.0", "id": id_, "result": result}


def _hex_bytes(value: Any, what: str) -> bytes:
    if not isinstance(value, str) or not value.startswith("0x"):
        raise ValueError(f"{what}: expected 0x-prefixed hex string")
    try:
        return bytes.fromhex(value[2:])
    except ValueError:
        raise ValueError(f"{what}: invalid hex") from None


def _to_hex(data: bytes) -> str:
    return "0x" + data.hex()


def _block_number(value: Any) -> int:
    if isinstance(value, bool):
        raise ValueError("block number: expected integer")
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value, 16) if value.startswith("0x") else int(value)
        except ValueError:
            raise ValueError(f"block number: cannot parse {value!r}") from None
    raise ValueError("block number: expected integer or string")


class Upstream:
    """Lazy per-client connection to the node RPC; frames forward verbatim in
    both directions. Upstream closing closes the client connection — asi's own
    reconnect logic takes over from there."""

    def __init__(self, url: str, client: "Client"):
        self._url = url
        self._client = client
        self._ws = None
        self._connect_lock = asyncio.Lock()
        self._pump: Optional[asyncio.Task] = None

    async def forward(self, raw: str) -> None:
        async with self._connect_lock:
            if self._ws is None:
                self._ws = await connect(self._url, max_size=None)
                self._pump = asyncio.create_task(self._pump_loop())
        await self._ws.send(raw)

    async def _pump_loop(self) -> None:
        ws = self._ws
        assert ws is not None
        try:
            async for message in ws:
                await self._client.send_raw(message)
        except Exception:
            logger.info("upstream connection ended", exc_info=True)
        finally:
            await self._client.close()

    async def close(self) -> None:
        if self._pump is not None:
            self._pump.cancel()
        if self._ws is not None:
            await self._ws.close()


class Client:
    """One downstream websocket connection."""

    def __init__(self, ws: ServerConnection, facade: "Facade"):
        self._ws = ws
        self._send_lock = asyncio.Lock()
        self.facade = facade
        self.upstream = Upstream(facade.upstream_url, self)

    async def send_raw(self, raw: Any) -> None:
        async with self._send_lock:
            await self._ws.send(raw)

    async def send_json(self, obj: Any) -> None:
        await self.send_raw(json.dumps(obj))

    async def close(self) -> None:
        await self._ws.close()


class Facade:
    def __init__(
        self,
        readers: ReaderManager,
        upstream_url: str,
        max_keys_page: int = 1000,
    ):
        self.readers = readers
        self.upstream_url = upstream_url
        self.max_keys_page = max_keys_page
        self.local_methods = {
            "state_getStorage": self._state_get_storage,
            "state_getStorageAt": self._state_get_storage,
            "state_queryStorageAt": self._state_query_storage_at,
            "state_getKeysPaged": self._state_get_keys_paged,
            "state_getKeys": self._state_get_keys,
            "state_getPairs": self._state_get_pairs,
            "chain_getBlockHash": self._chain_get_block_hash,
            "chain_getHead": self._chain_get_head,
            "chain_getFinalizedHead": self._chain_get_finalized_head,
            "chain_getHeader": self._chain_get_header,
            "chain_getBlock": self._chain_get_block,
        }

    # -- connection handling --------------------------------------------------

    async def handle_connection(self, ws: ServerConnection) -> None:
        client = Client(ws, self)
        tasks: set[asyncio.Task] = set()
        try:
            async for raw in ws:
                task = asyncio.create_task(self._handle_frame(client, raw))
                tasks.add(task)
                task.add_done_callback(tasks.discard)
        finally:
            for task in tasks:
                task.cancel()
            await client.upstream.close()

    async def _handle_frame(self, client: Client, raw: Any) -> None:
        try:
            msg = json.loads(raw)
        except (json.JSONDecodeError, UnicodeDecodeError):
            await self._forward(client, raw, None)
            return
        # Batches and non-request frames go upstream verbatim.
        if not isinstance(msg, dict) or "method" not in msg:
            await self._forward(client, raw, None)
            return
        handler = self.local_methods.get(msg["method"])
        if handler is None:
            await self._forward(client, raw, msg)
            return

        id_ = msg.get("id")
        params = msg.get("params") or []
        try:
            result = await self._run_local(handler, params)
        except LookupError:
            # Block/state unknown to the snapshot even after a refresh —
            # the node itself is the fallback for this one call.
            logger.info("%s: not in snapshot, falling back upstream", msg["method"])
            await self._forward(client, raw, msg)
            return
        except (ValueError, HeaderDecodeError) as e:
            await client.send_json(_err(id_, -32602, str(e)))
            return
        except Exception as e:
            logger.exception("local handler %s failed", msg["method"])
            await client.send_json(_err(id_, -32603, f"{type(e).__name__}: {e}"))
            return
        await client.send_json(_ok(id_, result))

    async def _run_local(self, handler, params: list) -> Any:
        try:
            return await handler(params)
        except LookupError:
            # Unknown block hash: likely newer than our snapshot — refresh
            # once and retry before giving up to the upstream fallback.
            await self.readers.refresh()
            return await handler(params)

    async def _forward(self, client: Client, raw: Any, msg: Optional[dict]) -> None:
        try:
            await client.upstream.forward(raw)
        except Exception as e:
            logger.warning("upstream forward failed: %s", e)
            id_ = msg.get("id") if isinstance(msg, dict) else None
            if id_ is not None:
                await client.send_json(_err(id_, -32001, f"upstream unavailable: {e}"))

    # -- shared helpers -------------------------------------------------------

    async def _best_hash(self) -> bytes:
        best = await self.readers.call("best_block")
        if best is None:
            raise RuntimeError("database has no best block")
        return bytes(best[1])

    async def _at(self, params: list, index: int) -> bytes:
        """Resolve the optional trailing block-hash param at ``index``."""
        if len(params) > index and params[index] is not None:
            return _hex_bytes(params[index], "block hash")
        return await self._best_hash()

    # -- state_* --------------------------------------------------------------

    async def _state_get_storage(self, params: list) -> Optional[str]:
        if not params:
            raise ValueError("state_getStorage: missing key")
        key = _hex_bytes(params[0], "storage key")
        at = await self._at(params, 1)
        value = await self.readers.call("get_storage", at, key)
        return None if value is None else _to_hex(bytes(value))

    async def _state_query_storage_at(self, params: list) -> list:
        if not params or not isinstance(params[0], list):
            raise ValueError("state_queryStorageAt: missing keys list")
        keys = [_hex_bytes(k, "storage key") for k in params[0]]
        at = await self._at(params, 1)
        values = await self.readers.call("get_storage_many", at, keys)
        changes = [
            [params[0][i], None if v is None else _to_hex(bytes(v))]
            for i, v in enumerate(values)
        ]
        return [{"block": _to_hex(at), "changes": changes}]

    async def _state_get_keys_paged(self, params: list) -> list:
        if len(params) < 2:
            raise ValueError("state_getKeysPaged: expected (prefix, count, ...)")
        prefix = b"" if params[0] is None else _hex_bytes(params[0], "prefix")
        count = params[1]
        if not isinstance(count, int) or count < 0:
            raise ValueError("state_getKeysPaged: count must be a non-negative int")
        count = min(count, self.max_keys_page)
        start_after = (
            _hex_bytes(params[2], "start key")
            if len(params) > 2 and params[2] is not None
            else None
        )
        at = await self._at(params, 3)
        pairs = await self.readers.call("storage_pairs", at, prefix, start_after, count)
        return [_to_hex(bytes(k)) for k, _ in pairs]

    async def _state_get_keys(self, params: list) -> list:
        if not params:
            raise ValueError("state_getKeys: missing prefix")
        prefix = b"" if params[0] is None else _hex_bytes(params[0], "prefix")
        at = await self._at(params, 1)
        keys: list[str] = []
        start_after = None
        while True:
            pairs = await self.readers.call(
                "storage_pairs", at, prefix, start_after, _GETKEYS_PAGE
            )
            keys.extend(_to_hex(bytes(k)) for k, _ in pairs)
            if len(pairs) < _GETKEYS_PAGE:
                return keys
            start_after = bytes(pairs[-1][0])

    async def _state_get_pairs(self, params: list) -> list:
        """Full `(key, value)` map under a prefix in ONE response — the
        method nodes keep behind `--rpc-methods unsafe`. This is the facade's
        performance play: one round trip and one trie traversal instead of
        ~N/500 getKeysPaged + queryStorageAt exchanges."""
        if not params:
            raise ValueError("state_getPairs: missing prefix")
        prefix = b"" if params[0] is None else _hex_bytes(params[0], "prefix")
        at = await self._at(params, 1)
        result: list = []
        start_after = None
        while True:
            pairs = await self.readers.call(
                "storage_pairs", at, prefix, start_after, _GETKEYS_PAGE
            )
            result.extend([_to_hex(bytes(k)), _to_hex(bytes(v))] for k, v in pairs)
            if len(pairs) < _GETKEYS_PAGE:
                return result
            start_after = bytes(pairs[-1][0])

    # -- chain_* --------------------------------------------------------------

    async def _chain_get_block_hash(self, params: list) -> Any:
        if not params or params[0] is None:
            return _to_hex(await self._best_hash())
        if isinstance(params[0], list):
            return [await self._one_block_hash(n) for n in params[0]]
        return await self._one_block_hash(params[0])

    async def _one_block_hash(self, value: Any) -> Optional[str]:
        n = _block_number(value)
        h = await self.readers.call("block_hash", n)
        if h is None:
            best = await self.readers.call("best_block")
            if best is not None and n > best[0]:
                # Beyond our snapshot but possibly known to the node — let
                # the upstream fallback answer instead of asserting null.
                raise LookupError(f"block {n} beyond snapshot head {best[0]}")
            return None
        return _to_hex(bytes(h))

    async def _chain_get_head(self, params: list) -> str:
        return _to_hex(await self._best_hash())

    async def _chain_get_finalized_head(self, params: list) -> str:
        finalized = await self.readers.call("finalized_block")
        if finalized is None:
            raise RuntimeError("database has no finalized block")
        return _to_hex(bytes(finalized[1]))

    async def _chain_get_header(self, params: list) -> Optional[dict]:
        at = await self._at(params, 0)
        raw = await self.readers.call("header", at)
        if raw is None:
            # Unknown to the snapshot ≠ nonexistent (we lag the node):
            # refresh/upstream-fallback rather than answering null.
            raise LookupError(f"no header for {_to_hex(at)} in snapshot")
        return decode_header(bytes(raw))

    async def _chain_get_block(self, params: list) -> Optional[dict]:
        at = await self._at(params, 0)
        raw = await self.readers.call("header", at)
        if raw is None:
            raise LookupError(f"no header for {_to_hex(at)} in snapshot")
        extrinsics = await self.readers.call("extrinsics", at)
        if extrinsics is None:
            # Header without body should not happen on an archive node; let
            # the node answer rather than invent an empty block.
            raise LookupError(f"no body for block {_to_hex(at)}")
        return {
            "block": {
                "header": decode_header(bytes(raw)),
                "extrinsics": [_to_hex(bytes(xt)) for xt in extrinsics],
            },
            "justifications": None,
        }
