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

Router mode (handshake header ``X-Wortel-Mode: router``) is for a client that
holds its own node connections and does its own fallback: no pass-through, no
upstream fallback, no refresh or wait caused by a request, and every call must
name its block. See ``docs/rpc-facade.md``.
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

MODE_HEADER = "X-Wortel-Mode"

# Router mode: the snapshot cannot answer (unknown block, by-number lookup
# above the finalized block, stale snapshot, no reader yet). The router
# retries on the node. Outside the codes jsonrpsee and substrate use.
NOT_IN_SNAPSHOT = -32090

# The node's own limit and error for state_getKeysPaged (sc-rpc
# state/mod.rs, sc-rpc-api state/error.rs); router mode answers the same.
STORAGE_KEYS_PAGED_MAX_COUNT = 1000
_INVALID_COUNT = 4002

# Methods served in router mode, advertised there by rpc_methods. All take
# an explicit block. A method joins only after examples/bench_router.py shows
# it beats the node.
ROUTER_METHODS = (
    "state_getStorage",
    "state_getStorageAt",
    "state_queryStorageAt",
    "state_getKeysPaged",
    "state_getKeys",
    "state_getPairs",
    "chain_getBlockHash",
    "chain_getHeader",
    "chain_getBlock",
)


class RpcError(Exception):
    """A handler error with a JSON-RPC code of its own."""

    def __init__(self, code: int, message: str):
        super().__init__(message)
        self.code = code


def _err(id_: Any, code: int, message: str, data: Any = None) -> dict:
    error = {"code": code, "message": message}
    if data is not None:
        error["data"] = data
    return {"jsonrpc": "2.0", "id": id_, "error": error}


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
        self.router = _mode(ws.request.headers) == "router"
        self.local = facade.router if self.router else facade.direct
        # Lazy: never connects in router mode, which forwards nothing.
        self.upstream = Upstream(facade.upstream_url, self)

    async def send_raw(self, raw: Any) -> None:
        async with self._send_lock:
            await self._ws.send(raw)

    async def send_json(self, obj: Any) -> None:
        await self.send_raw(json.dumps(obj))

    async def close(self) -> None:
        await self._ws.close()


def _mode(headers) -> Optional[str]:
    """Connection mode from the handshake headers: ``"direct"`` (no header),
    ``"router"``, or None for a value we do not know."""
    value = headers.get(MODE_HEADER)
    if value is None:
        return "direct"
    return "router" if value.strip().lower() == "router" else None


def health_check(readers: ReaderManager):
    """``process_request`` hook: answer plain-HTTP probes on the websocket
    port so k8s can use httpGet.

    ``GET /livez`` — 200 as long as the event loop answered. Does not touch
    the Reader; a stale snapshot must not restart the pod.

    ``GET /readyz`` — 200 when a Reader is open and can resolve the best
    block. Upstream health is deliberately not part of this: local reads
    keep working when the node RPC is down.

    ``GET /status`` — always 200, JSON: health with a reason, the snapshot's
    best and finalized blocks, its open time, refresh state, genesis hash.
    """

    def process_request(connection, request):
        path = request.path.split("?", 1)[0]
        if path == "/livez":
            return connection.respond(200, "ok\n")
        if path == "/status":
            response = connection.respond(200, json.dumps(readers.status()) + "\n")
            del response.headers["Content-Type"]
            response.headers["Content-Type"] = "application/json"
            return response
        if path != "/readyz":
            if _mode(request.headers) is None:
                # A typo must not silently get direct mode and pass-through.
                return connection.respond(400, f"unknown {MODE_HEADER}\n")
            return None  # continue with the websocket handshake
        try:
            best = readers.reader.best_block() if readers.reader else None
        except Exception as e:  # includes a stale snapshot mid-probe
            return connection.respond(503, f"reader error: {e}\n")
        if best is None:
            return connection.respond(503, "no reader / empty database\n")
        return connection.respond(200, f"ok best_block={best[0]}\n")

    return process_request


class Facade:
    def __init__(
        self,
        readers: ReaderManager,
        upstream_url: str,
        max_keys_page: int = 1000,
    ):
        self.readers = readers
        self.upstream_url = upstream_url
        self.direct = Local(readers, max_keys_page)
        self.router = Local(readers, STORAGE_KEYS_PAGED_MAX_COUNT, router=True)

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
            await self._pass(client, raw, None, -32700, "Parse error")
            return
        # Batches and non-request frames go upstream verbatim (the router
        # splits batches itself).
        if not isinstance(msg, dict) or "method" not in msg:
            await self._pass(client, raw, None, -32600, "Invalid request")
            return
        handler = client.local.methods.get(msg["method"])
        if handler is None:
            await self._pass(client, raw, msg, -32601, "Method not found")
            return

        id_ = msg.get("id")
        params = msg.get("params") or []
        try:
            if client.router and not isinstance(params, list):
                raise ValueError("router mode: positional params required")
            result = await self._run_local(client, handler, params)
        except LookupError as e:
            if client.router:
                logger.debug("%s: not in snapshot (%s)", msg["method"], e)
                await client.send_json(
                    _err(id_, NOT_IN_SNAPSHOT, "not in snapshot", str(e))
                )
                return
            # Block/state unknown to the snapshot even after a refresh —
            # the node itself is the fallback for this one call.
            logger.info("%s: not in snapshot, falling back upstream", msg["method"])
            await self._forward(client, raw, msg)
            return
        except RpcError as e:
            await client.send_json(_err(id_, e.code, str(e)))
            return
        except (ValueError, HeaderDecodeError) as e:
            await client.send_json(_err(id_, -32602, str(e)))
            return
        except Exception as e:
            logger.exception("local handler %s failed", msg["method"])
            await client.send_json(_err(id_, -32603, f"{type(e).__name__}: {e}"))
            return
        await client.send_json(_ok(id_, result))

    async def _run_local(self, client: Client, handler, params: list) -> Any:
        try:
            return await handler(params)
        except LookupError:
            if client.router:
                # A request never causes a reopen here: the router also sends
                # values that are not blocks, and it retries on the node.
                raise
            # Unknown block hash: likely newer than our snapshot — refresh
            # once and retry before giving up to the upstream fallback.
            await self.readers.refresh()
            return await handler(params)

    async def _pass(
        self, client: Client, raw: Any, msg: Optional[dict], code: int, message: str
    ) -> None:
        """Hand a frame we do not serve to the node. Router mode has no
        pass-through — the router holds its own node connections, under its
        own limits — and answers the JSON-RPC error instead."""
        if client.router:
            id_ = msg.get("id") if msg is not None else None
            await client.send_json(_err(id_, code, message))
            return
        await self._forward(client, raw, msg)

    async def _forward(self, client: Client, raw: Any, msg: Optional[dict]) -> None:
        try:
            await client.upstream.forward(raw)
        except Exception as e:
            logger.warning("upstream forward failed: %s", e)
            id_ = msg.get("id") if isinstance(msg, dict) else None
            if id_ is not None:
                await client.send_json(_err(id_, -32001, f"upstream unavailable: {e}"))


class Local:
    """The locally served methods, for one connection mode."""

    def __init__(
        self, readers: ReaderManager, max_keys_page: int, router: bool = False
    ):
        self.readers = readers
        self.max_keys_page = max_keys_page
        self.router = router
        self.methods = {
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
        if router:
            self.methods = {name: self.methods[name] for name in ROUTER_METHODS}
            self.methods["rpc_methods"] = self._rpc_methods

    # -- shared helpers -------------------------------------------------------

    async def _read(self, method: str, *args: Any) -> Any:
        # Router mode never waits for a reopen; the router asks the node.
        return await self.readers.call(method, *args, wait_on_stale=not self.router)

    async def _best_hash(self) -> bytes:
        if self.router:
            # The snapshot's head is minutes old, not the latest state. The
            # router never sends these; refusing guards against a routing bug.
            raise ValueError("router mode: a block is required")
        best = await self._read("best_block")
        if best is None:
            raise RuntimeError("database has no best block")
        return bytes(best[1])

    async def _at(self, params: list, index: int) -> bytes:
        """Resolve the optional trailing block-hash param at ``index``."""
        if len(params) > index and params[index] is not None:
            return _hex_bytes(params[index], "block hash")
        return await self._best_hash()

    async def _rpc_methods(self, params: list) -> dict:
        return {"methods": sorted(self.methods)}

    # -- state_* --------------------------------------------------------------

    async def _state_get_storage(self, params: list) -> Optional[str]:
        if not params:
            raise ValueError("state_getStorage: missing key")
        key = _hex_bytes(params[0], "storage key")
        at = await self._at(params, 1)
        value = await self._read("get_storage", at, key)
        return None if value is None else _to_hex(bytes(value))

    async def _state_query_storage_at(self, params: list) -> list:
        if not params or not isinstance(params[0], list):
            raise ValueError("state_queryStorageAt: missing keys list")
        keys = [_hex_bytes(k, "storage key") for k in params[0]]
        at = await self._at(params, 1)
        values = await self._read("get_storage_many", at, keys)
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
        if self.router and count > STORAGE_KEYS_PAGED_MAX_COUNT:
            # The node rejects this; truncating would give a different answer.
            raise RpcError(
                _INVALID_COUNT,
                f"count exceeds maximum value. value: {count}, "
                f"max: {STORAGE_KEYS_PAGED_MAX_COUNT}",
            )
        count = min(count, self.max_keys_page)
        start_after = (
            _hex_bytes(params[2], "start key")
            if len(params) > 2 and params[2] is not None
            else None
        )
        at = await self._at(params, 3)
        pairs = await self._read("storage_pairs", at, prefix, start_after, count)
        return [_to_hex(bytes(k)) for k, _ in pairs]

    async def _state_get_keys(self, params: list) -> list:
        if not params:
            raise ValueError("state_getKeys: missing prefix")
        prefix = b"" if params[0] is None else _hex_bytes(params[0], "prefix")
        at = await self._at(params, 1)
        keys: list[str] = []
        start_after = None
        while True:
            pairs = await self._read(
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
            pairs = await self._read(
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
        if self.router:
            # A number names one block for good only once it is finalized.
            snapshot = self.readers.snapshot
            finalized = snapshot and snapshot.finalized
            if finalized is None or n > finalized[0]:
                raise LookupError(f"block {n} above snapshot finalized block")
        h = await self._read("block_hash", n)
        if h is None:
            best = await self._read("best_block")
            if best is not None and n > best[0]:
                # Beyond our snapshot but possibly known to the node — let
                # the upstream fallback answer instead of asserting null.
                raise LookupError(f"block {n} beyond snapshot head {best[0]}")
            return None
        return _to_hex(bytes(h))

    async def _chain_get_head(self, params: list) -> str:
        return _to_hex(await self._best_hash())

    async def _chain_get_finalized_head(self, params: list) -> str:
        finalized = await self._read("finalized_block")
        if finalized is None:
            raise RuntimeError("database has no finalized block")
        return _to_hex(bytes(finalized[1]))

    async def _chain_get_header(self, params: list) -> Optional[dict]:
        at = await self._at(params, 0)
        raw = await self._read("header", at)
        if raw is None:
            if self.router:
                # The router's resolver reads null as "not a block".
                return None
            # Unknown to the snapshot ≠ nonexistent (we lag the node):
            # refresh/upstream-fallback rather than answering null.
            raise LookupError(f"no header for {_to_hex(at)} in snapshot")
        return decode_header(bytes(raw))

    async def _chain_get_block(self, params: list) -> Optional[dict]:
        at = await self._at(params, 0)
        raw = await self._read("header", at)
        if raw is None:
            raise LookupError(f"no header for {_to_hex(at)} in snapshot")
        extrinsics = await self._read("extrinsics", at)
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
