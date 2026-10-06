"""Facade behavior against a stub reader and a fake upstream node."""

import asyncio
import contextlib
import json

import pytest
import websockets
from websockets.asyncio.server import serve

from wortel.serve.__main__ import parse_args, self_check
from wortel.serve.readers import ReaderManager
from wortel.serve.server import (
    MODE_HEADER,
    NOT_IN_SNAPSHOT,
    ROUTER_CANDIDATES,
    ROUTER_METHODS,
    Facade,
    health_check,
)

from test_header import make_header

H1 = b"\x11" * 32
H2 = b"\x22" * 32
KEY_A = b"\xaa\x01"
KEY_B = b"\xaa\x02"
KEY_C = b"\xab\x01"
GENESIS = b"\x00" * 32


class StubReader:
    """In-memory reader mimicking wortel.Reader's surface and errors."""

    def __init__(self):
        header_raw, _ = make_header(2, [])
        self.headers = {H2: header_raw}
        self.bodies = {H2: [b"\x04\x01", b"\x08\x02\x03"]}
        self.hashes = {1: H1, 2: H2}
        self.storage = {H2: {KEY_A: b"\x01", KEY_B: b"\x02", KEY_C: b"\x03"}}
        self.fail_with = None  # raise this once on the next storage call

    def _maybe_fail(self):
        if self.fail_with is not None:
            e, self.fail_with = self.fail_with, None
            raise e

    def best_block(self):
        return (2, H2)

    def finalized_block(self):
        return (2, H2)

    def genesis_hash(self):
        return GENESIS

    def block_hash(self, n):
        return self.hashes.get(n)

    def header(self, block_hash):
        return self.headers.get(bytes(block_hash))

    def extrinsics(self, block_hash):
        return self.bodies.get(bytes(block_hash))

    def _block_storage(self, block_hash):
        try:
            return self.storage[bytes(block_hash)]
        except KeyError:
            raise LookupError(f"unknown block {block_hash.hex()}") from None

    def get_storage(self, block_hash, key):
        self._maybe_fail()
        return self._block_storage(block_hash).get(bytes(key))

    def get_storage_many(self, block_hash, keys):
        store = self._block_storage(block_hash)
        return [store.get(bytes(k)) for k in keys]

    def storage_pairs(self, block_hash, prefix, start_after=None, limit=None):
        store = self._block_storage(block_hash)
        pairs = sorted(
            (k, v)
            for k, v in store.items()
            if k.startswith(bytes(prefix))
            and (start_after is None or k > bytes(start_after))
        )
        return pairs[:limit] if limit is not None else pairs


class FakeUpstream:
    """Node stand-in: answers state_call, records everything it receives."""

    def __init__(self):
        self.received = []
        self.connections = []

    async def handler(self, ws):
        self.connections.append(ws)
        async for raw in ws:
            msg = json.loads(raw)
            self.received.append(msg)
            await ws.send(
                json.dumps(
                    {"jsonrpc": "2.0", "id": msg.get("id"), "result": "from-upstream"}
                )
            )


class FlakyFactory:
    """Reader factory whose opens fail while ``error`` is set."""

    def __init__(self, error=None):
        self.error = error
        self.opens = 0

    def __call__(self):
        self.opens += 1
        if self.error is not None:
            raise self.error
        return StubReader()


# The rig advertises every candidate in router mode, so each rule is tested
# whatever the benchmark-gated default set is.
@contextlib.asynccontextmanager
async def running(readers, router_methods=ROUTER_CANDIDATES):
    """A running facade over ``readers`` and a fake upstream; yields the
    facade's port and the upstream. Does not wait for the first open."""
    upstream = FakeUpstream()
    async with serve(upstream.handler, "127.0.0.1", 0, max_size=None) as up_server:
        up_port = up_server.sockets[0].getsockname()[1]
        facade = Facade(
            readers,
            f"ws://127.0.0.1:{up_port}",
            max_keys_page=1000,
            router_methods=router_methods,
        )
        async with serve(
            facade.handle_connection,
            "127.0.0.1",
            0,
            max_size=None,
            process_request=health_check(readers),
        ) as server:
            await readers.start()
            try:
                yield server.sockets[0].getsockname()[1], upstream
            finally:
                await readers.stop()


def router_connect(port):
    return websockets.connect(
        f"ws://127.0.0.1:{port}", additional_headers={MODE_HEADER: "router"}
    )


@pytest.fixture
async def rig():
    """A running facade wired to a stub reader and a fake upstream."""
    readers = ReaderManager(factory=StubReader, refresh_interval=3600)
    async with running(readers) as (port, upstream):
        await readers.wait_ready()
        async with websockets.connect(f"ws://127.0.0.1:{port}") as client:
            yield client, upstream, readers


@pytest.fixture
async def router(rig):
    """A router-mode connection to the same facade as ``rig``."""
    client, upstream, readers = rig
    async with router_connect(client.remote_address[1]) as router_client:
        yield router_client, upstream, readers


async def call(client, method, params, id_=1):
    await client.send(
        json.dumps({"jsonrpc": "2.0", "id": id_, "method": method, "params": params})
    )
    reply = json.loads(await client.recv())
    assert reply["id"] == id_
    return reply


def hx(b: bytes) -> str:
    return "0x" + b.hex()


async def test_get_storage(rig):
    client, _, _ = rig
    reply = await call(client, "state_getStorage", [hx(KEY_A), hx(H2)])
    assert reply["result"] == "0x01"


async def test_get_storage_missing_key_is_null(rig):
    client, _, _ = rig
    reply = await call(client, "state_getStorage", [hx(b"\xff\xff"), hx(H2)])
    assert reply["result"] is None


async def test_get_storage_default_at_uses_snapshot_best(rig):
    client, _, _ = rig
    reply = await call(client, "state_getStorage", [hx(KEY_B)])
    assert reply["result"] == "0x02"


async def test_query_storage_at(rig):
    client, _, _ = rig
    reply = await call(
        client, "state_queryStorageAt", [[hx(KEY_A), hx(b"\xff")], hx(H2)]
    )
    assert reply["result"] == [
        {"block": hx(H2), "changes": [[hx(KEY_A), "0x01"], [hx(b"\xff"), None]]}
    ]


async def test_get_keys_paged(rig):
    client, _, _ = rig
    reply = await call(client, "state_getKeysPaged", [hx(b"\xaa"), 10, None, hx(H2)])
    assert reply["result"] == [hx(KEY_A), hx(KEY_B)]
    # resume strictly after KEY_A
    reply = await call(
        client, "state_getKeysPaged", [hx(b"\xaa"), 10, hx(KEY_A), hx(H2)]
    )
    assert reply["result"] == [hx(KEY_B)]


async def test_get_pairs(rig):
    client, _, _ = rig
    reply = await call(client, "state_getPairs", [hx(b"\xaa"), hx(H2)])
    assert reply["result"] == [[hx(KEY_A), "0x01"], [hx(KEY_B), "0x02"]]
    reply = await call(client, "state_getPairs", [hx(b"\xab")])
    assert reply["result"] == [[hx(KEY_C), "0x03"]]


async def test_chain_methods(rig):
    client, _, _ = rig
    assert (await call(client, "chain_getBlockHash", [2]))["result"] == hx(H2)
    assert (await call(client, "chain_getBlockHash", [[1, 2, 0]]))["result"] == [
        hx(H1),
        hx(H2),
        None,
    ]
    assert (await call(client, "chain_getBlockHash", ["0x2"]))["result"] == hx(H2)
    assert (await call(client, "chain_getBlockHash", []))["result"] == hx(H2)
    assert (await call(client, "chain_getHead", []))["result"] == hx(H2)
    assert (await call(client, "chain_getFinalizedHead", []))["result"] == hx(H2)


async def test_future_block_number_falls_back_upstream(rig):
    # Beyond the snapshot head the node may know the block — never answer
    # null locally, forward instead.
    client, upstream, _ = rig
    reply = await call(client, "chain_getBlockHash", [99])
    assert reply["result"] == "from-upstream"
    assert upstream.received[0]["method"] == "chain_getBlockHash"
    reply = await call(client, "chain_getBlockHash", [[2, 99]])
    assert reply["result"] == "from-upstream"


async def test_unknown_header_hash_falls_back_upstream(rig):
    client, upstream, _ = rig
    reply = await call(client, "chain_getHeader", [hx(b"\x99" * 32)])
    assert reply["result"] == "from-upstream"
    assert upstream.received[0]["method"] == "chain_getHeader"


async def test_get_header_and_block(rig):
    client, _, _ = rig
    header = (await call(client, "chain_getHeader", [hx(H2)]))["result"]
    assert header["number"] == "0x2"
    block = (await call(client, "chain_getBlock", [hx(H2)]))["result"]
    assert block["block"]["header"] == header
    assert block["block"]["extrinsics"] == ["0x0401", "0x080203"]
    assert block["justifications"] is None


async def test_passthrough(rig):
    client, upstream, _ = rig
    reply = await call(client, "state_call", ["Metadata_metadata", "0x"])
    assert reply["result"] == "from-upstream"
    assert upstream.received[0]["method"] == "state_call"


async def test_unknown_hash_falls_back_upstream(rig):
    client, upstream, _ = rig
    unknown = b"\x99" * 32
    reply = await call(client, "state_getStorage", [hx(KEY_A), hx(unknown)])
    assert reply["result"] == "from-upstream"
    assert upstream.received[0]["method"] == "state_getStorage"


async def test_stale_snapshot_retries_transparently(rig):
    client, _, readers = rig
    readers.reader.fail_with = OSError("stale snapshot")
    reply = await call(client, "state_getStorage", [hx(KEY_A), hx(H2)])
    assert reply["result"] == "0x01"


async def test_invalid_params_is_error_not_crash(rig):
    client, _, _ = rig
    reply = await call(client, "state_getStorage", ["not-hex"])
    assert reply["error"]["code"] == -32602


def _http_get(port, path):
    import http.client

    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
    conn.request("GET", path)
    resp = conn.getresponse()
    return resp.status, resp.read().decode()


async def test_livez(rig):
    client, _, readers = rig
    port = client.remote_address[1]
    readers._reader = None
    status, body = await asyncio.to_thread(_http_get, port, "/livez")
    assert status == 200
    assert body == "ok\n"


async def test_readyz(rig):
    client, _, _ = rig
    port = client.remote_address[1]
    status, body = await asyncio.to_thread(_http_get, port, "/readyz")
    assert status == 200
    assert body == "ok best_block=2\n"


async def test_readyz_unready(rig):
    client, _, readers = rig
    port = client.remote_address[1]
    readers._reader = None
    status, body = await asyncio.to_thread(_http_get, port, "/readyz")
    assert status == 503
    assert "no reader" in body

    readers._reader = StubReader()

    def boom():
        raise OSError("stale snapshot")

    readers.reader.best_block = boom
    status, body = await asyncio.to_thread(_http_get, port, "/readyz")
    assert status == 503
    assert "reader error" in body


async def test_upstream_notification_forwarded(rig):
    client, upstream, _ = rig
    # Prime the upstream connection with any pass-through request.
    await call(client, "system_chain", [])
    notification = {
        "jsonrpc": "2.0",
        "method": "chain_newHead",
        "params": {"subscription": "abc", "result": {}},
    }
    await upstream.connections[0].send(json.dumps(notification))
    assert json.loads(await client.recv()) == notification


# -- router mode (X-Wortel-Mode: router) --------------------------------------


async def test_router_serves_block_addressed_reads(router):
    client, upstream, _ = router
    reply = await call(client, "state_getStorage", [hx(KEY_A), hx(H2)])
    assert reply["result"] == "0x01"
    reply = await call(client, "state_getPairs", [hx(b"\xab"), hx(H2)])
    assert reply["result"] == [[hx(KEY_C), "0x03"]]
    assert (await call(client, "chain_getBlockHash", [2]))["result"] == hx(H2)
    header = (await call(client, "chain_getHeader", [hx(H2)]))["result"]
    assert header["number"] == "0x2"
    block = (await call(client, "chain_getBlock", [hx(H2)]))["result"]
    assert block["block"]["header"] == header
    assert upstream.received == []


async def test_router_has_no_passthrough(router):
    client, upstream, _ = router
    for method, params in [
        ("state_call", ["Metadata_metadata", "0x"]),
        ("system_chain", []),
        ("chain_subscribeNewHeads", []),
    ]:
        reply = await call(client, method, params)
        assert reply["error"]["code"] == -32601
    # Batches are the router's job to split.
    await client.send(json.dumps([{"jsonrpc": "2.0", "id": 1, "method": "x"}]))
    assert json.loads(await client.recv())["error"]["code"] == -32600
    await client.send("not json")
    assert json.loads(await client.recv())["error"]["code"] == -32700
    assert upstream.connections == []


async def test_router_missing_block_is_not_in_snapshot(router):
    client, upstream, _ = router
    unknown = hx(b"\x99" * 32)
    for method, params in [
        ("state_getStorage", [hx(KEY_A), unknown]),
        ("state_queryStorageAt", [[hx(KEY_A)], unknown]),
        ("state_getKeysPaged", [hx(b"\xaa"), 10, None, unknown]),
        ("chain_getBlock", [unknown]),
        ("chain_getBlockHash", [99]),  # above the snapshot's finalized block
        ("chain_getBlockHash", [[2, 99]]),
    ]:
        reply = await call(client, method, params)
        assert reply["error"]["code"] == NOT_IN_SNAPSHOT, method
        assert reply["error"]["message"] == "not in snapshot"
    assert upstream.connections == []


async def test_router_request_never_refreshes(router):
    client, _, readers = router
    refreshes = []

    async def refresh(force=False):
        refreshes.append(force)

    readers.refresh = refresh
    reply = await call(client, "state_getStorage", [hx(KEY_A), hx(b"\x99" * 32)])
    assert reply["error"]["code"] == NOT_IN_SNAPSHOT
    assert (await call(client, "chain_getHeader", [hx(b"\x99" * 32)]))["result"] is None
    assert refreshes == []


async def test_router_unknown_header_is_null(router):
    client, upstream, _ = router
    reply = await call(client, "chain_getHeader", [hx(b"\x99" * 32)])
    assert "error" not in reply and reply["result"] is None
    assert upstream.connections == []


async def test_router_stale_snapshot_answers_at_once_and_refreshes(router):
    client, _, readers = router
    stale = readers.reader
    stale.fail_with = OSError("stale snapshot")
    reply = await call(client, "state_getStorage", [hx(KEY_A), hx(H2)])
    assert reply["error"]["code"] == NOT_IN_SNAPSHOT
    # The forced refresh runs in the background.
    await readers._refresh_task
    assert readers.reader is not stale
    reply = await call(client, "state_getStorage", [hx(KEY_A), hx(H2)])
    assert reply["result"] == "0x01"


async def test_router_keys_paged_count_above_node_max_is_error(rig, router):
    client, _, _ = router
    reply = await call(client, "state_getKeysPaged", [hx(b"\xaa"), 1001, None, hx(H2)])
    assert reply["error"] == {
        "code": 4002,
        "message": "count exceeds maximum value. value: 1001, max: 1000",
    }
    reply = await call(client, "state_getKeysPaged", [hx(b"\xaa"), 1000, None, hx(H2)])
    assert reply["result"] == [hx(KEY_A), hx(KEY_B)]
    # Direct mode keeps truncating to --max-keys-page.
    direct, _, _ = rig
    reply = await call(direct, "state_getKeysPaged", [hx(b"\xaa"), 1001, None, hx(H2)])
    assert reply["result"] == [hx(KEY_A), hx(KEY_B)]


async def test_router_refuses_calls_without_block(router):
    client, upstream, _ = router
    for method, params in [
        ("state_getStorage", [hx(KEY_A)]),
        ("state_getStorage", [hx(KEY_A), None]),
        ("state_queryStorageAt", [[hx(KEY_A)]]),
        ("state_getKeysPaged", [hx(b"\xaa"), 10]),
        ("state_getKeys", [hx(b"\xaa")]),
        ("state_getPairs", [hx(b"\xaa")]),
        ("chain_getHeader", []),
        ("chain_getBlock", []),
        ("chain_getBlockHash", []),
        ("chain_getBlockHash", [None]),
    ]:
        reply = await call(client, method, params)
        assert reply["error"]["code"] == -32602, method
    for method in ("chain_getHead", "chain_getFinalizedHead"):
        assert (await call(client, method, []))["error"]["code"] == -32601
    assert upstream.connections == []


async def test_router_rpc_methods_lists_what_it_serves(router):
    client, _, _ = router
    methods = (await call(client, "rpc_methods", []))["result"]["methods"]
    assert methods == sorted([*ROUTER_CANDIDATES, "rpc_methods"])
    for method in methods:
        reply = await call(client, method, [])
        assert reply.get("error", {}).get("code") != -32601, method


def test_router_methods_flag():
    assert parse_args(["--db", "x"]).router_methods == ROUTER_METHODS
    assert parse_args(["--db", "x", "--router-methods", "all"]).router_methods == (
        ROUTER_CANDIDATES
    )
    assert parse_args(
        ["--db", "x", "--router-methods", "chain_getHeader, state_getStorage"]
    ).router_methods == ("chain_getHeader", "state_getStorage")
    with pytest.raises(SystemExit):
        parse_args(["--db", "x", "--router-methods", "chain_getHead"])


async def test_router_default_set_is_the_benchmarked_one():
    assert set(ROUTER_METHODS) <= set(ROUTER_CANDIDATES)
    readers = ReaderManager(factory=StubReader, refresh_interval=3600)
    async with running(readers, router_methods=ROUTER_METHODS) as (port, _):
        await readers.wait_ready()
        async with router_connect(port) as client:
            methods = (await call(client, "rpc_methods", []))["result"]["methods"]
            assert methods == sorted([*ROUTER_METHODS, "rpc_methods"])
            for method in set(ROUTER_CANDIDATES) - set(ROUTER_METHODS):
                reply = await call(client, method, [hx(H2)])
                assert reply["error"]["code"] == -32601, method


async def test_direct_rpc_methods_passes_through(rig):
    client, upstream, _ = rig
    assert (await call(client, "rpc_methods", []))["result"] == "from-upstream"


async def test_unknown_mode_header_is_rejected(rig):
    client, _, _ = rig
    port = client.remote_address[1]
    with pytest.raises(websockets.exceptions.InvalidStatus) as e:
        async with websockets.connect(
            f"ws://127.0.0.1:{port}", additional_headers={MODE_HEADER: "ruoter"}
        ):
            pass
    assert e.value.response.status_code == 400


# -- /status, startup without a reader ----------------------------------------


def _status(port):
    import http.client

    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
    conn.request("GET", "/status")
    resp = conn.getresponse()
    assert resp.status == 200
    assert resp.getheader("Content-Type") == "application/json"
    return json.loads(resp.read())


async def test_status(rig):
    client, _, readers = rig
    before = readers.snapshot.opened_at
    status = await asyncio.to_thread(_status, client.remote_address[1])
    assert status == {
        "healthy": True,
        "reason": None,
        "best": {"number": 2, "hash": hx(H2)},
        "finalized": {"number": 2, "hash": hx(H2)},
        "snapshot_opened_at": before,
        "refreshing": False,
        "last_refresh_error": None,
        "genesis_hash": hx(GENESIS),
    }


async def test_status_reports_failed_refresh(rig):
    client, _, readers = rig
    port = client.remote_address[1]
    opened_at = readers.snapshot.opened_at
    readers._factory = FlakyFactory(OSError("disk gone"))
    with pytest.raises(RuntimeError):
        await readers.refresh(force=True)
    status = await asyncio.to_thread(_status, port)
    # The old reader keeps serving; the failure and the snapshot's age show.
    assert status["healthy"] is True
    assert status["snapshot_opened_at"] == opened_at
    assert "disk gone" in status["last_refresh_error"]["message"]
    assert status["last_refresh_error"]["at"] >= opened_at
    assert (await call(client, "chain_getBlockHash", [2]))["result"] == hx(H2)

    readers._factory = StubReader
    await readers.refresh(force=True)
    status = await asyncio.to_thread(_status, port)
    assert status["last_refresh_error"] is None
    assert status["snapshot_opened_at"] > opened_at


async def test_serves_without_reader_then_recovers():
    factory = FlakyFactory(OSError("no such database"))
    readers = ReaderManager(
        factory=factory, refresh_interval=3600, open_retry=(0.01, 0.02)
    )
    async with running(readers) as (port, upstream):
        while factory.opens < 2:  # the open is retried, never fatal
            await asyncio.sleep(0.01)
        assert await asyncio.to_thread(_http_get, port, "/livez") == (200, "ok\n")
        assert (await asyncio.to_thread(_http_get, port, "/readyz"))[0] == 503
        status = await asyncio.to_thread(_status, port)
        assert status["healthy"] is False
        assert "no such database" in status["reason"]
        assert status["best"] is None and status["snapshot_opened_at"] is None

        async with router_connect(port) as client:
            reply = await call(client, "state_getStorage", [hx(KEY_A), hx(H2)])
            assert reply["error"]["code"] == NOT_IN_SNAPSHOT
            assert (await call(client, "rpc_methods", []))["result"]["methods"]
        # Direct mode has the node: local methods fall back to it.
        async with websockets.connect(f"ws://127.0.0.1:{port}") as client:
            reply = await call(client, "state_getStorage", [hx(KEY_A), hx(H2)])
            assert reply["result"] == "from-upstream"

        factory.error = None
        await readers.wait_ready()
        status = await asyncio.to_thread(_status, port)
        assert status["healthy"] is True and status["reason"] is None
        async with router_connect(port) as client:
            reply = await call(client, "state_getStorage", [hx(KEY_A), hx(H2)])
            assert reply["result"] == "0x01"


async def test_failed_self_check_is_unhealthy_not_fatal():
    # StubReader has no state for block 1 — what a pruned node looks like.
    factory = FlakyFactory()
    readers = ReaderManager(
        factory=factory,
        refresh_interval=3600,
        check=self_check,
        open_retry=(0.01, 0.02),
    )
    async with running(readers) as (port, _):
        while factory.opens < 2:
            await asyncio.sleep(0.01)
        status = await asyncio.to_thread(_status, port)
        assert status["healthy"] is False
        assert "state for block 1 is unavailable" in status["reason"]
        assert readers.reader is None
