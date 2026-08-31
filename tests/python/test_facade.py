"""Facade behavior against a stub reader and a fake upstream node."""

import asyncio
import json

import pytest
import websockets
from websockets.asyncio.server import serve

from wortel.serve.readers import ReaderManager
from wortel.serve.server import Facade, health_check

from test_header import make_header

H1 = b"\x11" * 32
H2 = b"\x22" * 32
KEY_A = b"\xaa\x01"
KEY_B = b"\xaa\x02"
KEY_C = b"\xab\x01"


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


@pytest.fixture
async def rig():
    """A running facade wired to a stub reader and a fake upstream."""
    upstream = FakeUpstream()
    async with serve(upstream.handler, "127.0.0.1", 0, max_size=None) as up_server:
        up_port = up_server.sockets[0].getsockname()[1]
        readers = ReaderManager(factory=StubReader, refresh_interval=3600)
        await readers.start()
        facade = Facade(readers, f"ws://127.0.0.1:{up_port}", max_keys_page=1000)
        async with serve(
            facade.handle_connection,
            "127.0.0.1",
            0,
            max_size=None,
            process_request=health_check(readers),
        ) as server:
            port = server.sockets[0].getsockname()[1]
            async with websockets.connect(f"ws://127.0.0.1:{port}") as client:
                yield client, upstream, readers
        await readers.stop()


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
