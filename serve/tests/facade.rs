//! Facade behaviour over the wire: a real server over the fixture database
//! (one block, number 42) and a fake upstream node.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use sp_core::H256;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::{self, Message};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use wortel::fixture::{BLOCK_NUMBER, build_db, storage_entries};
use wortel::{OpenMode, Wortel};
use wortel_serve::local::{Local, ROUTER_CANDIDATES, ROUTER_METHODS};
use wortel_serve::readers::{Options, Readers};
use wortel_serve::rpc::{NOT_IN_SNAPSHOT, STORAGE_KEYS_PAGED_MAX_COUNT};
use wortel_serve::server::{App, MODE_HEADER, router};

const UNKNOWN: &str = "0x9999999999999999999999999999999999999999999999999999999999999999";

fn hx(bytes: &[u8]) -> String {
    format!("0x{}", hex(bytes))
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn tempo_prefix() -> String {
    hx(&wortel::keys::storage_prefix("SubtensorModule", "Tempo"))
}

/// Node stand-in: answers everything with "from-upstream", records it all.
#[derive(Default)]
struct FakeUpstream {
    received: Mutex<Vec<Value>>,
    connections: AtomicUsize,
    senders: Mutex<Vec<tokio::sync::mpsc::UnboundedSender<String>>>,
}

impl FakeUpstream {
    async fn start(self: &Arc<Self>) -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let this = self.clone();
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let this = this.clone();
                tokio::spawn(async move {
                    let ws = tokio_tungstenite::accept_async(stream).await.unwrap();
                    this.connections.fetch_add(1, Ordering::SeqCst);
                    let (mut tx, mut rx) = ws.split();
                    let (push, mut pushed) = tokio::sync::mpsc::unbounded_channel::<String>();
                    this.senders.lock().unwrap().push(push);
                    loop {
                        tokio::select! {
                            Some(Ok(msg)) = rx.next() => {
                                let Message::Text(text) = msg else { continue };
                                let msg: Value = serde_json::from_str(text.as_str()).unwrap();
                                let id = msg.get("id").cloned().unwrap_or(Value::Null);
                                this.received.lock().unwrap().push(msg);
                                let reply = json!({"jsonrpc": "2.0", "id": id, "result": "from-upstream"});
                                tx.send(Message::text(reply.to_string())).await.unwrap();
                            }
                            Some(text) = pushed.recv() => {
                                tx.send(Message::text(text)).await.unwrap();
                            }
                            else => break,
                        }
                    }
                });
            }
        });
        port
    }

    fn received(&self) -> Vec<Value> {
        self.received.lock().unwrap().clone()
    }

    fn connections(&self) -> usize {
        self.connections.load(Ordering::SeqCst)
    }

    /// Push a frame (a subscription notification) down the first connection.
    fn notify(&self, text: &str) {
        self.senders.lock().unwrap()[0]
            .send(text.to_string())
            .unwrap();
    }
}

/// Reader factory over the fixture whose opens fail while `fail` is set.
struct Factory {
    path: std::path::PathBuf,
    fail: Arc<AtomicBool>,
    opens: Arc<AtomicUsize>,
}

struct Rig {
    port: u16,
    upstream: Arc<FakeUpstream>,
    readers: Arc<Readers>,
    opens: Arc<AtomicUsize>,
    fail: Arc<AtomicBool>,
    block_hash: H256,
    extrinsics: Vec<Vec<u8>>,
    _dir: tempfile::TempDir,
}

struct RigOptions {
    check: bool,
    fail_first: bool,
    router_methods: Vec<String>,
}

impl Default for RigOptions {
    fn default() -> Self {
        RigOptions {
            check: false,
            fail_first: false,
            // The rig advertises every candidate in router mode, so each rule
            // is tested whatever the benchmark-gated default set is.
            router_methods: ROUTER_CANDIDATES.iter().map(|m| m.to_string()).collect(),
        }
    }
}

/// A running facade over the fixture and a fake upstream. Does not wait for
/// the first open.
async fn rig_with(options: RigOptions) -> Rig {
    let dir = tempfile::tempdir().unwrap();
    let (block_hash, extrinsics) = build_db(dir.path());
    let upstream = Arc::new(FakeUpstream::default());
    let up_port = upstream.start().await;

    let factory = Factory {
        path: dir.path().to_path_buf(),
        fail: Arc::new(AtomicBool::new(options.fail_first)),
        opens: Arc::new(AtomicUsize::new(0)),
    };
    let (fail, opens) = (factory.fail.clone(), factory.opens.clone());
    let readers = Readers::new(
        Box::new(move || {
            factory.opens.fetch_add(1, Ordering::SeqCst);
            if factory.fail.load(Ordering::SeqCst) {
                return Err(wortel::Error::InvalidInput("no such database".into()));
            }
            Wortel::open(&factory.path, OpenMode::ReadOnly)
        }),
        Options {
            refresh_interval: Duration::from_secs(3600),
            min_refresh_gap: Duration::from_millis(300),
            open_retry: (Duration::from_millis(10), Duration::from_millis(20)),
            check: options
                .check
                .then(|| Box::new(wortel_serve::self_check) as _),
        },
    );
    let app = Arc::new(App {
        readers: readers.clone(),
        upstream_url: format!("ws://127.0.0.1:{up_port}"),
        direct: Local::new(readers.clone(), 1000, false, &[]),
        router: Local::new(
            readers.clone(),
            STORAGE_KEYS_PAGED_MAX_COUNT,
            true,
            &options.router_methods,
        ),
    });
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(axum::serve(listener, router(app)).into_future());
    readers.start();
    Rig {
        port,
        upstream,
        readers,
        opens,
        fail,
        block_hash,
        extrinsics,
        _dir: dir,
    }
}

async fn rig() -> Rig {
    let rig = rig_with(RigOptions::default()).await;
    rig.readers.wait_ready().await;
    rig
}

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

async fn connect(port: u16, mode: Option<&str>) -> Result<Ws, tungstenite::Error> {
    let mut request = format!("ws://127.0.0.1:{port}")
        .into_client_request()
        .unwrap();
    if let Some(mode) = mode {
        request
            .headers_mut()
            .insert(MODE_HEADER, mode.parse().unwrap());
    }
    tokio_tungstenite::connect_async(request)
        .await
        .map(|(ws, _)| ws)
}

async fn direct(rig: &Rig) -> Ws {
    connect(rig.port, None).await.unwrap()
}

async fn router_ws(rig: &Rig) -> Ws {
    connect(rig.port, Some("router")).await.unwrap()
}

async fn call(ws: &mut Ws, method: &str, params: Value) -> Value {
    send(
        ws,
        json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).to_string(),
    )
    .await
}

async fn send(ws: &mut Ws, text: String) -> Value {
    ws.send(Message::text(text)).await.unwrap();
    recv(ws).await
}

async fn recv(ws: &mut Ws) -> Value {
    let reply = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .expect("reply within 5 s")
        .unwrap()
        .unwrap();
    serde_json::from_str(reply.to_text().unwrap()).unwrap()
}

fn code(reply: &Value) -> Option<i64> {
    reply["error"]["code"].as_i64()
}

async fn http_get(port: u16, path: &str) -> (u16, String, String) {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    stream
        .write_all(
            format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                .as_bytes(),
        )
        .await
        .unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).await.unwrap();
    let (head, body) = response.split_once("\r\n\r\n").unwrap();
    let status = head.split(' ').nth(1).unwrap().parse().unwrap();
    let content_type = head
        .lines()
        .find_map(|l| l.strip_prefix("content-type: "))
        .unwrap_or("")
        .to_string();
    (status, content_type, body.to_string())
}

async fn status(port: u16) -> Value {
    let (status, content_type, body) = http_get(port, "/status").await;
    assert_eq!(status, 200);
    assert_eq!(content_type, "application/json");
    serde_json::from_str(&body).unwrap()
}

fn sorted_tempo_keys() -> Vec<String> {
    let prefix = wortel::keys::storage_prefix("SubtensorModule", "Tempo");
    let mut keys: Vec<Vec<u8>> = storage_entries()
        .into_iter()
        .map(|(k, _)| k)
        .filter(|k| k.starts_with(&prefix))
        .collect();
    keys.sort();
    keys.iter().map(|k| hx(k)).collect()
}

// -- direct mode ---------------------------------------------------------------

#[tokio::test]
async fn get_storage() {
    let rig = rig().await;
    let mut ws = direct(&rig).await;
    let at = hx(rig.block_hash.as_bytes());
    let (key, value) = &storage_entries()[0];
    let reply = call(&mut ws, "state_getStorage", json!([hx(key), at])).await;
    assert_eq!(reply["result"], hx(value));
    let reply = call(&mut ws, "state_getStorage", json!(["0xffff", at])).await;
    assert_eq!(reply["result"], Value::Null);
    // No `at`: the snapshot's best block.
    let reply = call(&mut ws, "state_getStorageAt", json!([hx(key)])).await;
    assert_eq!(reply["result"], hx(value));
}

#[tokio::test]
async fn query_storage_at() {
    let rig = rig().await;
    let mut ws = direct(&rig).await;
    let at = hx(rig.block_hash.as_bytes());
    let (key, value) = &storage_entries()[0];
    let reply = call(
        &mut ws,
        "state_queryStorageAt",
        json!([[hx(key), "0xff"], at]),
    )
    .await;
    assert_eq!(
        reply["result"],
        json!([{"block": at, "changes": [[hx(key), hx(value)], ["0xff", null]]}])
    );
}

#[tokio::test]
async fn keys_paged_pairs_and_keys() {
    let rig = rig().await;
    let mut ws = direct(&rig).await;
    let at = hx(rig.block_hash.as_bytes());
    let keys = sorted_tempo_keys();
    let reply = call(
        &mut ws,
        "state_getKeysPaged",
        json!([tempo_prefix(), 10, null, at]),
    )
    .await;
    assert_eq!(reply["result"], json!(keys[..10]));
    // Resume strictly after the 10th key.
    let reply = call(
        &mut ws,
        "state_getKeysPaged",
        json!([tempo_prefix(), 10, keys[9], at]),
    )
    .await;
    assert_eq!(reply["result"], json!(keys[10..20]));
    // Direct mode truncates to --max-keys-page instead of erroring.
    let reply = call(
        &mut ws,
        "state_getKeysPaged",
        json!([tempo_prefix(), 5000, null, at]),
    )
    .await;
    assert_eq!(reply["result"], json!(keys));

    let reply = call(&mut ws, "state_getKeys", json!([tempo_prefix(), at])).await;
    assert_eq!(reply["result"], json!(keys));
    let reply = call(&mut ws, "state_getPairs", json!([tempo_prefix()])).await;
    let pairs = reply["result"].as_array().unwrap();
    assert_eq!(pairs.len(), keys.len());
    assert_eq!(pairs[0][0], keys[0]);
    let (k0, v0) = storage_entries()
        .into_iter()
        .find(|(k, _)| hx(k) == keys[0])
        .unwrap();
    assert_eq!(pairs[0], json!([hx(&k0), hx(&v0)]));
}

#[tokio::test]
async fn chain_methods() {
    let rig = rig().await;
    let mut ws = direct(&rig).await;
    let hash = hx(rig.block_hash.as_bytes());
    assert_eq!(
        call(&mut ws, "chain_getBlockHash", json!([42])).await["result"],
        hash
    );
    assert_eq!(
        call(&mut ws, "chain_getBlockHash", json!(["0x2a"])).await["result"],
        hash
    );
    assert_eq!(
        call(&mut ws, "chain_getBlockHash", json!([])).await["result"],
        hash
    );
    assert_eq!(
        call(&mut ws, "chain_getBlockHash", json!([null])).await["result"],
        hash
    );
    assert_eq!(
        call(&mut ws, "chain_getBlockHash", json!([[42, 7]])).await["result"],
        json!([hash, null])
    );
    assert_eq!(
        call(&mut ws, "chain_getHead", json!([])).await["result"],
        hash
    );
    assert_eq!(
        call(&mut ws, "chain_getFinalizedHead", json!([])).await["result"],
        hash
    );
}

#[tokio::test]
async fn header_and_block_json() {
    let rig = rig().await;
    let mut ws = direct(&rig).await;
    let hash = hx(rig.block_hash.as_bytes());
    let header = call(&mut ws, "chain_getHeader", json!([hash])).await["result"].clone();
    assert_eq!(header["number"], "0x2a");
    assert_eq!(header["parentHash"], hx(&[7u8; 32]));
    assert_eq!(header["extrinsicsRoot"], hx(&[8u8; 32]));
    assert_eq!(header["digest"], json!({"logs": []}));
    assert_eq!(header["stateRoot"].as_str().unwrap().len(), 66);
    // No hash: the snapshot's head.
    assert_eq!(
        call(&mut ws, "chain_getHeader", json!([])).await["result"],
        header
    );

    let block = call(&mut ws, "chain_getBlock", json!([hash])).await["result"].clone();
    assert_eq!(block["block"]["header"], header);
    let expected: Vec<String> = rig.extrinsics.iter().map(|xt| hx(xt)).collect();
    assert_eq!(block["block"]["extrinsics"], json!(expected));
    assert_eq!(block["justifications"], Value::Null);
}

#[tokio::test]
async fn unknown_blocks_fall_back_upstream() {
    let rig = rig().await;
    let mut ws = direct(&rig).await;
    let (key, _) = &storage_entries()[0];
    // Beyond the snapshot head the node may know the block — never answer
    // null locally, forward instead.
    for (method, params) in [
        ("chain_getBlockHash", json!([99])),
        ("chain_getBlockHash", json!([[42, 99]])),
        ("chain_getHeader", json!([UNKNOWN])),
        ("chain_getBlock", json!([UNKNOWN])),
        ("state_getStorage", json!([hx(key), UNKNOWN])),
    ] {
        let reply = call(&mut ws, method, params).await;
        assert_eq!(reply["result"], "from-upstream", "{method}");
        assert_eq!(rig.upstream.received().last().unwrap()["method"], method);
    }
}

#[tokio::test]
async fn passthrough() {
    let rig = rig().await;
    let mut ws = direct(&rig).await;
    let reply = call(&mut ws, "state_call", json!(["Metadata_metadata", "0x"])).await;
    assert_eq!(reply["result"], "from-upstream");
    assert_eq!(
        call(&mut ws, "rpc_methods", json!([])).await["result"],
        "from-upstream"
    );
    // Named params are the node's to interpret.
    let reply = send(
        &mut ws,
        json!({"jsonrpc": "2.0", "id": 7, "method": "state_getStorage", "params": {"key": "0x00"}})
            .to_string(),
    )
    .await;
    assert_eq!(reply["result"], "from-upstream");
    assert_eq!(reply["id"], 7);
    // A batch goes up verbatim.
    let reply = send(
        &mut ws,
        json!([{"jsonrpc": "2.0", "id": 1, "method": "x"}]).to_string(),
    )
    .await;
    assert_eq!(reply["result"], "from-upstream");
    let methods: Vec<String> = rig
        .upstream
        .received()
        .iter()
        .map(|m| {
            // The batch arrived as the array it was sent as.
            let method = m.pointer("/method").or_else(|| m.pointer("/0/method"));
            method.unwrap().as_str().unwrap().to_string()
        })
        .collect();
    assert_eq!(
        methods,
        ["state_call", "rpc_methods", "state_getStorage", "x"]
    );
    assert_eq!(rig.upstream.connections(), 1);

    // Notifications from the node reach the client untouched.
    let notification = json!({"jsonrpc": "2.0", "method": "chain_newHead", "params": {"subscription": "abc", "result": {}}});
    rig.upstream.notify(&notification.to_string());
    assert_eq!(recv(&mut ws).await, notification);
}

#[tokio::test]
async fn invalid_params_is_error_not_crash() {
    let rig = rig().await;
    let mut ws = direct(&rig).await;
    let reply = call(&mut ws, "state_getStorage", json!(["not-hex"])).await;
    assert_eq!(code(&reply), Some(-32602));
    let reply = call(&mut ws, "chain_getBlockHash", json!([[1, null]])).await;
    assert_eq!(code(&reply), Some(-32602));
    let reply = call(&mut ws, "state_getStorage", json!("0x00")).await;
    assert_eq!(code(&reply), Some(-32602));
    // The id comes back as sent.
    let reply = send(
        &mut ws,
        json!({"jsonrpc": "2.0", "id": "abc", "method": "state_getStorage", "params": ["x"]})
            .to_string(),
    )
    .await;
    assert_eq!(reply["id"], "abc");
}

#[tokio::test]
async fn probes() {
    let rig = rig().await;
    assert_eq!(http_get(rig.port, "/livez").await.0, 200);
    let (status, _, body) = http_get(rig.port, "/readyz").await;
    assert_eq!((status, body.as_str()), (200, "ok best_block=42\n"));
    let s = status_doc(&rig).await;
    let hash = hx(rig.block_hash.as_bytes());
    assert_eq!(s["healthy"], true);
    assert_eq!(s["reason"], Value::Null);
    assert_eq!(s["best"], json!({"number": BLOCK_NUMBER, "hash": hash}));
    assert_eq!(
        s["finalized"],
        json!({"number": BLOCK_NUMBER, "hash": hash})
    );
    assert_eq!(
        s["snapshot_opened_at"],
        json!(rig.readers.snapshot().unwrap().opened_at)
    );
    assert_eq!(s["refreshing"], false);
    assert_eq!(s["last_refresh_error"], Value::Null);
    assert_eq!(s["genesis_hash"], hx(&[1u8; 32]));
}

async fn status_doc(rig: &Rig) -> Value {
    status(rig.port).await
}

// -- router mode ---------------------------------------------------------------

#[tokio::test]
async fn router_serves_block_addressed_reads() {
    let rig = rig().await;
    let mut ws = router_ws(&rig).await;
    let at = hx(rig.block_hash.as_bytes());
    let (key, value) = &storage_entries()[0];
    assert_eq!(
        call(&mut ws, "state_getStorage", json!([hx(key), at])).await["result"],
        hx(value)
    );
    assert_eq!(
        call(&mut ws, "chain_getBlockHash", json!([42])).await["result"],
        at
    );
    let header = call(&mut ws, "chain_getHeader", json!([at])).await["result"].clone();
    assert_eq!(header["number"], "0x2a");
    let block = call(&mut ws, "chain_getBlock", json!([at])).await["result"].clone();
    assert_eq!(block["block"]["header"], header);
    let pairs = call(&mut ws, "state_getPairs", json!([tempo_prefix(), at])).await;
    assert_eq!(pairs["result"].as_array().unwrap().len(), 50);
    assert!(rig.upstream.received().is_empty());
}

#[tokio::test]
async fn router_has_no_passthrough() {
    let rig = rig().await;
    let mut ws = router_ws(&rig).await;
    for (method, params) in [
        ("state_call", json!(["Metadata_metadata", "0x"])),
        ("system_chain", json!([])),
        ("chain_subscribeNewHeads", json!([])),
    ] {
        let reply = call(&mut ws, method, params).await;
        assert_eq!(code(&reply), Some(-32601), "{method}");
        assert_eq!(reply["error"]["message"], "Method not found");
    }
    // Batches are the router's job to split.
    let reply = send(
        &mut ws,
        json!([{"jsonrpc": "2.0", "id": 1, "method": "x"}]).to_string(),
    )
    .await;
    assert_eq!(code(&reply), Some(-32600));
    let reply = send(&mut ws, "not json".to_string()).await;
    assert_eq!(code(&reply), Some(-32700));
    let reply = send(
        &mut ws,
        json!({"jsonrpc": "2.0", "id": 1, "method": "state_getStorage", "params": {"key": "0x"}})
            .to_string(),
    )
    .await;
    assert_eq!(code(&reply), Some(-32602));
    assert_eq!(rig.upstream.connections(), 0);
}

#[tokio::test]
async fn router_missing_block_is_not_in_snapshot() {
    let rig = rig().await;
    let mut ws = router_ws(&rig).await;
    let (key, _) = &storage_entries()[0];
    for (method, params) in [
        ("state_getStorage", json!([hx(key), UNKNOWN])),
        ("state_queryStorageAt", json!([[hx(key)], UNKNOWN])),
        (
            "state_getKeysPaged",
            json!([tempo_prefix(), 10, null, UNKNOWN]),
        ),
        ("chain_getBlock", json!([UNKNOWN])),
        ("chain_getBlockHash", json!([43])), // above the snapshot's finalized block
        ("chain_getBlockHash", json!([[42, 43]])),
    ] {
        let reply = call(&mut ws, method, params).await;
        assert_eq!(code(&reply), Some(NOT_IN_SNAPSHOT as i64), "{method}");
        assert_eq!(reply["error"]["message"], "not in snapshot");
        assert!(reply["error"]["data"].is_string());
    }
    assert_eq!(rig.upstream.connections(), 0);
}

#[tokio::test]
async fn router_request_never_refreshes() {
    let rig = rig().await;
    let mut ws = router_ws(&rig).await;
    let opens = rig.opens.load(Ordering::SeqCst);
    let (key, _) = &storage_entries()[0];
    let reply = call(&mut ws, "state_getStorage", json!([hx(key), UNKNOWN])).await;
    assert_eq!(code(&reply), Some(NOT_IN_SNAPSHOT as i64));
    let reply = call(&mut ws, "chain_getHeader", json!([UNKNOWN])).await;
    assert!(reply.get("error").is_none());
    assert_eq!(reply["result"], Value::Null);
    assert_eq!(rig.opens.load(Ordering::SeqCst), opens);

    // Direct mode does refresh (once, rate-limited) before falling back.
    tokio::time::sleep(Duration::from_millis(400)).await;
    let mut ws = direct(&rig).await;
    assert_eq!(
        call(&mut ws, "chain_getHeader", json!([UNKNOWN])).await["result"],
        "from-upstream"
    );
    assert_eq!(rig.opens.load(Ordering::SeqCst), opens + 1);
    assert_eq!(
        call(&mut ws, "chain_getHeader", json!([UNKNOWN])).await["result"],
        "from-upstream"
    );
    assert_eq!(rig.opens.load(Ordering::SeqCst), opens + 1);
}

#[tokio::test]
async fn router_keys_paged_count_above_node_max_is_error() {
    let rig = rig().await;
    let mut ws = router_ws(&rig).await;
    let at = hx(rig.block_hash.as_bytes());
    let reply = call(
        &mut ws,
        "state_getKeysPaged",
        json!([tempo_prefix(), 1001, null, at]),
    )
    .await;
    assert_eq!(
        reply["error"],
        json!({"code": 4002, "message": "count exceeds maximum value. value: 1001, max: 1000"})
    );
    let reply = call(
        &mut ws,
        "state_getKeysPaged",
        json!([tempo_prefix(), 1000, null, at]),
    )
    .await;
    assert_eq!(reply["result"], json!(sorted_tempo_keys()));
}

#[tokio::test]
async fn router_refuses_calls_without_block() {
    let rig = rig().await;
    let mut ws = router_ws(&rig).await;
    let key = hx(&storage_entries()[0].0);
    for (method, params) in [
        ("state_getStorage", json!([key])),
        ("state_getStorage", json!([key, null])),
        ("state_queryStorageAt", json!([[key]])),
        ("state_getKeysPaged", json!([tempo_prefix(), 10])),
        ("state_getKeys", json!([tempo_prefix()])),
        ("state_getPairs", json!([tempo_prefix()])),
        ("chain_getHeader", json!([])),
        ("chain_getBlock", json!([])),
        ("chain_getBlockHash", json!([])),
        ("chain_getBlockHash", json!([null])),
    ] {
        let reply = call(&mut ws, method, params).await;
        assert_eq!(code(&reply), Some(-32602), "{method}");
    }
    for method in ["chain_getHead", "chain_getFinalizedHead"] {
        assert_eq!(code(&call(&mut ws, method, json!([])).await), Some(-32601));
    }
    assert_eq!(rig.upstream.connections(), 0);
}

#[tokio::test]
async fn router_rpc_methods_lists_what_it_serves() {
    let rig = rig().await;
    let mut ws = router_ws(&rig).await;
    let methods = call(&mut ws, "rpc_methods", json!([])).await["result"]["methods"].clone();
    let mut expected: Vec<String> = ROUTER_CANDIDATES.iter().map(|m| m.to_string()).collect();
    expected.push("rpc_methods".into());
    expected.sort();
    assert_eq!(methods, json!(expected));
    for method in methods.as_array().unwrap() {
        let reply = call(&mut ws, method.as_str().unwrap(), json!([])).await;
        assert_ne!(code(&reply), Some(-32601), "{method}");
    }
}

#[tokio::test]
async fn router_default_set_is_the_benchmarked_one() {
    assert!(ROUTER_METHODS.iter().all(|m| ROUTER_CANDIDATES.contains(m)));
    let rig = rig_with(RigOptions {
        router_methods: ROUTER_METHODS.iter().map(|m| m.to_string()).collect(),
        ..RigOptions::default()
    })
    .await;
    rig.readers.wait_ready().await;
    let mut ws = router_ws(&rig).await;
    let methods = call(&mut ws, "rpc_methods", json!([])).await["result"]["methods"].clone();
    let mut expected: Vec<String> = ROUTER_METHODS.iter().map(|m| m.to_string()).collect();
    expected.push("rpc_methods".into());
    expected.sort();
    assert_eq!(methods, json!(expected));
    let at = hx(rig.block_hash.as_bytes());
    for method in ROUTER_CANDIDATES
        .iter()
        .filter(|m| !ROUTER_METHODS.contains(m))
    {
        assert_eq!(
            code(&call(&mut ws, method, json!([at])).await),
            Some(-32601),
            "{method}"
        );
    }
    assert_eq!(wortel_serve::router_methods("").unwrap(), ROUTER_METHODS);
    assert_eq!(
        wortel_serve::router_methods("all").unwrap(),
        ROUTER_CANDIDATES
    );
    assert_eq!(
        wortel_serve::router_methods("chain_getHeader, state_getStorage").unwrap(),
        ["chain_getHeader", "state_getStorage"]
    );
    assert!(wortel_serve::router_methods("chain_getHead").is_err());
}

#[tokio::test]
async fn unknown_mode_header_is_rejected() {
    let rig = rig().await;
    match connect(rig.port, Some("ruoter")).await {
        Err(tungstenite::Error::Http(response)) => assert_eq!(response.status(), 400),
        other => panic!("expected HTTP 400, got {other:?}"),
    }
}

// -- /status, startup without a reader -----------------------------------------

#[tokio::test]
async fn status_reports_failed_refresh() {
    let rig = rig().await;
    let opened_at = rig.readers.snapshot().unwrap().opened_at;
    rig.fail.store(true, Ordering::SeqCst);
    assert!(rig.readers.refresh(true).await.is_err());
    let s = status(rig.port).await;
    // The old reader keeps serving; the failure and the snapshot's age show.
    assert_eq!(s["healthy"], true);
    assert_eq!(s["snapshot_opened_at"], json!(opened_at));
    assert!(
        s["last_refresh_error"]["message"]
            .as_str()
            .unwrap()
            .contains("no such database")
    );
    assert!(s["last_refresh_error"]["at"].as_f64().unwrap() >= opened_at);
    let mut ws = direct(&rig).await;
    assert_eq!(
        call(&mut ws, "chain_getBlockHash", json!([42])).await["result"],
        hx(rig.block_hash.as_bytes())
    );

    rig.fail.store(false, Ordering::SeqCst);
    rig.readers.refresh(true).await.unwrap();
    let s = status(rig.port).await;
    assert_eq!(s["last_refresh_error"], Value::Null);
    assert!(s["snapshot_opened_at"].as_f64().unwrap() >= opened_at);
}

#[tokio::test]
async fn serves_without_reader_then_recovers() {
    let rig = rig_with(RigOptions {
        fail_first: true,
        ..RigOptions::default()
    })
    .await;
    while rig.opens.load(Ordering::SeqCst) < 2 {
        // The open is retried, never fatal.
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(http_get(rig.port, "/livez").await.0, 200);
    assert_eq!(http_get(rig.port, "/readyz").await.0, 503);
    let s = status(rig.port).await;
    assert_eq!(s["healthy"], false);
    assert!(s["reason"].as_str().unwrap().contains("no such database"));
    assert_eq!(s["best"], Value::Null);
    assert_eq!(s["snapshot_opened_at"], Value::Null);

    let at = hx(rig.block_hash.as_bytes());
    let key = hx(&storage_entries()[0].0);
    let mut ws = router_ws(&rig).await;
    let reply = call(&mut ws, "state_getStorage", json!([key, at])).await;
    assert_eq!(code(&reply), Some(NOT_IN_SNAPSHOT as i64));
    assert!(call(&mut ws, "rpc_methods", json!([])).await["result"]["methods"].is_array());
    // Direct mode has the node: local methods fall back to it.
    let mut ws = direct(&rig).await;
    assert_eq!(
        call(&mut ws, "state_getStorage", json!([key, at])).await["result"],
        "from-upstream"
    );

    rig.fail.store(false, Ordering::SeqCst);
    rig.readers.wait_ready().await;
    let s = status(rig.port).await;
    assert_eq!(s["healthy"], true);
    assert_eq!(s["reason"], Value::Null);
    let mut ws = router_ws(&rig).await;
    let reply = call(&mut ws, "state_getStorage", json!([key, at])).await;
    assert_eq!(reply["result"], hx(&storage_entries()[0].1));
}

#[tokio::test]
async fn failed_self_check_is_unhealthy_not_fatal() {
    // The fixture has no block 1 — what a pruned or wrong database looks like.
    let rig = rig_with(RigOptions {
        check: true,
        ..RigOptions::default()
    })
    .await;
    while rig.opens.load(Ordering::SeqCst) < 2 {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let s = status(rig.port).await;
    assert_eq!(s["healthy"], false);
    assert!(s["reason"].as_str().unwrap().contains("block 1"));
    assert!(rig.readers.snapshot().is_none());
}
