//! HTTP probes and the websocket JSON-RPC facade.
//!
//! Storage/chain reads are answered from the local RocksDB snapshot; every
//! other method — `state_call`, metadata, `system_*`, `author_*`,
//! subscriptions, batches, anything unrecognized — forwards verbatim to the
//! node's own RPC over a per-client upstream connection (1:1, so request ids
//! and subscription ids need no translation).
//!
//! Consistency rule: head-returning methods (`chain_getHead`,
//! `chain_getFinalizedHead`, `chain_getHeader`/`chain_getBlock` with no
//! hash) answer from the *snapshot*, so "ask head → read storage at head"
//! always succeeds against one consistent view. A hash the snapshot does not
//! know triggers one refresh + retry, then falls back to upstream for that
//! call.
//!
//! Router mode (handshake header `X-Wortel-Mode: router`) is for a client
//! that holds its own node connections and does its own fallback: no
//! pass-through, no upstream fallback, no refresh or wait caused by a
//! request, and every call must name its block. See `docs/rpc-facade.md`.

use std::sync::Arc;

use axum::Router;
use axum::extract::State;
use axum::extract::ws::{
    Message, WebSocket, WebSocketUpgrade, rejection::WebSocketUpgradeRejection,
};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use futures_util::stream::SplitSink;
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, tungstenite};

use crate::local::Local;
use crate::readers::Readers;
use crate::rpc::{self, Fail, Frame, Params, ParamsShape};

pub const MODE_HEADER: &str = "x-wortel-mode";

/// Incoming frames larger than this are refused; the node's own
/// `--rpc-max-request-size` default is 15 MB.
const MAX_INCOMING: usize = 64 << 20;

pub struct App {
    pub readers: Arc<Readers>,
    pub upstream_url: String,
    pub direct: Local,
    pub router: Local,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Mode {
    Direct,
    Router,
}

impl Mode {
    /// From the handshake headers: no header is direct mode; anything but
    /// `router` is refused, so a typo cannot silently get pass-through.
    fn from_headers(headers: &HeaderMap) -> Option<Mode> {
        match headers.get(MODE_HEADER) {
            None => Some(Mode::Direct),
            Some(value) => match value.to_str().map(|v| v.trim().to_ascii_lowercase()) {
                Ok(v) if v == "router" => Some(Mode::Router),
                _ => None,
            },
        }
    }
}

pub fn router(app: Arc<App>) -> Router {
    Router::new()
        .route("/livez", get(livez))
        .route("/readyz", get(readyz))
        .route("/status", get(status))
        .fallback(get(websocket))
        .with_state(app)
}

/// 200 as long as the event loop answered. Does not touch the reader: a
/// stale snapshot must not restart the pod.
async fn livez() -> &'static str {
    "ok\n"
}

/// 200 when a reader is open and can resolve the best block. Upstream
/// health is deliberately not part of this: local reads keep working when
/// the node RPC is down.
async fn readyz(State(app): State<Arc<App>>) -> Response {
    let Some(snap) = app.readers.snapshot() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "no reader / empty database\n",
        )
            .into_response();
    };
    match tokio::task::spawn_blocking(move || snap.reader.best_block()).await {
        Ok(Ok(Some((best, _)))) => format!("ok best_block={best}\n").into_response(),
        Ok(Ok(None)) => (
            StatusCode::SERVICE_UNAVAILABLE,
            "no reader / empty database\n",
        )
            .into_response(),
        Ok(Err(e)) => (
            StatusCode::SERVICE_UNAVAILABLE,
            format!("reader error: {e}\n"),
        )
            .into_response(),
        Err(e) => (
            StatusCode::SERVICE_UNAVAILABLE,
            format!("reader error: {e}\n"),
        )
            .into_response(),
    }
}

/// Always 200, JSON: health with a reason, the snapshot's best and finalized
/// blocks, its open time, refresh state, genesis hash.
async fn status(State(app): State<Arc<App>>) -> Response {
    let body = app.readers.status().to_string() + "\n";
    ([(header::CONTENT_TYPE, "application/json")], body).into_response()
}

async fn websocket(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    upgrade: Result<WebSocketUpgrade, WebSocketUpgradeRejection>,
) -> Response {
    let Some(mode) = Mode::from_headers(&headers) else {
        return (StatusCode::BAD_REQUEST, "unknown X-Wortel-Mode\n").into_response();
    };
    let upgrade = match upgrade {
        Ok(upgrade) => upgrade,
        Err(rejection) => return rejection.into_response(),
    };
    upgrade
        .max_message_size(MAX_INCOMING)
        .max_frame_size(MAX_INCOMING)
        .on_upgrade(move |socket| connection(app, socket, mode))
}

type UpstreamSink = SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, tungstenite::Message>;

/// Lazy per-client connection to the node RPC; frames forward verbatim in
/// both directions. Upstream closing closes the client connection — the
/// client's own reconnect logic takes over from there.
struct Upstream {
    url: String,
    sink: tokio::sync::Mutex<Option<UpstreamSink>>,
    pump: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl Upstream {
    async fn forward(&self, raw: &str, out: mpsc::Sender<Message>) -> Result<(), String> {
        let mut sink = self.sink.lock().await;
        if sink.is_none() {
            let config = WebSocketConfig::default()
                .max_message_size(None)
                .max_frame_size(None);
            let (ws, _) =
                tokio_tungstenite::connect_async_with_config(&self.url, Some(config), false)
                    .await
                    .map_err(|e| e.to_string())?;
            let (tx, mut rx) = ws.split();
            *sink = Some(tx);
            let pump = tokio::spawn(async move {
                while let Some(Ok(message)) = rx.next().await {
                    let message = match message {
                        tungstenite::Message::Text(text) => Message::Text(text.as_str().into()),
                        tungstenite::Message::Binary(bytes) => Message::Binary(bytes),
                        tungstenite::Message::Close(_) => break,
                        _ => continue,
                    };
                    if out.send(message).await.is_err() {
                        break;
                    }
                }
                tracing::info!("upstream connection ended");
                let _ = out.send(Message::Close(None)).await;
            });
            *self.pump.lock().unwrap() = Some(pump);
        }
        sink.as_mut()
            .expect("connected above")
            .send(tungstenite::Message::Text(raw.into()))
            .await
            .map_err(|e| e.to_string())
    }

    async fn close(&self) {
        if let Some(pump) = self.pump.lock().unwrap().take() {
            pump.abort();
        }
        if let Some(mut sink) = self.sink.lock().await.take() {
            let _ = sink.close().await;
        }
    }
}

/// One downstream websocket connection.
struct Client {
    app: Arc<App>,
    mode: Mode,
    out: mpsc::Sender<Message>,
    upstream: Upstream,
}

impl Client {
    fn local(&self) -> &Local {
        match self.mode {
            Mode::Direct => &self.app.direct,
            Mode::Router => &self.app.router,
        }
    }

    async fn send(&self, text: String) {
        let _ = self.out.send(Message::Text(text.into())).await;
    }

    /// Hand a frame we do not serve to the node. Router mode has no
    /// pass-through — the router holds its own node connections, under its
    /// own limits — and answers the JSON-RPC error instead.
    async fn pass(&self, raw: &str, id: Option<&str>, code: i32, message: &str) {
        if self.mode == Mode::Router {
            self.send(rpc::error(id.unwrap_or("null"), code, message, None))
                .await;
            return;
        }
        self.forward(raw, id).await;
    }

    async fn forward(&self, raw: &str, id: Option<&str>) {
        if let Err(e) = self.upstream.forward(raw, self.out.clone()).await {
            tracing::warn!("upstream forward failed: {e}");
            if let Some(id) = id.filter(|id| *id != "null") {
                self.send(rpc::error(
                    id,
                    rpc::UPSTREAM_UNAVAILABLE,
                    &format!("upstream unavailable: {e}"),
                    None,
                ))
                .await;
            }
        }
    }

    async fn handle(&self, raw: String) {
        let raw = raw.as_str();
        let request = match rpc::parse(raw) {
            Frame::Request(request) => request,
            Frame::NotJson => return self.pass(raw, None, rpc::PARSE_ERROR, "Parse error").await,
            // Batches and non-request frames go upstream verbatim (the router
            // splits batches itself).
            Frame::NotRequest => {
                return self
                    .pass(raw, None, rpc::INVALID_REQUEST, "Invalid request")
                    .await;
            }
        };
        let id = request.id();
        let method = request.method.clone();
        if !self.local().serves(&method) {
            return self
                .pass(raw, Some(id), rpc::METHOD_NOT_FOUND, "Method not found")
                .await;
        }
        let params = match Params::parse(request.params) {
            Ok(params) => params,
            Err(ParamsShape::Named) if self.mode == Mode::Router => {
                let fail = Fail::InvalidParams("router mode: positional params required".into());
                return self.send(rpc::fail(id, &fail)).await;
            }
            Err(ParamsShape::Named) => return self.forward(raw, Some(id)).await,
            Err(ParamsShape::Invalid) => {
                let fail = Fail::InvalidParams("params must be an array".into());
                return self.send(rpc::fail(id, &fail)).await;
            }
        };
        let result = match self.local().call(&method, &params).await {
            Err(Fail::NotInSnapshot(_)) if self.mode == Mode::Direct => {
                // Unknown block hash: likely newer than our snapshot — refresh
                // once and retry before giving up to the upstream fallback.
                let _ = self.app.readers.refresh(false).await;
                self.local().call(&method, &params).await
            }
            other => other,
        };
        match result {
            Ok(result) => self.send(rpc::ok(id, &result)).await,
            Err(Fail::NotInSnapshot(_)) if self.mode == Mode::Direct => {
                // Block/state unknown to the snapshot even after a refresh —
                // the node itself is the fallback for this one call.
                tracing::info!(method, "not in snapshot, falling back upstream");
                self.forward(raw, Some(id)).await;
            }
            Err(fail) => {
                match &fail {
                    Fail::NotInSnapshot(detail) => {
                        tracing::debug!(method, detail, "not in snapshot")
                    }
                    Fail::Internal(detail) => {
                        tracing::error!(method, detail, "local handler failed")
                    }
                    _ => {}
                }
                self.send(rpc::fail(id, &fail)).await
            }
        }
    }
}

async fn connection(app: Arc<App>, socket: WebSocket, mode: Mode) {
    let (mut sink, mut stream) = socket.split();
    let (out, mut out_rx) = mpsc::channel::<Message>(256);
    let writer = tokio::spawn(async move {
        while let Some(message) = out_rx.recv().await {
            let closing = matches!(message, Message::Close(_));
            if sink.send(message).await.is_err() || closing {
                break;
            }
        }
    });
    let client = Arc::new(Client {
        app: app.clone(),
        mode,
        out,
        upstream: Upstream {
            url: app.upstream_url.clone(),
            sink: tokio::sync::Mutex::new(None),
            pump: std::sync::Mutex::new(None),
        },
    });
    let mut tasks = JoinSet::new();
    while let Some(Ok(message)) = stream.next().await {
        let raw = match message {
            Message::Text(text) => text.to_string(),
            Message::Binary(bytes) => String::from_utf8_lossy(&bytes).into_owned(),
            Message::Close(frame) => {
                // Complete the close handshake; the writer stops after it.
                let _ = client.out.send(Message::Close(frame)).await;
                break;
            }
            _ => continue,
        };
        let client = client.clone();
        tasks.spawn(async move { client.handle(raw).await });
        // Keep the set from growing without bound on a busy connection.
        while tasks.try_join_next().is_some() {}
    }
    tasks.abort_all();
    client.upstream.close().await;
    // Dropping the last sender ends the writer once its queue is flushed.
    drop(client);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(1), writer).await;
}

pub async fn serve(app: Arc<App>, listener: tokio::net::TcpListener) -> std::io::Result<()> {
    axum::serve(listener, router(app))
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await
}
