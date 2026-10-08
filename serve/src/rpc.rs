//! JSON-RPC 2.0 envelopes, error objects and positional params.
//!
//! Frames are kept as text and only the envelope is parsed, so a frame the
//! facade does not serve can be forwarded to the node verbatim, and the
//! client's `id` is echoed back exactly as sent (number or string).

use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::value::RawValue;

pub const PARSE_ERROR: i32 = -32700;
pub const INVALID_REQUEST: i32 = -32600;
pub const METHOD_NOT_FOUND: i32 = -32601;
pub const INVALID_PARAMS: i32 = -32602;
pub const INTERNAL_ERROR: i32 = -32603;
/// The upstream node could not be reached for a pass-through call.
pub const UPSTREAM_UNAVAILABLE: i32 = -32001;

/// Router mode: the snapshot cannot answer (unknown block, by-number lookup
/// above the finalized block, stale snapshot, no reader yet). The router
/// retries on the node. Outside the codes jsonrpsee and substrate use.
pub const NOT_IN_SNAPSHOT: i32 = -32090;

/// The node's own limit and error for `state_getKeysPaged` (sc-rpc
/// `state/mod.rs`, sc-rpc-api `state/error.rs`: state base 4000 + 2).
pub const STORAGE_KEYS_PAGED_MAX_COUNT: u32 = 1000;
pub const INVALID_COUNT: i32 = 4002;

/// What a frame turned out to be.
pub enum Frame<'a> {
    Request(Request<'a>),
    /// Valid JSON, but a batch, a notification result, or no `method`.
    NotRequest,
    NotJson,
}

#[derive(Deserialize)]
pub struct Request<'a> {
    #[serde(borrow)]
    pub id: Option<&'a RawValue>,
    pub method: String,
    #[serde(borrow)]
    pub params: Option<&'a RawValue>,
}

impl<'a> Request<'a> {
    /// The id as it appeared in the request, or `null` for a notification.
    pub fn id(&self) -> &'a str {
        self.id.map(RawValue::get).unwrap_or("null")
    }
}

pub fn parse(raw: &str) -> Frame<'_> {
    match serde_json::from_str::<Request>(raw) {
        Ok(req) => Frame::Request(req),
        Err(_) => match serde_json::from_str::<&RawValue>(raw) {
            Ok(_) => Frame::NotRequest,
            Err(_) => Frame::NotJson,
        },
    }
}

/// A handler's failure, mapped to a JSON-RPC error by the connection.
#[derive(Debug, thiserror::Error)]
pub enum Fail {
    /// The snapshot cannot answer; direct mode refreshes and falls back to
    /// the node, router mode answers [`NOT_IN_SNAPSHOT`].
    #[error("{0}")]
    NotInSnapshot(String),
    #[error("{0}")]
    InvalidParams(String),
    /// An error with a code of its own (the node's `4002`).
    #[error("{1}")]
    Code(i32, String),
    #[error("{0}")]
    Internal(String),
}

pub fn ok(id: &str, result: &str) -> String {
    format!(r#"{{"jsonrpc":"2.0","id":{id},"result":{result}}}"#)
}

pub fn error(id: &str, code: i32, message: &str, data: Option<&str>) -> String {
    let message = serde_json::to_string(message).expect("string");
    match data {
        Some(data) => {
            let data = serde_json::to_string(data).expect("string");
            format!(
                r#"{{"jsonrpc":"2.0","id":{id},"error":{{"code":{code},"message":{message},"data":{data}}}}}"#
            )
        }
        None => format!(
            r#"{{"jsonrpc":"2.0","id":{id},"error":{{"code":{code},"message":{message}}}}}"#
        ),
    }
}

pub fn fail(id: &str, fail: &Fail) -> String {
    match fail {
        Fail::NotInSnapshot(detail) => error(id, NOT_IN_SNAPSHOT, "not in snapshot", Some(detail)),
        Fail::InvalidParams(m) => error(id, INVALID_PARAMS, m, None),
        Fail::Code(code, m) => error(id, *code, m, None),
        Fail::Internal(m) => error(id, INTERNAL_ERROR, m, None),
    }
}

/// Positional params. Missing and `null` entries are the same thing.
pub struct Params<'a>(Vec<&'a RawValue>);

pub enum ParamsShape {
    /// A JSON object: named params, which the local handlers do not take.
    Named,
    Invalid,
}

impl<'a> Params<'a> {
    pub fn parse(raw: Option<&'a RawValue>) -> Result<Self, ParamsShape> {
        let Some(raw) = raw else {
            return Ok(Params(Vec::new()));
        };
        let text = raw.get().trim_start();
        if text.starts_with('{') {
            return Err(ParamsShape::Named);
        }
        if text == "null" {
            return Ok(Params(Vec::new()));
        }
        serde_json::from_str::<Vec<&RawValue>>(raw.get())
            .map(Params)
            .map_err(|_| ParamsShape::Invalid)
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn raw(&self, index: usize) -> Option<&'a RawValue> {
        self.0.get(index).copied().filter(|v| v.get() != "null")
    }

    /// Param `index`, which must be present and not `null`.
    pub fn get<T: DeserializeOwned>(&self, index: usize, what: &str) -> Result<T, Fail> {
        match self.raw(index) {
            Some(raw) => serde_json::from_str(raw.get())
                .map_err(|e| Fail::InvalidParams(format!("{what}: {e}"))),
            None => Err(Fail::InvalidParams(format!("{what}: missing"))),
        }
    }

    /// Param `index` if present and not `null`.
    pub fn opt<T: DeserializeOwned>(&self, index: usize, what: &str) -> Result<Option<T>, Fail> {
        match self.raw(index) {
            Some(raw) => serde_json::from_str(raw.get())
                .map(Some)
                .map_err(|e| Fail::InvalidParams(format!("{what}: {e}"))),
            None => Ok(None),
        }
    }
}
