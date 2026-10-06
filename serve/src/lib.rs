//! `wortel-serve`: a JSON-RPC websocket sidecar over a [`wortel::Wortel`]
//! reader. Library form so the integration tests can run the server
//! in-process; `main.rs` is the CLI.

pub mod local;
pub mod readers;
pub mod rpc;
pub mod server;

use wortel::Wortel;

/// Reject a node that is not a plain archive node: state for an old block
/// must be present (`MissingState`). Failing keeps the facade unhealthy; the
/// reader manager retries the open.
pub fn self_check(reader: &Wortel) -> Result<(), String> {
    let hash1 = reader
        .block_hash(1)
        .map_err(|e| e.to_string())?
        .ok_or_else(|| "database has no block 1 — not an archive node?".to_string())?;
    let key = wortel::keys::storage_prefix("Timestamp", "Now");
    match reader.get_storage(&hash1, &key) {
        Ok(_) => Ok(()),
        Err(e @ (wortel::Error::MissingState(..) | wortel::Error::UnknownBlock(_))) => {
            Err(format!(
                "state for block 1 is unavailable ({e}); the node must run with \
             --state-pruning archive (plain, not archive-canonical)"
            ))
        }
        Err(e) => Err(e.to_string()),
    }
}

/// `--router-methods`: empty is the default set, `all` every candidate, else
/// a comma-separated subset of the candidates.
pub fn router_methods(spec: &str) -> Result<Vec<String>, String> {
    let spec = spec.trim();
    if spec.is_empty() {
        return Ok(local::ROUTER_METHODS
            .iter()
            .map(|m| m.to_string())
            .collect());
    }
    if spec == "all" {
        return Ok(local::ROUTER_CANDIDATES
            .iter()
            .map(|m| m.to_string())
            .collect());
    }
    let methods: Vec<String> = spec.split(',').map(|m| m.trim().to_string()).collect();
    let unknown: Vec<&str> = methods
        .iter()
        .filter(|m| !local::ROUTER_CANDIDATES.contains(&m.as_str()))
        .map(String::as_str)
        .collect();
    if !unknown.is_empty() {
        return Err(format!(
            "{} cannot be served in router mode; choose from {}",
            unknown.join(", "),
            local::ROUTER_CANDIDATES.join(", ")
        ));
    }
    Ok(methods)
}
