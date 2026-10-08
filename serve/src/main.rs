//! CLI entrypoint: `wortel-serve`.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use wortel::{OpenMode, Wortel};
use wortel_serve::local::{Local, ROUTER_METHODS};
use wortel_serve::readers::{Options, Readers};
use wortel_serve::rpc::STORAGE_KEYS_PAGED_MAX_COUNT;
use wortel_serve::server::{App, serve};

/// JSON-RPC websocket facade over a subtensor archive node's RocksDB.
#[derive(Parser, Debug)]
#[command(name = "wortel-serve")]
struct Args {
    /// Node database path (.../chains/<chain-id>/db/full)
    #[arg(long, env = "WORTEL_DB")]
    db: PathBuf,

    /// Node RPC websocket for pass-through
    #[arg(long, env = "WORTEL_UPSTREAM", default_value = "ws://127.0.0.1:9944")]
    upstream: String,

    /// host:port to bind
    #[arg(long, default_value = "0.0.0.0:9955")]
    listen: String,

    /// Seconds between reader reopens
    #[arg(long, default_value_t = 60.0)]
    refresh_interval: f64,

    /// Cap for state_getKeysPaged count (direct mode)
    #[arg(long, default_value_t = 1000)]
    max_keys_page: u32,

    /// Methods served in router mode: 'all' (for examples/bench_router.py)
    /// or a comma-separated subset of them; default is the benchmarked set
    #[arg(long, env = "WORTEL_ROUTER_METHODS", default_value = "")]
    router_methods: String,

    /// Serve a database that fails the archive self-check (block 1 state
    /// present). For development databases only
    #[arg(long)]
    skip_self_check: bool,

    /// Logging level or filter (tracing syntax)
    #[arg(long, env = "LOG_LEVEL", default_value = "info")]
    log_level: String,
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    let args = Args::parse();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_new(&args.log_level)
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let router_methods = match wortel_serve::router_methods(&args.router_methods) {
        Ok(methods) => methods,
        Err(e) => {
            eprintln!("wortel-serve: error: --router-methods: {e}");
            return std::process::ExitCode::from(2);
        }
    };

    let db = args.db.clone();
    let readers = Readers::new(
        Box::new(move || Wortel::open(&db, OpenMode::ReadOnly)),
        Options {
            refresh_interval: Duration::from_secs_f64(args.refresh_interval),
            check: (!args.skip_self_check).then(|| Box::new(wortel_serve::self_check) as _),
            ..Options::default()
        },
    );
    let app = Arc::new(App {
        readers: readers.clone(),
        upstream_url: args.upstream.clone(),
        direct: Local::new(readers.clone(), args.max_keys_page, false, &[]),
        router: Local::new(
            readers.clone(),
            STORAGE_KEYS_PAGED_MAX_COUNT,
            true,
            &router_methods,
        ),
    });

    // Listen before the first open: /livez and /status must answer during it,
    // and an open that fails is retried, never fatal — as a sidecar, an exited
    // container would mark the node's whole pod not-ready.
    let listener = match tokio::net::TcpListener::bind(&args.listen).await {
        Ok(listener) => listener,
        Err(e) => {
            eprintln!("wortel-serve: cannot listen on {}: {e}", args.listen);
            return std::process::ExitCode::from(1);
        }
    };
    tracing::info!("listening on {}, upstream {}", args.listen, args.upstream);
    if router_methods != ROUTER_METHODS {
        tracing::warn!(
            "router mode serves a non-default set: {}",
            router_methods.join(", ")
        );
    }
    tracing::info!(
        "opening reader at {} (this can take ~a minute cold)",
        args.db.display()
    );
    let _lifecycle = readers.start();
    match serve(app, listener).await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("wortel-serve: server error: {e}");
            std::process::ExitCode::from(1)
        }
    }
}
