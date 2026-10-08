//! Reader lifecycle: open with retry, background refresh, atomic swap,
//! stale-snapshot handling, status.
//!
//! A readonly [`Wortel`] is frozen at the on-disk snapshot taken at open
//! time, so the manager reopens a fresh reader every `refresh_interval` and
//! swaps it in atomically. In-flight calls keep the reader they started
//! with; the old one is dropped when the last of them returns.
//!
//! The manager never gives up: a failed first open (or a failed `check`) is
//! retried with backoff while [`Readers::status`] reports `healthy: false`;
//! a failed refresh keeps the old reader and is reported as
//! `last_refresh_error`.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use arc_swap::ArcSwapOption;
use serde_json::{Value, json};
use sp_core::H256;
use tokio::sync::watch;
use wortel::Wortel;

pub type Factory = Box<dyn Fn() -> wortel::Result<Wortel> + Send + Sync>;
/// Run on the first reader before it is served; an `Err` rejects it (wrong
/// pruning mode) and the open is retried like any other failed open.
pub type Check = Box<dyn Fn(&Wortel) -> Result<(), String> + Send + Sync>;

/// What the current reader sees. Fixed for the reader's lifetime, so it is
/// read once at open and `status()` never touches RocksDB.
pub struct Snapshot {
    pub reader: Wortel,
    /// Unix time the open started; the view is no newer than this.
    pub opened_at: f64,
    pub best: (u32, H256),
    pub finalized: Option<(u32, H256)>,
    pub genesis: Option<H256>,
}

pub struct Options {
    pub refresh_interval: Duration,
    /// Non-forced refreshes closer together than this are skipped.
    pub min_refresh_gap: Duration,
    /// `(initial, max)` backoff between failed opens.
    pub open_retry: (Duration, Duration),
    pub check: Option<Check>,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            refresh_interval: Duration::from_secs(60),
            min_refresh_gap: Duration::from_secs(5),
            open_retry: (Duration::from_secs(1), Duration::from_secs(60)),
            check: None,
        }
    }
}

#[derive(Default)]
struct State {
    last_refresh: Option<Instant>,
    last_error: Option<(String, f64)>,
    refreshing: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum ReadError {
    #[error("no snapshot open")]
    NoReader,
    #[error("stale snapshot")]
    Stale,
    /// Unknown block or missing state: the caller decides.
    #[error("{0}")]
    Lookup(wortel::Error),
    #[error("refresh failed: {0}")]
    Refresh(String),
    #[error("{0}")]
    Other(wortel::Error),
}

pub struct Readers {
    factory: Factory,
    options: Options,
    current: ArcSwapOption<Snapshot>,
    /// Held for the whole of one open; `refresh` callers queue on it and
    /// then find the swap already done.
    refresh_lock: Arc<tokio::sync::Mutex<()>>,
    generation: AtomicU64,
    state: Mutex<State>,
    ready: watch::Sender<bool>,
}

fn now() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

fn block_json(block: Option<(u32, H256)>) -> Value {
    match block {
        Some((number, hash)) => json!({ "number": number, "hash": hash }),
        None => Value::Null,
    }
}

impl Readers {
    pub fn new(factory: Factory, options: Options) -> Arc<Self> {
        Arc::new(Readers {
            factory,
            options,
            current: ArcSwapOption::empty(),
            refresh_lock: Arc::new(tokio::sync::Mutex::new(())),
            generation: AtomicU64::new(0),
            state: Mutex::new(State::default()),
            ready: watch::Sender::new(false),
        })
    }

    /// Start opening in the background and return at once, so the caller
    /// can listen (and answer probes) during the first open.
    pub fn start(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let this = self.clone();
        tokio::spawn(async move { this.run().await })
    }

    pub async fn wait_ready(&self) {
        let mut rx = self.ready.subscribe();
        while !*rx.borrow_and_update() {
            if rx.changed().await.is_err() {
                return;
            }
        }
    }

    pub fn snapshot(&self) -> Option<Arc<Snapshot>> {
        self.current.load_full()
    }

    /// The `GET /status` document.
    pub fn status(&self) -> Value {
        let snap = self.snapshot();
        let state = self.state.lock().unwrap();
        let reason = match (&snap, &state.last_error) {
            (Some(_), _) => Value::Null,
            (None, Some((message, _))) => Value::String(message.clone()),
            (None, None) => Value::String("opening database".into()),
        };
        json!({
            "healthy": snap.is_some(),
            "reason": reason,
            "best": snap.as_ref().map(|s| block_json(Some(s.best))).unwrap_or(Value::Null),
            "finalized": snap.as_ref().map(|s| block_json(s.finalized)).unwrap_or(Value::Null),
            "snapshot_opened_at": snap.as_ref().map(|s| s.opened_at),
            "refreshing": state.refreshing,
            "last_refresh_error": state
                .last_error
                .as_ref()
                .map(|(message, at)| json!({ "message": message, "at": at })),
            "genesis_hash": snap.as_ref().and_then(|s| s.genesis),
        })
    }

    fn open(&self, first: bool) -> Result<Snapshot, String> {
        let opened_at = now();
        let reader = (self.factory)().map_err(|e| format!("cannot open database: {e}"))?;
        let best = reader
            .best_block()
            .map_err(|e| e.to_string())?
            .ok_or_else(|| "database has no blocks".to_string())?;
        if first && let Some(check) = &self.options.check {
            check(&reader)?;
        }
        let finalized = reader.finalized_block().map_err(|e| e.to_string())?;
        let genesis = reader.genesis_hash().map_err(|e| e.to_string())?;
        Ok(Snapshot {
            reader,
            opened_at,
            best,
            finalized,
            genesis,
        })
    }

    async fn do_refresh(self: &Arc<Self>) -> Result<(), String> {
        let first = self.snapshot().is_none();
        self.state.lock().unwrap().refreshing = true;
        let this = self.clone();
        let opened = tokio::task::spawn_blocking(move || this.open(first))
            .await
            .map_err(|e| format!("open task failed: {e}"));
        let result = match opened {
            Ok(Ok(snapshot)) => {
                let best = snapshot.best.0;
                self.current.store(Some(Arc::new(snapshot)));
                self.generation.fetch_add(1, Ordering::SeqCst);
                let mut state = self.state.lock().unwrap();
                state.last_error = None;
                state.last_refresh = Some(Instant::now());
                drop(state);
                self.ready.send_replace(true);
                if first {
                    tracing::info!(best, "reader open");
                } else {
                    tracing::debug!(best, "reader swapped");
                }
                Ok(())
            }
            Ok(Err(message)) | Err(message) => {
                self.state.lock().unwrap().last_error = Some((message.clone(), now()));
                tracing::error!(
                    "reader open failed: {message}{}",
                    if first { "" } else { "; keeping old reader" }
                );
                Err(message)
            }
        };
        self.state.lock().unwrap().refreshing = false;
        result
    }

    /// Open a new reader and swap it in. Concurrent callers share one
    /// in-flight refresh instead of stacking opens.
    ///
    /// Non-forced refreshes (speculative "maybe this block is newer than our
    /// snapshot" ones) are rate-limited to one per `min_refresh_gap` so a
    /// client polling for a future block cannot keep the manager reopening,
    /// and do nothing before the first open (the open loop owns that one).
    /// Forced refreshes (stale snapshot, the periodic loop) always run.
    pub async fn refresh(self: &Arc<Self>, force: bool) -> Result<(), String> {
        if self.snapshot().is_none() && !force {
            return Ok(());
        }
        let generation = self.generation.load(Ordering::SeqCst);
        let _guard = self.refresh_lock.lock().await;
        if self.generation.load(Ordering::SeqCst) != generation {
            return Ok(()); // the refresh we queued behind did it
        }
        if !force {
            let last = self.state.lock().unwrap().last_refresh;
            if let Some(last) = last
                && last.elapsed() < self.options.min_refresh_gap
            {
                return Ok(());
            }
        }
        self.do_refresh().await
    }

    /// Start a forced refresh without waiting for it, unless one is running.
    pub fn kick_refresh(self: &Arc<Self>) {
        let Ok(guard) = self.refresh_lock.clone().try_lock_owned() else {
            return;
        };
        tracing::warn!("stale snapshot; refreshing in background");
        let this = self.clone();
        tokio::spawn(async move {
            let _guard = guard;
            let _ = this.do_refresh().await;
        });
    }

    async fn run(self: Arc<Self>) {
        let (mut delay, max_delay) = self.options.open_retry;
        while self.snapshot().is_none() {
            if self.refresh(true).await.is_err() {
                tracing::info!("retrying open in {:.0?}", delay);
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(max_delay);
            }
        }
        loop {
            tokio::time::sleep(self.options.refresh_interval).await;
            // Failures are logged and recorded by do_refresh; the old reader stays.
            let _ = self.refresh(true).await;
        }
    }

    /// Run `f` against the current reader on the blocking pool. One
    /// transparent refresh+retry on a stale snapshot; with
    /// `wait_on_stale = false` the refresh is started in the background and
    /// [`ReadError::Stale`] is returned at once instead of holding the caller
    /// for a whole reopen.
    pub async fn read<T, F>(self: &Arc<Self>, wait_on_stale: bool, f: F) -> Result<T, ReadError>
    where
        T: Send + 'static,
        F: Fn(&Wortel) -> wortel::Result<T> + Send + Sync + 'static,
    {
        let f = Arc::new(f);
        let snap = self.snapshot().ok_or(ReadError::NoReader)?;
        let error = match run(snap.clone(), f.clone()).await {
            Ok(value) => return Ok(value),
            Err(e) => e,
        };
        if !error.is_stale_snapshot() {
            return Err(classify(error));
        }
        if !wait_on_stale {
            // A reader swapped in since this call started is already fresh.
            if self
                .snapshot()
                .is_some_and(|current| Arc::ptr_eq(&current, &snap))
            {
                self.kick_refresh();
            }
            return Err(ReadError::Stale);
        }
        tracing::warn!("stale snapshot; refreshing");
        self.refresh(true).await.map_err(ReadError::Refresh)?;
        let snap = self.snapshot().ok_or(ReadError::NoReader)?;
        run(snap, f).await.map_err(classify)
    }
}

async fn run<T, F>(snap: Arc<Snapshot>, f: Arc<F>) -> wortel::Result<T>
where
    T: Send + 'static,
    F: Fn(&Wortel) -> wortel::Result<T> + Send + Sync + 'static,
{
    tokio::task::spawn_blocking(move || f(&snap.reader))
        .await
        .unwrap_or_else(|e| {
            Err(wortel::Error::InvalidInput(format!(
                "read task failed: {e}"
            )))
        })
}

fn classify(e: wortel::Error) -> ReadError {
    match e {
        wortel::Error::UnknownBlock(_) | wortel::Error::MissingState(..) => ReadError::Lookup(e),
        other => ReadError::Other(other),
    }
}
