"""Reader lifecycle: open with retry, background refresh, atomic swap,
stale-snapshot handling, status.

A readonly ``wortel.Reader`` is frozen at the on-disk snapshot taken at open
time, so the manager reopens a fresh Reader every ``refresh_interval`` seconds
and swaps it in atomically. In-flight calls keep the object they started with;
the old Reader is dropped by GC once the last call returns.

The manager never gives up: a failed first open (or a failed ``check``) is
retried with backoff while ``status()`` reports ``healthy: false``; a failed
refresh keeps the old Reader and is reported as ``last_refresh_error``.

Error contract of the bindings (see ``src/py.rs``):

- ``OSError``     — stale snapshot (node compaction deleted an SST under us):
                    refresh and retry.
- ``LookupError`` — unknown block / missing state: the caller decides
                    (the facade refreshes once, then falls back to upstream;
                    in router mode it answers "not in snapshot").
- ``ValueError``  — invalid input from the client: surface as-is.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Snapshot:
    """What the current Reader sees. Fixed for the Reader's lifetime, so it is
    read once at open and ``status()`` never touches RocksDB."""

    opened_at: float  # unix time the open started; the view is no newer
    best: tuple[int, bytes]
    finalized: Optional[tuple[int, bytes]]
    genesis: Optional[bytes]


def _block(block: Optional[tuple[int, bytes]]) -> Optional[dict]:
    if block is None:
        return None
    return {"number": block[0], "hash": "0x" + block[1].hex()}


def _retrieve(task: asyncio.Task) -> None:
    # A refresh nobody awaits (stale snapshot in router mode) must not warn
    # about a never-retrieved exception; _do_refresh logged and recorded it.
    if not task.cancelled():
        task.exception()


class ReaderManager:
    """Owns the current Reader and coalesces refreshes.

    ``factory`` is a zero-argument callable returning a fresh reader; in
    production ``lambda: wortel.Reader(db_path)``. Injectable for tests.

    ``check`` is called with the first reader before it is served and raises
    to reject it (wrong pruning mode); the open is then retried like any other
    failed open. ``open_retry`` is the ``(initial, max)`` backoff in seconds.
    """

    def __init__(
        self,
        factory: Callable[[], Any],
        refresh_interval: float = 60.0,
        min_refresh_gap: float = 5.0,
        check: Optional[Callable[[Any], None]] = None,
        open_retry: tuple[float, float] = (1.0, 60.0),
    ):
        self._factory = factory
        self._refresh_interval = refresh_interval
        self._min_refresh_gap = min_refresh_gap
        self._check = check
        self._open_retry = open_retry
        self._reader: Any = None
        self._snapshot: Optional[Snapshot] = None
        self._ready = asyncio.Event()
        self._refresh_task: Optional[asyncio.Task] = None
        self._loop_task: Optional[asyncio.Task] = None
        self._last_refresh = 0.0
        self._last_error: Optional[tuple[str, float]] = None

    async def start(self) -> None:
        """Start opening in the background and return at once, so the caller
        can listen (and answer probes) during the first open."""
        self._loop_task = asyncio.create_task(self._run())

    async def wait_ready(self) -> None:
        await self._ready.wait()

    async def stop(self) -> None:
        for task in (self._loop_task, self._refresh_task):
            if task is not None:
                task.cancel()
        self._loop_task = self._refresh_task = None

    @property
    def reader(self) -> Any:
        return self._reader

    @property
    def snapshot(self) -> Optional[Snapshot]:
        return self._snapshot if self._reader is not None else None

    def status(self) -> dict:
        """The ``GET /status`` document."""
        snap = self.snapshot
        error = self._last_error
        reason = None
        if snap is None:
            reason = error[0] if error else "opening database"
        return {
            "healthy": snap is not None,
            "reason": reason,
            "best": _block(snap.best) if snap else None,
            "finalized": _block(snap.finalized) if snap else None,
            "snapshot_opened_at": snap.opened_at if snap else None,
            "refreshing": self._refreshing(),
            "last_refresh_error": (
                {"message": error[0], "at": error[1]} if error else None
            ),
            "genesis_hash": (
                "0x" + snap.genesis.hex() if snap and snap.genesis else None
            ),
        }

    def _refreshing(self) -> bool:
        return self._refresh_task is not None and not self._refresh_task.done()

    def _spawn_refresh(self) -> asyncio.Task:
        self._refresh_task = asyncio.create_task(self._do_refresh())
        self._refresh_task.add_done_callback(_retrieve)
        return self._refresh_task

    async def refresh(self, force: bool = False) -> None:
        """Open a new Reader and swap it in. Concurrent callers share one
        in-flight refresh instead of stacking opens.

        Non-forced refreshes (speculative "maybe this block is newer than our
        snapshot" ones) are rate-limited to one per ``min_refresh_gap`` so a
        client polling for a future block cannot keep the manager reopening,
        and do nothing before the first open (the open loop owns that one).
        Forced refreshes (stale snapshot, the periodic loop) always run.
        """
        if self._reader is None and not force:
            return
        task = self._refresh_task
        if task is not None and not task.done():
            await asyncio.shield(task)
            return
        now = asyncio.get_running_loop().time()
        if not force and now - self._last_refresh < self._min_refresh_gap:
            return
        await asyncio.shield(self._spawn_refresh())

    def _open(self, first: bool) -> tuple[Any, Snapshot]:
        opened_at = time.time()
        try:
            reader = self._factory()
        except Exception as e:
            raise RuntimeError(f"cannot open database: {e}") from e
        best = reader.best_block()
        if best is None:
            raise RuntimeError("database has no blocks")
        if first and self._check is not None:
            self._check(reader)
        finalized = reader.finalized_block()
        genesis = reader.genesis_hash()
        return reader, Snapshot(
            opened_at=opened_at,
            best=(best[0], bytes(best[1])),
            finalized=finalized and (finalized[0], bytes(finalized[1])),
            genesis=genesis and bytes(genesis),
        )

    async def _do_refresh(self) -> None:
        first = self._reader is None
        logger.debug("opening reader")
        try:
            reader, snapshot = await asyncio.to_thread(self._open, first)
        except Exception as e:
            self._last_error = (str(e), time.time())
            logger.error(
                "reader open failed: %s%s", e, "" if first else "; keeping old reader"
            )
            raise
        self._reader, self._snapshot = reader, snapshot
        self._last_error = None
        self._last_refresh = asyncio.get_running_loop().time()
        self._ready.set()
        if first:
            logger.info("reader open: best block %s", snapshot.best[0])
        else:
            logger.debug("reader swapped")

    async def _run(self) -> None:
        delay, max_delay = self._open_retry
        while self._reader is None:
            try:
                await self.refresh(force=True)
            except Exception:
                logger.info("retrying open in %.0f s", delay)
                await asyncio.sleep(delay)
                delay = min(delay * 2, max_delay)
        while True:
            await asyncio.sleep(self._refresh_interval)
            try:
                await self.refresh(force=True)
            except Exception:
                pass  # logged and recorded by _do_refresh; old reader stays

    async def call(self, method: str, *args: Any, wait_on_stale: bool = True) -> Any:
        """Run ``reader.method(*args)`` in a worker thread (the bindings
        release the GIL). One transparent refresh+retry on a stale snapshot;
        every other exception propagates to the caller.

        With ``wait_on_stale=False`` a stale snapshot starts the refresh in
        the background and raises ``LookupError`` at once, instead of holding
        the caller for a whole reopen. No reader yet is a ``LookupError`` too.
        """
        reader = self._reader
        if reader is None:
            raise LookupError("no snapshot open")
        try:
            return await asyncio.to_thread(getattr(reader, method), *args)
        except OSError:
            if not wait_on_stale:
                # A reader swapped in since this call started is already fresh.
                if reader is self._reader and not self._refreshing():
                    logger.warning(
                        "stale snapshot on %s; refreshing in background", method
                    )
                    self._spawn_refresh()
                raise LookupError(f"stale snapshot on {method}") from None
            logger.warning("stale snapshot on %s; refreshing", method)
            await self.refresh(force=True)
            return await asyncio.to_thread(getattr(self._reader, method), *args)
