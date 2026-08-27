"""Reader lifecycle: open, background refresh, atomic swap, stale-snapshot
retry.

A readonly ``wortel.Reader`` is frozen at the on-disk snapshot taken at open
time, so the manager reopens a fresh Reader every ``refresh_interval`` seconds
and swaps it in atomically. In-flight calls keep the object they started with;
the old Reader is dropped by GC once the last call returns.

Error contract of the bindings (see ``src/py.rs``):

- ``OSError``     — stale snapshot (node compaction deleted an SST under us):
                    refresh and retry.
- ``LookupError`` — unknown block / missing state: the caller decides
                    (the facade refreshes once, then falls back to upstream).
- ``ValueError``  — invalid input from the client: surface as-is.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class ReaderManager:
    """Owns the current Reader and coalesces refreshes.

    ``factory`` is a zero-argument callable returning a fresh reader; in
    production ``lambda: wortel.Reader(db_path)``. Injectable for tests.
    """

    def __init__(
        self,
        factory: Callable[[], Any],
        refresh_interval: float = 60.0,
        min_refresh_gap: float = 5.0,
    ):
        self._factory = factory
        self._refresh_interval = refresh_interval
        self._min_refresh_gap = min_refresh_gap
        self._reader: Any = None
        self._refresh_task: Optional[asyncio.Task] = None
        self._loop_task: Optional[asyncio.Task] = None
        self._last_refresh = 0.0

    async def start(self) -> None:
        self._reader = await asyncio.to_thread(self._factory)
        self._last_refresh = asyncio.get_running_loop().time()
        self._loop_task = asyncio.create_task(self._refresh_loop())

    async def stop(self) -> None:
        for task in (self._loop_task, self._refresh_task):
            if task is not None:
                task.cancel()
        self._loop_task = self._refresh_task = None

    @property
    def reader(self) -> Any:
        return self._reader

    async def refresh(self, force: bool = False) -> None:
        """Open a new Reader and swap it in. Concurrent callers share one
        in-flight refresh instead of stacking opens.

        Non-forced refreshes (speculative "maybe this block is newer than our
        snapshot" ones) are rate-limited to one per ``min_refresh_gap`` so a
        client polling for a future block cannot keep the manager reopening.
        Forced refreshes (stale snapshot, the periodic loop) always run.
        """
        if self._refresh_task is not None and not self._refresh_task.done():
            await asyncio.shield(self._refresh_task)
            return
        now = asyncio.get_running_loop().time()
        if not force and now - self._last_refresh < self._min_refresh_gap:
            return
        self._refresh_task = asyncio.create_task(self._do_refresh())
        await asyncio.shield(self._refresh_task)

    async def _do_refresh(self) -> None:
        logger.info("reopening reader")
        self._reader = await asyncio.to_thread(self._factory)
        self._last_refresh = asyncio.get_running_loop().time()
        logger.info("reader swapped")

    async def _refresh_loop(self) -> None:
        while True:
            await asyncio.sleep(self._refresh_interval)
            try:
                await self.refresh(force=True)
            except Exception:
                logger.exception("background refresh failed; keeping old reader")

    async def call(self, method: str, *args: Any) -> Any:
        """Run ``reader.method(*args)`` in a worker thread (the bindings
        release the GIL). One transparent refresh+retry on a stale snapshot;
        every other exception propagates to the caller."""
        reader = self._reader
        try:
            return await asyncio.to_thread(getattr(reader, method), *args)
        except OSError:
            logger.warning("stale snapshot on %s; refreshing", method)
            await self.refresh(force=True)
            return await asyncio.to_thread(getattr(self._reader, method), *args)
