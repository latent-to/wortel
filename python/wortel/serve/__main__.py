"""CLI entrypoint: ``python -m wortel.serve`` / ``wortel-serve``."""

from __future__ import annotations

import argparse
import asyncio
import logging
import os

from websockets.asyncio.server import serve

import wortel

from .readers import ReaderManager
from .server import Facade, health_check

logger = logging.getLogger("wortel.serve")


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="wortel-serve",
        description="JSON-RPC websocket facade over a subtensor archive node's RocksDB",
    )
    p.add_argument(
        "--db",
        default=os.getenv("WORTEL_DB"),
        help="node database path (.../chains/<chain-id>/db/full) [env WORTEL_DB]",
    )
    p.add_argument(
        "--upstream",
        default=os.getenv("WORTEL_UPSTREAM", "ws://127.0.0.1:9944"),
        help="node RPC websocket for pass-through [env WORTEL_UPSTREAM]",
    )
    p.add_argument("--listen", default="0.0.0.0:9955", help="host:port to bind")
    p.add_argument(
        "--refresh-interval",
        type=float,
        default=60.0,
        help="seconds between Reader reopens",
    )
    p.add_argument(
        "--max-keys-page",
        type=int,
        default=1000,
        help="cap for state_getKeysPaged count (direct mode)",
    )
    p.add_argument(
        "--log-level",
        default=os.getenv("LOG_LEVEL", "INFO"),
        help="logging level [env LOG_LEVEL]",
    )
    args = p.parse_args(argv)
    if not args.db:
        p.error("--db (or WORTEL_DB) is required")
    return args


def self_check(reader) -> None:
    """Reject a node that is not a plain archive node: state for an old block
    must be present (`MissingState` maps to LookupError). Raising keeps the
    facade unhealthy; the ReaderManager retries the open."""
    hash1 = reader.block_hash(1)
    if hash1 is None:
        raise RuntimeError("database has no block 1 — not an archive node?")
    key = wortel.storage_prefix("Timestamp", "Now")
    try:
        reader.get_storage(bytes(hash1), key)
    except LookupError as e:
        raise RuntimeError(
            f"state for block 1 is unavailable ({e}); the node must run with "
            "--state-pruning archive (plain, not archive-canonical)"
        ) from None


async def run(args: argparse.Namespace) -> None:
    readers = ReaderManager(
        factory=lambda: wortel.Reader(args.db),
        refresh_interval=args.refresh_interval,
        check=self_check,
    )
    facade = Facade(readers, args.upstream, max_keys_page=args.max_keys_page)
    host, _, port = args.listen.rpartition(":")
    # Listen before the first open: /livez and /status must answer during it,
    # and an open that fails is retried, never fatal — as a sidecar, an exited
    # container would mark the node's whole pod not-ready.
    async with serve(
        facade.handle_connection,
        host or "0.0.0.0",
        int(port),
        max_size=None,
        process_request=health_check(readers),
    ):
        logger.info("listening on %s, upstream %s", args.listen, args.upstream)
        logger.info("opening reader at %s (this can take ~a minute cold)", args.db)
        await readers.start()
        await asyncio.Future()


def main(argv=None) -> None:
    args = parse_args(argv)
    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    if args.log_level.upper() != "DEBUG":
        logging.getLogger("websockets").setLevel(logging.WARNING)
    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
