"""JSON-RPC websocket facade sidecar (``python -m wortel.serve``).

See ``docs/rpc-facade.md`` for the design.
"""

from .readers import ReaderManager
from .server import Facade

__all__ = ["Facade", "ReaderManager"]
