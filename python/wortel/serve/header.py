"""SCALE decoding of a substrate block header into the JSON shape the node's
``chain_getHeader`` RPC returns.

The layout is fixed for subtensor (H256 hashes, u32 block numbers):

    parent_hash(32) ‖ compact(number) ‖ state_root(32) ‖ extrinsics_root(32)
    ‖ compact(len) ‖ digest items…

Each digest item is returned as the hex of its full SCALE encoding (variant
byte included), exactly as the node does. Digest variants (sp-runtime
``DigestItem``):

    0 = Other(Vec<u8>)
    4 = Consensus([u8;4], Vec<u8>)
    5 = Seal([u8;4], Vec<u8>)
    6 = PreRuntime([u8;4], Vec<u8>)
    8 = RuntimeEnvironmentUpdated
"""

from __future__ import annotations


class HeaderDecodeError(ValueError):
    pass


def decode_compact(data: bytes, offset: int) -> tuple[int, int]:
    """Decode a SCALE compact integer at ``offset``; return (value, new_offset)."""
    if offset >= len(data):
        raise HeaderDecodeError("compact: out of bounds")
    b0 = data[offset]
    mode = b0 & 0b11
    if mode == 0:
        return b0 >> 2, offset + 1
    if mode == 1:
        end = offset + 2
    elif mode == 2:
        end = offset + 4
    else:
        n = (b0 >> 2) + 4
        start, end = offset + 1, offset + 1 + n
        if end > len(data):
            raise HeaderDecodeError("compact: out of bounds")
        return int.from_bytes(data[start:end], "little"), end
    if end > len(data):
        raise HeaderDecodeError("compact: out of bounds")
    return int.from_bytes(data[offset:end], "little") >> 2, end


def _digest_item_end(data: bytes, offset: int) -> int:
    """Return the end offset of the digest item starting at ``offset``."""
    if offset >= len(data):
        raise HeaderDecodeError("digest item: out of bounds")
    variant = data[offset]
    pos = offset + 1
    if variant == 8:  # RuntimeEnvironmentUpdated — unit variant
        return pos
    if variant in (4, 5, 6):  # Consensus / Seal / PreRuntime: engine id first
        pos += 4
    elif variant != 0:  # 0 = Other
        raise HeaderDecodeError(f"digest item: unknown variant {variant}")
    length, pos = decode_compact(data, pos)
    end = pos + length
    if end > len(data):
        raise HeaderDecodeError("digest item: payload out of bounds")
    return end


def decode_header(raw: bytes) -> dict:
    """Decode SCALE header bytes to the ``chain_getHeader`` JSON shape."""
    if len(raw) < 32:
        raise HeaderDecodeError("header too short")
    parent_hash = raw[0:32]
    number, pos = decode_compact(raw, 32)
    if pos + 64 > len(raw):
        raise HeaderDecodeError("header too short")
    state_root = raw[pos : pos + 32]
    extrinsics_root = raw[pos + 32 : pos + 64]
    pos += 64
    n_logs, pos = decode_compact(raw, pos)
    logs = []
    for _ in range(n_logs):
        end = _digest_item_end(raw, pos)
        logs.append("0x" + raw[pos:end].hex())
        pos = end
    if pos != len(raw):
        raise HeaderDecodeError(f"{len(raw) - pos} trailing bytes after digest")
    return {
        "parentHash": "0x" + parent_hash.hex(),
        "number": hex(number),
        "stateRoot": "0x" + state_root.hex(),
        "extrinsicsRoot": "0x" + extrinsics_root.hex(),
        "digest": {"logs": logs},
    }
