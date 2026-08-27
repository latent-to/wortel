import pytest

from wortel.serve.header import HeaderDecodeError, decode_compact, decode_header


def compact(n: int) -> bytes:
    if n < 1 << 6:
        return bytes([n << 2])
    if n < 1 << 14:
        return ((n << 2) | 0b01).to_bytes(2, "little")
    if n < 1 << 30:
        return ((n << 2) | 0b10).to_bytes(4, "little")
    payload = n.to_bytes((n.bit_length() + 7) // 8, "little")
    return bytes([((len(payload) - 4) << 2) | 0b11]) + payload


@pytest.mark.parametrize(
    "n", [0, 1, 63, 64, 16383, 16384, (1 << 30) - 1, 1 << 30, 1 << 40]
)
def test_compact_roundtrip(n):
    encoded = compact(n)
    value, offset = decode_compact(encoded, 0)
    assert value == n
    assert offset == len(encoded)


def make_header(number: int, logs: list[bytes]) -> tuple[bytes, dict]:
    parent = bytes(range(32))
    state_root = bytes(range(32, 64))
    extrinsics_root = bytes(range(64, 96))
    raw = (
        parent
        + compact(number)
        + state_root
        + extrinsics_root
        + compact(len(logs))
        + b"".join(logs)
    )
    expected = {
        "parentHash": "0x" + parent.hex(),
        "number": hex(number),
        "stateRoot": "0x" + state_root.hex(),
        "extrinsicsRoot": "0x" + extrinsics_root.hex(),
        "digest": {"logs": ["0x" + log.hex() for log in logs]},
    }
    return raw, expected


def test_header_no_logs():
    raw, expected = make_header(5, [])
    assert decode_header(raw) == expected


def test_header_all_digest_variants():
    logs = [
        bytes([6]) + b"aura" + compact(8) + b"\xaa" * 8,  # PreRuntime
        bytes([4]) + b"fron" + compact(3) + b"\xbb" * 3,  # Consensus
        bytes([5]) + b"aura" + compact(64) + b"\xcc" * 64,  # Seal
        bytes([0]) + compact(5) + b"hello",  # Other
        bytes([8]),  # RuntimeEnvironmentUpdated
    ]
    raw, expected = make_header(8_700_000, logs)
    assert decode_header(raw) == expected


def test_header_large_block_number():
    raw, expected = make_header(4_294_967_295, [])
    assert decode_header(raw) == expected


def test_header_trailing_bytes_rejected():
    raw, _ = make_header(1, [])
    with pytest.raises(HeaderDecodeError):
        decode_header(raw + b"\x00")


def test_header_unknown_digest_variant_rejected():
    raw, _ = make_header(1, [bytes([7]) + b"xxxx"])
    with pytest.raises(HeaderDecodeError):
        decode_header(raw)


def test_header_truncated_rejected():
    raw, _ = make_header(1, [])
    with pytest.raises(HeaderDecodeError):
        decode_header(raw[:40])
