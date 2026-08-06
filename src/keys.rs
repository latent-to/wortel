//! Storage-key construction helpers matching FRAME's hashers.
//!
//! These are conveniences: `wortel` itself only deals in raw keys, so callers
//! that already build keys (e.g. via `scalecodec` metadata in Python) can keep
//! doing that and pass the bytes through unchanged.

use sp_core::hashing::{blake2_128, twox_64, twox_128};

/// `twox128(pallet) ++ twox128(item)` — the 32-byte prefix every storage
/// entry of a `#[pallet::storage]` item starts with.
pub fn storage_prefix(pallet: &str, item: &str) -> [u8; 32] {
    let mut out = [0u8; 32];
    out[..16].copy_from_slice(&twox_128(pallet.as_bytes()));
    out[16..].copy_from_slice(&twox_128(item.as_bytes()));
    out
}

/// FRAME `Twox64Concat` hasher: `twox64(data) ++ data`.
pub fn twox_64_concat(data: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(8 + data.len());
    out.extend_from_slice(&twox_64(data));
    out.extend_from_slice(data);
    out
}

/// FRAME `Blake2_128Concat` hasher: `blake2_128(data) ++ data`.
pub fn blake2_128_concat(data: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(16 + data.len());
    out.extend_from_slice(&blake2_128(data));
    out.extend_from_slice(data);
    out
}

/// Full key for a `StorageValue`.
pub fn value_key(pallet: &str, item: &str) -> [u8; 32] {
    storage_prefix(pallet, item)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex(b: &[u8]) -> String {
        b.iter().map(|x| format!("{x:02x}")).collect()
    }

    #[test]
    fn system_account_prefix_matches_known_value() {
        // Well-known prefix, e.g. what async-substrate-interface produces
        // for substrate.create_storage_key("System", "Account").
        assert_eq!(
            hex(&storage_prefix("System", "Account")),
            "26aa394eea5630e07c48ae0c9558cef7b99d880ec681799c0cf30e8886371da9"
        );
    }

    #[test]
    fn twox64_concat_keeps_raw_suffix() {
        let k = twox_64_concat(&[1, 2, 3]);
        assert_eq!(k.len(), 11);
        assert_eq!(&k[8..], &[1, 2, 3]);
    }

    #[test]
    fn blake2_128_concat_keeps_raw_suffix() {
        let k = blake2_128_concat(&[9]);
        assert_eq!(k.len(), 17);
        assert_eq!(k[16], 9);
    }
}
