use crate::error::LogError;
use crate::time::now_ms;
use crate::NativeLogAppendProfile;
use sha2::{Digest, Sha256};

pub(crate) fn calculate_raw_cid_v1_from_bytes(bytes: &[u8]) -> String {
    calculate_raw_cid_v1_parts(bytes).0
}

pub(crate) fn calculate_raw_cid_v1_parts(bytes: &[u8]) -> (String, [u8; 32]) {
    calculate_raw_cid_v1_parts_profiled(bytes, None)
}

pub(crate) fn calculate_raw_cid_v1_parts_profiled(
    bytes: &[u8],
    mut profile: Option<&mut NativeLogAppendProfile>,
) -> (String, [u8; 32]) {
    let hash_started = profile.as_ref().map(|_| now_ms());
    let digest = Sha256::digest(bytes);
    let digest_bytes: [u8; 32] = digest.into();
    if let Some(started) = hash_started {
        if let Some(profile) = profile.as_deref_mut() {
            profile.cid_hash_ms += now_ms() - started;
        }
    }
    let string_started = profile.as_ref().map(|_| now_ms());
    let cid = raw_cid_v1_string_from_digest(&digest_bytes);
    if let Some(started) = string_started {
        if let Some(profile) = profile.as_deref_mut() {
            profile.cid_string_ms += now_ms() - started;
        }
    }
    (cid, digest_bytes)
}

pub(crate) fn calculate_raw_digest_profiled(
    bytes: &[u8],
    mut profile: Option<&mut NativeLogAppendProfile>,
) -> [u8; 32] {
    let hash_started = profile.as_ref().map(|_| now_ms());
    let digest = Sha256::digest(bytes);
    let digest_bytes: [u8; 32] = digest.into();
    if let Some(started) = hash_started {
        if let Some(profile) = profile.as_deref_mut() {
            profile.cid_hash_ms += now_ms() - started;
            profile.cid_ms += now_ms() - started;
        }
    }
    digest_bytes
}

pub(crate) fn raw_cid_v1_string_from_digest(digest_bytes: &[u8; 32]) -> String {
    let mut cid = [0u8; 36];
    cid[0] = 0x01; // CIDv1
    cid[1] = 0x55; // raw codec
    cid[2] = 0x12; // sha2-256 multihash code
    cid[3] = 0x20; // 32 byte digest
    cid[4..].copy_from_slice(digest_bytes.as_slice());
    let mut encoded = String::with_capacity(51);
    encoded.push('z');
    bs58::encode(cid)
        .onto(&mut encoded)
        .expect("base58 encoding into String should not fail");
    encoded
}

pub(crate) fn raw_cid_v1_digest_from_string(cid: &str) -> Result<[u8; 32], LogError> {
    let encoded = cid
        .strip_prefix('z')
        .ok_or_else(|| LogError::ExpectedBase58btcCid)?;
    let mut decoded = [0u8; 36];
    let decoded_len = bs58::decode(encoded)
        .onto(&mut decoded)
        .map_err(|_| LogError::InvalidBase58btcCid)?;
    if decoded_len != 36
        || decoded[0] != 0x01
        || decoded[1] != 0x55
        || decoded[2] != 0x12
        || decoded[3] != 0x20
    {
        return Err(LogError::ExpectedRawCidV1Sha256Cid);
    }
    let mut digest = [0u8; 32];
    digest.copy_from_slice(&decoded[4..36]);
    Ok(digest)
}
