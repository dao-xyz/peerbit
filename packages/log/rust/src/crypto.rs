use crate::codec::{
    parse_plain_entry_v0_storage_signature, parse_plain_signature_with_key_ref,
    unsigned_entry_v0_storage_for_signing,
};
use crate::error::LogError;
use ed25519_dalek::{verify_batch, Signature, Signer, SigningKey, Verifier, VerifyingKey};
use std::collections::HashMap;

pub(crate) fn parse_entry_v0_ed25519_storage_slices(
    blocks: &[&[u8]],
) -> Result<(Vec<Signature>, Vec<VerifyingKey>, Vec<Vec<u8>>), LogError> {
    let mut parsed_signatures = Vec::with_capacity(blocks.len());
    let mut parsed_public_keys = Vec::with_capacity(blocks.len());
    let mut parsed_messages = Vec::with_capacity(blocks.len());
    let mut verifying_key_cache = HashMap::new();

    for bytes in blocks {
        let parsed = parse_plain_entry_v0_storage_signature(bytes)?;
        validate_signature_lengths(&parsed.signature, &parsed.public_key)?;

        let signature_bytes: [u8; 64] = parsed
            .signature
            .as_slice()
            .try_into()
            .map_err(|_| LogError::ExpectedEd25519SignatureLength64)?;
        let verifying_key = cached_verifying_key(&mut verifying_key_cache, &parsed.public_key)?;
        parsed_signatures.push(Signature::from_bytes(&signature_bytes));
        parsed_public_keys.push(verifying_key);
        parsed_messages.push(parsed.signable);
    }

    Ok((parsed_signatures, parsed_public_keys, parsed_messages))
}

pub struct PreparedEntryV0SignatureInput<'a> {
    pub storage_bytes: &'a [u8],
    pub signable_prefix_len: usize,
    pub signature_with_key_start: usize,
    pub signature_with_key_len: usize,
}

pub(crate) fn prepared_entry_v0_signature_with_key<'a>(
    input: &PreparedEntryV0SignatureInput<'a>,
) -> Result<&'a [u8], LogError> {
    let end = input
        .signature_with_key_start
        .checked_add(input.signature_with_key_len)
        .ok_or_else(|| LogError::SignatureOffsetOverflow)?;
    if end > input.storage_bytes.len() {
        return Err(LogError::InvalidPreparedSignatureOffset);
    }
    Ok(&input.storage_bytes[input.signature_with_key_start..end])
}

pub(crate) fn parse_prepared_entry_v0_ed25519_storage_slices(
    entries: &[PreparedEntryV0SignatureInput<'_>],
) -> Result<(Vec<Signature>, Vec<VerifyingKey>, Vec<Vec<u8>>), LogError> {
    let mut parsed_signatures = Vec::with_capacity(entries.len());
    let mut parsed_public_keys = Vec::with_capacity(entries.len());
    let mut parsed_messages = Vec::with_capacity(entries.len());
    let mut verifying_key_cache = HashMap::new();

    for entry in entries {
        let parsed =
            parse_plain_signature_with_key_ref(prepared_entry_v0_signature_with_key(entry)?)?;
        validate_signature_lengths(parsed.signature, parsed.public_key)?;

        let signature_bytes: [u8; 64] = parsed
            .signature
            .try_into()
            .map_err(|_| LogError::ExpectedEd25519SignatureLength64)?;
        let verifying_key = cached_verifying_key(&mut verifying_key_cache, parsed.public_key)?;
        parsed_signatures.push(Signature::from_bytes(&signature_bytes));
        parsed_public_keys.push(verifying_key);
        parsed_messages.push(unsigned_entry_v0_storage_for_signing(
            entry.storage_bytes,
            entry.signable_prefix_len,
        )?);
    }

    Ok((parsed_signatures, parsed_public_keys, parsed_messages))
}

pub fn verify_prepared_entry_v0_ed25519_storage_slices(
    entries: &[PreparedEntryV0SignatureInput<'_>],
) -> Result<Vec<u8>, LogError> {
    if entries.is_empty() {
        return Ok(Vec::new());
    }

    let (parsed_signatures, parsed_public_keys, parsed_messages) =
        parse_prepared_entry_v0_ed25519_storage_slices(entries)?;
    let message_refs = parsed_messages
        .iter()
        .map(|message| message.as_slice())
        .collect::<Vec<_>>();
    if verify_batch(&message_refs, &parsed_signatures, &parsed_public_keys).is_ok() {
        return Ok(vec![1u8; entries.len()]);
    }

    let mut out = Vec::with_capacity(entries.len());
    for i in 0..parsed_signatures.len() {
        out.push(
            if parsed_public_keys[i]
                .verify(&parsed_messages[i], &parsed_signatures[i])
                .is_ok()
            {
                1
            } else {
                0
            },
        );
    }

    Ok(out)
}

pub fn verify_prepared_entry_v0_ed25519_storage_slices_all(
    entries: &[PreparedEntryV0SignatureInput<'_>],
) -> Result<bool, LogError> {
    if entries.is_empty() {
        return Ok(true);
    }

    let (parsed_signatures, parsed_public_keys, parsed_messages) =
        parse_prepared_entry_v0_ed25519_storage_slices(entries)?;
    let message_refs = parsed_messages
        .iter()
        .map(|message| message.as_slice())
        .collect::<Vec<_>>();
    if verify_batch(&message_refs, &parsed_signatures, &parsed_public_keys).is_ok() {
        return Ok(true);
    }

    for i in 0..parsed_signatures.len() {
        if parsed_public_keys[i]
            .verify(&parsed_messages[i], &parsed_signatures[i])
            .is_err()
        {
            return Ok(false);
        }
    }

    Ok(true)
}

pub fn verify_entry_v0_ed25519_storage_slices(blocks: &[&[u8]]) -> Result<Vec<u8>, LogError> {
    if blocks.is_empty() {
        return Ok(Vec::new());
    }

    let (parsed_signatures, parsed_public_keys, parsed_messages) =
        parse_entry_v0_ed25519_storage_slices(blocks)?;
    let message_refs = parsed_messages
        .iter()
        .map(|message| message.as_slice())
        .collect::<Vec<_>>();
    if verify_batch(&message_refs, &parsed_signatures, &parsed_public_keys).is_ok() {
        return Ok(vec![1u8; blocks.len()]);
    }

    let mut out = Vec::with_capacity(blocks.len());
    for i in 0..parsed_signatures.len() {
        out.push(
            if parsed_public_keys[i]
                .verify(&parsed_messages[i], &parsed_signatures[i])
                .is_ok()
            {
                1
            } else {
                0
            },
        );
    }

    Ok(out)
}

pub fn verify_entry_v0_ed25519_storage_slices_all(blocks: &[&[u8]]) -> Result<bool, LogError> {
    if blocks.is_empty() {
        return Ok(true);
    }

    let (parsed_signatures, parsed_public_keys, parsed_messages) =
        parse_entry_v0_ed25519_storage_slices(blocks)?;
    let message_refs = parsed_messages
        .iter()
        .map(|message| message.as_slice())
        .collect::<Vec<_>>();
    if verify_batch(&message_refs, &parsed_signatures, &parsed_public_keys).is_ok() {
        return Ok(true);
    }

    for i in 0..parsed_signatures.len() {
        if parsed_public_keys[i]
            .verify(&parsed_messages[i], &parsed_signatures[i])
            .is_err()
        {
            return Ok(false);
        }
    }

    Ok(true)
}

pub(crate) fn validate_signature_lengths(
    signature: &[u8],
    public_key: &[u8],
) -> Result<(), LogError> {
    if signature.len() != 64 {
        return Err(LogError::ExpectedEd25519SignatureLength64);
    }
    if public_key.len() != 32 {
        return Err(LogError::ExpectedEd25519PublicKeyLength32);
    }
    Ok(())
}

pub(crate) fn cached_verifying_key(
    cache: &mut HashMap<[u8; 32], VerifyingKey>,
    public_key: &[u8],
) -> Result<VerifyingKey, LogError> {
    let public_key_bytes: [u8; 32] = public_key
        .try_into()
        .map_err(|_| LogError::ExpectedEd25519PublicKeyLength32)?;
    if let Some(verifying_key) = cache.get(&public_key_bytes) {
        return Ok(*verifying_key);
    }
    let verifying_key = VerifyingKey::from_bytes(&public_key_bytes)
        .map_err(|_| LogError::InvalidEd25519PublicKey)?;
    cache.insert(public_key_bytes, verifying_key);
    Ok(verifying_key)
}

pub(crate) fn sign_ed25519_raw(
    private_key: &[u8],
    public_key: &[u8],
    data: &[u8],
) -> Result<Vec<u8>, LogError> {
    let signing_key = validate_ed25519_keypair(private_key, public_key)?;
    Ok(sign_ed25519_with_key(&signing_key, data).to_vec())
}

pub(crate) fn sign_ed25519_with_key(signing_key: &SigningKey, data: &[u8]) -> [u8; 64] {
    signing_key.sign(data).to_bytes()
}

pub(crate) fn validate_ed25519_keypair(
    private_key: &[u8],
    public_key: &[u8],
) -> Result<SigningKey, LogError> {
    if private_key.len() != 32 {
        return Err(LogError::ExpectedEd25519PrivateKeyLength32);
    }
    if public_key.len() != 32 {
        return Err(LogError::ExpectedEd25519PublicKeyLength32);
    }
    let signing_key = SigningKey::from_bytes(
        private_key
            .try_into()
            .map_err(|_| LogError::ExpectedEd25519PrivateKeyLength32)?,
    );
    if signing_key.verifying_key().to_bytes().as_slice() != public_key {
        return Err(LogError::Ed25519KeypairMismatch);
    }
    Ok(signing_key)
}
