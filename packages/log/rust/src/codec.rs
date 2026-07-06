use crate::error::LogError;

const SIGNED_ENTRY_EXTRA_CAPACITY: usize = 128;

pub(crate) struct EntryV0EncodeInput {
    pub(crate) clock_id: Vec<u8>,
    pub(crate) wall_time: u64,
    pub(crate) logical: u32,
    pub(crate) gid: String,
    pub(crate) next: Vec<String>,
    pub(crate) entry_type: u8,
    pub(crate) meta_data: Option<Vec<u8>>,
    pub(crate) payload_data: Vec<u8>,
}

pub(crate) struct SignatureInput {
    pub(crate) signature: Vec<u8>,
    pub(crate) public_key: Vec<u8>,
    pub(crate) prehash: u8,
}

pub(crate) struct ParsedEntryV0StorageSignature {
    pub(crate) signable: Vec<u8>,
    pub(crate) signature: Vec<u8>,
    pub(crate) public_key: Vec<u8>,
}

pub(crate) struct ParsedSignatureWithKey {
    pub(crate) signature: Vec<u8>,
    pub(crate) public_key: Vec<u8>,
}

pub(crate) struct ParsedSignatureWithKeyRef<'a> {
    pub(crate) signature: &'a [u8],
    pub(crate) public_key: &'a [u8],
}

pub(crate) struct ParsedPlainEntryV0Storage<'a> {
    pub(crate) signable_prefix_len: usize,
    pub(crate) meta: &'a [u8],
    pub(crate) payload: &'a [u8],
    pub(crate) signature_with_key: &'a [u8],
    pub(crate) signature_with_key_start: usize,
    pub(crate) signature_with_key_len: usize,
}

pub(crate) struct ParsedRawEntryV0Meta {
    pub(crate) clock_id: Vec<u8>,
    pub(crate) wall_time: u64,
    pub(crate) logical: u32,
    pub(crate) gid: String,
    pub(crate) next: Vec<String>,
    pub(crate) entry_type: u8,
    pub(crate) meta_data: Option<Vec<u8>>,
}

pub(crate) struct ParsedRawEntryV0Payload<'a> {
    pub(crate) data: &'a [u8],
}

pub(crate) struct BorshReader<'a> {
    pub(crate) bytes: &'a [u8],
    pub(crate) offset: usize,
}

impl<'a> BorshReader<'a> {
    pub(crate) fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    pub(crate) fn is_done(&self) -> bool {
        self.offset == self.bytes.len()
    }

    /// Bytes left to read. `offset` is only ever advanced to a value validated
    /// as `<= bytes.len()` by `read_exact`, so the subtraction never underflows.
    pub(crate) fn remaining(&self) -> usize {
        self.bytes.len() - self.offset
    }

    pub(crate) fn read_exact(
        &mut self,
        len: usize,
        label: &'static str,
    ) -> Result<&'a [u8], LogError> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or_else(|| LogError::StorageOffsetOverflow)?;
        if end > self.bytes.len() {
            return Err(LogError::UnexpectedEndOfStorage(label));
        }
        let out = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(out)
    }

    pub(crate) fn read_u8(&mut self, label: &'static str) -> Result<u8, LogError> {
        Ok(self.read_exact(1, label)?[0])
    }

    pub(crate) fn read_u32(&mut self, label: &'static str) -> Result<u32, LogError> {
        let bytes = self.read_exact(4, label)?;
        Ok(u32::from_le_bytes(
            bytes.try_into().map_err(|_| LogError::ExpectedU32Bytes)?,
        ))
    }

    pub(crate) fn read_u64(&mut self, label: &'static str) -> Result<u64, LogError> {
        let bytes = self.read_exact(8, label)?;
        Ok(u64::from_le_bytes(
            bytes.try_into().map_err(|_| LogError::ExpectedU64Bytes)?,
        ))
    }

    pub(crate) fn read_bytes(&mut self, label: &'static str) -> Result<&'a [u8], LogError> {
        let len = self.read_u32(label)? as usize;
        self.read_exact(len, label)
    }

    pub(crate) fn read_string(&mut self, label: &'static str) -> Result<String, LogError> {
        let bytes = self.read_bytes(label)?;
        String::from_utf8(bytes.to_vec()).map_err(|_| LogError::ExpectedUtf8String(label))
    }
}

pub(crate) fn read_plain_decrypted_thing_bytes<'a>(
    reader: &mut BorshReader<'a>,
    label: &'static str,
) -> Result<&'a [u8], LogError> {
    if reader.read_u8(label)? != 0 {
        return Err(LogError::OnlyPlaintextStorage);
    }
    if reader.read_u8(label)? != 0 {
        return Err(LogError::OnlyDecryptedStorage);
    }
    reader.read_bytes(label)
}

pub(crate) fn parse_plain_entry_v0_storage(
    bytes: &[u8],
) -> Result<ParsedPlainEntryV0Storage<'_>, LogError> {
    let mut reader = BorshReader::new(bytes);
    if reader.read_u8("entry variant")? != 0 {
        return Err(LogError::ExpectedEntryV0Variant);
    }
    let meta = read_plain_decrypted_thing_bytes(&mut reader, "entry meta")?;
    let payload = read_plain_decrypted_thing_bytes(&mut reader, "entry payload")?;
    reader.read_exact(4, "entry reserved bytes")?;
    let signable_prefix_len = reader.offset;
    if reader.read_u8("entry signatures option")? != 1 {
        return Err(LogError::ExpectedEntryV0Signatures);
    }
    if reader.read_u8("signatures variant")? != 0 {
        return Err(LogError::ExpectedSignaturesVariant);
    }
    if reader.read_u32("signatures length")? != 1 {
        return Err(LogError::ExpectedExactlyOneSignature);
    }
    let signature_with_key =
        read_plain_decrypted_thing_bytes(&mut reader, "entry signature with key")?;
    let signature_with_key_start = signature_with_key.as_ptr() as usize - bytes.as_ptr() as usize;
    let signature_with_key_len = signature_with_key.len();
    if reader.read_u8("entry hash option")? != 0 {
        return Err(LogError::ExpectedEmptyHashOption);
    }
    if !reader.is_done() {
        return Err(LogError::UnexpectedTrailingStorageBytes);
    }

    Ok(ParsedPlainEntryV0Storage {
        signable_prefix_len,
        meta,
        payload,
        signature_with_key,
        signature_with_key_start,
        signature_with_key_len,
    })
}

pub(crate) fn unsigned_entry_v0_storage_for_signing(
    bytes: &[u8],
    signable_prefix_len: usize,
) -> Result<Vec<u8>, LogError> {
    if signable_prefix_len > bytes.len() {
        return Err(LogError::InvalidSignablePrefixLength);
    }
    let mut out = Vec::with_capacity(signable_prefix_len + 2);
    out.extend_from_slice(&bytes[..signable_prefix_len]);
    write_u8(&mut out, 0); // signatures option
    write_u8(&mut out, 0); // hash option
    Ok(out)
}

pub(crate) fn parse_plain_entry_v0_storage_signature(
    bytes: &[u8],
) -> Result<ParsedEntryV0StorageSignature, LogError> {
    let storage = parse_plain_entry_v0_storage(bytes)?;
    let parsed_signature = parse_plain_signature_with_key(storage.signature_with_key)?;

    Ok(ParsedEntryV0StorageSignature {
        signable: unsigned_entry_v0_storage_for_signing(bytes, storage.signable_prefix_len)?,
        signature: parsed_signature.signature,
        public_key: parsed_signature.public_key,
    })
}

pub fn entry_v0_signature_public_key_from_storage_bytes(bytes: &[u8]) -> Result<Vec<u8>, LogError> {
    let storage = parse_plain_entry_v0_storage(bytes)?;
    Ok(parse_plain_signature_with_key(storage.signature_with_key)?.public_key)
}

pub(crate) fn parse_plain_signature_with_key_ref(
    bytes: &[u8],
) -> Result<ParsedSignatureWithKeyRef<'_>, LogError> {
    let mut signature_reader = BorshReader::new(bytes);
    if signature_reader.read_u8("signature variant")? != 0 {
        return Err(LogError::ExpectedSignatureWithKeyVariant);
    }
    let signature = signature_reader.read_bytes("signature bytes")?;
    if signature_reader.read_u8("signature public key variant")? != 0 {
        return Err(LogError::OnlyEd25519Signatures);
    }
    let public_key = signature_reader.read_exact(32, "signature public key")?;
    if signature_reader.read_u8("signature prehash")? != 0 {
        return Err(LogError::OnlyNonPrehashedSignatures);
    }
    if !signature_reader.is_done() {
        return Err(LogError::UnexpectedTrailingSignatureWithKeyBytes);
    }

    Ok(ParsedSignatureWithKeyRef {
        signature,
        public_key,
    })
}

pub(crate) fn parse_plain_signature_with_key(
    bytes: &[u8],
) -> Result<ParsedSignatureWithKey, LogError> {
    let parsed = parse_plain_signature_with_key_ref(bytes)?;
    Ok(ParsedSignatureWithKey {
        signature: parsed.signature.to_vec(),
        public_key: parsed.public_key.to_vec(),
    })
}

pub(crate) fn read_string_vec(
    reader: &mut BorshReader<'_>,
    label: &'static str,
) -> Result<Vec<String>, LogError> {
    let len = reader.read_u32(label)? as usize;
    // Each declared string is read via `read_string`, which first consumes a
    // 4-byte u32 length prefix. A count of `len` strings therefore needs at
    // least `len * 4` bytes to remain; if it does not, the declared vector
    // cannot be backed by the input, so reject before pre-allocating rather
    // than letting an attacker-controlled `len` drive a huge `with_capacity`.
    if len.saturating_mul(4) > reader.remaining() {
        return Err(LogError::UnexpectedEndOfStorage(label));
    }
    let mut values = Vec::with_capacity(len);
    for _ in 0..len {
        values.push(reader.read_string(label)?);
    }
    Ok(values)
}

pub(crate) fn read_optional_bytes(
    reader: &mut BorshReader<'_>,
    label: &'static str,
) -> Result<Option<Vec<u8>>, LogError> {
    match reader.read_u8(label)? {
        0 => Ok(None),
        1 => Ok(Some(reader.read_bytes(label)?.to_vec())),
        _ => Err(LogError::ExpectedOptionalBytesTag(label)),
    }
}

pub(crate) fn parse_raw_entry_v0_meta(bytes: &[u8]) -> Result<ParsedRawEntryV0Meta, LogError> {
    let mut reader = BorshReader::new(bytes);
    if reader.read_u8("meta variant")? != 0 {
        return Err(LogError::ExpectedEntryV0MetaVariant);
    }
    if reader.read_u8("clock variant")? != 0 {
        return Err(LogError::ExpectedLamportClockVariant);
    }
    let clock_id = reader.read_bytes("clock id")?.to_vec();
    if reader.read_u8("timestamp variant")? != 0 {
        return Err(LogError::ExpectedTimestampVariant);
    }
    let wall_time = reader.read_u64("timestamp wall time")?;
    let logical = reader.read_u32("timestamp logical")?;
    let gid = reader.read_string("meta gid")?;
    let next = read_string_vec(&mut reader, "meta next")?;
    let entry_type = reader.read_u8("meta type")?;
    let meta_data = read_optional_bytes(&mut reader, "meta data")?;
    if !reader.is_done() {
        return Err(LogError::UnexpectedTrailingMetaBytes);
    }
    Ok(ParsedRawEntryV0Meta {
        clock_id,
        wall_time,
        logical,
        gid,
        next,
        entry_type,
        meta_data,
    })
}

pub(crate) fn parse_raw_entry_v0_payload(
    bytes: &[u8],
) -> Result<ParsedRawEntryV0Payload<'_>, LogError> {
    let mut reader = BorshReader::new(bytes);
    if reader.read_u8("payload variant")? != 0 {
        return Err(LogError::ExpectedEntryV0PayloadVariant);
    }
    let data = reader.read_bytes("payload data")?;
    if !reader.is_done() {
        return Err(LogError::UnexpectedTrailingPayloadBytes);
    }
    Ok(ParsedRawEntryV0Payload { data })
}

pub(crate) fn encode_entry_v0(
    input: EntryV0EncodeInput,
    signature: Option<SignatureInput>,
) -> Vec<u8> {
    let meta = encode_meta(&input);
    let payload = encode_payload(&input.payload_data);
    encode_entry_v0_parts(&meta, &payload, signature)
}

pub(crate) fn encode_entry_v0_parts(
    meta: &[u8],
    payload: &[u8],
    signature: Option<SignatureInput>,
) -> Vec<u8> {
    let signature_with_key = signature
        .as_ref()
        .map(|signature| encode_signature_with_key(signature));
    encode_entry_v0_parts_with_signature_bytes(meta, payload, signature_with_key.as_deref())
}

pub(crate) fn encode_entry_v0_parts_unsigned_for_signing(meta: &[u8], payload: &[u8]) -> Vec<u8> {
    encode_entry_v0_parts_with_signature_bytes_and_extra_capacity(
        meta,
        payload,
        None,
        SIGNED_ENTRY_EXTRA_CAPACITY,
    )
}

pub(crate) fn encode_entry_v0_payload_data_unsigned_for_signing(
    meta: &[u8],
    payload_data: &[u8],
) -> Vec<u8> {
    let payload_len = 1 + 4 + payload_data.len();
    let mut out = Vec::with_capacity(
        1 + decrypted_thing_encoded_len(meta.len())
            + decrypted_thing_encoded_len(payload_len)
            + 4
            + 1
            + 1
            + SIGNED_ENTRY_EXTRA_CAPACITY,
    );
    write_u8(&mut out, 0); // EntryV0 variant
    write_decrypted_thing(&mut out, meta);
    write_u8(&mut out, 0); // MaybeEncrypted variant
    write_u8(&mut out, 0); // DecryptedThing variant
    write_u32(&mut out, payload_len as u32);
    write_u8(&mut out, 0); // Payload variant
    write_bytes(&mut out, payload_data);
    out.extend_from_slice(&[0, 0, 0, 0]); // reserved
    write_u8(&mut out, 0); // signatures option
    write_u8(&mut out, 0); // hash option
    out
}

pub(crate) fn encode_entry_v0_parts_with_signature_bytes(
    meta: &[u8],
    payload: &[u8],
    signature_with_key: Option<&[u8]>,
) -> Vec<u8> {
    encode_entry_v0_parts_with_signature_bytes_and_extra_capacity(
        meta,
        payload,
        signature_with_key,
        0,
    )
}

pub(crate) fn encode_entry_v0_parts_with_signature_bytes_and_extra_capacity(
    meta: &[u8],
    payload: &[u8],
    signature_with_key: Option<&[u8]>,
    extra_capacity: usize,
) -> Vec<u8> {
    let signature_len = signature_with_key
        .map(|signature_with_key| 1 + 4 + decrypted_thing_encoded_len(signature_with_key.len()))
        .unwrap_or(0);
    let mut out = Vec::with_capacity(
        1 + decrypted_thing_encoded_len(meta.len())
            + decrypted_thing_encoded_len(payload.len())
            + 4
            + 1
            + signature_len
            + 1
            + extra_capacity,
    );
    write_u8(&mut out, 0); // EntryV0 variant
    write_decrypted_thing(&mut out, meta);
    write_decrypted_thing(&mut out, payload);
    out.extend_from_slice(&[0, 0, 0, 0]); // reserved
    match signature_with_key {
        Some(signature_with_key) => {
            write_u8(&mut out, 1);
            write_signatures_encoded(&mut out, signature_with_key);
        }
        None => write_u8(&mut out, 0),
    }
    write_u8(&mut out, 0); // hash option
    out
}

pub(crate) fn signable_entry_to_signed_storage(
    mut signable_entry: Vec<u8>,
    signature_with_key: &[u8],
) -> Vec<u8> {
    debug_assert!(signable_entry.len() >= 2);
    signable_entry.truncate(signable_entry.len().saturating_sub(2));
    signable_entry.reserve(1 + 1 + 4 + decrypted_thing_encoded_len(signature_with_key.len()) + 1);
    write_u8(&mut signable_entry, 1);
    write_signatures_encoded(&mut signable_entry, signature_with_key);
    write_u8(&mut signable_entry, 0); // hash option
    signable_entry
}

pub(crate) fn encode_meta(input: &EntryV0EncodeInput) -> Vec<u8> {
    encode_meta_parts(
        &input.clock_id,
        input.wall_time,
        input.logical,
        &input.gid,
        &input.next,
        input.entry_type,
        input.meta_data.as_deref(),
    )
}

pub(crate) fn encode_meta_parts(
    clock_id: &[u8],
    wall_time: u64,
    logical: u32,
    gid: &str,
    next: &[String],
    entry_type: u8,
    meta_data: Option<&[u8]>,
) -> Vec<u8> {
    let next_bytes = next.iter().map(|next| 4 + next.len()).sum::<usize>();
    let meta_data_bytes = meta_data.map(|data| 4 + data.len()).unwrap_or(0);
    let mut out = Vec::with_capacity(
        1 + 1
            + 4
            + clock_id.len()
            + 1
            + 8
            + 4
            + 4
            + gid.len()
            + 4
            + next_bytes
            + 1
            + 1
            + meta_data_bytes,
    );
    write_u8(&mut out, 0); // Meta variant
    write_clock(&mut out, clock_id, wall_time, logical);
    write_string(&mut out, gid);
    write_u32(&mut out, next.len() as u32);
    for next_hash in next {
        write_string(&mut out, next_hash);
    }
    write_u8(&mut out, entry_type);
    match meta_data {
        Some(data) => {
            write_u8(&mut out, 1);
            write_bytes(&mut out, data);
        }
        None => write_u8(&mut out, 0),
    }
    out
}

pub(crate) fn write_clock(out: &mut Vec<u8>, clock_id: &[u8], wall_time: u64, logical: u32) {
    write_u8(out, 0); // LamportClock variant
    write_bytes(out, clock_id);
    write_u8(out, 0); // Timestamp variant
    write_u64(out, wall_time);
    write_u32(out, logical);
}

pub(crate) fn encode_payload(data: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + 4 + data.len());
    write_u8(&mut out, 0); // Payload variant
    write_bytes(&mut out, data);
    out
}

pub(crate) fn write_signatures_encoded(out: &mut Vec<u8>, signature_with_key: &[u8]) {
    write_u8(out, 0); // Signatures variant
    write_u32(out, 1);
    write_decrypted_thing(out, signature_with_key);
}

pub(crate) fn encode_signature_with_key(signature: &SignatureInput) -> Vec<u8> {
    encode_signature_with_key_parts(
        &signature.signature,
        &signature.public_key,
        signature.prehash,
    )
}

pub(crate) fn encode_signature_with_key_parts(
    signature: &[u8],
    public_key: &[u8],
    prehash: u8,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + 4 + signature.len() + 1 + public_key.len() + 1);
    write_u8(&mut out, 0); // SignatureWithKey variant
    write_bytes(&mut out, signature);
    write_u8(&mut out, 0); // Ed25519PublicKey variant
    out.extend_from_slice(public_key);
    write_u8(&mut out, prehash);
    out
}

pub(crate) fn decrypted_thing_encoded_len(data_len: usize) -> usize {
    2 + 4 + data_len
}

pub(crate) fn write_decrypted_thing(out: &mut Vec<u8>, data: &[u8]) {
    write_u8(out, 0); // MaybeEncrypted variant
    write_u8(out, 0); // DecryptedThing variant
    write_bytes(out, data);
}

pub(crate) fn write_string(out: &mut Vec<u8>, value: &str) {
    write_bytes(out, value.as_bytes());
}

pub(crate) fn write_bytes(out: &mut Vec<u8>, value: &[u8]) {
    write_u32(out, value.len() as u32);
    out.extend_from_slice(value);
}

pub(crate) fn write_u8(out: &mut Vec<u8>, value: u8) {
    out.push(value);
}

pub(crate) fn write_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

pub(crate) fn write_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}
