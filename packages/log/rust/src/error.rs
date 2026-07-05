use wasm_bindgen::JsValue;

/// Error type for the native log core. Every failure path in the core
/// modules reports one of these variants instead of constructing a
/// `JsValue`, so the crate can be consumed as a plain rlib on non-wasm
/// targets without aborting the process. The `Display` output reproduces
/// the exact message strings historically thrown across the wasm boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogError {
    ExpectedEd25519PrivateKeyLength32,
    ExpectedEd25519PublicKeyLength32,
    ExpectedEd25519SignatureLength64,
    InvalidEd25519PublicKey,
    Ed25519KeypairMismatch,
    PayloadByteLengthExceedsU32,
    RawEntryBlockHashLengthMismatch,
    RawEntryHashMismatch,
    ExpectedBase58btcCid,
    InvalidBase58btcCid,
    ExpectedRawCidV1Sha256Cid,
    StorageOffsetOverflow,
    UnexpectedEndOfStorage(&'static str),
    ExpectedU32Bytes,
    ExpectedU64Bytes,
    ExpectedUtf8String(&'static str),
    ExpectedOptionalBytesTag(&'static str),
    OnlyPlaintextStorage,
    OnlyDecryptedStorage,
    ExpectedEntryV0Variant,
    ExpectedEntryV0Signatures,
    ExpectedSignaturesVariant,
    ExpectedExactlyOneSignature,
    ExpectedEmptyHashOption,
    UnexpectedTrailingStorageBytes,
    InvalidSignablePrefixLength,
    ExpectedSignatureWithKeyVariant,
    OnlyEd25519Signatures,
    OnlyNonPrehashedSignatures,
    UnexpectedTrailingSignatureWithKeyBytes,
    ExpectedEntryV0MetaVariant,
    ExpectedLamportClockVariant,
    ExpectedTimestampVariant,
    UnexpectedTrailingMetaBytes,
    ExpectedEntryV0PayloadVariant,
    UnexpectedTrailingPayloadBytes,
    SignatureOffsetOverflow,
    InvalidPreparedSignatureOffset,
}

impl std::fmt::Display for LogError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LogError::ExpectedEd25519PrivateKeyLength32 => {
                f.write_str("Expected Ed25519 private key length 32")
            }
            LogError::ExpectedEd25519PublicKeyLength32 => {
                f.write_str("Expected Ed25519 public key length 32")
            }
            LogError::ExpectedEd25519SignatureLength64 => {
                f.write_str("Expected Ed25519 signature length 64")
            }
            LogError::InvalidEd25519PublicKey => f.write_str("Invalid Ed25519 public key"),
            LogError::Ed25519KeypairMismatch => {
                f.write_str("Ed25519 public key does not match private key")
            }
            LogError::PayloadByteLengthExceedsU32 => f.write_str("Payload byte length exceeds u32"),
            LogError::RawEntryBlockHashLengthMismatch => {
                f.write_str("Expected equal raw entry block and hash lengths")
            }
            LogError::RawEntryHashMismatch => f.write_str("Raw entry hash did not match bytes"),
            LogError::ExpectedBase58btcCid => f.write_str("Expected base58btc CID"),
            LogError::InvalidBase58btcCid => f.write_str("Invalid base58btc CID"),
            LogError::ExpectedRawCidV1Sha256Cid => f.write_str("Expected raw CIDv1 sha2-256 CID"),
            LogError::StorageOffsetOverflow => f.write_str("EntryV0 storage offset overflow"),
            LogError::UnexpectedEndOfStorage(label) => {
                write!(f, "Unexpected end of EntryV0 storage while reading {label}")
            }
            LogError::ExpectedU32Bytes => f.write_str("Expected u32 bytes"),
            LogError::ExpectedU64Bytes => f.write_str("Expected u64 bytes"),
            LogError::ExpectedUtf8String(label) => {
                write!(f, "Expected UTF-8 string for {label}")
            }
            LogError::ExpectedOptionalBytesTag(label) => {
                write!(f, "Expected optional bytes tag for {label}")
            }
            LogError::OnlyPlaintextStorage => {
                f.write_str("Only plaintext EntryV0 storage can be verified natively")
            }
            LogError::OnlyDecryptedStorage => {
                f.write_str("Only decrypted EntryV0 storage can be verified natively")
            }
            LogError::ExpectedEntryV0Variant => f.write_str("Expected EntryV0 variant"),
            LogError::ExpectedEntryV0Signatures => f.write_str("Expected EntryV0 signatures"),
            LogError::ExpectedSignaturesVariant => f.write_str("Expected Signatures variant"),
            LogError::ExpectedExactlyOneSignature => {
                f.write_str("Expected exactly one EntryV0 signature for native verification")
            }
            LogError::ExpectedEmptyHashOption => {
                f.write_str("Expected EntryV0 hash option to be empty")
            }
            LogError::UnexpectedTrailingStorageBytes => {
                f.write_str("Unexpected trailing EntryV0 storage bytes")
            }
            LogError::InvalidSignablePrefixLength => {
                f.write_str("Invalid EntryV0 signable prefix length")
            }
            LogError::ExpectedSignatureWithKeyVariant => {
                f.write_str("Expected SignatureWithKey variant")
            }
            LogError::OnlyEd25519Signatures => {
                f.write_str("Only Ed25519 EntryV0 signatures can be verified natively")
            }
            LogError::OnlyNonPrehashedSignatures => {
                f.write_str("Only non-prehashed EntryV0 signatures can be verified natively")
            }
            LogError::UnexpectedTrailingSignatureWithKeyBytes => {
                f.write_str("Unexpected trailing SignatureWithKey bytes")
            }
            LogError::ExpectedEntryV0MetaVariant => f.write_str("Expected EntryV0 meta variant"),
            LogError::ExpectedLamportClockVariant => f.write_str("Expected LamportClock variant"),
            LogError::ExpectedTimestampVariant => f.write_str("Expected Timestamp variant"),
            LogError::UnexpectedTrailingMetaBytes => {
                f.write_str("Unexpected trailing EntryV0 meta bytes")
            }
            LogError::ExpectedEntryV0PayloadVariant => {
                f.write_str("Expected EntryV0 payload variant")
            }
            LogError::UnexpectedTrailingPayloadBytes => {
                f.write_str("Unexpected trailing EntryV0 payload bytes")
            }
            LogError::SignatureOffsetOverflow => f.write_str("EntryV0 signature offset overflow"),
            LogError::InvalidPreparedSignatureOffset => {
                f.write_str("Invalid prepared EntryV0 signature offset")
            }
        }
    }
}

impl std::error::Error for LogError {}

impl From<LogError> for JsValue {
    fn from(error: LogError) -> Self {
        JsValue::from_str(&error.to_string())
    }
}
