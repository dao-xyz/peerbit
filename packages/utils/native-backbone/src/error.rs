use peerbit_indexer_core::persistence::DecodeError;
use peerbit_indexer_core::schema::SchemaError;
use peerbit_indexer_core::wire::WireError;
use peerbit_log_rust::LogError;
use peerbit_shared_log_rust::SharedLogError;
use wasm_bindgen::JsValue;

/// Error type for the native backbone core. Every failure path in the core
/// modules reports one of these variants instead of constructing a
/// `JsValue`, so the crate can be consumed as a plain rlib on non-wasm
/// targets without aborting the process. The `Display` output reproduces
/// the exact message strings historically thrown across the wasm boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BackboneError {
    Expected(&'static str),
    ExpectedArray(&'static str),
    ExpectedString(&'static str),
    ExpectedBoolean(&'static str),
    ExpectedNumber(&'static str),
    ExpectedBytes(&'static str),
    ExpectedU64String(&'static str),
    ExpectedStringArray,
    ExpectedBytesArray,
    ExpectedUnsignedIntegerArray,
    MismatchedInputLengths(&'static str),
    MustBeNumber(&'static str),
    MustBeArray(&'static str),
    MissingOrInvalid(&'static str),
    HashDigestTooShortU32,
    HashDigestTooShortU64,
    ResolutionMustBeU32OrU64,
    ExpectedReservedBytes,
    WireSyncStashFrameTaken,
    WireSyncPinnedEntryMissing,
    CoordinateCountOverflow,
    Wire(WireError),
    SharedLog(SharedLogError),
    Schema(SchemaError),
    Decode(DecodeError),
    Log(LogError),
    Message(String),
}

impl std::fmt::Display for BackboneError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BackboneError::Expected(label) => write!(f, "Expected {label}"),
            BackboneError::ExpectedArray(label) => write!(f, "Expected {label} array"),
            BackboneError::ExpectedString(label) => write!(f, "Expected {label} string"),
            BackboneError::ExpectedBoolean(label) => write!(f, "Expected {label} boolean"),
            BackboneError::ExpectedNumber(label) => write!(f, "Expected {label} number"),
            BackboneError::ExpectedBytes(label) => write!(f, "Expected {label} bytes"),
            BackboneError::ExpectedU64String(label) => {
                write!(f, "Expected {label} u64 string")
            }
            BackboneError::ExpectedStringArray => f.write_str("Expected string array"),
            BackboneError::ExpectedBytesArray => f.write_str("Expected bytes array"),
            BackboneError::ExpectedUnsignedIntegerArray => {
                f.write_str("Expected unsigned integer array")
            }
            BackboneError::MismatchedInputLengths(label) => {
                write!(f, "Mismatched {label} input lengths")
            }
            BackboneError::MustBeNumber(label) => write!(f, "{label} must be a number"),
            BackboneError::MustBeArray(field) => write!(f, "{field} must be an array"),
            BackboneError::MissingOrInvalid(field) => write!(f, "Missing or invalid {field}"),
            BackboneError::HashDigestTooShortU32 => {
                f.write_str("hash digest must have at least 4 bytes")
            }
            BackboneError::HashDigestTooShortU64 => {
                f.write_str("hash digest must have at least 8 bytes")
            }
            BackboneError::ResolutionMustBeU32OrU64 => f.write_str("resolution must be u32 or u64"),
            BackboneError::ExpectedReservedBytes => f.write_str("expected 4 reserved bytes"),
            BackboneError::WireSyncStashFrameTaken => {
                f.write_str("wire sync stash frame already taken")
            }
            BackboneError::WireSyncPinnedEntryMissing => {
                f.write_str("wire sync pinned stash entry missing")
            }
            BackboneError::CoordinateCountOverflow => f.write_str("Coordinate count overflow"),
            BackboneError::Wire(error) => write!(f, "{error}"),
            BackboneError::SharedLog(error) => write!(f, "{error}"),
            BackboneError::Schema(error) => write!(f, "{error}"),
            BackboneError::Decode(error) => write!(f, "{error}"),
            BackboneError::Log(error) => write!(f, "{error}"),
            BackboneError::Message(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for BackboneError {}

impl From<WireError> for BackboneError {
    fn from(error: WireError) -> Self {
        BackboneError::Wire(error)
    }
}

impl From<SchemaError> for BackboneError {
    fn from(error: SchemaError) -> Self {
        BackboneError::Schema(error)
    }
}

impl From<DecodeError> for BackboneError {
    fn from(error: DecodeError) -> Self {
        BackboneError::Decode(error)
    }
}

impl From<LogError> for BackboneError {
    fn from(error: LogError) -> Self {
        BackboneError::Log(error)
    }
}

impl From<SharedLogError> for BackboneError {
    fn from(error: SharedLogError) -> Self {
        BackboneError::SharedLog(error)
    }
}

impl From<BackboneError> for JsValue {
    fn from(error: BackboneError) -> Self {
        JsValue::from_str(&error.to_string())
    }
}
