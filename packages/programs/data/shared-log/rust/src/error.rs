use wasm_bindgen::JsValue;

/// Error type for the shared-log range planner core. Every failure path in
/// the internal planning and parsing logic reports one of these variants
/// instead of constructing a `JsValue`, so the crate can be consumed as a
/// plain rlib on non-wasm targets. The `Display` output reproduces the exact
/// message strings historically thrown across the wasm boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SharedLogError {
    Expected(&'static str),
    ExpectedUnsignedIntegerString,
    ExpectedStringArray,
    ExpectedNumberArray,
    ExpectedUnsignedIntegerArray,
    ExpectedOptionalUnsignedInteger,
    ExpectedOptionalStringArray,
    ExpectedOptionalGidString,
    ExpectedLeaderSampleRow,
    ExpectedLeaderHashString,
    ExpectedLeaderIntersectingBool,
    MismatchedInputLengths(&'static str),
    MissingCompactAppendFacts,
}

impl std::fmt::Display for SharedLogError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SharedLogError::Expected(label) => write!(f, "Expected {label}"),
            SharedLogError::ExpectedUnsignedIntegerString => {
                f.write_str("Expected unsigned integer string")
            }
            SharedLogError::ExpectedStringArray => f.write_str("Expected string array"),
            SharedLogError::ExpectedNumberArray => f.write_str("Expected number array"),
            SharedLogError::ExpectedUnsignedIntegerArray => {
                f.write_str("Expected unsigned integer array")
            }
            SharedLogError::ExpectedOptionalUnsignedInteger => {
                f.write_str("Expected optional unsigned integer")
            }
            SharedLogError::ExpectedOptionalStringArray => {
                f.write_str("Expected optional string array")
            }
            SharedLogError::ExpectedOptionalGidString => {
                f.write_str("Expected optional gid string")
            }
            SharedLogError::ExpectedLeaderSampleRow => f.write_str("Expected leader sample row"),
            SharedLogError::ExpectedLeaderHashString => f.write_str("Expected leader hash string"),
            SharedLogError::ExpectedLeaderIntersectingBool => {
                f.write_str("Expected leader intersecting bool")
            }
            SharedLogError::MismatchedInputLengths(label) => {
                write!(f, "Mismatched {label} input lengths")
            }
            SharedLogError::MissingCompactAppendFacts => {
                f.write_str("Missing compact append facts")
            }
        }
    }
}

impl std::error::Error for SharedLogError {}

impl From<SharedLogError> for JsValue {
    fn from(error: SharedLogError) -> Self {
        JsValue::from_str(&error.to_string())
    }
}
