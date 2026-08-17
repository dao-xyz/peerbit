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
    InvalidRangeSnapshotLimit(&'static str),
    RangeSnapshotIndexInconsistent,
    RangeSnapshotResolutionMismatch,
    RangeSnapshotInvalidRange,
    RangeSnapshotAccountingOverflow,
    InvalidRebalanceCollisionBucketLimit(&'static str),
    RebalanceCollisionBucketIndexInconsistent,
    RebalanceCollisionBucketResolutionMismatch,
    RebalanceCollisionBucketAccountingOverflow,
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
            SharedLogError::InvalidRangeSnapshotLimit(label) => {
                write!(f, "Invalid native range snapshot limit: {label}")
            }
            SharedLogError::RangeSnapshotIndexInconsistent => {
                f.write_str("Native range snapshot index is inconsistent")
            }
            SharedLogError::RangeSnapshotResolutionMismatch => {
                f.write_str("Native range snapshot value exceeds its resolution")
            }
            SharedLogError::RangeSnapshotInvalidRange => {
                f.write_str("Native range snapshot contains an invalid range")
            }
            SharedLogError::RangeSnapshotAccountingOverflow => {
                f.write_str("Native range snapshot accounting overflow")
            }
            SharedLogError::InvalidRebalanceCollisionBucketLimit(label) => {
                write!(
                    f,
                    "Invalid native rebalance collision bucket limit: {label}"
                )
            }
            SharedLogError::RebalanceCollisionBucketIndexInconsistent => {
                f.write_str("Native rebalance collision bucket index is inconsistent")
            }
            SharedLogError::RebalanceCollisionBucketResolutionMismatch => {
                f.write_str("Native rebalance collision bucket value exceeds its resolution")
            }
            SharedLogError::RebalanceCollisionBucketAccountingOverflow => {
                f.write_str("Native rebalance collision bucket accounting overflow")
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
