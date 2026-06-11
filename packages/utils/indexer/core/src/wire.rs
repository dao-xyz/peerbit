//! Little-endian wire-decoding primitives shared by the native crates.

use std::fmt;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WireError {
    Truncated(&'static str),
    InvalidUtf8(&'static str),
    Trailing(&'static str),
}

impl fmt::Display for WireError {
    // The rendered text is surfaced verbatim across the wasm boundary, so it
    // must stay byte-for-byte stable.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Truncated(label) => write!(formatter, "Truncated {label}"),
            Self::InvalidUtf8(label) => write!(formatter, "Invalid utf-8 {label}"),
            Self::Trailing(label) => write!(formatter, "Trailing {label} bytes"),
        }
    }
}

impl std::error::Error for WireError {}

pub fn read_u32(bytes: &[u8], offset: &mut usize, label: &'static str) -> Result<u32, WireError> {
    let end = offset.checked_add(4).ok_or(WireError::Truncated(label))?;
    if end > bytes.len() {
        return Err(WireError::Truncated(label));
    }
    let value = u32::from_le_bytes(
        bytes[*offset..end]
            .try_into()
            .expect("slice length checked"),
    );
    *offset = end;
    Ok(value)
}

pub fn read_u64(bytes: &[u8], offset: &mut usize, label: &'static str) -> Result<u64, WireError> {
    let end = offset.checked_add(8).ok_or(WireError::Truncated(label))?;
    if end > bytes.len() {
        return Err(WireError::Truncated(label));
    }
    let value = u64::from_le_bytes(
        bytes[*offset..end]
            .try_into()
            .expect("slice length checked"),
    );
    *offset = end;
    Ok(value)
}

pub fn read_bool(bytes: &[u8], offset: &mut usize, label: &'static str) -> Result<bool, WireError> {
    if *offset >= bytes.len() {
        return Err(WireError::Truncated(label));
    }
    let value = bytes[*offset] != 0;
    *offset += 1;
    Ok(value)
}

pub fn read_encoded_string(
    bytes: &[u8],
    offset: &mut usize,
    label: &'static str,
) -> Result<String, WireError> {
    let length = read_u32(bytes, offset, label)? as usize;
    let end = offset
        .checked_add(length)
        .ok_or(WireError::Truncated(label))?;
    if end > bytes.len() {
        return Err(WireError::Truncated(label));
    }
    let value = std::str::from_utf8(&bytes[*offset..end])
        .map_err(|_| WireError::InvalidUtf8(label))?
        .to_string();
    *offset = end;
    Ok(value)
}

pub fn read_bytes(
    bytes: &[u8],
    offset: &mut usize,
    label: &'static str,
) -> Result<Vec<u8>, WireError> {
    let length = read_u32(bytes, offset, label)? as usize;
    let end = offset
        .checked_add(length)
        .ok_or(WireError::Truncated(label))?;
    if end > bytes.len() {
        return Err(WireError::Truncated(label));
    }
    let value = bytes[*offset..end].to_vec();
    *offset = end;
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::{read_bool, read_bytes, read_encoded_string, read_u32, read_u64, WireError};

    #[test]
    fn reads_advance_offset_in_order() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&7u32.to_le_bytes());
        bytes.extend_from_slice(&8u64.to_le_bytes());
        bytes.push(1);
        bytes.extend_from_slice(&2u32.to_le_bytes());
        bytes.extend_from_slice(b"hi");
        bytes.extend_from_slice(&3u32.to_le_bytes());
        bytes.extend_from_slice(&[4, 5, 6]);

        let mut offset = 0;
        assert_eq!(read_u32(&bytes, &mut offset, "u32").unwrap(), 7);
        assert_eq!(read_u64(&bytes, &mut offset, "u64").unwrap(), 8);
        assert!(read_bool(&bytes, &mut offset, "bool").unwrap());
        assert_eq!(
            read_encoded_string(&bytes, &mut offset, "string").unwrap(),
            "hi"
        );
        assert_eq!(read_bytes(&bytes, &mut offset, "bytes").unwrap(), [4, 5, 6]);
        assert_eq!(offset, bytes.len());
    }

    #[test]
    fn truncated_reads_carry_their_label() {
        let mut offset = 0;
        assert_eq!(
            read_u32(&[1, 2], &mut offset, "field"),
            Err(WireError::Truncated("field"))
        );
        assert_eq!(offset, 0);
    }

    #[test]
    fn corrupt_length_prefix_errors_instead_of_overreading() {
        let bytes = u32::MAX.to_le_bytes();
        let mut offset = 0;
        assert_eq!(
            read_bytes(&bytes, &mut offset, "payload"),
            Err(WireError::Truncated("payload"))
        );
    }

    #[test]
    fn invalid_utf8_errors_with_label() {
        let mut bytes = 1u32.to_le_bytes().to_vec();
        bytes.push(0xff);
        let mut offset = 0;
        assert_eq!(
            read_encoded_string(&bytes, &mut offset, "name"),
            Err(WireError::InvalidUtf8("name"))
        );
    }
}
