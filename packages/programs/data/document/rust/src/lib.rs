use js_sys::{Array, Uint32Array, Uint8Array};
use wasm_bindgen::prelude::*;

fn parse_u64(value: &str, field: &str) -> Result<u64, JsValue> {
    value
        .parse::<u64>()
        .map_err(|_| JsValue::from_str(&format!("Invalid {field} value")))
}

fn write_u32_le(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn write_u64_le(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn encode_context_suffix_inner(
    created: u64,
    modified: u64,
    head: &str,
    gid: &str,
    size: u32,
) -> Vec<u8> {
    let head_bytes = head.as_bytes();
    let gid_bytes = gid.as_bytes();
    let mut out = Vec::with_capacity(1 + 8 + 8 + 4 + head_bytes.len() + 4 + gid_bytes.len() + 4);
    out.push(0);
    write_u64_le(&mut out, created);
    write_u64_le(&mut out, modified);
    write_u32_le(&mut out, head_bytes.len() as u32);
    out.extend_from_slice(head_bytes);
    write_u32_le(&mut out, gid_bytes.len() as u32);
    out.extend_from_slice(gid_bytes);
    write_u32_le(&mut out, size);
    out
}

#[wasm_bindgen]
pub fn encode_context_suffix(
    created: &str,
    modified: &str,
    head: &str,
    gid: &str,
    size: u32,
) -> Result<Uint8Array, JsValue> {
    let created = parse_u64(created, "created")?;
    let modified = parse_u64(modified, "modified")?;
    let bytes = encode_context_suffix_inner(created, modified, head, gid, size);
    Ok(Uint8Array::from(bytes.as_slice()))
}

#[wasm_bindgen]
pub fn encode_context_suffix_batch(
    createds: Array,
    modifieds: Array,
    heads: Array,
    gids: Array,
    sizes: Uint32Array,
) -> Result<Array, JsValue> {
    let len = createds.length();
    if modifieds.length() != len
        || heads.length() != len
        || gids.length() != len
        || sizes.length() != len
    {
        return Err(JsValue::from_str("Mismatched context batch lengths"));
    }

    let out = Array::new_with_length(len);
    for index in 0..len {
        let created = createds
            .get(index)
            .as_string()
            .ok_or_else(|| JsValue::from_str("Invalid created value"))?;
        let modified = modifieds
            .get(index)
            .as_string()
            .ok_or_else(|| JsValue::from_str("Invalid modified value"))?;
        let head = heads
            .get(index)
            .as_string()
            .ok_or_else(|| JsValue::from_str("Invalid head value"))?;
        let gid = gids
            .get(index)
            .as_string()
            .ok_or_else(|| JsValue::from_str("Invalid gid value"))?;
        let bytes = encode_context_suffix_inner(
            parse_u64(&created, "created")?,
            parse_u64(&modified, "modified")?,
            &head,
            &gid,
            sizes.get_index(index),
        );
        out.set(index, Uint8Array::from(bytes.as_slice()).into());
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::encode_context_suffix_inner;

    #[test]
    fn encodes_context_suffix() {
        let bytes = encode_context_suffix_inner(1, 2, "head", "gid", 3);
        assert_eq!(bytes[0], 0);
        assert_eq!(&bytes[1..9], &1u64.to_le_bytes());
        assert_eq!(&bytes[9..17], &2u64.to_le_bytes());
        assert_eq!(&bytes[17..21], &4u32.to_le_bytes());
        assert_eq!(&bytes[21..25], b"head");
        assert_eq!(&bytes[25..29], &3u32.to_le_bytes());
        assert_eq!(&bytes[29..32], b"gid");
        assert_eq!(&bytes[32..36], &3u32.to_le_bytes());
    }
}
