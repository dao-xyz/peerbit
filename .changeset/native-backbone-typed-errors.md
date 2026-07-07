---
"@peerbit/native-backbone": patch
"@peerbit/shared-log-rust": patch
---

Typed native error paths for the backbone core (part 1)

- native-backbone: new `BackboneError` enum (Display reproduces the exact
  message strings historically thrown across the wasm boundary; single
  `From<BackboneError> for JsValue` touchpoint). The js_interop helpers,
  leaf modules (coordinates, sync_send, wire_sync), graph/profile paths and
  the shared-log planner glue now report typed errors internally; every
  `#[wasm_bindgen]` export keeps its exact signature. All 159
  `js_sys::Date::now()` profiling sites now go through a
  `cfg(target_arch)` clock shim so the crate can compile natively.
- Deliberate validation hardening in the JS marshaling helpers: byte
  fields reject non-Uint8Array values instead of coercing garbage, f64
  integer conversions reject non-finite/negative/fractional/out-of-range
  values (including the 2^64 rounding trap) instead of truncating, and
  present-but-non-string optional fields error instead of reading as
  absent. Two `expect()` aborts in wire-sync became typed errors.
- shared-log-rust: new `SharedLogError` enum following the same pattern;
  internal planner/parsing helpers are typed, wasm surface unchanged, and
  a typed `put_entry_coordinates_core` lets dependants skip the
  string/Array round-trip.
