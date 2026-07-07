---
"@peerbit/native-backbone": patch
---

Typed native error paths for the backbone core (part 2)

Completes the JsValue→typed-error refactor started in the previous release:
the append transaction modules (append_tx/storage, facts, committed_no_next,
committed_latest, mod), the document projection/query/index paths
(documents.rs) and the raw-receive verify/commit hot path (raw_receive.rs)
now report a typed `BackboneError` internally instead of constructing
`JsValue`s, so the crate no longer aborts on error when consumed as a native
rlib. Every `#[wasm_bindgen]` export keeps its exact signature and every
error message string is reproduced byte-for-byte.

Notable non-mechanical changes, each behavior-preserving:

- Four append dispatch paths now call log-rust's typed `_core` builders
  directly instead of its JsValue wrappers, rebuilding the frozen result-row
  layouts locally (facts rows via the pre-existing `committed_entry_facts_to_row`,
  trim rows via a byte-identical replica of `log_trim_entries_to_rows` fed by
  the same `trim_oldest_log_entries_core`).
- The pending latest-batch append state no longer captures `js_sys::Array`
  handles: it holds owned facts and trimmed entries, and the JS rows are built
  at the emit boundary. The log append/commit/trim side effects still happen at
  the same point; only row construction is deferred (skipped entirely when a
  later fallible planning step aborts the append).
- Two `expect()` calls that trapped the whole wasm instance (in wire-sync, and
  a partial-verify-hashes invariant in raw-receive) became typed errors.
- The duplicate `js_error`/`decode_error` funnels in documents.rs were deleted
  once all their call sites were typed.

The two Ed25519 verification `.ok()` fallbacks in raw-receive are intentionally
preserved as documented, control-flow-unchanged swallows: their only error is a
signature-slice parse failure (a non-Ed25519 scheme or malformed bytes), and
deferring to the TypeScript verification fallback is correct for mixed-scheme
verification. The swallow is now explicit and commented rather than a bare
`.ok()`.
