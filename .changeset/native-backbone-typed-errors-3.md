---
"@peerbit/native-backbone": patch
---

Typed native error paths for the backbone core (final slice)

Eliminates the last `Result<_, JsValue>` error surface in the crate: the four
`document_index_*_append_commit` builders and
`validate_document_index_required_previous_signer` in documents.rs now report a
typed `BackboneError` instead of constructing `JsValue`s. These were the only
paths still pinned to `JsValue` by the frozen
`make_document_index_commit` closure contract in
`append_tx/committed_latest`; that contract is retyped to
`-> Result<DocumentIndexAppendCommit, BackboneError>` in the same change, so the
builders and their closures now type-check end to end without a JsValue seam.

The required-previous-signer validator's two error literals become dedicated
variants (`PreviousDocumentSignerPublicKeyUnavailable` and
`PreviousDocumentSignerPublicKeyPolicyMismatch`) rendering their historical
strings byte-for-byte. With every caller now typed, the local
`js_wrapper_error` verbatim-forward seam in committed_latest.rs is dead and is
removed. Every `#[wasm_bindgen]` export keeps its exact signature and reaches JS
only through the single `From<BackboneError> for JsValue` conversion.
