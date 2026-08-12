---
"@peerbit/document": major
---

Retire the document `compatibility: 6 | 7` open option. The option is removed from `SetupOptions` and any explicitly-defined value now rejects at open with the document-named `DocumentCompatibilityRetiredError` (an explicitly-present `undefined` stays accepted); the historical 6 -> log v8 / 7 -> log v9 mapping is gone, so no value can reach shared-log compatibility semantics. Compatibility-6 encode (`PutWithKeyOperation`) is retired, but persisted-data decode remains: `PutWithKeyOperation` (tag 0) and `DeleteByStringKeyOperation` (tag 2) stay registered and readable — old stores stay openable without any option.
