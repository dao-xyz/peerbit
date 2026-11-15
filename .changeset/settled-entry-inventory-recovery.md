---
"@peerbit/shared-log": patch
---

Recover eligible held entries after a writable transport replacement without requiring a new topic session. Bound recovery to small authoritative inventory pages, check fresh session-bound presence before sending payloads, and invalidate interrupted passes across local mutations or ownership changes. Presence checks are not persisted delivery receipts. Legacy peers keep their existing synchronization protocol.
