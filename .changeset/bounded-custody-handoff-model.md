---
"@peerbit/shared-log": patch
---

Add an internal, unwired V1 custody handoff codec and bounded per-record persistence model. The slice authenticates one-entry handoff and receipt artifacts, models crash-safe source and destination transitions with strict A/B recovery, and deliberately exposes no transfer scheduling, receipt transport, release, deletion, or prune authority.
