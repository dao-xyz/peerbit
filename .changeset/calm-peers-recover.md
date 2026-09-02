---
"@peerbit/shared-log": patch
---

Re-arm replication-info V2 requests after a temporary receive gate consumes a scheduled attempt, preventing reconnect recovery from stalling without a timer or in-flight request.
