---
"@peerbit/stream": patch
---

End an unresolved single-recipient direct delivery wait when its closed stream has a current writable replacement, allowing caller recovery without waiting for the obsolete ACK timeout. Preserve authenticated peer sessions and normal ACK handling for relayed, redundant, explicit and shared same-ID deliveries. This does not change persisted-receipt guarantees or reset application retry backoff.
