---
"@peerbit/shared-log": patch
---

The reconnect entry inventory no longer asks a peer again about entries the peer already confirmed when a local write restarts the pass. Confirmations are kept for a peer while its session, transport session and receive epoch, the ownership snapshot and the entry index stay the same, up to 65,536 hashes per log, and are dropped when the pass completes or the peer reconnects or leaves. A pass still completes only after one walk without a local write.
