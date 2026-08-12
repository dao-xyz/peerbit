---
"@peerbit/shared-log": major
---

Retire the pre-v10 replication-info network compatibility modes. `SharedLogOptions.compatibility` is removed; any explicitly-defined value passed at open — including 10, which previously behaved like the default — now rejects with `CompatibilityModeRetiredError` before any open-time side effect (an explicitly-present `undefined` stays accepted). The legacy wire variants [1,0]-[1,4] remain registered decode tombstones and published exports, so frames from old peers still decode (and are dropped by the default inbound guard); `ReplicationPingMessage` ([1,5]) is unaffected.
