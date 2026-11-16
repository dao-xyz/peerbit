---
"@peerbit/blocks": patch
"@peerbit/shared-log": patch
"peerbit": patch
---

Writes no longer wait for best-effort provider announcements. `put`, `putMany`, `putKnown` and `putKnownMany` (and so `SharedLog.append` and document puts) resolve once the blocks are stored locally, so a peer whose only bootstrap never answers no longer waits up to the 2-second bootstrap dial budget on every write. `onPut`/`onPutMany` now run afterwards as bounded background work, and `stop()` discards waiting CIDs and aborts running hooks through a `signal` passed as their new optional second argument. Closing a peer or a log therefore no longer waits for an unanswered provider dial or a congested control send. Use `waitForStoredNotifications()` to wait for the announcements of earlier writes, or set `awaitPutHooks: true` to keep the previous behaviour of awaiting the hooks inside each write.

In the background queue, duplicate CIDs coalesce and each write is queued whole while fewer than 4096 CIDs wait, so one large write is announced in full. A write that arrives while 4096 or more CIDs wait is not announced at all: its CIDs are counted as `dropped` in `getStoredNotificationTelemetry()`, and the first drop of each store generation is logged in the `peerbit:transport:blocks:stored-notifications:warn` scope (later ones at debug).

`onPutMany` also receives `batch`: true when its CIDs include a write of several blocks, false when the call only gathers writes of one block each that queued behind a running announcement. A SharedLog starts its log-wide provider lease only for batches, so whether a writer that does not replicate advertises the whole log no longer depends on whether its single appends happened to queue together. A native document batch is announced as one write.
