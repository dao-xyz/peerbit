---
"@peerbit/blocks": patch
"@peerbit/shared-log": patch
"peerbit": patch
---

Writes no longer wait for best-effort provider announcements. `put`, `putMany`, `putKnown` and `putKnownMany` (and so `SharedLog.append` and document puts) resolve once the blocks are stored locally, so a peer whose only bootstrap never answers no longer waits up to the 2-second bootstrap dial budget on every write. `onPut`/`onPutMany` now run afterwards as bounded background work: duplicate CIDs coalesce, at most 4096 CIDs wait (later ones are dropped and counted in `getStoredNotificationTelemetry()`), and `stop()` discards waiting CIDs and aborts running hooks through a `signal` passed as their new optional second argument. Closing a peer or a log therefore no longer waits for an unanswered provider dial. Use `waitForStoredNotifications()` to wait for the announcements of earlier writes, or set `awaitPutHooks: true` to keep the previous behaviour of awaiting the hooks inside each write.
