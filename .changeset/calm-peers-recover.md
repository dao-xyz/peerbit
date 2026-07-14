---
"peerbit": patch
---

Add opt-in automatic bootstrap recovery with single-flight attempts, bounded
exponential backoff and jitter, browser online wakeups, refreshed default
bootstrap discovery, and lifecycle-safe teardown. Recovery remains disabled by
default so creating a client does not introduce unexpected network activity.
