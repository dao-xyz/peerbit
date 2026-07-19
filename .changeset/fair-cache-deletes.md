---
"@peerbit/cache": patch
---

Release deleted cache values immediately and keep size accounting exact across
delete/re-add and custom-size replacement cycles. Refresh replacement order so
head-only FIFO/TTL trimming cannot retain older expired entries behind it.
