---
"@peerbit/log": patch
"@peerbit/shared-log": patch
"@peerbit/document": patch
---

Support required independent document batches with JavaScript authorization and
the default replication target in auto mode. Authorize each signed entry before
batch storage, preserve captured bytes and input order, and retain conservative
local commit evidence if later projection or delivery fails. Ordinary writes
keep their existing behavior; batching does not imply an atomic transaction or
a particular fsync count.

Preserve explicit independent heads in native graph batch updates instead of
treating those entries as a chain.
