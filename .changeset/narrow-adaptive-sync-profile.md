---
"@peerbit/shared-log": patch
---

Add opt-in aggregate sync.profile diagnostics for adaptive rebalance ticks, placement passes, and repair dispatch, plus existing-head hit counts. Reuse existing computations without new metadata scans or default logging, isolate these summaries from throwing sinks, and document elapsed-time and payload-byte boundaries.
