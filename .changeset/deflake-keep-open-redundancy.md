---
"@peerbit/document": patch
---

Stabilize the "keep-open search recovers existing replicators outside the
initial cover" test. It asserted that a single keep-open search recovers all
documents within a fixed 30s wait, which was flaky on slow/loaded CI runners
where recovery occasionally needed longer (the search resolves with a partial
result when the budget expires). The test now retries the partial-cover
keep-open search with a shorter per-attempt budget until every document is
recovered (bounded at 120s), which is faster on capable hardware and robust
under load while still failing if recovery is genuinely broken. No product
change.
