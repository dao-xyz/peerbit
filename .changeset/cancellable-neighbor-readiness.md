---
"@peerbit/program": patch
---

Forward caller cancellation to the neighbor probes in `Program.waitFor`, preserving the abort reason and skipping subsequent subscriber requests when aborted during those probes. Ordinary neighbor failures remain best-effort; readiness polling and timeout defaults are unchanged.
