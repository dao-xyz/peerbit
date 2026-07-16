---
"@peerbit/program-react": patch
---

Keep `useProgram` opens, listeners, and cleanup owned by their React effect session. Address-bearing object targets remain equivalent by address, while inline targets without an address retain the legacy shared request identity and can opt into intentional replacement with `id`. Stale client or lifecycle-policy requests cannot publish a program, synchronous setup failures no longer strand opened programs, and transient close failures are retried before replacement opens.
