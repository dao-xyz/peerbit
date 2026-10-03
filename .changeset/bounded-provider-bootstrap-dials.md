---
"@peerbit/pubsub": patch
---

Apply the existing per-address dial timeout and lifecycle cancellation to fanout bootstrap and candidate connections even without join diagnostics. Dialing and stream readiness share the same abort budget, so best-effort provider announcements do not wait indefinitely on an abort-aware bootstrap dial. Provider hooks remain awaited, and bootstrap selection and registration policies are unchanged.
