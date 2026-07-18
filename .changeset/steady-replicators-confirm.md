---
"@peerbit/shared-log": patch
---

Repair silently dropped best-effort replication announcements with an
eight-peer, fairly rotating cohort of acknowledged full-state transport
deliveries. Each mutation generation performs at most three attempts per
target, and newer state preempts stale in-flight sends. Acknowledgement confirms
delivery of the signed envelope rather than receiver-local application.
