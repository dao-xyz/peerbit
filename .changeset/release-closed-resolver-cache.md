---
"@peerbit/document": patch
---

Release cached ordinary document values when the index closes or drops, including teardown failures after it has become closed. Late cache admissions cannot retain values while the index remains closed. Shared-parent releases that leave the index open retain their cache; reopening creates a fresh one. Program-valued document lifecycle ownership is unchanged.
