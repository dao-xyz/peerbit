---
"@peerbit/shared-log": patch
---

Cancel pending sync admissions when their entry is removed. A lookup that started before the removal can no longer re-request the removed entry or clear a newer admission of the same hash; it keeps its lookup quota until it settles, and later advertisements are checked normally. The README now documents the 60-second pending-key lifetime. A request for a CUT entry's parent that was never stored locally is not cancelled and can stay pending until that lifetime ends.
