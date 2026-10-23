---
"@peerbit/shared-log": patch
---

Cancel pending sync admissions when their entry is removed, so a delayed lookup cannot recreate the removed entry's request. Keep lookup quota reserved until the lookup settles and allow subsequent advertisements to be checked normally.
