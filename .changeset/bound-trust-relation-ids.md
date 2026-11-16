---
"@peerbit/trusted-network": patch
---

Refuse a TrustedNetwork relation put whose id is not the id of its own truster and trustee. Before, a trusted member could put its own relation under another relation's id. That replaced the other relation's row (for example, so the root stopped trusting a key it had added) and let the member delete the row as its new owner. IdentityGraph's `canPerform` makes the same check, but the native policy that IdentityGraph opens with by default is unchanged and cannot bind ids.

`add`, `revoke`, `getRelation` and `hasRelation` now ignore a row whose truster and trustee do not match the pair asked for, so the truster can put its relation over a row written before this change, and the truster that row names can still delete it. This repair is partial: a `revoke` after it can leave an earlier entry of the relation on some replicas, and syncing that entry restores the relation.

Entry types and next pointers are still not checked. The log removes the entries a CUT entry points to, so a trusted writer can still remove another writer's relation by pointing an entry of its own relation at that relation's entry. That predates this change and is tracked in https://github.com/dao-xyz/peerbit/issues/1539.
