---
"@peerbit/trusted-network": patch
---

Walk trust graphs completely through converging paths (diamonds) and cycles. `getPathGenerator` now expands each key once, by its hashcode, and yields each reachable relation once. Before, it stopped at the first relation whose `id` array it had already seen: walks ended early when the index returned cached relations, and a cycle never ended when it returned freshly decoded ones. `isTrusted` no longer rejects a key that has a path to the root, and `getTrusted` returns each reachable key exactly once.

Behavior change: `hasPathToTarget`, and with it `hasPath`, now tests the key the resolver steps to, `resolver.next(relation)`, instead of always testing `relation.from`. For resolvers whose `next` is `relation.from` (`getFromByTo`, and `getFromByToLocalOnly`, which `isTrusted` uses) this is the same test as before. With a resolver whose `next` is `relation.to` (`getToByFrom`, `getToByFromLocalOnly`), a target now matches when the walk reaches it, including as the last key of a path: for a -> b -> c, `hasPath(a, c, db, getToByFrom)` used to return `false` and now returns `true`.

Relation ownership checks and wire and storage formats are unchanged.
