---
"@peerbit/blocks": patch
"@peerbit/log": patch
"@peerbit/rpc": patch
"@peerbit/shared-log": patch
"@peerbit/document": patch
---

Fix native-backbone handling of entries with next-dependencies and surface RPC codec failures

- blocks: RemoteBlocks exposes `putKnownManyColumns` (delegating to its local store) when the local store supports it, so the log's columnar raw-receive fast path keeps engaging through the wrapper.
- shared-log: always open the log on RemoteBlocks (whose local layer already is the native block store when the backbone is active). A replicate:false observer previously got the raw native store, which drops the remote options joins rely on — syncing a head whose parents were not local reported "sync OK" while persisting nothing, so a subsequent `log.get(head)` missed and document resolution returned null (hit by any `remote: { replicate: true }` document query for a head with `meta.next`, e.g. file-share ready-manifests).
- log: prepared native entries built with `cachePreparedEntries: false` now remember their signer, so `entry.publicKeys` works even while payload/signature bytes live only in the native store. Chained puts (`meta: { next: [...] }`) previously crashed with "Missing data" when the document index commit read the hollow entry's public keys.
- document: the `keep: "self"` predicate now resolves signer keys via `getPublicKeys` (tolerant of prepared native entries) instead of the throwing `signatures` getter, and treats an unknowable signer as not-ours instead of failing the prune evaluation.
- rpc: BorshErrors past the envelope decode are no longer swallowed as "message for a different namespace" — they are logged with their stage (request decode, response encode/decode) and dispatched as a typed `codecError` event, so payloads that fail to (de)serialize no longer disappear into silent request timeouts.
