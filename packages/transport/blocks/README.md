# Direct block

Block swap/share protocol built on top of [Direct Stream](./../direct-stream/README.md)

Remote responses are treated as opaque bytes. Their CID is verified before a
response can resolve a read, be persisted, or teach the provider cache about
the sender. The transport does not decode DAG-CBOR responses while performing
that integrity check.

## Scoped physical reclamation

`scopedBlockReclamation` opts a persistent Node `DirectBlock` into a separate
block-service-owned namespace with durable scope references:

```ts
const blocks = new DirectBlock(components, {
	directory: "./peerbit-blocks",
	scopedBlockReclamation: true,
});
await blocks.start();

const reclamation = blocks.localReclamation;
if (
	blocks.observedLocalStoreSafety?.enforcedReclamation !==
		"scoped-references-v1" ||
	reclamation?.health().status !== "ready"
) {
	throw new Error("scoped reclamation is unavailable");
}

// Persist this random, stable 32-byte local namespace id with private metadata.
const scope = reclamation.openScope(scopeKey);
const cid = await scope.put(bytes);
await scope.release(cid); // deletes only after this managed CID has no scopes
```

`Peerbit.create` exposes the same opt-in as
`storage.scopedBlockReclamation: true`. The default memory and OPFS/browser
paths, and a `DirectBlock` using the Rust stream core, expose no capability and
keep `enforcedReclamation: "none"` in both safety views. The legacy
`localStoreSafety` property always retains its original declaration-only type;
the additive `observedLocalStoreSafety` property reports service-minted
enforcement. `Peerbit.create` rejects combining the opt-in with a custom/Rust
storage factory or an external/custom blocks service, so that configuration is
not silently ignored. Code must check both `localReclamation` and its `health()`
before relying on it. Its operations are local storage operations; they do not
replicate or announce blocks.

Managed storage is intentionally isolated from legacy `put`/`rm`: it never
adopts or deletes raw blocks, even when a CID aliases both namespaces. Each
operation copies and verifies at most one bounded block. Recovery examines
only the requested CID; there is no unbounded startup sweep. Corrupt or
oversized reference state and ambiguous durable mutations fault the capability
without deleting bytes. Reopening resets the runtime fault and lets an
ambiguous mutation be reconciled from its durable before/after state; it does
not repair corrupt metadata, which must be restored or investigated by the
operator.

The managed `limits.maxCidBytes` bound applies only to scoped operations.
Legacy raw block APIs continue accepting valid CID strings above that bound,
up to a private 64 KiB parser-work ceiling. Raw APIs reject the wrapper's
physical sublevel-key prefix before CID parsing, and their iterator never
exposes those internal rows.

Use exactly one owning `DirectBlock` for each physical store or directory.
ClassicLevel locks out a second process, but the reference records are not a
cross-controller compare-and-swap protocol; two wrappers over the same
`AnyStore` in one process are unsupported and could lose reference updates.

Scope keys prevent accidental reference collisions, not hostile code running
inside the same process. Use collision-resistant, privately controlled values;
code that knows another caller's key can open that scope.

Do not also raw-`put` a block (or fetch it with `remote.replicate: true`) if its
last scoped release is expected to remove the only local copy; either action
deliberately creates an independent raw alias.

`"reclaimed"` means the managed key deletion was durably fenced; it does not
promise secure erasure or immediate LSM-file space recovery.

Enabling this is forward-only for the managed entries: older Peerbit versions
do not read the managed sublevel. Keep a backup or release all managed
references before rolling back.

## Eager responses

`eagerBlocks` is an optional receiver-local optimization. It retains a valid
`BlockResponse` that arrived before a matching local read, so the next read can
consume it without another request. It does not change the direct-block wire
protocol, block addressing, or the normal requested-read path.

The option is disabled by default in `DirectBlock` and `SharedLog`. Enable the
compatible bounded behavior explicitly:

```ts
new DirectBlock(components, {
	eagerBlocks: true,
});
```

`true` applies these defaults:

- at most 1,000 validated entries and 32 MiB of retained block bytes;
- at most 10 MiB per unsolicited block and a 10-second TTL;
- two simultaneous integrity checks;
- at most 64 entries and 20 MiB waiting for or undergoing validation.

The defaults can be overridden while retaining the old `cacheSize` option:

```ts
new DirectBlock(components, {
	eagerBlocks: {
		cacheSize: 256,
		maxBytes: 16 * 1024 * 1024,
		maxBlockBytes: 2 * 1024 * 1024,
		ttlMs: 5_000,
		validationConcurrency: 2,
		maxPendingEntries: 32,
		maxPendingBytes: 4 * 1024 * 1024,
	},
});
```

Unsolicited entries accept only raw blocks with SHA-256 CIDs. Peerbit copies
only the exact block range, verifies the CID before cache admission, and learns
the response sender as a provider only after validation. DAG-CBOR is excluded
because logically decoding even hash-valid attacker-controlled object graphs
can expand a small wire payload into unbounded transient heap use. DAG-CBOR,
other codecs, and custom hashers remain available through the unchanged
requested-read path; they simply miss this optimization.

`getEagerBlockCacheTelemetry()` exposes current/peak entry, retained-byte and
pending-validation budgets plus admission/rejection counters. The telemetry is
local diagnostics and is not sent over the network. Byte counters cover block
payload buffers.
