# Direct block

Block swap/share protocol built on top of [Direct Stream](./../direct-stream/README.md)

Remote responses are treated as opaque bytes. Their CID is verified before a
response can resolve a read, be persisted, or teach the provider cache about
the sender. The transport does not decode DAG-CBOR responses while performing
that integrity check.

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
