# Rust Storage And Indexer

Peerbit can opt into the experimental Rust-backed key/value store and indexer
without changing the default SQLite/Level-backed client setup.

```ts
import { Peerbit } from "peerbit";
import { createRustPeerbitOptions } from "peerbit/rust";

const peer = await Peerbit.create({
	directory: "./peerbit-data",
	...createRustPeerbitOptions(),
});
```

`createRustPeerbitOptions()` wires the client cache, keychain store, block store,
and root indexer to the Rust-backed implementations:

- `@peerbit/any-store-rust` for Peerbit `AnyStore` instances.
- `@peerbit/indexer-rust` for the client-wide indexer.
- Shared-log programs still use their own log configuration, but the shared-log
  package enables the optional native log graph when `@peerbit/log-rust` is
  available.

The helper is exported from `peerbit/rust` so regular `peerbit` imports do not
load the Rust/WASM packages unless the application opts in.

## Options

```ts
const peer = await Peerbit.create({
	directory: "./peerbit-data",
	...createRustPeerbitOptions({
		storage: {
			default: { durability: "normal" },
			blocks: { durability: "normal" },
			keychain: { durability: "strict" },
		},
		indexer: {
			persistence: { durability: "normal" },
		},
	}),
});
```

Use `strict` durability only where the caller needs each write flushed before
its promise resolves. `normal` durability keeps the WAL append path fast and
compacts snapshots during normal lifecycle operations.
