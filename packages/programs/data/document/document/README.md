# Documents

Distributed document store.

This store is built on top of the base store. This store allows for type-safe document storage and retrieval accross peers. 

As of now, go through the [tests](./src//__tests__/index.integration.test.ts) for documentation on how to use the module.

## Durable remote delivery

Document puts can opt in to waiting for crash-safe persistence on current remote
replica owners:

```typescript
await store.docs.putMany(documents, {
	unique: true,
	delivery: {
		reliability: "persisted",
		minAcks: 2,
		timeout: 120_000,
	},
});
```

`minAcks` counts distinct remote owners; it does not increase the store's
configured replication degree. A successful return proves persistence at the
receipt instant, not permanent custody or Byzantine correctness. The local
commit happens first, so a later delivery failure reports the exact committed
hashes in a `PersistedDeliveryError` with `retrySafe === false`.

Persisted delivery supports `put`, independent distinct-key `putMany`, and a
single `del` operation. For a delete, the receipt covers the exact `CUT`
tombstone entry. It does not wait for remote document-index or change-event
side effects, prove permanent tombstone retention, or compact prior history.

## Required local batching and failure accounting

Callers that cannot accept a sequential `put` fallback can opt in explicitly:

```typescript
import { DocumentBatchCommitError } from "@peerbit/document";

try {
	const result = await store.docs.putMany(documents, {
		batching: "required",
		unique: true,
		target: "none",
	});
	// result.entries has the same order as the captured input documents.
} catch (error) {
	if (!(error instanceof DocumentBatchCommitError)) throw error;
	console.log(error.localCommit, error.committedItems, error.cause);
}
```

Required mode captures the input array, document encodings, keys, and supported
append options at invocation. It rejects unsupported document modes/options and
duplicate keys before append, and never falls back to sequential document puts.
The default behavior is unchanged. This requires the existing independent batch
append path, not an all-or-none storage transaction or a particular number of
fsync calls. Backend durability configuration still applies.

`DocumentBatchCommitError.committedItems` is an immutable array of `{ index, hash }`
pairs: indexes refer to the captured input array, not a later mutated array.
The local outcome is separate from projection and remote delivery:

- `not-started`: no local append was handed off; `retrySafe` is true for replaying
  the local append only, not for application/policy callback side effects.
- `committed`: the complete ordered local append is confirmed. Do not replay it;
  projection, change-event, or remote-receipt work may still have failed. Inspect
  `cause` to determine the failed phase.
- `indeterminate`: native preparation/append may have changed state without a
  complete success acknowledgment, or native durable persistence failed.
  `recoveryRequired` is true and automatic replay is unsafe. An empty evidence
  array does **not** mean nothing committed. Exact indexes are exposed only when
  the complete ordered batch is known.

`recoveryRequired` describes an unresolved local append outcome: false does not
mean a failed document projection needs no repair. Required batching can also be
combined with persisted delivery (omit `target: "none"` in that case). A remote
receipt failure retains local commit evidence and the `PersistedDeliveryError`
as `cause`; only successful persisted delivery proves the requested remote
durability. No transaction atomicity or permanent custody is implied.



Example 
```typescript 
import { field, option, serialize, variant } from "@dao-xyz/borsh";
import { Program } from "@peerbit/program";
import { Peerbit } from "peerbit";
import {
	Documents,
	DocumentIndex,
	SearchRequest,
	StringMatch,
	StringMatchMethod,
	Results,
} from "@peerbit/document";


@variant("document")
class Document {
    @field({ type: "string" })
    id: string;

    @field({ type: option("string") })
    name?: string;

    @field({ type: option("u64") })
    number?: bigint;

    constructor(opts?: Document) {
        if (opts) {
            Object.assign(this, opts);
        }
    }
}

@variant("test_documents")
class TestStore extends Program {
    @field({ type: Documents })
    docs: Documents<Document>;

    constructor(properties?: { docs: Documents<Document> }) {
        super();
        if (properties) {
            this.docs = properties.docs;
        }
    }
    async open(): Promise<void> {
        await this.docs.open({ 
			type: Document, 
			index: {
				fields: (obj) => obj // here you can filter and transform what fields you want to index
			}})
    }
}

// later 

const peer = await Peerbit.create ({libp2p: your_libp2p_instance})
const store = peer.open(new TestStore());
console.log(store.address) /// this address can be opened by another peer 


// insert
let doc = new Document({
    id: "1",
    name: "hello world",
    number: 1n,
});
let doc2 = new Document({
    id: "2",
    name: "hello world",
    number: 2n,
});

let doc3 = new Document({
    id: "3",
    name: "foo",
    number: 3n,
});

await store.docs.put(doc);
await store.docs.put(doc2);
await store.docs.put(doc3);


// search for documents from another peer
const peer2 = await Peerbit.create ({libp2: another_libp2p_instance})
const store2 = peer2.open(store.address);

let responses: Document[] = await store2.docs.index.search(
    new SearchRequest({
        query: [
          new StringMatch({
                key: "name",
                value: "ello",
				method: StringMatchMethod.contains
            }),
        ],
    })
);
expect(responses]).to.have.length(2);
expect(responses.map((x) => x.value.id)).to.deep.equal(["1", "2"]);
```
