import { yamux } from "@chainsafe/libp2p-yamux";
import { field, option, variant } from "@dao-xyz/borsh";
import { tcp } from "@libp2p/tcp";
import { SearchRequest } from "@peerbit/document-interface";
import { Sort } from "@peerbit/indexer-interface";
import { Program } from "@peerbit/program";
import { Peerbit, createLibp2pExtended } from "peerbit";
import { v4 as uuid } from "uuid";
import { createDocumentDomainFromProperty } from "../src/domain.js";
import { Documents, type SetupOptions } from "../src/program.js";

// Run with "node --loader ts-node/esm ./benchmark/iterate-replicate-2.ts"

@variant("document")
class Document {
	@field({ type: "string" })
	id: string;

	@field({ type: option("string") })
	name?: string;

	@field({ type: option("u64") })
	number?: bigint;

	constructor(opts: Document) {
		if (opts) {
			this.id = opts.id;
			this.name = opts.name;
			this.number = opts.number;
		}
	}
}

@variant("test_documents")
class TestStore extends Program<Partial<SetupOptions<Document>>> {
	@field({ type: Documents })
	docs: Documents<Document>;

	constructor() {
		super();
		this.docs = new Documents();
	}

	async open(options?: Partial<SetupOptions<Document>>): Promise<void> {
		await this.docs.open({
			...options,
			type: Document,
			domain: createDocumentDomainFromProperty({
				property: "number",
				resolution: "u64",
				mergeSegmentMaxDelta: 1000,
			}),
			replicas: {
				min: 1,
			},
		});
	}
}

const peers = await Promise.all(
	[
		await createLibp2pExtended({
			transports: [tcp()],
			streamMuxers: [yamux()],
		}),
		await createLibp2pExtended({
			connectionManager: {},
			transports: [tcp()],
			streamMuxers: [yamux()],
		}),
	].map((x) => Peerbit.create({ libp2p: x })),
);

await peers[0].dial(peers[1].getMultiaddrs());

const host = await peers[0].open<TestStore>(new TestStore(), {
	args: {
		replicate: {
			factor: 1,
		},
	},
});

const createDoc = (number: bigint) => {
	return new Document({
		id: uuid(),
		name: uuid(),
		number,
	});
};

// warmup
console.log("Inserting");
const insertionCount = 1e4;
for (let i = 0; i < insertionCount; i++) {
	if (i % 1e3 === 0 && i > 0) {
		console.log("... " + i + " ...");
	}
	await host.docs.put(createDoc(BigInt(i)), { unique: true });
}
console.log("Inserted: " + insertionCount);

const client = await peers[1].open<TestStore>(host.clone(), {
	args: {
		replicate: false,
	},
});

await client.docs.log.waitForReplicator(host.node.identity.publicKey);

let iterator = client.docs.index.iterate(
	new SearchRequest({ sort: new Sort({ key: "number" }) }),
	{
		remote: {
			replicate: true,
		},
	},
);

const t0 = +new Date();
let uniqueResults = new Set<string>();
let c = 0;
while (iterator.done() !== true) {
	console.log("i:", c);
	const results = await iterator.next(10);
	c++;
	for (const result of results) {
		uniqueResults.add(result.id);
	}
}

console.log(
	"done, fetched results: ",
	uniqueResults.size +
		". Number of segments: " +
		(await client.docs.log.getMyReplicationSegments()).length +
		". In " +
		Math.round(+new Date() - t0) +
		" ms",
);

await client.close();
await host.close();
await Promise.all(peers.map((x) => x.stop()));
await Promise.all(peers.map((x) => x.libp2p.stop()));
