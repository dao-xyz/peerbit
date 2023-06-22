import { field, option, variant } from "@dao-xyz/borsh";
import { Documents } from "@peerbit/document";
import { Program } from "@peerbit/program";
import { v4 as uuid } from "uuid";
import { Peerbit } from "../peer.js";
import { createLibp2pExtended } from "../libp2p.js";
import { tcp } from "@libp2p/tcp";
import { randomBytes } from "@peerbit/crypto";
import { waitFor } from "@peerbit/time";

/**
 * A test meant for profiling purposes
 */

@variant("document")
class Document {
	@field({ type: "string" })
	id: string;

	@field({ type: option("string") })
	name?: string;

	@field({ type: Uint8Array })
	bytes: Uint8Array;

	constructor(opts: Document) {
		if (opts) {
			this.id = opts.id;
			this.name = opts.name;
			this.bytes = opts.bytes;
		}
	}
}

@variant("test_documents")
class TestStore extends Program {
	@field({ type: Documents })
	docs: Documents<Document>;

	constructor() {
		super();
		this.docs = new Documents();
	}

	async setup(): Promise<void> {
		await this.docs.setup({ type: Document });
	}
}
const RANDOM_BYTES = randomBytes(14 * 1000);

describe("profile", () => {
	let stores: TestStore[];
	let peers: Peerbit[];

	beforeEach(async () => {
		peers = await Promise.all(
			[
				await createLibp2pExtended({ transports: [tcp()] }),
				await createLibp2pExtended({ transports: [tcp()] }),
				await createLibp2pExtended({ transports: [tcp()] }),
			].map((x) => Peerbit.create({ libp2p: x }))
		);

		await peers[0].dial(peers[1]);
		await peers[1].dial(peers[2]);

		stores = [];

		// Create store
		let address: string | undefined = undefined;

		for (const [i, client] of peers.entries()) {
			const store: TestStore = await client.open(address || new TestStore());

			stores.push(store);
		}
	});

	afterEach(async () => {
		await Promise.all(peers.map((x) => x.stop()));
		await Promise.all(peers.map((n) => n.libp2p.stop()));
	});
	it("puts", async () => {
		let COUNT = 100;
		const writeStore = stores[0];
		let promises: Promise<any>[] = [];
		for (let i = 0; i < COUNT; i++) {
			const doc = new Document({
				id: uuid(),
				name: uuid(),
				bytes: RANDOM_BYTES,
			});
			await writeStore.docs.put(doc, { unique: true });
		}
		await Promise.all(promises);
		await waitFor(() => stores[stores.length - 1].docs.index.size === COUNT);
	});
});
