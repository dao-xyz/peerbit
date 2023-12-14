import { field, option, variant } from "@dao-xyz/borsh";
import { Documents } from "../index.js";
import { Program } from "@peerbit/program";
import { v4 as uuid } from "uuid";
import { randomBytes } from "@peerbit/crypto";
import { delay, waitForResolved } from "@peerbit/time";
import { TestSession } from "@peerbit/test-utils";

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

	async open(): Promise<void> {
		await this.docs.open({ type: Document });
	}
}
const RANDOM_BYTES = randomBytes(14 * 1000);

describe("profile", () => {
	let stores: TestStore[];
	let session: TestSession;

	beforeEach(async () => {
		session = await TestSession.disconnected(3);

		await session.peers[0].dial(session.peers[1].getMultiaddrs());
		await session.peers[1].dial(session.peers[2].getMultiaddrs());

		stores = [];

		// Create store
		for (const [i, client] of session.peers.entries()) {
			const store: TestStore = await (stores.length === 0
				? client.open(new TestStore())
				: TestStore.open(stores[0].address, client));
			stores.push(store);
		}
		await stores[0].waitFor(session.peers[1].peerId);
		await stores[0].waitFor(session.peers[2].peerId);
	});

	afterEach(async () => {
		await session.stop();
	});
	it("puts", async () => {
		let COUNT = 10;
		const writeStore = stores[0];
		let promises: Promise<any>[] = [];
		for (let i = 0; i < COUNT; i++) {
			if (i === 0) {
				await delay(5000);
			}
			const doc = new Document({
				id: uuid(),
				name: uuid(),
				bytes: RANDOM_BYTES
			});
			await writeStore.docs.put(doc, { unique: true });
		}
		await Promise.all(promises);
		await waitForResolved(() =>
			expect(stores[stores.length - 1].docs.index.size).toEqual(COUNT)
		);
	});
});
