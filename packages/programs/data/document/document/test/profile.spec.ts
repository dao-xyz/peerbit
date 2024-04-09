import { field, option, variant } from "@dao-xyz/borsh";
import { Documents } from "../src/index.js";
import { Program } from "@peerbit/program";
import { v4 as uuid } from "uuid";
import { randomBytes } from "@peerbit/crypto";
import { waitForResolved } from "@peerbit/time";
import { TestSession } from "@peerbit/test-utils";
import type { RoleOptions } from "@peerbit/shared-log";
import { expect } from 'chai'
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

@variant("test_documents_profiling")
class TestStore extends Program {
	@field({ type: Documents })
	docs: Documents<Document>;

	constructor() {
		super();
		this.docs = new Documents();
	}

	async open(properties?: { role: RoleOptions }): Promise<void> {
		await this.docs.open({ type: Document, role: properties?.role });
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
		for (const [_i, client] of session.peers.entries()) {
			const store: TestStore = await (stores.length === 0
				? client.open(new TestStore(), {
					args: {
						role: {
							type: "replicator",
							factor: 1
						}
					}
				})
				: TestStore.open(stores[0].address, client, {
					args: {
						role: {
							type: "replicator",
							factor: 1
						}
					}
				}));
			stores.push(store);
		}
		await stores[0].waitFor(session.peers[1].peerId);
		await stores[1].waitFor(session.peers[2].peerId);
	});

	afterEach(async () => {
		await session.stop();
	});
	it("puts", async () => {
		let COUNT = 100;
		const writeStore = stores[0];
		let promises: Promise<any>[] = [];
		for (let i = 0; i < COUNT; i++) {
			const doc = new Document({
				id: uuid(),
				name: uuid(),
				bytes: RANDOM_BYTES
			});
			await writeStore.docs.put(doc, { unique: true });
		}

		await Promise.all(promises);
		await waitForResolved(async () =>
			expect(await stores[stores.length - 1].docs.index.getSize()).equal(
				COUNT
			)
		);
	});
});
