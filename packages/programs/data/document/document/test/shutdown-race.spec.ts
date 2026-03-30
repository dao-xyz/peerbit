/**
 * Regression: putWithContext() throws NotStartedError on late replication
 * writes after program shutdown.
 *
 * When the index has been stopped/closed and a late replication write arrives,
 * this.index.put() throws NotStartedError which surfaces as an unhandled
 * rejection. The fix catches NotStartedError in putWithContext() and returns
 * gracefully.
 */

import { field, variant } from "@dao-xyz/borsh";
import { Program } from "@peerbit/program";
import { Documents } from "../src/program.js";
import { TestSession } from "@peerbit/test-utils";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import { v4 as uuid } from "uuid";

@variant(0)
class TestDocument {
	@field({ type: "string" })
	id: string;

	@field({ type: "string" })
	name: string;

	constructor(properties?: { id: string; name: string }) {
		this.id = properties?.id ?? uuid();
		this.name = properties?.name ?? "";
	}
}

@variant("test_shutdown_race")
class TestStore extends Program {
	@field({ type: Documents })
	documents: Documents<TestDocument>;

	constructor() {
		super();
		this.documents = new Documents();
	}

	async open(): Promise<void> {
		await this.documents.open({
			type: TestDocument,
			index: { idProperty: "id" },
		});
	}
}

describe("@peerbit/document — shutdown race", () => {
	let session: TestSession;

	afterEach(async () => {
		await session?.stop();
	});

	it("putWithContext() should not throw NotStartedError when index is stopped during replication", async () => {
		session = await TestSession.connected(2);

		const unhandledErrors: Error[] = [];
		const processHandler = (reason: unknown) => {
			unhandledErrors.push(
				reason instanceof Error ? reason : new Error(String(reason)),
			);
		};
		process.on("unhandledRejection", processHandler);

		try {
			const store0 = await session.peers[0].open(new TestStore());
			const store1: TestStore = await session.peers[1].open(store0.clone());

			await store0.documents.waitFor(
				store1.documents.node.identity.publicKey,
			);
			await store1.documents.waitFor(
				store0.documents.node.identity.publicKey,
			);

			await store0.documents.put(
				new TestDocument({ id: "doc-1", name: "hello" }),
			);

			// Brief delay then close — race the replication write
			await new Promise((r) => setTimeout(r, 100));
			await store1.close();

			// Allow lingering async callbacks to settle
			await new Promise((r) => setTimeout(r, 500));

			const notStartedErrors = unhandledErrors.filter(
				(e) =>
					e?.constructor?.name === "NotStartedError" ||
					e?.message?.includes("Not started"),
			);
			expect(
				notStartedErrors,
				"Expected no unhandled NotStartedError rejections",
			).to.have.lengthOf(0);
		} finally {
			process.removeListener("unhandledRejection", processHandler);
		}
	});

	it("closing a store after replication completes should not throw", async () => {
		session = await TestSession.connected(2);

		const store0 = await session.peers[0].open(new TestStore());
		const store1: TestStore = await session.peers[1].open(store0.clone());

		await store0.documents.waitFor(
			store1.documents.node.identity.publicKey,
		);
		await store1.documents.waitFor(
			store0.documents.node.identity.publicKey,
		);

		await store0.documents.put(
			new TestDocument({ id: "doc-2", name: "world" }),
		);

		await waitForResolved(async () =>
			expect(await store1.documents.index.index.count()).equal(1),
		);

		await store1.close();
		await store0.close();
	});
});
