import { Ed25519Keypair } from "@peerbit/crypto";
import { HashmapIndices } from "@peerbit/indexer-simple";
import { createNativeLogBlockStore } from "@peerbit/log-rust";
import { expect } from "chai";
import type { EntryIndexHashMutationLockOwner } from "../src/entry-index.js";
import { Log } from "../src/log.js";

describe("entry index inventory mutation generation", () => {
	let log: Log<Uint8Array>;
	let store: Awaited<ReturnType<typeof createNativeLogBlockStore>>;

	beforeEach(async () => {
		store = await createNativeLogBlockStore();
		await store.start();
		log = new Log();
		await log.open(store, await Ed25519Keypair.create(), {
			indexer: new HashmapIndices(),
			nativeGraph: true,
		});
	});

	afterEach(async () => {
		await log.close();
		await store.stop();
	});

	it("invalidates synchronously on admission, including a released no-op", async () => {
		const index = log.entryIndex;
		const before = index.captureMutationGeneration()!;
		expect(before).to.be.a("symbol");
		expect(index.isMutationGenerationCurrent(before)).to.equal(true);
		const pending = index.acquireHashMutationLocks(["not-written"]);
		try {
			expect(index.captureMutationGeneration()).to.equal(undefined);
			expect(index.isMutationGenerationCurrent(before)).to.equal(false);
		} finally {
			index.releaseHashMutationLocks(await pending);
		}
		expect(index.captureMutationGeneration())
			.to.be.a("symbol")
			.and.not.equal(before);
		expect(index.isMutationGenerationCurrent(before)).to.equal(false);
	});

	it("cannot capture between a held owner, queued exclusive, and later owner", async () => {
		const index = log.entryIndex;
		const before = index.captureMutationGeneration()!;
		const held = await index.acquireHashMutationLocks(["held"]);
		const exclusivePending = index.acquireExclusiveMutationLock();
		const laterPending = index.acquireHashMutationLocks(["later"]);
		let heldReleased = false;
		let exclusive: EntryIndexHashMutationLockOwner | undefined;
		let later: EntryIndexHashMutationLockOwner | undefined;
		try {
			expect(index.captureMutationGeneration()).to.equal(undefined);
			index.releaseHashMutationLocks(held);
			heldReleased = true;
			exclusive = await exclusivePending;
			expect(index.captureMutationGeneration()).to.equal(undefined);
			index.releaseHashMutationLocks(exclusive);
			exclusive = undefined;
			later = await laterPending;
			expect(index.captureMutationGeneration()).to.equal(undefined);
		} finally {
			if (!heldReleased) index.releaseHashMutationLocks(held);
			if (exclusive) index.releaseHashMutationLocks(exclusive);
			else if (!later) index.releaseHashMutationLocks(await exclusivePending);
			index.releaseHashMutationLocks(later ?? (await laterPending));
		}
		expect(index.captureMutationGeneration()).to.be.a("symbol");
		expect(index.isMutationGenerationCurrent(before)).to.equal(false);
	});

	it("keeps an ownerless native transaction busy through acknowledgement", () => {
		const index = log.entryIndex;
		const before = index.captureMutationGeneration()!;
		const transaction = index.beginNativeCommittedAppendFactsTransaction();
		try {
			expect(index.captureMutationGeneration()).to.equal(undefined);
			expect(index.isMutationGenerationCurrent(before)).to.equal(false);
		} finally {
			index.acknowledgeNativeCommittedAppendFacts(transaction);
		}
		expect(index.captureMutationGeneration()).to.be.a("symbol");
		expect(index.isMutationGenerationCurrent(before)).to.equal(false);
	});

	it("borrowed transaction acknowledgement does not release outer ownership", async () => {
		const index = log.entryIndex;
		const owner = await index.acquireHashMutationLocks(["outer"]);
		try {
			const transaction = index.beginNativeCommittedAppendFactsTransaction(
				[],
				owner,
			);
			index.acknowledgeNativeCommittedAppendFacts(transaction);
			expect(index.captureMutationGeneration()).to.equal(undefined);
		} finally {
			index.releaseHashMutationLocks(owner);
		}
		expect(index.captureMutationGeneration()).to.be.a("symbol");
	});

	it("fails closed on poison and does not resurrect a pre-recovery generation", async () => {
		const index = log.entryIndex;
		const before = index.captureMutationGeneration()!;
		index.poisonNativeDurableTransactionMutations(new Error("retained intent"));
		try {
			expect(() => index.captureMutationGeneration()).to.throw("poisoned");
			expect(() => index.isMutationGenerationCurrent(before)).to.throw(
				"poisoned",
			);
		} finally {
			index.clearNativeDurableTransactionMutationFailure();
		}
		expect(index.isMutationGenerationCurrent(before)).to.equal(false);
		const recovered = index.captureMutationGeneration()!;
		const failure = new Error("failed recovery attempt");
		expect(() =>
			index.withExclusiveMutationRecovery(() => {
				throw failure;
			}),
		).to.throw(failure);
		expect(index.captureMutationGeneration()).to.be.a("symbol");
		expect(index.isMutationGenerationCurrent(recovered)).to.equal(false);
	});
});
