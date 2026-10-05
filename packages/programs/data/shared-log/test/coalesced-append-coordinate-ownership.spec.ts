import { create as createRustIndexer } from "@peerbit/indexer-rust";
import { TestSession } from "@peerbit/test-utils";
import { expect } from "chai";
import pDefer from "p-defer";
import sinon from "sinon";
import { EventStore } from "./utils/stores/event-store.js";

describe("coalesced append coordinate ownership", () => {
	let session: TestSession | undefined;

	afterEach(async () => {
		sinon.restore();
		await session?.stop();
		session = undefined;
	});

	const fixture = async () => {
		session = await TestSession.disconnected(1, {
			indexer: (directory) => createRustIndexer(directory),
		});
		const store = await session.peers[0].open(new EventStore<string, any>(), {
			args: {
				replicate: { factor: 1 },
				timeUntilRoleMaturity: 0,
				trim: { type: "length", to: 1 },
			},
		});
		const shared = store.log as any;
		const append = (value: string, trimTo = 1) =>
			shared.appendLocallyPrepared(
				{ op: "ADD", value },
				{
					target: "none",
					replicate: false,
					meta: { next: [] },
					trim: { type: "length", to: trimTo },
				},
			);
		expect(shared._nativeSharedLogState, "required native coordinate planner")
			.to.exist;
		return { store, shared, append, index: store.log.log.entryIndex };
	};

	it("fails closed when the coalesced trim coordinate callback rejects", async () => {
		const { store, append, index } = await fixture();
		const first = await append("first");
		const failure = new Error("injected coalesced coordinate failure");
		const coordinates = store.log.entryCoordinatesIndex as any;
		const write =
			coordinates.putSharedLogCoordinateFieldsAndDeleteHashesNoReturn.bind(
				coordinates,
			);
		let committedHash: string | undefined;
		sinon
			.stub(coordinates, "putSharedLogCoordinateFieldsAndDeleteHashesNoReturn")
			.callsFake(async (fields: any, hashes: any) => {
				committedHash = fields.hash;
				await write(fields, hashes);
				throw failure;
			});
		const error = await append("second").then(
			() => {
				throw new Error("Expected coalesced completion failure");
			},
			(error: unknown) => error,
		);
		expect(error).to.equal(failure);
		expect(committedHash).to.be.a("string");
		expect(await index.getShallow(first.entry.hash)).to.equal(undefined);
		expect((await index.getShallow(committedHash!))?.value.head).to.equal(true);
		await expect(store.add("after-failure")).to.be.rejectedWith(
			/recovery is required/i,
		);
	});

	it("rechecks a queued replacement head before native planning", async () => {
		const { store, shared, append, index } = await fixture();
		const parent = await append("parent", 100);
		const lower = store.log.log as any;
		const child = await lower.createAppendEntry(
			{ op: "ADD", value: "child" },
			{
				__peerbitCanAppendAlreadyValidated: true,
				meta: { data: parent.entry.meta.data },
			},
			[parent.entry],
		);
		const parentFacts = lower.createPreparedAppendFacts([parent.entry])[0];
		const childFacts = lower.createPreparedAppendFacts([child])[0];
		const lifecycle = shared.captureReplicationOwnershipLifecycle();
		const owner = await index.acquireExclusiveMutationLock();
		const attempted = pDefer<void>();
		const acquire = index.acquireHashMutationLocks.bind(index);
		sinon.stub(index, "acquireHashMutationLocks").callsFake((hashes) => {
			const pending = acquire(hashes);
			attempted.resolve();
			return pending;
		});
		const nativePlan = sinon.spy(shared, "planNativeLocalAppendFacts");
		const pending = shared.planAndPersistNativeLocalAppendFacts(
			parentFacts,
			1,
			[],
			lifecycle,
		);
		void pending.catch(() => {});
		try {
			await attempted.promise;
			await index.put(child, {
				unique: true,
				isHead: true,
				toMultiHash: false,
				hashMutationLockOwner: owner,
			});
			await shared.planAndPersistNativeLocalAppendFacts(
				childFacts,
				1,
				[],
				lifecycle,
				{ hashMutationLockOwner: owner },
			);
		} finally {
			index.releaseHashMutationLocks(owner);
			await Promise.allSettled([pending]);
		}
		expect(await pending).to.equal(undefined);
		expect(nativePlan.callCount).to.equal(1);
		expect(nativePlan.firstCall.args[0].hash).to.equal(child.hash);
		expect(
			shared._nativeSharedLogState.getEntryCoordinates(parent.entry.hash),
		).to.equal(undefined);
		expect(shared._nativeSharedLogState.getEntryCoordinates(child.hash)).to
			.exist;
	});

	it("completes each physical trim chunk without an extra standalone delete or final put", async () => {
		const { store, shared, append, index } = await fixture();
		const previous = [];
		for (let i = 0; i < 3; i++) previous.push(await append(`seed-${i}`, 100));
		const oldest = index.getOldestManyMaybe.bind(index);
		sinon
			.stub(index, "getOldestManyMaybe")
			.callsFake((limit, resolve) => oldest(Math.min(limit, 1), resolve));
		const coordinates = store.log.entryCoordinatesIndex as any;
		const put = sinon.spy(
			coordinates,
			"putSharedLogCoordinateFieldsAndDeleteHashesNoReturn",
		);
		const del = sinon.spy(coordinates, "delIds");
		const nativeDelete = sinon.spy(
			shared._nativeSharedLogState,
			"deleteEntryCoordinatesBatch",
		);
		const result = await append("replacement");
		expect(result.removed).to.have.length(3);
		expect(put.callCount).to.equal(3);
		expect(del.callCount).to.equal(0);
		expect(nativeDelete.callCount).to.equal(0);
		expect(put.getCalls().flatMap((call) => call.args[1])).to.have.members(
			previous.map((item) => item.entry.hash),
		);
		expect(store.log.log.length).to.equal(1);
		expect(
			shared._nativeSharedLogState.getEntryCoordinateHashes(),
		).to.deep.equal([result.entry.hash]);
	});
});
