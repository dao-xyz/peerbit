import { TestSession } from "@peerbit/test-utils";
import { create as createRustIndexer } from "@peerbit/indexer-rust";
import { expect } from "chai";
import sinon from "sinon";
import { EventStore } from "./utils/stores/index.js";

describe("append", () => {
	let session: TestSession;

	before(async () => {});

	afterEach(async () => {
		await session.stop();
	});

	it("canAppend checked once", async () => {
		session = await TestSession.disconnected(1);

		const store = await session.peers[0].open(new EventStore<string, any>());
		const canAppend = sinon.spy(store.log.canAppend);
		store.log.canAppend = canAppend;
		await store.add("a");

		expect(canAppend.callCount).to.be.eq(1);
	});

	it("override option canAppend checked once", async () => {
		session = await TestSession.disconnected(1);

		const store = await session.peers[0].open(new EventStore<string, any>());
		const canAppend = sinon.spy(store.log.canAppend);
		store.log.canAppend = canAppend;

		let canAppendOverride = false;
		await store.add("a", {
			canAppend: () => {
				canAppendOverride = true;
				return true;
			},
		});
		expect(canAppend.callCount).to.be.eq(1);
		expect(canAppendOverride).to.be.true;
	});

	it("appendMany appends a local chain with one shared-log change", async () => {
		session = await TestSession.disconnected(1);
		const changes: any[] = [];
		const store = await session.peers[0].open(new EventStore<string, any>(), {
			args: {
				onChange: (change) => {
					changes.push(change);
				},
				replicate: false,
			},
		});

		const result = await store.addMany(["a", "b", "c"], {
			replicate: false,
			target: "none",
		});

		expect(result.entries).to.have.length(3);
		expect(result.entries[1].meta.next).to.deep.equal([result.entries[0].hash]);
		expect(result.entries[2].meta.next).to.deep.equal([result.entries[1].hash]);
		expect((await store.log.log.getHeads().all()).map((head) => head.hash)).to.deep.equal([
			result.entries[2].hash,
		]);
		expect(changes).to.have.length(1);
		expect(changes[0].added.map((added: any) => added.entry.hash)).to.deep.equal(
			result.entries.map((entry) => entry.hash),
		);
		expect(changes[0].added.map((added: any) => added.head)).to.deep.equal([
			false,
			false,
			true,
		]);
	});

	it("appendMany plans local append assignments in one native batch", async () => {
		session = await TestSession.disconnected(1);
		const store = await session.peers[0].open(new EventStore<string, any>(), {
			args: {
				replicate: false,
				timeUntilRoleMaturity: 0,
			},
		});
		const nativeState = (store.log as any)._nativeSharedLogState;
		expect(nativeState).to.exist;
		const batchSpy = sinon.spy(nativeState, "planAppendForGidsBatch");
		const singleSpy = sinon.spy(nativeState, "planAppendForGid");
		try {
			await store.addMany(["a", "b", "c"], {
				delivery: true,
				replicate: false,
			});

			expect(batchSpy.callCount).equal(1);
			expect(singleSpy.callCount).equal(0);
		} finally {
			batchSpy.restore();
			singleSpy.restore();
		}
	});

	it("appendMany plans target-none local assignments in one native batch", async () => {
		session = await TestSession.disconnected(1);
		const store = await session.peers[0].open(new EventStore<string, any>(), {
			args: {
				replicate: false,
				timeUntilRoleMaturity: 0,
			},
		});
		const nativeState = (store.log as any)._nativeSharedLogState;
		expect(nativeState).to.exist;
		const batchSpy = sinon.spy(nativeState, "planAppendForGidsBatch");
		const localSingleSpy = sinon.spy(nativeState, "planLocalAppendForGid");
		const deliverySingleSpy = sinon.spy(nativeState, "planAppendForGid");
		try {
			await store.addMany(["a", "b", "c"], {
				target: "none",
			});

			expect(batchSpy.callCount).equal(1);
			expect(localSingleSpy.callCount).equal(0);
			expect(deliverySingleSpy.callCount).equal(0);
		} finally {
			deliverySingleSpy.restore();
			localSingleSpy.restore();
			batchSpy.restore();
		}
	});

	it("appendMany coalesces a local chain to the final shared-log head", async () => {
		session = await TestSession.disconnected(1);
		const store = await session.peers[0].open(new EventStore<string, any>(), {
			args: {
				replicate: false,
				timeUntilRoleMaturity: 0,
			},
		});
		const nativeState = (store.log as any)._nativeSharedLogState;
		expect(nativeState).to.exist;
		const batchSpy = sinon.spy(nativeState, "planAppendForGidsBatch");
		const singleSpy = sinon.spy(nativeState, "planAppendForGid");
		try {
			const result = await store.addMany(["a", "b", "c"], {
				replicate: false,
			});

			expect(batchSpy.callCount).equal(0);
			expect(singleSpy.callCount).equal(1);
			const coordinateRows = await store.log.entryCoordinatesIndex
				.iterate({}, { shape: { hash: true } })
				.all();
			expect(coordinateRows).to.have.length(1);
			expect(coordinateRows[0]!.value.hash).equal(result.entries[2]!.hash);
		} finally {
			batchSpy.restore();
			singleSpy.restore();
		}
	});

	it("uses append facts hash number for coordinate persistence", async () => {
		session = await TestSession.disconnected(1);
		const store = await session.peers[0].open(new EventStore<string, any>(), {
			args: {
				replicate: { factor: 1 },
				timeUntilRoleMaturity: 0,
			},
		});
		expect((store.log as any)._nativeSharedLogState).to.exist;
		const entryHashNumberSpy = sinon.spy(store.log as any, "getEntryHashNumber");
		const factsHashNumberSpy = sinon.spy(
			store.log as any,
			"getAppendFactsHashNumber",
		);
		try {
			await store.log.appendLocallyPrepared(
				{ op: "ADD", value: "a" },
				{
					replicate: false,
					target: "none",
				},
			);

			expect(entryHashNumberSpy.callCount).equal(0);
			expect(factsHashNumberSpy.callCount).equal(1);
		} finally {
			factsHashNumberSpy.restore();
			entryHashNumberSpy.restore();
		}
	});

	it("persists native append coordinate fields from the prepared plan", async () => {
		session = await TestSession.disconnected(1, {
			indexer: (directory) => createRustIndexer(directory),
		});
		const store = await session.peers[0].open(new EventStore<string, any>(), {
			args: {
				replicate: { factor: 1 },
				timeUntilRoleMaturity: 0,
			},
		});
		const coordinateIndex = store.log.entryCoordinatesIndex as any;
		const putDeleteSpy = sinon.spy(
			coordinateIndex,
			"putSharedLogCoordinateFieldsAndDeleteHashesNoReturn",
		);
		const returningPutDeleteSpy = sinon.spy(
			coordinateIndex,
			"putSharedLogCoordinateFieldsAndDeleteHashes",
		);
		const genericPutDeleteSpy = sinon.spy(
			coordinateIndex,
			"putSharedLogCoordinateFieldsAndDeleteIds",
		);
		const legacyPutDeleteSpy = sinon.spy(
			coordinateIndex,
			"putSharedLogCoordinateAndDeleteIds",
		);
		const createSpy = sinon.spy(
			store.log as any,
			"createCoordinatePersistenceEntry",
		);
		const createFactsSpy = sinon.spy(
			store.log as any,
			"createCoordinatePersistenceEntryFromNativePlanFacts",
		);
		const materializeCoordinateSpy = sinon.spy(
			store.log as any,
			"createCoordinateEntryFromNativeFields",
		);
		const planEntrySpy = sinon.spy(
			store.log as any,
			"planNativeLocalAppendEntry",
		);
		const planFactsSpy = sinon.spy(
			store.log as any,
			"planNativeLocalAppendFacts",
		);
		const nativeKernelSpy = sinon.spy(
			store.log as any,
			"processNativePreparedTargetNoneAppend",
		);
		const genericProcessSpy = sinon.spy(store.log as any, "processLocalAppend");
		const persistCoordinateSpy = sinon.spy(
			store.log as any,
			"persistCoordinate",
		);
		const persistPreparedSpy = sinon.spy(
			store.log as any,
			"persistPreparedCoordinate",
		);
		try {
			const result = await store.log.appendLocallyPrepared(
				{ op: "ADD", value: "a" },
				{
					replicate: false,
					target: "none",
				},
			);

			expect(createSpy.callCount).equal(0);
			expect(createFactsSpy.callCount).equal(1);
			expect(materializeCoordinateSpy.callCount).equal(0);
			expect(planEntrySpy.callCount).equal(0);
			expect(planFactsSpy.callCount).equal(1);
			expect(nativeKernelSpy.callCount).equal(1);
			expect(genericProcessSpy.callCount).equal(0);
			expect(persistCoordinateSpy.callCount).equal(0);
			expect(persistPreparedSpy.callCount).equal(1);
			expect(putDeleteSpy.callCount).equal(1);
			expect(returningPutDeleteSpy.callCount).equal(0);
			expect(genericPutDeleteSpy.callCount).equal(0);
			expect(legacyPutDeleteSpy.callCount).equal(0);
			const fields = putDeleteSpy.firstCall.args[0];
			expect(fields.hash).equal(result.entry.hash);
			expect(fields.gid).equal(result.entry.meta.gid);
			expect(fields.hashNumber).equal(
				(store.log as any).getEntryHashNumber(result.entry),
			);
			expect(fields.coordinates).to.deep.equal(
				(store.log as any)._nativeSharedLogState.getEntryCoordinates(
					result.entry.hash,
				),
			);
			expect(fields.metaBytes).to.deep.equal(
				(result.entry as any).getMetaBytes(),
			);
			expect(result.appendCommit.hash).equal(result.entry.hash);
			expect(result.appendCommit.gid).equal(result.entry.meta.gid);
			expect(result.appendCommit.wallTime).equal(
				result.entry.meta.clock.timestamp.wallTime,
			);
			expect(result.appendCommit.payloadSize).equal(
				result.entry.payload.byteLength,
			);
			expect(result.appendCommit.coordinateFields).to.deep.equal(fields);
		} finally {
			persistPreparedSpy.restore();
			persistCoordinateSpy.restore();
			genericProcessSpy.restore();
			nativeKernelSpy.restore();
			planFactsSpy.restore();
			planEntrySpy.restore();
			materializeCoordinateSpy.restore();
			createFactsSpy.restore();
			createSpy.restore();
			legacyPutDeleteSpy.restore();
			genericPutDeleteSpy.restore();
			returningPutDeleteSpy.restore();
			putDeleteSpy.restore();
		}
	});

	it("coalesces prepared append trim coordinate deletes into the coordinate put", async () => {
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
		const coordinateIndex = store.log.entryCoordinatesIndex as any;
		const nativeState = (store.log as any)._nativeSharedLogState;
		const putDeleteSpy = sinon.spy(
			coordinateIndex,
			"putSharedLogCoordinateFieldsAndDeleteHashesNoReturn",
		);
		const returningPutDeleteSpy = sinon.spy(
			coordinateIndex,
			"putSharedLogCoordinateFieldsAndDeleteHashes",
		);
		const genericPutDeleteSpy = sinon.spy(
			coordinateIndex,
			"putSharedLogCoordinateFieldsAndDeleteIds",
		);
		const delIdsSpy = sinon.spy(coordinateIndex, "delIds");
		const nativeDeleteSpy = sinon.spy(
			nativeState,
			"deleteEntryCoordinatesBatch",
		);
		try {
			const first = await store.log.appendLocallyPrepared(
				{ op: "ADD", value: "a" },
				{
					replicate: false,
					target: "none",
				},
			);
			putDeleteSpy.resetHistory();
			delIdsSpy.resetHistory();
			nativeDeleteSpy.resetHistory();

			await store.log.appendLocallyPrepared(
				{ op: "ADD", value: "b" },
				{
					replicate: false,
					target: "none",
				},
			);

			expect(delIdsSpy.callCount).equal(0);
			expect(putDeleteSpy.callCount).equal(1);
			expect(putDeleteSpy.firstCall.args[1]).to.deep.equal([first.entry.hash]);
			expect(returningPutDeleteSpy.callCount).equal(0);
			expect(genericPutDeleteSpy.callCount).equal(0);
			expect(nativeDeleteSpy.callCount).equal(0);
			expect(nativeState.getEntryCoordinates(first.entry.hash)).equal(undefined);
		} finally {
			nativeDeleteSpy.restore();
			delIdsSpy.restore();
			genericPutDeleteSpy.restore();
			returningPutDeleteSpy.restore();
			putDeleteSpy.restore();
		}
	});

	it("uses the prepared payload native transaction without eager entry materialization", async () => {
		session = await TestSession.disconnected(1, {
			indexer: (directory) => createRustIndexer(directory),
		});
		const store = await session.peers[0].open(new EventStore<string, any>(), {
			args: {
				replicate: { factor: 1 },
				timeUntilRoleMaturity: 0,
			},
		});
		const transactionSpy = sinon.spy(
			store.log as any,
			"finishPreparedPayloadNativeAppendTransaction",
		);
		const processTransactionSpy = sinon.spy(
			store.log as any,
			"processNativePreparedTargetNoneAppendTransaction",
		);
		const genericProcessSpy = sinon.spy(
			store.log as any,
			"processNativePreparedTargetNoneAppend",
		);
			const nativePersistSpy = sinon.spy(
				store.log as any,
				"persistPreparedCoordinateNativeTransaction",
			);
			const coordinateIndex = store.log.entryCoordinatesIndex as any;
			const encodedCoordinatePersistSpy = sinon.spy(
				coordinateIndex,
				"putSharedLogCoordinateFieldsEncodedAndDeleteHashesNoReturn",
			);
			const genericPersistSpy = sinon.spy(
				store.log as any,
				"persistPreparedCoordinate",
			);
		const materializeSpy = sinon.spy(
			store.log as any,
			"materializePreparedAppendResultEntry",
		);
		try {
			const result = await store.log.appendLocallyPreparedPayloadCommitOnly(
				new Uint8Array([1, 2, 3]),
				{
					replicate: false,
					target: "none",
				},
			);

			if (!result) {
				throw new Error("Expected native transaction result");
			}
			expect(transactionSpy.callCount).equal(1);
			expect(processTransactionSpy.callCount).equal(1);
			expect(genericProcessSpy.callCount).equal(0);
			expect(nativePersistSpy.callCount).equal(1);
			expect(encodedCoordinatePersistSpy.callCount).equal(1);
			expect(genericPersistSpy.callCount).equal(0);
			expect(materializeSpy.callCount).equal(0);
			expect(result.appendCommit.hash).to.be.a("string");
			expect(materializeSpy.callCount).equal(0);
			expect(result.entry.hash).equal(result.appendCommit.hash);
			expect(materializeSpy.callCount).equal(1);
		} finally {
			materializeSpy.restore();
			genericPersistSpy.restore();
			encodedCoordinatePersistSpy.restore();
			nativePersistSpy.restore();
			genericProcessSpy.restore();
			processTransactionSpy.restore();
			transactionSpy.restore();
		}
	});

	it("dispatches resident native repair coordinates without eager materialization", async () => {
		session = await TestSession.disconnected(1, {
			indexer: (directory) => createRustIndexer(directory),
		});
		const store = await session.peers[0].open(new EventStore<string, any>(), {
			args: {
				replicate: { factor: 1 },
				timeUntilRoleMaturity: 0,
			},
		});
		const result = await store.log.appendLocallyPrepared(
			{ op: "ADD", value: "a" },
			{
				replicate: false,
				target: "none",
			},
		);
		const residentEntries = (store.log as any)
			._residentEntryCoordinatesByHash as Map<string, any>;
		const residentEntry = residentEntries.get(result.entry.hash);
		expect(residentEntry).to.exist;
		expect(residentEntry.getMetaBytes).equal(undefined);

		const materializeSpy = sinon.spy(
			store.log as any,
			"materializeResidentCoordinateEntry",
		);
		const syncStub = sinon
			.stub(store.log.syncronizer, "onMaybeMissingEntries")
			.resolves();
		try {
			await (store.log as any).sendRepairEntriesWithTransport(
				"target-peer",
				new Map([[result.entry.hash, residentEntry]]),
				"rateless",
				{ bypassKnownPeers: true },
			);

			expect(materializeSpy.callCount).equal(0);
			expect(syncStub.callCount).equal(1);
			const entries = syncStub.firstCall.args[0].entries as Map<string, any>;
			expect(entries.get(result.entry.hash)).equal(residentEntry);
		} finally {
			syncStub.restore();
			materializeSpy.restore();
		}
	});

	it("uses cached self replicator state for native prepared append planning", async () => {
		session = await TestSession.disconnected(1, {
			indexer: (directory) => createRustIndexer(directory),
		});
		const store = await session.peers[0].open(new EventStore<string, any>(), {
			args: {
				replicate: { factor: 1 },
				timeUntilRoleMaturity: 0,
			},
		});
		const countReplicationSegmentsSpy = sinon.spy(
			store.log as any,
			"countReplicationSegments",
		);
		try {
			await store.log.appendLocallyPrepared(
				{ op: "ADD", value: "a" },
				{
					replicate: false,
					target: "none",
				},
			);

			expect(countReplicationSegmentsSpy.callCount).equal(0);
		} finally {
			countReplicationSegmentsSpy.restore();
		}
	});

	it("skips append delivery materialization when native delivery has no remote recipients", async () => {
		session = await TestSession.disconnected(1, {
			indexer: (directory) => createRustIndexer(directory),
		});
		const store = await session.peers[0].open(new EventStore<string, any>(), {
			args: {
				replicate: { factor: 1 },
				timeUntilRoleMaturity: 0,
			},
		});
		const nativeState = (store.log as any)._nativeSharedLogState;
		expect(nativeState).to.exist;
		const nativePlanSpy = sinon.spy(nativeState, "planAppendForGid");
		const createRepairEntrySpy = sinon.spy(
			store.log as any,
			"createEntryReplicatedForRepair",
		);
		const subscribersStub = sinon
			.stub(store.log as any, "_getTopicSubscribers")
			.resolves([]);
		try {
			await store.log.appendLocallyPrepared(
				{ op: "ADD", value: "a" },
				{ replicate: false },
			);

			expect(nativePlanSpy.callCount).equal(1);
			expect(subscribersStub.callCount).equal(1);
			expect(createRepairEntrySpy.callCount).equal(0);
		} finally {
			subscribersStub.restore();
			createRepairEntrySpy.restore();
			nativePlanSpy.restore();
		}
	});
});
