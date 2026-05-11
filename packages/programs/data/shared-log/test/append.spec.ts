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

	it("reuses native append plan hash number for coordinate persistence", async () => {
		session = await TestSession.disconnected(1);
		const store = await session.peers[0].open(new EventStore<string, any>(), {
			args: {
				replicate: { factor: 1 },
				timeUntilRoleMaturity: 0,
			},
		});
		expect((store.log as any)._nativeSharedLogState).to.exist;
		const hashNumberSpy = sinon.spy(store.log as any, "getEntryHashNumber");
		try {
			await store.log.appendLocallyPrepared(
				{ op: "ADD", value: "a" },
				{
					replicate: false,
					target: "none",
				},
			);

			expect(hashNumberSpy.callCount).equal(1);
		} finally {
			hashNumberSpy.restore();
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
			"putSharedLogCoordinateAndDeleteIds",
		);
		const createSpy = sinon.spy(
			store.log as any,
			"createCoordinatePersistenceEntry",
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
			expect(putDeleteSpy.callCount).equal(1);
			const fields = putDeleteSpy.firstCall.args[1];
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
		} finally {
			createSpy.restore();
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
		const putDeleteSpy = sinon.spy(
			coordinateIndex,
			"putSharedLogCoordinateAndDeleteIds",
		);
		const delIdsSpy = sinon.spy(coordinateIndex, "delIds");
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

			await store.log.appendLocallyPrepared(
				{ op: "ADD", value: "b" },
				{
					replicate: false,
					target: "none",
				},
			);

			expect(delIdsSpy.callCount).equal(0);
			expect(putDeleteSpy.callCount).equal(1);
			expect(putDeleteSpy.firstCall.args[2]).to.deep.equal([first.entry.hash]);
		} finally {
			delIdsSpy.restore();
			putDeleteSpy.restore();
		}
	});
});
