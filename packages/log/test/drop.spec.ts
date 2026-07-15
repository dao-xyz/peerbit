import { AnyBlockStore } from "@peerbit/blocks";
import { Ed25519Keypair } from "@peerbit/crypto";
import { HashmapIndices } from "@peerbit/indexer-simple";
import { expect } from "chai";
import sinon from "sinon";
import { Log } from "../src/log.js";

describe("drop", () => {
	let log: Log<Uint8Array>;
	let store: AnyBlockStore;
	let uniqueByte = 100;

	const stageNativeFinalizer = async () => {
		const { entry: template } = await log.append(
			new Uint8Array([uniqueByte++]),
		);
		const hash = await store.put(new Uint8Array([uniqueByte++]));
		const shallowEntry = template.toShallow(true);
		shallowEntry.hash = hash;
		shallowEntry.meta.next = [];
		const transaction =
			log.entryIndex.beginNativeCommittedAppendFactsTransaction([hash]);
		await log.entryIndex.putNativeCommittedAppendFacts(
			{
				hash,
				unique: true,
				externalNextHashes: [],
				shallowEntry,
				isHead: true,
			},
			transaction,
		);
		const finalizer = (log as any).createNativeCommittedAppendFinalizer({
			transaction,
			hashes: [hash],
		});
		return { finalizer, hash, transaction };
	};
	beforeEach(async () => {
		log = new Log();
		store = new AnyBlockStore();
		await store.start();
		await log.open(store, await Ed25519Keypair.create());
	});

	afterEach(async () => {
		await log.close();
		await store.stop();
	});
	it("drops entries", async () => {
		const e0 = await log.append(new Uint8Array([1]));
		expect(log.length).equal(1);
		let loadedEntry = await store.get(e0.entry.hash);
		expect(loadedEntry).to.exist;
		await log.drop();
		loadedEntry = await store.get(e0.entry.hash);
		expect(loadedEntry).equal(undefined);
	});

	it("completes a drop queued behind close and erases the retained block", async () => {
		const { entry } = await log.append(new Uint8Array([9]));
		const clear = sinon.spy(log.entryIndex, "clear");
		const retain = sinon.spy(log.entryIndex, "retainBlockHashesForDrop");
		const remove = sinon.spy(store, "rm");
		const closing = log.close();
		const dropping = log.drop();
		await Promise.all([closing, dropping]);
		expect(clear.calledOnce).to.equal(true);
		expect(retain.calledOnce).to.equal(true);
		expect(remove.calledWith(entry.hash)).to.equal(true);
		expect(await store.get(entry.hash)).to.equal(undefined);
		expect(log.closed).to.equal(true);
		await log.drop();
	});

	it("does not scan or retain every hash for an ordinary close", async () => {
		const preservingLog = new Log<Uint8Array>();
		await preservingLog.open(store, await Ed25519Keypair.create(), {
			indexer: new HashmapIndices(),
		});
		try {
			await preservingLog.append(new Uint8Array([12]));
			const retain = sinon.spy(
				preservingLog.entryIndex,
				"retainBlockHashesForDrop",
			);
			await preservingLog.close();
			expect(retain.callCount).to.equal(0);
		} finally {
			await preservingLog.close();
		}
	});

	it("drops indexed blocks after an earlier close", async () => {
		const { entry } = await log.append(new Uint8Array([12]));
		await log.close();
		expect(await (log as any)._indexer.properties.db.status()).to.equal(
			"closed",
		);
		expect(await store.get(entry.hash)).to.not.equal(undefined);
		await log.drop();
		expect(await store.get(entry.hash)).to.equal(undefined);
	});

	it("drops blocks requested while a destructive stop is in flight", async () => {
		const { entry } = await log.append(new Uint8Array([15]));
		const indexer = (log as any)._indexer;
		const originalStop = indexer.stop.bind(indexer);
		let markStopping!: () => void;
		const stopping = new Promise<void>((resolve) => {
			markStopping = resolve;
		});
		let releaseStop!: () => void;
		const stopGate = new Promise<void>((resolve) => {
			releaseStop = resolve;
		});
		const stop = sinon.stub(indexer, "stop").callsFake(async () => {
			markStopping();
			await stopGate;
			await originalStop();
		});
		const retain = sinon.spy(log.entryIndex, "retainBlockHashesForDrop");
		const closing = log.close();
		try {
			await stopping;
			const dropping = log.drop();
			releaseStop();
			await Promise.all([closing, dropping]);
			expect(retain.calledOnce).to.equal(true);
			expect(await store.get(entry.hash)).to.equal(undefined);
			expect(log.closed).to.equal(true);
		} finally {
			releaseStop();
			await closing.catch(() => undefined);
			await log.drop().catch(() => undefined);
			stop.restore();
		}
	});

	it("rejects reopen during close and keeps repeated close on one owner", async () => {
		await log.append(new Uint8Array([10]));
		const internals = log as any;
		const indexer = internals._indexer;
		const originalStop = indexer.stop.bind(indexer);
		let markStopping!: () => void;
		const stopping = new Promise<void>((resolve) => {
			markStopping = resolve;
		});
		let releaseStop!: () => void;
		const stopGate = new Promise<void>((resolve) => {
			releaseStop = resolve;
		});
		const stop = sinon.stub(indexer, "stop").callsFake(async () => {
			markStopping();
			await stopGate;
			await originalStop();
		});

		const firstClose = log.close();
		try {
			await stopping;
			const secondClose = log.close();
			expect(secondClose).to.equal(firstClose);
			await expect(log.open(store, internals._identity)).rejectedWith(
				"Log close must complete before reopening",
			);
			releaseStop();
			await Promise.all([firstClose, secondClose]);
			expect(log.closed).to.equal(true);
			expect(stop.calledOnce).to.equal(true);
		} finally {
			releaseStop();
			await firstClose.catch(() => undefined);
		}
	});

	it("rejects reopen during drop and keeps repeated drop on one owner", async () => {
		const { entry } = await log.append(new Uint8Array([11]));
		const internals = log as any;
		const originalClear = internals._entryIndex.clear.bind(
			internals._entryIndex,
		);
		let markClearing!: () => void;
		const clearing = new Promise<void>((resolve) => {
			markClearing = resolve;
		});
		let releaseClear!: () => void;
		const clearGate = new Promise<void>((resolve) => {
			releaseClear = resolve;
		});
		const clear = sinon
			.stub(internals._entryIndex, "clear")
			.callsFake(async () => {
				markClearing();
				await clearGate;
				await originalClear();
			});

		const firstDrop = log.drop();
		try {
			await clearing;
			const secondDrop = log.drop();
			expect(secondDrop).to.equal(firstDrop);
			await expect(log.open(store, internals._identity)).rejectedWith(
				"Log drop must complete before reopening",
			);
			releaseClear();
			await Promise.all([firstDrop, secondDrop]);
			expect(await store.get(entry.hash)).to.equal(undefined);
			expect(clear.calledOnce).to.equal(true);
		} finally {
			releaseClear();
			await firstDrop.catch(() => undefined);
		}
	});

	it("rolls back an open finalizer before clear without deadlocking", async () => {
		const { hash } = await stageNativeFinalizer();
		await log.drop();
		expect(await store.get(hash)).to.equal(undefined);
	});

	it("waits for an acknowledging finalizer, then erases its publication", async () => {
		const { finalizer, hash } = await stageNativeFinalizer();
		let markAcknowledging!: () => void;
		const acknowledging = new Promise<void>((resolve) => {
			markAcknowledging = resolve;
		});
		let releaseAcknowledgement!: () => void;
		const acknowledgementGate = new Promise<void>((resolve) => {
			releaseAcknowledgement = resolve;
		});
		const acknowledge = finalizer.acknowledge(async () => {
			markAcknowledging();
			await acknowledgementGate;
		});
		await acknowledging;

		let settled = false;
		const dropping = log.drop().finally(() => {
			settled = true;
		});
		await Promise.resolve();
		await Promise.resolve();
		expect(settled).to.equal(false);
		releaseAcknowledgement();
		await Promise.all([acknowledge, dropping]);
		expect(await store.get(hash)).to.equal(undefined);
		await Promise.resolve();
		expect(await store.get(hash)).to.equal(undefined);
	});

	it("rejects finalizer admission after terminal drop", async () => {
		await log.drop();
		expect(() =>
			(log as any).createNativeCommittedAppendFinalizer({
				transaction: {},
				hashes: ["late"],
			}),
		).to.throw("log is closing or dropped");
	});

	it("waits for an in-flight acknowledgement before close settles", async () => {
		const { finalizer, hash } = await stageNativeFinalizer();
		let markAcknowledging!: () => void;
		const acknowledging = new Promise<void>((resolve) => {
			markAcknowledging = resolve;
		});
		let releaseAcknowledgement!: () => void;
		const acknowledgementGate = new Promise<void>((resolve) => {
			releaseAcknowledgement = resolve;
		});
		const acknowledge = finalizer.acknowledge(async () => {
			markAcknowledging();
			await acknowledgementGate;
		});
		await acknowledging;

		let settled = false;
		const closing = log.close().finally(() => {
			settled = true;
		});
		await Promise.resolve();
		await Promise.resolve();
		expect(settled).to.equal(false);
		releaseAcknowledgement();
		await Promise.all([acknowledge, closing]);
		expect(await store.get(hash)).to.not.equal(undefined);
		const lengthAfterClose = log.entryIndex.length;
		await Promise.resolve();
		expect(log.entryIndex.length).to.equal(lengthAfterClose);
	});

	it("keeps a failed terminal rollback rollback-only for close retry", async () => {
		const { finalizer, hash } = await stageNativeFinalizer();
		const entryIndex = log.entryIndex;
		const originalRollback =
			entryIndex.rollbackNativeCommittedAppendFacts.bind(entryIndex);
		const failure = new Error("transient native rollback failure");
		const rollback = sinon.stub(
			entryIndex,
			"rollbackNativeCommittedAppendFacts",
		);
		rollback.onFirstCall().rejects(failure);
		rollback.onSecondCall().callsFake(originalRollback);

		const closeFailure = await log.close().then(
			() => undefined,
			(error: unknown) => error,
		);
		expect(closeFailure).to.be.instanceOf(AggregateError);
		expect(rollback.calledOnce).to.equal(true);
		expect((log as any)._nativeCommittedAppendFinalizers.size).to.equal(1);
		await expect(finalizer.acknowledge()).rejectedWith("already rolled back");
		expect(() => finalizer.retainForRecovery()).to.throw(
			"cannot be retained for recovery",
		);
		expect(await store.get(hash)).to.equal(undefined);

		await log.close();
		expect(rollback.callCount).to.equal(2);
		expect((log as any)._nativeCommittedAppendFinalizers.size).to.equal(0);
		expect(await store.get(hash)).to.equal(undefined);
	});

	it("retains the indexer when close stop fails and retries the stage", async () => {
		await log.append(new Uint8Array([5]));
		const internals = log as any;
		const indexer = internals._indexer;
		const originalStop = indexer.stop.bind(indexer);
		const failure = new Error("transient close stop failure");
		const stop = sinon.stub(indexer, "stop");
		stop.onFirstCall().callsFake(async () => {
			await originalStop();
			throw failure;
		});
		stop.onSecondCall().callsFake(originalStop);

		expect(
			await log.close().then(
				() => undefined,
				(error: unknown) => error,
			),
		).to.equal(failure);
		expect(stop.calledOnce).to.equal(true);
		expect(internals._indexer).to.equal(indexer);
		await expect(log.open(store, internals._identity)).rejectedWith(
			"Failed log close must be retried before reopening",
		);
		expect(stop.calledOnce).to.equal(true);
		expect(internals._indexer).to.equal(indexer);

		await log.close();
		expect(stop.callCount).to.equal(2);
		expect(internals._indexer).to.equal(indexer);
	});

	it("retries a failed clear and erases the actual block before completing", async () => {
		const { entry } = await log.append(new Uint8Array([2]));
		const internals = log as unknown as {
			_closed: boolean;
			_loadedOnce: boolean;
			_entryIndex: { clear: () => Promise<void> };
			_indexer?: { drop: () => Promise<void>; stop: () => Promise<void> };
		};
		const first = new Error("entry clear failed");
		const originalClear = internals._entryIndex.clear.bind(
			internals._entryIndex,
		);
		const clear = sinon.stub(internals._entryIndex, "clear");
		clear.onFirstCall().rejects(first);
		clear.onSecondCall().callsFake(originalClear);
		const drop = sinon.spy(internals._indexer!, "drop");
		const stop = sinon.spy(internals._indexer!, "stop");

		const failure = await log.drop().then(
			() => undefined,
			(error: unknown) => error,
		);
		expect(failure).to.equal(first);
		expect(clear.calledOnce).to.equal(true);
		expect(drop.callCount).to.equal(0);
		expect(stop.callCount).to.equal(0);
		expect(internals._closed).to.equal(true);
		expect(internals._indexer).to.not.equal(undefined);
		expect(await store.get(entry.hash)).to.not.equal(undefined);
		await expect(log.open(store, (log as any)._identity)).rejectedWith(
			"Failed log drop must be retried before reopening",
		);
		expect(clear.calledOnce).to.equal(true);
		expect(await store.get(entry.hash)).to.not.equal(undefined);

		await log.drop();
		expect(clear.callCount).to.equal(2);
		expect(drop.calledOnce).to.equal(true);
		expect(stop.calledOnce).to.equal(true);
		expect(internals._loadedOnce).to.equal(false);
		expect(internals._indexer).to.equal(undefined);
		expect(await store.get(entry.hash)).to.equal(undefined);

		await log.drop();
		expect(clear.callCount).to.equal(2);
		expect(drop.calledOnce).to.equal(true);
		expect(stop.calledOnce).to.equal(true);
	});

	it("keeps failed drop dominant when close is requested next", async () => {
		const { entry } = await log.append(new Uint8Array([13]));
		const originalClear = log.entryIndex.clear.bind(log.entryIndex);
		const failure = new Error("first erase attempt failed");
		const clear = sinon.stub(log.entryIndex, "clear");
		clear.onFirstCall().rejects(failure);
		clear.onSecondCall().callsFake(originalClear);

		expect(
			await log.drop().then(
				() => undefined,
				(error: unknown) => error,
			),
		).to.equal(failure);
		await log.close();
		expect(clear.callCount).to.equal(2);
		expect(await store.get(entry.hash)).to.equal(undefined);
		expect((log as any)._lifecycleState).to.equal("dropped");
	});

	it("retries clear after an inner index drop rejects post-apply", async () => {
		const { entry } = await log.append(new Uint8Array([14]));
		const index = log.entryIndex.properties.index;
		const originalDrop = index.drop.bind(index);
		const failure = new Error("post-apply entry-index drop failure");
		const innerDrop = sinon.stub(index, "drop");
		innerDrop.onFirstCall().callsFake(async () => {
			await originalDrop();
			throw failure;
		});
		innerDrop.onSecondCall().callsFake(originalDrop);

		expect(
			await log.drop().then(
				() => undefined,
				(error: unknown) => error,
			),
		).to.equal(failure);
		expect(await store.get(entry.hash)).to.equal(undefined);
		await log.drop();
		expect(innerDrop.callCount).to.be.greaterThanOrEqual(2);
		expect((log as any)._lifecycleState).to.equal("dropped");
	});

	it("retries index drop without repeating a completed entry clear", async () => {
		const { entry } = await log.append(new Uint8Array([3]));
		const internals = log as unknown as {
			_entryIndex: { clear: () => Promise<void> };
			_indexer?: { drop: () => Promise<void>; stop: () => Promise<void> };
		};
		const clear = sinon.spy(internals._entryIndex, "clear");
		const indexer = internals._indexer!;
		const originalDrop = indexer.drop.bind(indexer);
		const failure = new Error("index drop failed");
		const drop = sinon.stub(indexer, "drop");
		drop.onFirstCall().rejects(failure);
		drop.onSecondCall().callsFake(originalDrop);
		const stop = sinon.spy(indexer, "stop");

		expect(
			await log.drop().then(
				() => undefined,
				(error: unknown) => error,
			),
		).to.equal(failure);
		expect(clear.calledOnce).to.equal(true);
		expect(drop.calledOnce).to.equal(true);
		expect(stop.callCount).to.equal(0);
		expect(await store.get(entry.hash)).to.equal(undefined);
		expect(internals._indexer).to.equal(indexer);

		await log.drop();
		expect(clear.calledOnce).to.equal(true);
		expect(drop.callCount).to.equal(2);
		expect(stop.calledOnce).to.equal(true);
		expect(internals._indexer).to.equal(undefined);
	});

	it("retains the dropped indexer until a transient stop failure is retried", async () => {
		await log.append(new Uint8Array([4]));
		const internals = log as unknown as {
			_entryIndex: { clear: () => Promise<void> };
			_indexer?: { drop: () => Promise<void>; stop: () => Promise<void> };
		};
		const clear = sinon.spy(internals._entryIndex, "clear");
		const indexer = internals._indexer!;
		const drop = sinon.spy(indexer, "drop");
		const originalStop = indexer.stop.bind(indexer);
		const failure = new Error("index stop failed");
		const stop = sinon.stub(indexer, "stop");
		stop.onFirstCall().rejects(failure);
		stop.onSecondCall().callsFake(originalStop);

		expect(
			await log.drop().then(
				() => undefined,
				(error: unknown) => error,
			),
		).to.equal(failure);
		expect(clear.calledOnce).to.equal(true);
		expect(drop.calledOnce).to.equal(true);
		expect(stop.calledOnce).to.equal(true);
		expect(internals._indexer).to.equal(indexer);

		await log.drop();
		expect(clear.calledOnce).to.equal(true);
		expect(drop.calledOnce).to.equal(true);
		expect(stop.callCount).to.equal(2);
		expect(internals._indexer).to.equal(undefined);
	});
});
