import { toId } from "@peerbit/indexer-interface";
import { create as createSQLiteIndices } from "@peerbit/indexer-sqlite3";
import type { Entry } from "@peerbit/log";
import { TestSession } from "@peerbit/test-utils";
import { expect } from "chai";
import pDefer from "p-defer";
import sinon from "sinon";
import {
	EntryWithRefs,
	ExchangeHeadsMessage,
	RawEntryWithRefs,
	RawExchangeHeadsMessage,
} from "../src/exchange-heads.js";
import { createReplicationDomainHash } from "../src/replication-domain-hash.js";
import { SimpleSyncronizer } from "../src/sync/simple.js";
import { EventStore } from "./utils/stores/event-store.js";

const setup = {
	domain: createReplicationDomainHash("u32"),
	type: "u32" as const,
	syncronizer: SimpleSyncronizer,
	name: "cancelled-receive-coordinates",
};
const openArgs = {
	replicate: false as const,
	keep: () => true,
	timeUntilRoleMaturity: 0,
	nativeGraph: false,
	setup,
};
const exchange = (entries: Entry<any>[]) =>
	new ExchangeHeadsMessage({
		heads: entries.map(
			(entry) => new EntryWithRefs({ entry, gidRefrences: [] }),
		),
	});

describe("receive admission cancelled coordinate completion", () => {
	let session: TestSession | undefined;
	const pending: Promise<unknown>[] = [];
	let releaseCallback = () => {};
	const track = <T>(promise: Promise<T>) => {
		// Own rejections from the moment concurrent work is submitted.
		void promise.catch(() => {});
		pending.push(promise);
		return promise;
	};

	afterEach(async () => {
		releaseCallback();
		await Promise.allSettled(pending.splice(0));
		sinon.restore();
		await session?.stop();
		session = undefined;
	});

	const fixture = async (
		callbackFailure?: Error,
		indexer?: typeof createSQLiteIndices,
		nativeGraph = false,
		nativeBackbone?: false,
	) => {
		session = await TestSession.disconnected(2, { indexer });
		const source = await session.peers[0].open(new EventStore<string, any>(), {
			args: { ...openArgs, nativeGraph, nativeBackbone },
		});
		const first = (
			await source.add("first", { target: "none", meta: { next: [] } })
		).entry;
		const second = (
			await source.add("second", { target: "none", meta: { next: [] } })
		).entry;
		const entered = pDefer<void>();
		const gate = pDefer<void>();
		releaseCallback = gate.resolve;
		const observed: string[] = [];
		let gated = false;
		const target = await session.peers[1].open(source.clone(), {
			args: {
				...openArgs,
				nativeGraph,
				nativeBackbone,
				onChange: async (change) => {
					for (const { entry } of change.added) observed.push(entry.hash);
					if (
						!gated &&
						change.added.some(({ entry }) => entry.hash === first.hash)
					) {
						gated = true;
						entered.resolve();
						await gate.promise;
						if (callbackFailure) throw callbackFailure;
					}
				},
			},
		});
		const shared = target.log as any;
		const joining = sinon.spy(target.log.log, "join");
		const receiving = (entries: Entry<any>[]) =>
			track(
				target.log.onMessage(exchange(entries), {
					from: source.node.identity.publicKey,
				} as any),
			);
		const enter = async () => {
			await entered.promise;
			expect(await target.log.log.has(first.hash)).to.equal(true);
			expect(
				await target.log.entryCoordinatesIndex.get(toId(first.hash)),
			).not.to.equal(undefined);
		};
		return {
			source,
			target,
			first,
			second,
			shared,
			joining,
			receiving,
			enter,
			gate,
			observed,
		};
	};

	it("batches independent admitted heads once at the SQLite coordinate adapter", async () => {
		// Batch admission needs native cut-check planning; coordinate storage stays SQLite.
		const f = await fixture(undefined, createSQLiteIndices, true, false);
		expect(f.target.log.log.entryIndex.properties.nativeGraph).to.exist;
		expect(f.shared._nativeBackbone, "SQLite coordinate authority").to.equal(
			undefined,
		);
		const index = f.target.log.entryCoordinatesIndex;
		const batch = sinon.spy(index, "putBatch");
		const single = sinon.spy(index, "put");
		const lowerCommit = sinon.spy(
			f.target.log.log.entryIndex,
			"putAppendFactsBatch",
		);
		const entries = [f.first, f.second];
		const message = new RawExchangeHeadsMessage({
			heads: await Promise.all(
				entries.map(async (entry) => {
					const bytes = await f.source.log.log.blocks.get(entry.hash);
					expect(bytes).to.be.instanceOf(Uint8Array);
					return new RawEntryWithRefs({
						hash: entry.hash,
						bytes: bytes!,
						gidRefrences: [],
					});
				}),
			),
		});
		f.gate.resolve();
		await f.target.log.onMessage(message, {
			from: f.source.node.identity.publicKey,
		} as any);
		expect(lowerCommit.callCount).to.equal(1);
		expect(lowerCommit.firstCall.args[0]).to.have.length(2);
		expect(batch.callCount).to.equal(1);
		expect(
			batch.firstCall.args[0].map((row: { hash: string }) => row.hash),
		).to.have.members(entries.map((entry) => entry.hash));
		expect(single.callCount).to.equal(0);
		expect(
			(await index.iterate({}).all()).map(({ value }) => value.hash),
		).to.have.members(entries.map((entry) => entry.hash));
	});

	for (const count of [1, 2]) {
		it(`completes coordinates for an admitted prefix of ${count} heads after a live peer drain`, async () => {
			const f = await fixture();
			const receive = f.receiving(
				count === 1 ? [f.first] : [f.first, f.second],
			);
			await f.enter();
			let drained = false;
			const drain = track(
				f.shared
					.drainPeerReceiveHandlers(f.source.node.identity.publicKey.hashcode())
					.then(() => {
						drained = true;
					}),
			);
			expect(f.joining.firstCall.args[1]?.signal?.aborted).to.equal(true);
			// A replacement receive belongs to the new bucket and must survive the
			// old receive's cancellation without being rolled back or drained by it.
			const replacement = (
				await f.source.add("replacement", {
					target: "none",
					meta: { next: [] },
				})
			).entry;
			await f.receiving([replacement]);
			expect(drained).to.equal(false);
			f.gate.resolve();
			await Promise.all([receive, drain]);
			expect(await f.target.log.log.has(f.first.hash)).to.equal(true);
			expect(await f.target.log.log.has(f.second.hash)).to.equal(false);
			expect(await f.target.log.log.has(replacement.hash)).to.equal(true);
			expect(f.observed).to.deep.equal([f.first.hash, replacement.hash]);
			expect(f.shared._activeReceiveHandlersByPeer.size).to.equal(0);
			expect(f.shared._receiveHandlerDrainByPeer.size).to.equal(0);
			const hashes = (
				await f.target.log.entryCoordinatesIndex.iterate({}).all()
			).map(({ value }) => value.hash);
			expect(hashes).to.have.members([f.first.hash, replacement.hash]);
		});
	}

	it("does not resurrect a parent coordinate superseded by a replacement receive", async () => {
		const f = await fixture();
		const receive = f.receiving([f.first]);
		await f.enter();
		const drain = track(
			f.shared.drainPeerReceiveHandlers(
				f.source.node.identity.publicKey.hashcode(),
			),
		);
		const child = (
			await f.source.add("replacement child", {
				target: "none",
				meta: { next: [f.first] },
			})
		).entry;
		await f.receiving([child]);
		f.gate.resolve();
		await Promise.all([receive, drain]);
		expect(await f.target.log.log.has(f.first.hash)).to.equal(true);
		expect(await f.target.log.log.has(child.hash)).to.equal(true);
		const hashes = (
			await f.target.log.entryCoordinatesIndex.iterate({}).all()
		).map(({ value }) => value.hash);
		expect(hashes).to.deep.equal([child.hash]);
	});

	it("does not convert a genuine callback failure racing cancellation into completion", async () => {
		const failure = new Error("receive callback failed after lower commit");
		const f = await fixture(failure);
		const confirmation = sinon.spy(f.shared, "sendRepairConfirmation");
		const receive = f.receiving([f.first]);
		await f.enter();
		const drain = track(
			f.shared.drainPeerReceiveHandlers(
				f.source.node.identity.publicKey.hashcode(),
			),
		);
		f.gate.resolve();
		await Promise.all([receive, drain]);
		const outcome = await f.joining.firstCall.returnValue.then(
			() => undefined,
			(error: unknown) => error,
		);
		expect(outcome).to.equal(failure);
		expect(confirmation.callCount).to.equal(0);
		expect(await f.target.log.log.has(f.first.hash)).to.equal(true);
		expect(
			await f.target.log.entryCoordinatesIndex.get(toId(f.first.hash)),
		).not.to.equal(undefined);
	});

	it("owns the parent head check through its coordinate write before admitting a child", async () => {
		const f = await fixture();
		const atWrite = pDefer<void>();
		const writeGate = pDefer<void>();
		const originalRelease = releaseCallback;
		releaseCallback = () => {
			originalRelease();
			writeGate.resolve();
		};
		const coordinates = f.shared._coordinates;
		const persist =
			coordinates.persistPreparedCoordinatesOwned.bind(coordinates);
		let gated = false;
		sinon
			.stub(coordinates, "persistPreparedCoordinatesOwned")
			.callsFake(async (...args: any[]) => {
				if (
					!gated &&
					args[0].some((write: { hash: string }) => write.hash === f.first.hash)
				) {
					gated = true;
					atWrite.resolve();
					await writeGate.promise;
				}
				return persist(...args);
			});
		const receive = f.receiving([f.first]);
		await atWrite.promise;
		const drain = track(
			f.shared.drainPeerReceiveHandlers(
				f.source.node.identity.publicKey.hashcode(),
			),
		);
		const child = (
			await f.source.add("child during coordinate write", {
				target: "none",
				meta: { next: [f.first] },
			})
		).entry;
		const attemptingChild = pDefer<void>();
		const index = f.target.log.log.entryIndex;
		const acquire = index.acquireHashMutationLocks.bind(index);
		sinon.stub(index, "acquireHashMutationLocks").callsFake((hashes) => {
			const values = [...hashes];
			const result = acquire(values);
			if (values.includes(child.hash)) attemptingChild.resolve();
			return result;
		});
		const replacement = f.receiving([child]);
		await attemptingChild.promise;
		expect(await f.target.log.log.has(child.hash)).to.equal(false);
		f.gate.resolve();
		writeGate.resolve();
		await Promise.all([receive, drain, replacement]);
		const hashes = (
			await f.target.log.entryCoordinatesIndex.iterate({}).all()
		).map(({ value }) => value.hash);
		expect(hashes).to.deep.equal([child.hash]);
		expect(await f.target.log.log.has(f.first.hash)).to.equal(true);
		expect(await f.target.log.log.has(child.hash)).to.equal(true);
	});

	it("keeps only the retained child coordinate when both parent and child arrive", async () => {
		const f = await fixture();
		const child = (
			await f.source.add("child in same receive", {
				target: "none",
				meta: { next: [f.first] },
			})
		).entry;
		f.gate.resolve();
		await f.receiving([child, f.first]);
		const hashes = (
			await f.target.log.entryCoordinatesIndex.iterate({}).all()
		).map(({ value }) => value.hash);
		expect(hashes).to.deep.equal([child.hash]);
		expect(await f.target.log.log.has(f.first.hash)).to.equal(true);
		expect(await f.target.log.log.has(child.hash)).to.equal(true);
	});

	it("removes coordinates with physical deletion and permits a later readmission", async () => {
		const f = await fixture();
		f.gate.resolve();
		await f.receiving([f.first]);
		await f.target.log.log.delete(f.first.hash);
		expect(
			await f.target.log.entryCoordinatesIndex.get(toId(f.first.hash)),
		).to.equal(undefined);
		await f.receiving([f.first]);
		expect(await f.target.log.log.has(f.first.hash)).to.equal(true);
		expect(await f.target.log.log.blocks.has(f.first.hash)).to.equal(true);
		expect(
			await f.target.log.entryCoordinatesIndex.get(toId(f.first.hash)),
		).not.to.equal(undefined);
	});
});
