import { Entry } from "@peerbit/log";
import { TestSession } from "@peerbit/test-utils";
import { expect } from "chai";
import sinon from "sinon";
import { v4 as uuid } from "uuid";
import { EntryWithRefs, ExchangeHeadsMessage } from "../src/exchange-heads.js";
import { createReplicationDomainHash } from "../src/replication-domain-hash.js";
import { SimpleSyncronizer } from "../src/sync/simple.js";
import { EventStore } from "./utils/stores/event-store.js";

const setup = {
	domain: createReplicationDomainHash("u32"),
	type: "u32" as const,
	syncronizer: SimpleSyncronizer,
	name: "u32-simple-receive-admission",
};

const exchange = (entry: Entry<any>, gidRefrences: string[] = []) =>
	new ExchangeHeadsMessage({
		heads: [new EntryWithRefs({ entry, gidRefrences })],
	});

const makeParentUnavailable = (parentHash: string) => {
	const originalFromMultihash = Entry.fromMultihash;
	return sinon.stub(Entry, "fromMultihash").callsFake((...args: any[]) => {
		if (args[1] === parentHash) {
			throw Object.assign(new Error("parent intentionally unavailable"), {
				name: "AbortError",
			});
		}
		return (originalFromMultihash as any)(...args);
	});
};

describe("receive admission", () => {
	it("only confirms and coordinates top-level entries admitted by the lower log", async () => {
		const session = await TestSession.disconnected(2);
		try {
			const store = new EventStore<string, any>();
			const source = await session.peers[0].open(store.clone(), {
				args: { replicate: false, setup },
			});
			const target = await session.peers[1].open(store.clone(), {
				args: {
					replicate: false,
					keep: () => true,
					setup,
					timeUntilRoleMaturity: 0,
				},
			});

			const { entry: parent } = await source.add(uuid(), {
				meta: { next: [] },
			});
			const { entry: child } = await source.add(uuid(), {
				meta: { next: [parent] },
			});
			const context = { from: source.node.identity.publicKey } as any;
			const sharedLog = target.log as any;
			const confirmationStub = sinon
				.stub(sharedLog, "sendRepairConfirmation")
				.resolves();
			const pruneSpy = sinon.spy(sharedLog, "pruneJoinedEntriesNoLongerLed");
			let missingParentStub: sinon.SinonStub | undefined =
				makeParentUnavailable(parent.hash);
			try {
				await target.log.onMessage(exchange(child), context);

				expect(await target.log.log.has(child.hash)).to.equal(false);
				expect(await target.log.entryCoordinatesIndex.count()).to.equal(0);
				expect(confirmationStub.callCount).to.equal(0);
				expect(pruneSpy.callCount).to.equal(1);
				expect(pruneSpy.firstCall.args[0]).to.deep.equal([]);

				missingParentStub.restore();
				missingParentStub = undefined;
				await target.log.onMessage(exchange(parent), context);
				await target.log.onMessage(exchange(child), context);

				expect(await target.log.log.has(parent.hash)).to.equal(true);
				expect(await target.log.log.has(child.hash)).to.equal(true);
				const coordinateHashes = (
					await target.log.entryCoordinatesIndex.iterate({}).all()
				).map((result) => result.value.hash);
				expect(coordinateHashes).to.include(child.hash);
				const confirmedHashes = confirmationStub
					.getCalls()
					.flatMap((call) => [...(call.args[1] as Set<string>)]);
				expect(confirmedHashes).to.include(parent.hash);
				expect(confirmedHashes).to.include(child.hash);
				expect(
					pruneSpy.lastCall.args[0].map((entry: any) => entry.hash),
				).to.deep.equal([child.hash]);
			} finally {
				missingParentStub?.restore();
				pruneSpy.restore();
				confirmationStub.restore();
			}
		} finally {
			await session.stop();
		}
	});

	it("does not enqueue a rejected reference-only child through toDelete", async () => {
		const session = await TestSession.disconnected(2);
		try {
			const store = new EventStore<string, any>();
			const source = await session.peers[0].open(store.clone(), {
				args: { replicate: false, setup },
			});
			const target = await session.peers[1].open(store.clone(), {
				args: {
					replicate: false,
					keep: () => false,
					setup,
					timeUntilRoleMaturity: 0,
				},
			});
			const { entry: parent } = await source.add(uuid(), {
				meta: { next: [] },
			});
			const { entry: child } = await source.add(uuid(), {
				meta: { next: [parent] },
			});

			const sharedLog = target.log as any;
			const sourceHash = source.node.identity.publicKey.hashcode();
			const leaderPlanStub = sinon
				.stub(sharedLog, "planEntryLeaderBatch")
				.resolves([
					{
						coordinates: [1, 2],
						leaders: new Map([[sourceHash, { intersecting: true }]]),
						isLeader: false,
					},
				]);
			const referenceHeadStub = sinon
				.stub(sharedLog, "hasAnyHeadForGidSets")
				.resolves([true]);
			const rebalanceStub = sharedLog.rebalanceParticipationDebounced
				? sinon
						.stub(sharedLog.rebalanceParticipationDebounced, "call")
						.returns(undefined)
				: undefined;
			const pruneSpy = sinon.spy(sharedLog, "pruneDebouncedFnAddIfNotKeeping");
			let missingParentStub: sinon.SinonStub | undefined =
				makeParentUnavailable(parent.hash);
			try {
				await target.log.onMessage(exchange(child, ["referenced-gid"]), {
					from: source.node.identity.publicKey,
				} as any);

				expect(referenceHeadStub.callCount).to.equal(1);
				expect(leaderPlanStub.callCount).to.equal(1);
				expect(await target.log.log.has(child.hash)).to.equal(false);
				expect(pruneSpy.callCount).to.equal(0);

				missingParentStub.restore();
				missingParentStub = undefined;
				await target.log.log.join([parent]);
				await sharedLog.pruneDebouncedFn.flush();
				expect(await target.log.log.has(parent.hash)).to.equal(true);
				expect(sharedLog._checkedPrune.hasActiveWork(child.hash)).to.equal(
					false,
				);
			} finally {
				missingParentStub?.restore();
				pruneSpy.restore();
				rebalanceStub?.restore();
				referenceHeadStub.restore();
				leaderPlanStub.restore();
			}
		} finally {
			await session.stop();
		}
	});

	it("does not enqueue a rejected lower-replica child through maybeDelete", async () => {
		const session = await TestSession.disconnected(2);
		try {
			const store = new EventStore<string, any>();
			const source = await session.peers[0].open(store.clone(), {
				args: { replicate: false, setup },
			});
			const target = await session.peers[1].open(store.clone(), {
				args: {
					replicate: false,
					setup,
					timeUntilRoleMaturity: 0,
				},
			});
			const { entry: parent } = await source.add(uuid(), {
				meta: { next: [] },
			});
			const { entry: child } = await source.add(uuid(), {
				meta: { next: [parent] },
			});

			const sharedLog = target.log as any;
			const targetHash = target.node.identity.publicKey.hashcode();
			const maxReplicasBatchStub = sinon
				.stub(sharedLog, "getMaxReplicasFromHeadsBatch")
				.callsFake(async (...args: unknown[]) => {
					const gids = args[0] as Iterable<string>;
					return new Map([...gids].map((gid) => [gid, 3]));
				});
			const leaderPlanStub = sinon
				.stub(sharedLog, "planEntryLeaderBatch")
				.resolves([
					{
						coordinates: [1, 2, 3],
						leaders: new Map([[targetHash, { intersecting: true }]]),
						isLeader: true,
					},
				]);
			const maxReplicasStub = sinon
				.stub(sharedLog, "getMaxReplicasFromHeads")
				.resolves(3);
			const isLeaderStub = sinon.stub(sharedLog, "isLeader").resolves(false);
			const rebalanceStub = sharedLog.rebalanceParticipationDebounced
				? sinon
						.stub(sharedLog.rebalanceParticipationDebounced, "call")
						.returns(undefined)
				: undefined;
			const pruneSpy = sinon.spy(sharedLog, "pruneDebouncedFnAddIfNotKeeping");
			let missingParentStub: sinon.SinonStub | undefined =
				makeParentUnavailable(parent.hash);
			try {
				await target.log.onMessage(exchange(child), {
					from: source.node.identity.publicKey,
				} as any);

				expect(maxReplicasBatchStub.callCount).to.equal(1);
				expect(leaderPlanStub.callCount).to.equal(1);
				expect(leaderPlanStub.firstCall.args[0][0].replicas).to.equal(3);
				expect(await target.log.log.has(child.hash)).to.equal(false);
				expect(maxReplicasStub.callCount).to.equal(0);
				expect(isLeaderStub.callCount).to.equal(0);
				expect(pruneSpy.callCount).to.equal(0);

				missingParentStub.restore();
				missingParentStub = undefined;
				await target.log.log.join([parent]);
				await sharedLog.pruneDebouncedFn.flush();
				expect(await target.log.log.has(parent.hash)).to.equal(true);
				expect(sharedLog._checkedPrune.hasActiveWork(child.hash)).to.equal(
					false,
				);
			} finally {
				missingParentStub?.restore();
				pruneSpy.restore();
				rebalanceStub?.restore();
				isLeaderStub.restore();
				maxReplicasStub.restore();
				leaderPlanStub.restore();
				maxReplicasBatchStub.restore();
			}
		} finally {
			await session.stop();
		}
	});
});
