import { Ed25519Keypair, type PublicSignKey } from "@peerbit/crypto";
import { UnsubcriptionEvent } from "@peerbit/pubsub-interface";
import { delay, waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import pDefer from "p-defer";
import { Peerbit } from "peerbit";
import sinon from "sinon";
import { SharedLog } from "../src/index.js";
import {
	ReplicationIntent,
	ReplicationRangeIndexableU64,
} from "../src/ranges.js";
import { EventStore } from "./utils/stores/index.js";

// Assertion messages start with "passes:" for placement passes, delivery and
// membership events, and with "hold:" for the bookkeeping of held removals.

type Change = {
	type: string;
	range: { hash: string; idString: string; rangeHash: string };
};

type Replica = {
	index: number;
	peer: Peerbit;
	store: EventStore<string, any>;
	log: any;
	hash: string;
	/** Held-removal profile events, as outcome or outcome:reason, by owner. */
	holds: { peer?: string; outcome: string }[];
	leaves: string[];
	joins: string[];
	/** When pubsub reported each remote peer unreachable on the log topic. */
	unreachableAt: Map<string, number>;
	passes: sinon.SinonSpy;
	joinRepair: sinon.SinonSpy;
};

const pairKey = (left: number, right: number) =>
	left < right ? `${left}:${right}` : `${right}:${left}`;

const flattenChanges = (argument: any): Change[] =>
	Array.isArray(argument?.[0]) ? argument.flat() : (argument ?? []);

/** Placement passes whose changes include a range owned by `hash`. */
const passesReferencing = (replica: Replica, hash: string): Change[][] =>
	replica.passes
		.getCalls()
		.map((call) => flattenChanges(call.args[0]))
		.filter((changes) => changes.some((change) => change.range.hash === hash));

const changeTypes = (changes: Change[], hash: string) => [
	...new Set(
		changes
			.filter((change) => change.range.hash === hash)
			.map((change) => change.type),
	),
];

const holdsFor = (replica: Replica, hash: string) =>
	replica.holds
		.filter((event) => event.peer === hash)
		.map((event) => event.outcome);

const joinRepairCalledFor = (
	replica: Pick<Replica, "joinRepair">,
	hash: string,
) =>
	replica.joinRepair
		.getCalls()
		.some((call) => call.args[0]?.has?.(hash) === true);

describe("receive admission transport-loss reconnect", function () {
	this.timeout(60_000);

	let peers: Peerbit[] = [];
	let sandbox: sinon.SinonSandbox;
	const cut = new Set<string>();

	beforeEach(() => {
		peers = [];
		cut.clear();
		sandbox = sinon.createSandbox();
	});

	afterEach(async () => {
		cut.clear();
		sandbox.restore();
		await Promise.all(peers.splice(0).map((peer) => peer.stop()));
	});

	/**
	 * Real peers, all full replicas, connected pairwise. The first peer appends
	 * 20 entries; the spies are installed once every peer holds them and no
	 * placement pass is pending or running.
	 */
	const openReplicas = async (properties: {
		count: number;
		min: number;
		args?: Record<string, any>;
	}): Promise<Replica[]> => {
		for (let index = 0; index < properties.count; index++) {
			const isCut = (peerId: { equals(other: unknown): boolean }) => {
				const other = peers.findIndex((peer) =>
					peerId.equals(peer.libp2p.peerId),
				);
				return other >= 0 && cut.has(pairKey(index, other));
			};
			peers.push(
				await Peerbit.create({
					libp2p: {
						connectionGater: {
							denyDialPeer: async (peerId) => isCut(peerId),
							denyInboundEncryptedConnection: async (peerId) => isCut(peerId),
						},
					},
				}),
			);
		}
		const template = new EventStore<string, any>();
		const replicas: Replica[] = [];
		for (const [index, peer] of peers.entries()) {
			const holds: Replica["holds"] = [];
			const store = await peer.open(template.clone(), {
				args: {
					replicas: { min: properties.min },
					replicate: 1,
					timeUntilRoleMaturity: 0,
					sync: {
						profile: (event) => {
							if (event.name === "sharedLog.transportLoss.park") {
								const outcome = String(event.details?.outcome);
								const reason = event.details?.reason;
								holds.push({
									peer: event.peer,
									outcome: reason ? `${outcome}:${reason}` : outcome,
								});
							}
						},
					},
					...properties.args,
				},
			});
			replicas.push({
				index,
				peer,
				store,
				log: store.log as any,
				hash: peer.identity.publicKey.hashcode(),
				holds,
				leaves: [],
				joins: [],
				unreachableAt: new Map(),
				passes: undefined as any,
				joinRepair: undefined as any,
			});
		}
		for (let left = 0; left < peers.length; left++) {
			for (let right = left + 1; right < peers.length; right++) {
				await peers[left].dial(peers[right].getMultiaddrs());
			}
		}
		for (let i = 0; i < 20; i++) {
			await replicas[0].store.add(`entry ${i}`);
		}
		const hashes = replicas.map((replica) => replica.hash);
		await waitForResolved(
			async () => {
				for (const replica of replicas) {
					expect(replica.store.log.log.length).to.equal(20);
					expect([...(await replica.log.getReplicators())]).to.have.members(
						hashes,
					);
				}
			},
			{ timeout: 20_000 },
		);
		await waitForResolved(
			() => {
				for (const replica of replicas) {
					for (const other of replicas) {
						if (other === replica) continue;
						expect(
							replica.log._v2Receive._receiveStates.get(other.hash)?.phase,
						).to.equal("active");
						expect(
							replica.log._v2Send._sendStates.get(other.hash)?.established,
						).to.equal(true);
					}
					expect(replica.log.replicationChangeDebounceFn.size()).to.equal(0);
					expect(replica.log._placementPassesInFlight?.size ?? 0).to.equal(0);
				}
			},
			{ timeout: 20_000 },
		);
		for (const replica of replicas) {
			replica.passes = sandbox.spy(replica.log, "onReplicationChange");
			replica.joinRepair = sandbox.spy(
				replica.log,
				"scheduleJoinAuthoritativeRepair",
			);
			replica.log.events.addEventListener("replicator:leave", (event: any) =>
				replica.leaves.push(event.detail.publicKey.hashcode()),
			);
			replica.log.events.addEventListener("replicator:join", (event: any) =>
				replica.joins.push(event.detail.publicKey.hashcode()),
			);
			replica.peer.services.pubsub.addEventListener(
				"unsubscribe",
				({ detail }) => {
					if (
						detail.reason === "peer-unreachable" &&
						detail.topics.includes(replica.log.topic)
					) {
						replica.unreachableAt.set(
							detail.from.hashcode(),
							performance.now(),
						);
					}
				},
			);
		}
		return replicas;
	};

	/** Cut each pair and wait until both sides report the other unreachable. */
	const partition = async (pairs: [Replica, Replica][]) => {
		for (const [left, right] of pairs) {
			cut.add(pairKey(left.index, right.index));
		}
		await Promise.all(
			pairs.flatMap(([left, right]) => [
				left.peer.hangUp(right.peer.identity.publicKey),
				right.peer.hangUp(left.peer.identity.publicKey),
			]),
		);
		await waitForResolved(
			() => {
				for (const [left, right] of pairs) {
					for (const [from, to] of [
						[left, right],
						[right, left],
					]) {
						expect(
							from.peer.libp2p.getConnections(to.peer.libp2p.peerId),
						).to.have.length(0);
						expect(
							from.unreachableAt.has(to.hash),
							`peer ${from.index} reports peer ${to.index} unreachable`,
						).to.equal(true);
					}
				}
			},
			{ timeout: 5_000 },
		);
	};

	/**
	 * Let the dials denied during the partition retire, lift the cut in the same
	 * turn, dial once and wait for the renewed handshake. Returns the dial time.
	 */
	const heal = async (left: Replica, right: Replica) => {
		await waitForResolved(
			() => {
				for (const [from, to] of [
					[left, right],
					[right, left],
				]) {
					expect(
						from.peer.libp2p
							.getDialQueue()
							.filter((dial) => dial.peerId?.equals(to.peer.libp2p.peerId)),
					).to.have.length(0);
				}
				cut.delete(pairKey(left.index, right.index));
			},
			{ timeout: 5_000 },
		);
		const dialedAt = performance.now();
		await left.peer.dial(right.peer.getMultiaddrs());
		await waitForResolved(
			async () => {
				for (const [from, to] of [
					[left, right],
					[right, left],
				]) {
					expect(
						from.log._v2Receive._receiveStates.get(to.hash)?.phase,
					).to.equal("active");
					expect(
						from.log._v2Send._sendStates.get(to.hash)?.established,
					).to.equal(true);
					expect([...(await from.log.getReplicators())]).to.include(to.hash);
				}
			},
			{ timeout: 10_000 },
		);
		return dialedAt;
	};

	const expectDeliveredBy = async (
		replica: Replica,
		hash: string,
		deadline: number,
		message: string,
	) => {
		await waitForResolved(
			async () => {
				expect(await replica.store.log.log.has(hash), message).to.equal(true);
			},
			{ timeout: Math.max(1, deadline - performance.now()) },
		);
	};

	const waitUntil = (time: number) =>
		delay(Math.max(0, time - performance.now()));

	for (const min of [2, 3]) {
		it(`returns unchanged after a real partition without rebalance passes (replicas ${min})`, async () => {
			const graceMs = 3_000;
			const [a, b] = await openReplicas({ count: 2, min });
			for (const replica of [a, b]) {
				replica.log.transportLossRemovalGraceMs = graceMs;
			}
			const pairs: [Replica, Replica][] = [
				[a, b],
				[b, a],
			];

			const partitionedAt = performance.now();
			await partition([[a, b]]);
			const { entry: duringPartition } = await a.store.add(
				"written during the partition",
			);
			const dialedAt = await heal(a, b);

			for (const [replica, other] of pairs) {
				expect(
					passesReferencing(replica, other.hash),
					`passes: peer ${replica.index} ran placement passes for peer ${other.index} between the partition and the heal`,
				).to.have.length(0);
			}
			await waitUntil(partitionedAt + graceMs + 500);
			for (const [replica, other] of pairs) {
				expect(
					passesReferencing(replica, other.hash),
					`passes: peer ${replica.index} ran placement passes for peer ${other.index} once the hold would have ended`,
				).to.have.length(0);
			}
			await expectDeliveredBy(
				b,
				duringPartition.hash,
				dialedAt + 5_000,
				"passes: the entry written during the partition reaches the returned peer within 5 s of the heal",
			);
			const healedAt = performance.now();
			const { entry: afterHeal } = await a.store.add("written after the heal");
			await expectDeliveredBy(
				b,
				afterHeal.hash,
				healedAt + 5_000,
				"passes: an entry written after the heal reaches the returned peer within 5 s",
			);

			for (const [replica, other] of pairs) {
				expect(
					holdsFor(replica, other.hash),
					`hold: peer ${replica.index} held and restored peer ${other.index}`,
				).to.deep.equal(["parked", "restored"]);
				expect(
					joinRepairCalledFor(replica, other.hash),
					`hold: peer ${replica.index} scheduled the authoritative repair for peer ${other.index}`,
				).to.equal(true);
				expect(
					replica.log._transportLossParks?.size,
					`hold: peer ${replica.index} holds no removal at the end`,
				).to.equal(0);
				expect(
					replica.leaves.filter((hash) => hash === other.hash),
					`passes: peer ${replica.index} saw peer ${other.index} leave once`,
				).to.have.length(1);
				expect(
					replica.joins.filter((hash) => hash === other.hash),
					`passes: peer ${replica.index} saw peer ${other.index} join once`,
				).to.have.length(1);
			}
		});
	}

	it("runs the usual passes when the partition outlasts the hold", async () => {
		const [a, b] = await openReplicas({ count: 2, min: 2 });
		for (const replica of [a, b]) {
			replica.log.transportLossRemovalGraceMs = 300;
		}
		const pairs: [Replica, Replica][] = [
			[a, b],
			[b, a],
		];
		await partition([[a, b]]);
		const { entry: duringPartition } = await a.store.add(
			"written during the partition",
		);
		// The partition has outlasted the hold once each side ran its removal pass.
		await waitForResolved(
			() => {
				for (const [replica, other] of pairs) {
					expect(
						passesReferencing(replica, other.hash).some(
							(changes) =>
								changeTypes(changes, other.hash).join() === "removed",
						),
						`passes: peer ${replica.index} ran a removal pass for peer ${other.index} before the heal`,
					).to.equal(true);
				}
			},
			{ timeout: 5_000 },
		);
		const dialedAt = await heal(a, b);

		for (const [replica, other] of pairs) {
			await waitForResolved(
				() =>
					expect(
						passesReferencing(replica, other.hash),
						`passes: peer ${replica.index} ran a removal and an addition pass for peer ${other.index}`,
					).to.have.length.of.at.least(2),
				{ timeout: 10_000 },
			);
		}
		await delay(500);
		for (const [replica, other] of pairs) {
			const passes = passesReferencing(replica, other.hash);
			expect(
				passes,
				`passes: peer ${replica.index} ran exactly two passes for peer ${other.index}`,
			).to.have.length(2);
			expect(
				changeTypes(passes[0], other.hash),
				`passes: peer ${replica.index}'s first pass removes peer ${other.index}`,
			).to.deep.equal(["removed"]);
			expect(
				changeTypes(passes[1], other.hash),
				`passes: peer ${replica.index}'s second pass adds peer ${other.index}`,
			).to.deep.equal(["added"]);
		}
		await expectDeliveredBy(
			b,
			duringPartition.hash,
			dialedAt + 5_000,
			"passes: the entry written during the partition reaches the returned peer within 5 s of the heal",
		);
		for (const [replica, other] of pairs) {
			expect(
				holdsFor(replica, other.hash),
				`hold: peer ${replica.index} held peer ${other.index} until the hold ended`,
			).to.deep.equal(["parked", "released:grace"]);
		}
	});

	it("releases the held removal before a changed return", async () => {
		const [a, b] = await openReplicas({ count: 2, min: 2 });
		for (const replica of [a, b]) {
			replica.log.transportLossRemovalGraceMs = 3_000;
		}
		const oldRows: Change["range"][] = (
			await a.log.replicationIndex.iterate({ query: { hash: b.hash } }).all()
		).map((result: any) => result.value);
		expect(oldRows).to.have.length.of.at.least(1);
		const queued = sandbox.spy(a.log.replicationChangeDebounceFn, "add");

		await partition([[a, b]]);
		await waitForResolved(() => expect(a.leaves).to.include(b.hash));
		await b.store.log.replicate({ factor: 0.5, offset: 0 }, { reset: true });
		const newRows: Change["range"][] =
			await b.store.log.getMyReplicationSegments();
		expect(newRows).to.have.length.of.at.least(1);

		// The departure has been handled (the leave event follows the removal),
		// so a removal it queued would already be in the debouncer or its pass.
		expect(
			queued
				.getCalls()
				.filter(
					(call) =>
						call.args[0]?.type === "removed" &&
						call.args[0].range.hash === b.hash,
				),
			"passes: the departure queued no removal before the return",
		).to.have.length(0);
		expect(
			passesReferencing(a, b.hash),
			"passes: the departure ran no placement pass before the return",
		).to.have.length(0);
		const joinRepairCallsBeforeReturn = a.joinRepair.callCount;
		await heal(a, b);

		const occurrences = (type: string, row: Change["range"]) =>
			a.passes
				.getCalls()
				.flatMap((call) => flattenChanges(call.args[0]))
				.filter(
					(change) =>
						change.type === type &&
						change.range.idString === row.idString &&
						change.range.rangeHash === row.rangeHash,
				).length;
		await waitForResolved(
			() => {
				for (const row of oldRows) {
					expect(
						occurrences("removed", row),
						"passes: the old range is removed",
					).to.be.at.least(1);
				}
				for (const row of newRows) {
					expect(
						occurrences("added", row),
						"passes: the new range is added",
					).to.be.at.least(1);
				}
			},
			{ timeout: 10_000 },
		);
		await delay(500);
		for (const row of oldRows) {
			expect(
				occurrences("removed", row),
				"passes: the old range is removed exactly once",
			).to.equal(1);
		}
		for (const row of newRows) {
			expect(
				occurrences("added", row),
				"passes: the new range is added exactly once",
			).to.equal(1);
		}
		const firstPassWith = (type: string, rows: Change["range"][]) =>
			a.passes
				.getCalls()
				.findIndex((call) =>
					flattenChanges(call.args[0]).some(
						(change) =>
							change.type === type &&
							rows.some(
								(row) =>
									row.idString === change.range.idString &&
									row.rangeHash === change.range.rangeHash,
							),
					),
				);
		expect(
			firstPassWith("removed", oldRows) <= firstPassWith("added", newRows),
			"passes: the old range is removed no later than the new range is added",
		).to.equal(true);
		// A pass that adds the returned peer schedules this repair itself, so
		// only a call made before that pass shows the return scheduled it.
		const repairCall = a.joinRepair
			.getCalls()
			.slice(joinRepairCallsBeforeReturn)
			.find((call) => call.args[0]?.has?.(b.hash) === true);
		const firstAdditionPass = a.passes
			.getCalls()
			.find((call) =>
				flattenChanges(call.args[0]).some(
					(change) => change.type === "added" && change.range.hash === b.hash,
				),
			);
		expect(
			repairCall !== undefined &&
				firstAdditionPass !== undefined &&
				repairCall.calledBefore(firstAdditionPass),
			"hold: the changed return scheduled the authoritative repair before the addition pass",
		).to.equal(true);
		expect(
			holdsFor(a, b.hash),
			"hold: the changed return ended the hold",
		).to.deep.equal(["parked", "released:changed"]);

		const writtenAt = performance.now();
		const { entry } = await a.store.add("written after the changed return");
		await expectDeliveredBy(
			b,
			entry.hash,
			writtenAt + 5_000,
			"passes: an entry written after the heal reaches the returned peer within 5 s",
		);
	});

	const causes: {
		name: string;
		remove: (a: Replica, b: Replica) => Promise<unknown>;
	}[] = [
		{
			name: "a remote close",
			remove: async (a, b) => {
				await b.store.close();
				await waitForResolved(() => expect(a.leaves).to.include(b.hash));
			},
		},
		{
			// A close publishes an empty replication reset before it unsubscribes,
			// so the unsubscribe itself finds no rows. Deliver it directly.
			name: "a remote unsubscribe",
			remove: (a, b) =>
				a.log._onUnsubscription(
					new CustomEvent("unsubscribe", {
						detail: new UnsubcriptionEvent(
							b.peer.identity.publicKey,
							[a.log.topic],
							"remote-unsubscribe",
						),
					}),
				),
		},
		{
			name: "an unsubscribe without a reason",
			remove: (a, b) =>
				a.log._onUnsubscription(
					new CustomEvent("unsubscribe", {
						detail: new UnsubcriptionEvent(b.peer.identity.publicKey, [
							a.log.topic,
						]),
					}),
				),
		},
		{
			name: "a peer session reset",
			remove: (a, b) =>
				a.log._onUnsubscription(
					new CustomEvent("unsubscribe", {
						detail: new UnsubcriptionEvent(
							b.peer.identity.publicKey,
							[a.log.topic],
							"peer-session-reset",
						),
					}),
				),
		},
		{
			name: "a liveness eviction",
			remove: (a, b) =>
				a.log.removeReplicator(b.peer.identity.publicKey, {
					shouldRemove: () => true,
				}),
		},
		{
			name: "an unreplicate",
			remove: async (a, b) => {
				await b.store.log.unreplicate();
				await waitForResolved(() => expect(a.leaves).to.include(b.hash));
			},
		},
	];
	for (const cause of causes) {
		it(`queues the removal at once after ${cause.name}`, async () => {
			const [a, b] = await openReplicas({ count: 2, min: 2 });
			const rows: Change["range"][] = (
				await a.log.replicationIndex.iterate({ query: { hash: b.hash } }).all()
			).map((result: any) => result.value);
			expect(rows).to.have.length.of.at.least(1);
			const queued = sandbox.spy(a.log.replicationChangeDebounceFn, "add");

			await cause.remove(a, b);

			for (const row of rows) {
				expect(
					queued
						.getCalls()
						.some(
							(call) =>
								call.args[0]?.type === "removed" &&
								call.args[0].range.idString === row.idString,
						),
					"passes: the removal is queued for a placement pass at once",
				).to.equal(true);
			}
			expect(a.holds, "hold: nothing is held").to.deep.equal([]);
			expect(
				a.log._transportLossParks?.size ?? 0,
				"hold: nothing is held",
			).to.equal(0);
		});
	}

	it("releases the held removal when another pass starts", async () => {
		const [a, b] = await openReplicas({ count: 2, min: 2 });
		for (const replica of [a, b]) {
			replica.log.transportLossRemovalGraceMs = 3_000;
		}
		await partition([[a, b]]);
		await waitForResolved(() => expect(a.leaves).to.include(b.hash));

		const rebalancedAt = performance.now();
		await a.log.rebalanceAll();
		await waitForResolved(
			() =>
				expect(
					passesReferencing(a, b.hash).some(
						(changes) => changeTypes(changes, b.hash).join() === "removed",
					),
					"passes: a removal pass for the departed peer ran",
				).to.equal(true),
			{ timeout: Math.max(1, rebalancedAt + 1_000 - performance.now()) },
		);

		await heal(a, b);
		await waitForResolved(
			() =>
				expect(
					passesReferencing(a, b.hash).some((changes) =>
						changeTypes(changes, b.hash).includes("added"),
					),
					"passes: an addition pass for the returned peer ran",
				).to.equal(true),
			{ timeout: 10_000 },
		);
		expect(holdsFor(a, b.hash), "hold: the pass ended the hold").to.deep.equal([
			"parked",
			"released:pass",
		]);
	});

	it("does not hold a removal while more owners than the replica floor remain", async () => {
		const [a, b, c] = await openReplicas({ count: 3, min: 2 });
		await partition([
			[a, c],
			[b, c],
		]);
		const departedAt = a.unreachableAt.get(c.hash)!;
		await waitForResolved(
			() =>
				expect(
					passesReferencing(a, c.hash).some(
						(changes) => changeTypes(changes, c.hash).join() === "removed",
					),
					"passes: a removal pass for the departed peer ran",
				).to.equal(true),
			{ timeout: Math.max(1, departedAt + 2_000 - performance.now()) },
		);
		expect(a.holds, "hold: nothing is held").to.deep.equal([]);
	});

	const variants: {
		name: string;
		args?: Record<string, any>;
		prepare?: (replica: Replica) => void;
	}[] = [
		{
			name: "the strict full-replica fallback turned off",
			args: { strictFullReplicaFallback: false },
		},
		{
			name: "a custom findLeaders",
			prepare: (replica) => {
				replica.log.findLeaders = (...args: unknown[]) =>
					(SharedLog.prototype as any).findLeaders.apply(replica.log, args);
			},
		},
	];
	for (const variant of variants) {
		it(`does not hold a removal with ${variant.name}`, async () => {
			const [a, b] = await openReplicas({
				count: 2,
				min: 2,
				args: variant.args,
			});
			variant.prepare?.(a);
			await partition([[a, b]]);
			const departedAt = a.unreachableAt.get(b.hash)!;
			await waitForResolved(
				() =>
					expect(
						passesReferencing(a, b.hash).some(
							(changes) => changeTypes(changes, b.hash).join() === "removed",
						),
						"passes: a removal pass for the departed peer ran",
					).to.equal(true),
				{ timeout: Math.max(1, departedAt + 2_000 - performance.now()) },
			);
			expect(a.holds, "hold: nothing is held").to.deep.equal([]);
		});
	}

	it("drops a held removal on close", async () => {
		const [a, b] = await openReplicas({ count: 2, min: 2 });
		expect(
			a.log._transportLossParks,
			"hold: the log keeps its held removals in a map",
		).to.be.instanceOf(Map);
		const queued = sandbox.spy(a.log.replicationChangeDebounceFn, "add");
		const removalsQueued = () =>
			queued
				.getCalls()
				.filter(
					(call) =>
						call.args[0]?.type === "removed" &&
						call.args[0].range.hash === b.hash,
				).length;

		await partition([[a, b]]);
		await waitForResolved(() =>
			expect(
				holdsFor(a, b.hash),
				"hold: peer 0 holds the removal of peer 1",
			).to.deep.equal(["parked"]),
		);
		const timer = a.log._transportLossParks.get(b.hash)?.timer;
		expect(timer, "hold: the held removal has a timer").to.exist;
		expect(
			a.log._repairRetryTimers.has(timer),
			"hold: the held removal's timer is one of the repair timers",
		).to.equal(true);
		const queuedBeforeClose = removalsQueued();

		await a.store.close();
		expect(
			a.log._transportLossParks?.size ?? 0,
			"hold: closing drops the held removal",
		).to.equal(0);
		expect(
			a.log._repairRetryTimers?.has(timer) ?? false,
			"hold: the held removal's timer leaves the repair timers",
		).to.equal(false);
		expect(removalsQueued(), "hold: no removal is queued after close").to.equal(
			queuedBeforeClose,
		);

		await Promise.all(peers.splice(0).map((peer) => peer.stop()));
		expect(
			removalsQueued(),
			"hold: no removal is queued after the peers stop",
		).to.equal(queuedBeforeClose);
	});

	it("restores one owner while another stays held", async () => {
		const graceMs = 2_000;
		const [a, b, c] = await openReplicas({ count: 3, min: 3 });
		for (const replica of [a, b, c]) {
			replica.log.transportLossRemovalGraceMs = graceMs;
		}
		const partitionedAt = performance.now();
		// C is cut from B too: once A and B reconnect, a route to C through B
		// would let C come back unchanged and be restored like B.
		await partition([
			[a, b],
			[a, c],
			[b, c],
		]);
		await waitForResolved(() =>
			expect(a.leaves).to.include.members([b.hash, c.hash]),
		);
		await heal(a, b);

		expect(
			passesReferencing(a, b.hash),
			"passes: the returned peer needed no placement pass",
		).to.have.length(0);
		await waitUntil(partitionedAt + graceMs);
		await waitForResolved(
			() =>
				expect(
					passesReferencing(a, c.hash),
					"passes: a removal pass for the peer that stayed away ran",
				).to.have.length.of.at.least(1),
			{ timeout: 10_000 },
		);
		await delay(500);
		expect(
			passesReferencing(a, b.hash),
			"passes: the returned peer needed no placement pass",
		).to.have.length(0);
		const passesForC = passesReferencing(a, c.hash);
		expect(
			passesForC,
			"passes: one pass for the peer that stayed away",
		).to.have.length(1);
		expect(
			changeTypes(passesForC[0], c.hash),
			"passes: that pass only removes the peer that stayed away",
		).to.deep.equal(["removed"]);

		expect(
			holdsFor(a, b.hash),
			"hold: the returned peer was held and restored",
		).to.deep.equal(["parked", "restored"]);
		expect(
			holdsFor(a, c.hash),
			"hold: the peer that stayed away was held until the hold ended",
		).to.deep.equal(["parked", "released:grace"]);
	});

	describe("with one peer and a remote owner", () => {
		// One replica and a remote owner that exists only as a replication range,
		// so each rule that decides a hold runs without network timing. Batches
		// are far apart: a queued change stays queued, and no placement pass
		// starts unless a test flushes the change debouncer. Liveness probes are
		// stopped after open: the remote owner has no key that pubsub can
		// resolve, so the first probe would remove it before a test does.
		const batchIntervalMs = 600_000;

		type Owner = {
			log: any;
			/** Held-removal profile events, as outcome or outcome:reason. */
			holds: string[];
			remote: PublicSignKey;
			remoteHash: string;
			/** The remote owner's only range. */
			row: unknown;
			/** This replica's own range. */
			ownRange: unknown;
			queued: sinon.SinonSpy;
			joinRepair: sinon.SinonSpy;
		};

		const openOwner = async (): Promise<Owner> => {
			const peer = await Peerbit.create();
			peers.push(peer);
			const holds: string[] = [];
			const log = new SharedLog<any, any>() as any;
			await peer.open(log, {
				args: {
					replicas: { min: 2 },
					replicate: 1,
					timeUntilRoleMaturity: 0,
					distributionDebounceTime: batchIntervalMs,
					sync: {
						profile: (event: any) => {
							if (event.name === "sharedLog.transportLoss.park") {
								const outcome = String(event.details?.outcome);
								const reason = event.details?.reason;
								holds.push(reason ? `${outcome}:${reason}` : outcome);
							}
						},
					},
				},
			});
			log._liveness.stopReplicatorLivenessSweep();
			const remote = (await Ed25519Keypair.create()).publicKey;
			const remoteHash = remote.hashcode();
			const row = new log.indexableDomain.constructorRange({
				offset: log.indexableDomain.numbers.zero,
				width: log.indexableDomain.numbers.maxValue,
				publicKeyHash: remoteHash,
				mode: ReplicationIntent.NonStrict,
				timestamp: BigInt(Date.now() - 60_000),
			});
			await log.addReplicationRange([row], remote, { reset: true });
			await log.replicationChangeDebounceFn.flush();
			await waitForResolved(
				() => {
					expect(log.replicationChangeDebounceFn.size()).to.equal(0);
					expect(log._placementPassesInFlight?.size ?? 0).to.equal(0);
				},
				{ timeout: 10_000 },
			);
			const [ownRange] = await log.getMyReplicationSegments();
			return {
				log,
				holds,
				remote,
				remoteHash,
				row,
				ownRange,
				queued: sandbox.spy(log.replicationChangeDebounceFn, "add"),
				joinRepair: sandbox.spy(log, "scheduleJoinAuthoritativeRepair"),
			};
		};

		/** Changes queued for the remote owner's range, of one type. */
		const changesQueued = (owner: Owner, type: string) =>
			owner.queued
				.getCalls()
				.filter(
					(call) =>
						call.args[0]?.type === type &&
						call.args[0].range.hash === owner.remoteHash,
				).length;

		/** Pubsub reports the remote owner unreachable. */
		const depart = (owner: Owner, options: Record<string, unknown> = {}) =>
			owner.log.removeReplicator(owner.remote, {
				transportLoss: true,
				...options,
			});

		/** The remote owner's next Full brings back its range unchanged. */
		const returnUnchanged = (owner: Owner) =>
			owner.log.addReplicationRange([owner.row], owner.remote, {
				reset: true,
			});

		/** Leave a change for this replica's own range waiting for a batch. */
		const queueOwnChange = (owner: Owner) =>
			owner.log.replicationChangeDebounceFn.add({
				range: owner.ownRange,
				type: "added",
				timestamp: BigInt(Date.now()),
			});

		/** Register checked-prune work. Returns a function that drops it. */
		const trackPruneCandidate = (owner: Owner) => {
			const hash = "transport-loss-prune-candidate";
			owner.log._checkedPrune.trackCandidate(
				hash,
				{ hash },
				new Set([owner.log.node.identity.publicKey.hashcode()]),
			);
			return () => owner.log._checkedPrune.invalidateCandidateToken(hash);
		};

		/**
		 * Fail the full-replica leader read of the replication index once, the
		 * first time `armed` holds when such a read starts.
		 */
		const failLeaderRead = (owner: Owner, armed: () => boolean) => {
			const failed = { reads: 0 };
			const index = owner.log.replicationIndex;
			const iterate = index.iterate.bind(index);
			sandbox.stub(index, "iterate").callsFake((request: any, options: any) => {
				const iterator = iterate(request, options);
				if (
					failed.reads === 0 &&
					request?.query === undefined &&
					options?.shape?.mode === true &&
					armed()
				) {
					failed.reads++;
					iterator.next = async () => {
						throw new Error("replication index read failed");
					};
				}
				return iterator;
			});
			return failed;
		};

		/** A return that is not restored ends the hold and queues both passes. */
		const expectUsualReturn = (owner: Owner) => {
			expect(
				changesQueued(owner, "removed"),
				"passes: the held removal is queued",
			).to.equal(1);
			expect(
				changesQueued(owner, "added"),
				"passes: the returned range is queued",
			).to.equal(1);
			expect(owner.holds, "hold: the return ended the hold").to.deep.equal([
				"parked",
				"released:changed",
			]);
			expect(
				joinRepairCalledFor(owner, owner.remoteHash),
				"hold: the return scheduled the authoritative repair",
			).to.equal(true);
			expect(
				owner.log._transportLossParks?.size ?? 0,
				"hold: nothing is held afterwards",
			).to.equal(0);
		};

		it("restores an unchanged return without queueing either pass", async () => {
			const owner = await openOwner();
			await depart(owner);
			const sweeps = sandbox.spy(owner.log, "scheduleRepairSweep");
			await returnUnchanged(owner);
			expect(
				changesQueued(owner, "removed"),
				"passes: no removal is queued",
			).to.equal(0);
			expect(
				changesQueued(owner, "added"),
				"passes: no addition is queued",
			).to.equal(0);
			expect(
				owner.holds,
				"hold: the removal was held and restored",
			).to.deep.equal(["parked", "restored"]);
			expect(
				joinRepairCalledFor(owner, owner.remoteHash),
				"hold: the restore scheduled the authoritative repair",
			).to.equal(true);
			// The departure cleared the owner's known-peer hints, so the ones it
			// has now came from its current connection, and the restore's sweep
			// honours them.
			const ownerSweepBypasses = sweeps
				.getCalls()
				.map((call) => call.args[0])
				.filter(
					(options) =>
						options?.mode === "join-authoritative" &&
						[...(options.peers ?? [])].includes(owner.remoteHash),
				)
				.map((options) => options.bypassKnownPeerHints);
			expect(
				ownerSweepBypasses,
				"hold: the restore's sweep honours the owner's known-peer hints",
			).to.include(false);
			expect(
				ownerSweepBypasses,
				"hold: no sweep for the owner bypasses its known-peer hints",
			).not.to.include(true);
			expect(
				owner.log._transportLossParks?.size ?? 0,
				"hold: nothing is held afterwards",
			).to.equal(0);
		});

		it("does not hold a removal while checked-prune work exists", async () => {
			const owner = await openOwner();
			const dropPruneWork = trackPruneCandidate(owner);
			try {
				await depart(owner);
			} finally {
				dropPruneWork();
			}
			expect(
				changesQueued(owner, "removed"),
				"passes: the removal is queued at once",
			).to.equal(1);
			// Not even a declined hold: the removal never tries to hold.
			expect(owner.holds, "hold: nothing is held").to.deep.equal([]);
		});

		it("declines to hold a removal when checked-prune work starts during it", async () => {
			const owner = await openOwner();
			let dropPruneWork = () => {};
			// A committed removal announces the change before it confirms a hold.
			owner.log.events.addEventListener(
				"replication:change",
				() => {
					dropPruneWork = trackPruneCandidate(owner);
				},
				{ once: true },
			);
			try {
				await depart(owner);
			} finally {
				dropPruneWork();
			}
			expect(
				changesQueued(owner, "removed"),
				"passes: the removal is queued at once",
			).to.equal(1);
			expect(
				owner.holds,
				"hold: the prune work declined the hold",
			).to.deep.equal(["released:declined"]);
		});

		it("declines to hold a removal while a placement batch is pending", async () => {
			const owner = await openOwner();
			void queueOwnChange(owner);
			await depart(owner);
			expect(
				changesQueued(owner, "removed"),
				"passes: the removal is queued at once",
			).to.equal(1);
			expect(
				owner.holds,
				"hold: the pending batch declined the hold",
			).to.deep.equal(["released:declined"]);
		});

		it("declines to hold a removal while a placement pass runs", async () => {
			const owner = await openOwner();
			const started = pDefer<void>();
			const finish = pDefer<void>();
			const pass = owner.log.onReplicationChange;
			sandbox.stub(owner.log, "onReplicationChange").callsFake(async function (
				this: unknown,
				...args: unknown[]
			) {
				started.resolve();
				await finish.promise;
				return pass.apply(this, args);
			});
			// The change debouncer starts this pass, as it starts every batch.
			void queueOwnChange(owner);
			const passes = owner.log.replicationChangeDebounceFn.flush();
			await started.promise;
			await depart(owner);
			expect(
				changesQueued(owner, "removed"),
				"passes: the removal is queued while the pass runs",
			).to.equal(1);
			finish.resolve();
			await passes;
			expect(
				owner.holds,
				"hold: the running pass declined the hold",
			).to.deep.equal(["released:declined"]);
		});

		it("does not hold a removal on an adaptive log", async () => {
			const owner = await openOwner();
			owner.log._isAdaptiveReplicating = true;
			try {
				await depart(owner);
			} finally {
				owner.log._isAdaptiveReplicating = false;
			}
			expect(
				changesQueued(owner, "removed"),
				"passes: the removal is queued at once",
			).to.equal(1);
			expect(owner.holds, "hold: nothing is held").to.deep.equal([]);
		});

		it("queues the removal at once when the hold check cannot read the replication index", async () => {
			const owner = await openOwner();
			const failed = failLeaderRead(
				owner,
				() => changesQueued(owner, "removed") === 0,
			);
			const disconnected = sandbox.spy(
				owner.log.syncronizer,
				"onPeerDisconnected",
			);
			const removals: unknown[] = [];
			const removed = await depart(owner, {
				onRemoved: (state: unknown) => removals.push(state),
			});
			expect(removed, "passes: the removal completes").to.equal(true);
			expect(
				changesQueued(owner, "removed"),
				"passes: the removal is queued at once",
			).to.equal(1);
			expect(
				removals,
				"passes: the removal reports the departed replicator",
			).to.deep.equal([{ wasReplicator: true }]);
			expect(
				disconnected.calledWith(owner.remoteHash),
				"passes: the departed owner's sync state is cleaned up",
			).to.equal(true);
			expect(failed.reads, "hold: the hold check's read failed").to.equal(1);
			expect(owner.holds, "hold: nothing is held").to.deep.equal([]);
		});

		it("ends the hold when checked-prune work exists at the return", async () => {
			const owner = await openOwner();
			await depart(owner);
			const dropPruneWork = trackPruneCandidate(owner);
			try {
				await returnUnchanged(owner);
			} finally {
				dropPruneWork();
			}
			expectUsualReturn(owner);
		});

		it("ends the hold when a placement batch is pending at the return", async () => {
			const owner = await openOwner();
			await depart(owner);
			void queueOwnChange(owner);
			await returnUnchanged(owner);
			expectUsualReturn(owner);
		});

		it("ends the hold when the return check cannot read the replication index", async () => {
			const owner = await openOwner();
			await depart(owner);
			const failed = failLeaderRead(
				owner,
				() => changesQueued(owner, "added") === 0,
			);
			await returnUnchanged(owner);
			expectUsualReturn(owner);
			expect(failed.reads, "hold: the restore check's read failed").to.equal(1);
		});

		it("ends the hold on an empty return", async () => {
			const owner = await openOwner();
			await depart(owner);
			await owner.log.addReplicationRange([], owner.remote, { reset: true });
			expect(
				changesQueued(owner, "removed"),
				"passes: the held removal is queued",
			).to.equal(1);
			expect(
				owner.holds,
				"hold: the empty return ended the hold",
			).to.deep.equal(["parked", "released:changed"]);
		});

		it("ends the hold when the held owner is removed again", async () => {
			const owner = await openOwner();
			await depart(owner);
			await owner.log.removeReplicator(owner.remote, {
				shouldRemove: () => true,
			});
			expect(
				changesQueued(owner, "removed"),
				"passes: the held removal is queued",
			).to.equal(1);
			expect(
				owner.holds,
				"hold: the second removal ended the hold",
			).to.deep.equal(["parked", "released:changed"]);
		});
	});

	describe("range identity", () => {
		it("matches only byte-identical added rows", async () => {
			// Loaded when the test runs, so the other tests in this file also run
			// against a build that predates the module.
			const { rowsMatchTransportLossPark } = await import(
				new URL("../src/transport-loss-park.js", import.meta.url).href
			);
			const owner = "transport-loss-owner";
			const id = (seed: number) => new Uint8Array(32).fill(seed);
			const row = (
				seed: number,
				change: Partial<{
					id: Uint8Array;
					offset: bigint;
					width: bigint;
					mode: ReplicationIntent;
					timestamp: bigint;
				}> = {},
			) =>
				new ReplicationRangeIndexableU64({
					id: id(seed),
					publicKeyHash: owner,
					offset: 1_000n * BigInt(seed),
					width: 500n,
					mode: ReplicationIntent.NonStrict,
					timestamp: 7n,
					...change,
				});
			const held = [row(1), row(2)];
			const park = {
				rows: held,
				rowsById: new Map(held.map((range) => [range.idString, range])),
			};
			const added = (...ranges: ReplicationRangeIndexableU64[]) =>
				ranges.map((range) => ({ type: "added", range }));
			const cases: [string, { type: string; range: unknown }[], boolean][] = [
				["identical rows", added(row(1), row(2)), true],
				["identical rows in another order", added(row(2), row(1)), true],
				["another timestamp", added(row(1), row(2, { timestamp: 8n })), false],
				["another offset", added(row(1), row(2, { offset: 2_001n })), false],
				["another width", added(row(1), row(2, { width: 501n })), false],
				[
					"another mode",
					added(row(1), row(2, { mode: ReplicationIntent.Strict })),
					false,
				],
				["another id", added(row(1), row(2, { id: id(3) })), false],
				["one row too many", added(row(1), row(2), row(3)), false],
				["one row too few", added(row(1)), false],
				[
					"a removed diff",
					[{ type: "removed", range: row(1) }, ...added(row(2))],
					false,
				],
				[
					"a replaced diff",
					[{ type: "replaced", range: row(1) }, ...added(row(2))],
					false,
				],
				["a repeated id", added(row(1), row(1)), false],
			];
			for (const [name, diffs, expected] of cases) {
				expect(rowsMatchTransportLossPark(park, diffs), name).to.equal(
					expected,
				);
			}
		});
	});
});
