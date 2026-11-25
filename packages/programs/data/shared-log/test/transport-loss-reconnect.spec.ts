import { Ed25519Keypair, type PublicSignKey } from "@peerbit/crypto";
import { UnsubcriptionEvent } from "@peerbit/pubsub-interface";
import { delay, waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import pDefer from "p-defer";
import { Peerbit } from "peerbit";
import sinon from "sinon";
import { PersistedDeliveryError, SharedLog } from "../src/index.js";
import {
	ReplicationIntent,
	ReplicationRangeIndexableU64,
} from "../src/ranges.js";
import { SYNC_MESSAGE_PRIORITY } from "../src/sync/simple.js";
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
	/**
	 * Held-removal profile events, as outcome or outcome:reason, by owner, with
	 * when each arrived and, for a restore, what it sent to the owner.
	 */
	holds: {
		peer?: string;
		outcome: string;
		at: number;
		pushed?: number;
		evicted?: number;
	}[];
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

/** A numeric profile detail, or undefined when the event does not carry it. */
const countDetail = (value: unknown) =>
	typeof value === "number" ? value : undefined;

/**
 * Fail at once, with a hold: message, when the log lacks a method that only
 * exists with held removals, instead of spying on or waiting for it.
 */
const expectHeldRemovalMethod = (log: any, name: string) =>
	expect(
		typeof log[name],
		`hold: the log has ${name}, which only exists with held removals`,
	).to.equal("function");

/** Pushes to the given peer, in call order. */
const pushesTo = (push: sinon.SinonSpy, hash: string) =>
	push.getCalls().filter((call) => call.args[0] === hash);

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
									at: performance.now(),
									pushed: countDetail(event.details?.pushed),
									evicted: countDetail(event.details?.evicted),
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

	/**
	 * Cut each pair and wait until both sides report the other unreachable,
	 * except for the reports listed in `unawaited`, as [reporter, peer].
	 */
	const partition = async (
		pairs: [Replica, Replica][],
		unawaited: [Replica, Replica][] = [],
	) => {
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
						if (
							unawaited.some(
								([reporter, peer]) => reporter === from && peer === to,
							)
						) {
							continue;
						}
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

	/**
	 * Watch the replica's log for `hash` from now on, without waiting for it.
	 * `arrived` resolves with when the entry was first seen, or with undefined
	 * if it was not seen within `timeoutMs` or the watch was stopped.
	 */
	const watchArrival = (replica: Replica, hash: string, timeoutMs: number) => {
		const deadline = performance.now() + timeoutMs;
		let stopped = false;
		const arrived = (async () => {
			while (!stopped && performance.now() < deadline) {
				try {
					if (await replica.store.log.log.has(hash)) {
						return performance.now();
					}
				} catch {
					return undefined;
				}
				await delay(5);
			}
			return undefined;
		})();
		return {
			arrived,
			stop: () => {
				stopped = true;
			},
		};
	};

	type RestorePushSpies = {
		push: sinon.SinonSpy;
		restore?: sinon.SinonSpy;
	};

	/**
	 * Spy on what a replica's restores send. Without held removals there is no
	 * restore to spy on: `required` then fails at once with a hold: message,
	 * and otherwise the test goes on to its own assertions.
	 */
	const spyOnRestorePush = (
		replica: Replica,
		required = true,
	): RestorePushSpies => {
		if (required) {
			expectHeldRemovalMethod(replica.log, "tryRestoreTransportLossPark");
		}
		return {
			push: sandbox.spy(replica.log, "pushEntryHashes"),
			restore:
				typeof replica.log.tryRestoreTransportLossPark === "function"
					? sandbox.spy(replica.log, "tryRestoreTransportLossPark")
					: undefined,
		};
	};

	/**
	 * The restore's push: the replica's restore of `other` reports at least one
	 * entry sent, and a repair-hinted push of `hash` to `other` was made after
	 * that restore began and before the authoritative repair it scheduled. A
	 * repair that was already waiting can push the same hash during the hold,
	 * before the restore, so the push alone would not show the restore sent it.
	 */
	const expectRestorePush = (
		replica: Replica,
		spies: RestorePushSpies,
		other: Replica,
		hash: string,
		message: string,
	) => {
		expect(spies.restore, "hold: the log restores held removals").to.exist;
		expect(
			replica.holds.some(
				(event) =>
					event.peer === other.hash &&
					event.outcome === "restored" &&
					(event.pushed ?? 0) >= 1,
			),
			`${message}: the restore reports entries sent`,
		).to.equal(true);
		const sent = spies
			.restore!.getCalls()
			.filter((call) => call.args[0] === other.hash)
			.some((restore) => {
				const repair = replica.joinRepair
					.getCalls()
					.find(
						(call) =>
							call.args[0]?.has?.(other.hash) === true &&
							restore.calledBefore(call),
					);
				return pushesTo(spies.push, other.hash).some(
					(push) =>
						push.args[1]?.includes(hash) === true &&
						push.args[2]?.repairHint === true &&
						restore.calledBefore(push) &&
						repair !== undefined &&
						push.calledBefore(repair),
				);
			});
		expect(sent, message).to.equal(true);
	};

	/** The replica's restore event for `other`. */
	const restoredEvent = (replica: Replica, other: Replica) => {
		const event = replica.holds.find(
			(hold) => hold.peer === other.hash && hold.outcome === "restored",
		);
		expect(event, `hold: peer ${replica.index} restored peer ${other.index}`).to
			.exist;
		return event!;
	};

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
			// Not required: without held removals this test fails on its passes.
			const restorePush = spyOnRestorePush(a, false);

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
			expectRestorePush(
				a,
				restorePush,
				b,
				duringPartition.hash,
				"passes: peer 0's restore sent peer 1 the entry written during the partition before its repair",
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
		// The leave is dispatched once the removal has returned.
		await waitForResolved(() =>
			expect(a.leaves, "passes: peer 0 saw peer 2 leave").to.include(c.hash),
		);
		expect(
			a.log._transportLossLedgers?.size ?? 0,
			"hold: nothing is recorded for the departed peer afterwards",
		).to.equal(0);
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
		// Recorded for the held peer in case it is restored.
		await a.store.add("written during the hold");

		await a.store.close();
		expect(
			a.log._transportLossParks?.size ?? 0,
			"hold: closing drops the held removal",
		).to.equal(0);
		expect(
			a.log._transportLossLedgers?.size ?? 0,
			"hold: closing drops what was recorded for the held peer",
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

	it("delivers the entry written during the partition within a second of the restore when a repair for the peer started during the hold", async () => {
		const [a, b] = await openReplicas({ count: 2, min: 2 });
		expectHeldRemovalMethod(a.log, "scheduleUnchangedSuccessorRepair");
		for (const replica of [a, b]) {
			replica.log.transportLossRemovalGraceMs = 5_000;
		}
		const pairs: [Replica, Replica][] = [
			[a, b],
			[b, a],
		];
		const restorePush = spyOnRestorePush(a);
		const scheduleRepair = a.log.scheduleUnchangedSuccessorRepair;
		let repairWaitingAtRestore: boolean | undefined;
		let restoreBypassesKnownPeerHints: unknown;
		sandbox.stub(a.log, "scheduleUnchangedSuccessorRepair").callsFake(function (
			this: unknown,
			...args: unknown[]
		) {
			// The first call for the held peer is its restore's.
			if (args[0] === b.hash && repairWaitingAtRestore === undefined) {
				repairWaitingAtRestore =
					a.log._repairFrontierActiveTargetsByMode
						.get("join-authoritative")
						?.has(b.hash) === true;
				restoreBypassesKnownPeerHints = args[2];
			}
			return scheduleRepair.apply(this, args);
		});

		await partition([[a, b]]);
		const { entry: duringPartition } = await a.store.add(
			"written during the partition",
		);
		// The departure's cleanup retires any repair for the peer, so the sweep
		// below starts once it is done, as a delayed sweep would.
		await waitForResolved(() =>
			expect(a.leaves, "passes: peer 0 saw peer 1 leave").to.include(b.hash),
		);
		// What a delayed authoritative sweep does when it fires during a hold:
		// it starts a repair for the held peer.
		a.log.scheduleRepairSweep({ mode: "join-authoritative", peers: [b.hash] });
		// That repair's second round pushes the entry into the lost transport and
		// then waits two seconds for its next round.
		await waitForResolved(
			() =>
				expect(
					pushesTo(restorePush.push, b.hash).some(
						(call) => call.args[1]?.includes(duringPartition.hash) === true,
					),
					"hold: the repair started during the hold pushed the entry to the held peer",
				).to.equal(true),
			{ timeout: 4_000 },
		);
		expect(
			holdsFor(a, b.hash),
			"hold: peer 0 still holds peer 1 once that repair has pushed",
		).to.deep.equal(["parked"]);
		// Started before the heal, so the arrival is timed wherever it lands.
		const arrival = watchArrival(b, duringPartition.hash, 20_000);
		try {
			await heal(a, b);

			expect(
				repairWaitingAtRestore,
				"hold: a repair for peer 1 was waiting when peer 0 restored it",
			).to.equal(true);
			expect(
				restoreBypassesKnownPeerHints,
				"hold: peer 0's restore repairs peer 1 honouring the known-peer hints that peer 0 cleared when peer 1 left",
			).to.equal(false);
			const restoredAt = restoredEvent(a, b).at;
			const arrivedAt = await arrival.arrived;
			expect(
				arrivedAt,
				"passes: the entry written during the partition reaches the returned peer",
			).to.be.a("number");
			expect(
				arrivedAt! - restoredAt,
				"passes: the entry written during the partition reaches the returned peer within 1 s of the restore (ms after it)",
			).to.be.at.most(1_000);
		} finally {
			arrival.stop();
		}
		expectRestorePush(
			a,
			restorePush,
			b,
			duringPartition.hash,
			"passes: peer 0's restore sent peer 1 the entry written during the partition before its repair",
		);
		for (const [replica, other] of pairs) {
			expect(
				passesReferencing(replica, other.hash),
				`passes: peer ${replica.index} ran placement passes for peer ${other.index}`,
			).to.have.length(0);
			expect(
				holdsFor(replica, other.hash),
				`hold: peer ${replica.index} held and restored peer ${other.index}`,
			).to.deep.equal(["parked", "restored"]);
		}
	});

	it("sends the returned peer what a third peer wrote while it was away", async () => {
		const [a, b, c] = await openReplicas({ count: 3, min: 3 });
		expectHeldRemovalMethod(a.log, "tryRestoreTransportLossPark");
		for (const replica of [a, b, c]) {
			replica.log.transportLossRemovalGraceMs = 5_000;
		}
		// The third peer's report of the second is not awaited: when its own
		// link drops it can still route to the second peer through the first,
		// and that route can lapse without a report. Nothing here depends on it.
		await partition(
			[
				[a, b],
				[b, c],
			],
			[[c, b]],
		);
		const { entry: fromThird } = await c.store.add(
			"written by the third peer while the second was away",
		);
		await waitForResolved(
			async () =>
				expect(
					await a.store.log.log.has(fromThird.hash),
					"passes: the third peer's entry reaches the first peer",
				).to.equal(true),
			{ timeout: 5_000 },
		);
		// No route from the third peer reaches the second one after the heal.
		await partition([[a, c]]);
		const restorePush = spyOnRestorePush(a);
		const dialedAt = await heal(a, b);

		expectRestorePush(
			a,
			restorePush,
			b,
			fromThird.hash,
			"passes: peer 0's restore sent peer 1 the third peer's entry before its repair",
		);
		await expectDeliveredBy(
			b,
			fromThird.hash,
			dialedAt + 5_000,
			"passes: the third peer's entry reaches the returned peer within 5 s of the heal",
		);
		for (const [replica, other] of [
			[a, b],
			[b, a],
		] as [Replica, Replica][]) {
			expect(
				holdsFor(replica, other.hash),
				`hold: peer ${replica.index} held and restored peer ${other.index}`,
			).to.deep.equal(["parked", "restored"]);
		}
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
			/** The same events by owner hash. */
			holdsByPeer: Map<string, string[]>;
			/** What each restore sent to its owner. */
			deliveries: { peer?: string; pushed?: number; evicted?: number }[];
			remote: PublicSignKey;
			remoteHash: string;
			/** The remote owner's only range. */
			row: unknown;
			/** This replica's own range. */
			ownRange: unknown;
			queued: sinon.SinonSpy;
			joinRepair: sinon.SinonSpy;
		};

		/** A remote owner's full range on the log, the way a Full would add it. */
		const addRemoteRow = async (log: any) => {
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
			return { remote, remoteHash, row };
		};

		const openOwner = async ({ min = 2 } = {}): Promise<Owner> => {
			const peer = await Peerbit.create();
			peers.push(peer);
			const holds: string[] = [];
			const holdsByPeer = new Map<string, string[]>();
			const deliveries: Owner["deliveries"] = [];
			const log = new SharedLog<any, any>() as any;
			await peer.open(log, {
				args: {
					replicas: { min },
					replicate: 1,
					timeUntilRoleMaturity: 0,
					distributionDebounceTime: batchIntervalMs,
					sync: {
						profile: (event: any) => {
							if (event.name === "sharedLog.transportLoss.park") {
								const outcome = String(event.details?.outcome);
								const reason = event.details?.reason;
								const hold = reason ? `${outcome}:${reason}` : outcome;
								holds.push(hold);
								const byPeer = holdsByPeer.get(event.peer) ?? [];
								byPeer.push(hold);
								holdsByPeer.set(event.peer, byPeer);
								if (outcome === "restored") {
									deliveries.push({
										peer: event.peer,
										pushed: countDetail(event.details?.pushed),
										evicted: countDetail(event.details?.evicted),
									});
								}
							}
						},
					},
				},
			});
			log._liveness.stopReplicatorLivenessSweep();
			const { remote, remoteHash, row } = await addRemoteRow(log);
			const [ownRange] = await log.getMyReplicationSegments();
			return {
				log,
				holds,
				holdsByPeer,
				deliveries,
				remote,
				remoteHash,
				row,
				ownRange,
				queued: sandbox.spy(log.replicationChangeDebounceFn, "add"),
				joinRepair: sandbox.spy(log, "scheduleJoinAuthoritativeRepair"),
			};
		};

		/** A second remote owner on the same log, created like the first. */
		const addRemoteOwner = async (owner: Owner): Promise<Owner> => ({
			...owner,
			...(await addRemoteRow(owner.log)),
		});

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

		/** Append one entry on this replica. */
		const appendEntry = async (owner: Owner, seed: number) =>
			(await owner.log.append(new Uint8Array([seed]))).entry as {
				hash: string;
			};

		/** The authoritative repair scheduled for the owner after `after` calls. */
		const repairCallFor = (owner: Owner, after = 0) =>
			owner.joinRepair
				.getCalls()
				.slice(after)
				.find((call) => call.args[0]?.has?.(owner.remoteHash) === true);

		/**
		 * The hashes of each push to the owner made after the first
		 * `pushesBefore` calls and before the authoritative repair scheduled
		 * after `repairsBefore` calls: what a restore that starts after both
		 * counts sends. Queued append backfill can push to the owner later.
		 */
		const restorePushes = (
			owner: Owner,
			push: sinon.SinonStub,
			pushesBefore: number,
			repairsBefore: number,
		) => {
			const repair = repairCallFor(owner, repairsBefore);
			return push
				.getCalls()
				.slice(pushesBefore)
				.filter(
					(call) =>
						call.args[0] === owner.remoteHash &&
						(repair === undefined || call.calledBefore(repair)),
				)
				.map((call) => call.args[1]);
		};

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
			expect(
				owner.log._transportLossLedgers?.size ?? 0,
				"hold: nothing is recorded for the owner afterwards",
			).to.equal(0);
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
			const push = sandbox.stub(owner.log, "pushEntryHashes").resolves();
			await depart(owner);
			// Recorded for the held owner in case it is restored.
			await appendEntry(owner, 1);
			await owner.log.addReplicationRange([], owner.remote, { reset: true });
			expect(
				changesQueued(owner, "removed"),
				"passes: the held removal is queued",
			).to.equal(1);
			expect(
				owner.holds,
				"hold: the empty return ended the hold",
			).to.deep.equal(["parked", "released:changed"]);
			expect(
				pushesTo(push, owner.remoteHash),
				"passes: nothing is sent to an owner whose hold ended without a restore",
			).to.have.length(0);
			expect(
				owner.log._transportLossLedgers?.size ?? 0,
				"hold: nothing is recorded for the owner afterwards",
			).to.equal(0);
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

		it("sends the entries written during a hold to the owner when it is restored", async () => {
			const owner = await openOwner();
			const push = sandbox.stub(owner.log, "pushEntryHashes").resolves();
			await depart(owner);
			const first = await appendEntry(owner, 1);
			const second = await appendEntry(owner, 2);
			const third = await appendEntry(owner, 3);
			// Stands in for the owner's confirmation of the second entry.
			owner.log.markEntriesKnownByPeer([second.hash], owner.remoteHash);
			await returnUnchanged(owner);

			expect(
				owner.holds,
				"hold: the removal was held and restored",
			).to.deep.equal(["parked", "restored"]);
			expect(
				changesQueued(owner, "removed"),
				"passes: no removal is queued",
			).to.equal(0);
			expect(
				changesQueued(owner, "added"),
				"passes: no addition is queued",
			).to.equal(0);
			const pushes = pushesTo(push, owner.remoteHash);
			expect(
				pushes,
				"passes: the restore pushed to the owner once",
			).to.have.length(1);
			const [, hashes, options] = pushes[0].args;
			expect(
				hashes,
				"passes: the restore sent the entries the owner has not confirmed, oldest first",
			).to.deep.equal([first.hash, third.hash]);
			expect(options?.repairHint, "passes: the push is repair-hinted").to.equal(
				true,
			);
			expect(
				options?.priority,
				"passes: the push has the repair's priority",
			).to.equal(SYNC_MESSAGE_PRIORITY);
			expect(
				options?.signal,
				"passes: the push stops with the ownership lifecycle",
			).to.equal(
				owner.log._instanceLifecycle.ownershipLifecycleController.signal,
			);
			const repair = repairCallFor(owner);
			expect(
				repair !== undefined && pushes[0].calledBefore(repair),
				"passes: the push comes before the authoritative repair",
			).to.equal(true);
			expect(
				owner.deliveries,
				"passes: the restore reports what it sent",
			).to.deep.equal([{ peer: owner.remoteHash, pushed: 2, evicted: 0 }]);
			expect(
				owner.log._transportLossLedgers?.size ?? 0,
				"hold: nothing is recorded for the owner afterwards",
			).to.equal(0);
			expect(
				options?.isStillCurrent?.(),
				"passes: the push goes on while the owner's session and receive epoch last",
			).to.equal(true);
			owner.log._peerSessions.advanceReceiveEpoch(owner.remoteHash);
			expect(
				options?.isStillCurrent?.(),
				"passes: the push stops once the owner's receive epoch moves on",
			).to.equal(false);
		});

		it("keeps the newest entries when the ledger is full", async () => {
			const owner = await openOwner();
			const push = sandbox.stub(owner.log, "pushEntryHashes").resolves();
			owner.log.transportLossLedgerLimit = 2;
			await depart(owner);
			await appendEntry(owner, 1);
			const second = await appendEntry(owner, 2);
			const third = await appendEntry(owner, 3);
			await returnUnchanged(owner);

			expect(
				owner.holds,
				"hold: a full ledger does not end the hold",
			).to.deep.equal(["parked", "restored"]);
			expect(
				changesQueued(owner, "removed"),
				"passes: no removal is queued",
			).to.equal(0);
			expect(
				changesQueued(owner, "added"),
				"passes: no addition is queued",
			).to.equal(0);
			expect(
				pushesTo(push, owner.remoteHash).map((call) => call.args[1]),
				"passes: the restore sent the newest entries",
			).to.deep.equal([[second.hash, third.hash]]);
			expect(
				owner.deliveries,
				"passes: the restore reports what it sent and what the ledger dropped",
			).to.deep.equal([{ peer: owner.remoteHash, pushed: 2, evicted: 1 }]);
		});

		it("records an append made after the removal started, before the hold exists", async () => {
			const owner = await openOwner();
			expectHeldRemovalMethod(owner.log, "tryCreateTransportLossPark");
			const push = sandbox.stub(owner.log, "pushEntryHashes").resolves();
			const holding = pDefer<void>();
			const gate = pDefer<void>();
			let holdAttempted = false;
			const createHold = owner.log.tryCreateTransportLossPark;
			sandbox
				.stub(owner.log, "tryCreateTransportLossPark")
				.callsFake(async function (this: unknown, ...args: unknown[]) {
					holdAttempted = true;
					holding.resolve();
					await gate.promise;
					return createHold.apply(this, args);
				});

			const departed = depart(owner);
			let entry!: { hash: string };
			try {
				await Promise.race([holding.promise, departed]);
				expect(
					holdAttempted,
					"hold: the removal tried to hold the owner",
				).to.equal(true);
				// The owner's rows are already deleted: this entry's plan leaves it out.
				entry = await appendEntry(owner, 1);
			} finally {
				gate.resolve();
			}
			await departed;
			await returnUnchanged(owner);

			expect(
				owner.holds,
				"hold: the removal was held and restored",
			).to.deep.equal(["parked", "restored"]);
			expect(
				pushesTo(push, owner.remoteHash).map((call) => call.args[1]),
				"passes: the restore sent the entry appended before the hold existed",
			).to.deep.equal([[entry.hash]]);
		});

		it("records an append planned with the owner before its removal started", async () => {
			const owner = await openOwner();
			const push = sandbox.stub(owner.log, "pushEntryHashes").resolves();
			let appendedHash: string | undefined;
			const processAppend = owner.log.processLocalAppend;
			sandbox.stub(owner.log, "processLocalAppend").callsFake(function (
				this: unknown,
				...args: any[]
			) {
				appendedHash ??= args[0]?.hash;
				return processAppend.apply(this, args);
			});
			// Hold the append once its plan, which has the owner as a recipient,
			// has run and released its hash locks.
			const planned = pDefer<void>();
			const planGate = pDefer<void>();
			const coordinates = owner.log._coordinates;
			const withMutationOwner = coordinates.withCoordinateMutationOwner;
			let planHeld = false;
			sandbox
				.stub(coordinates, "withCoordinateMutationOwner")
				.callsFake(function (this: unknown, ...args: any[]) {
					const hashes = [...args[0]];
					const result = withMutationOwner.call(this, hashes, ...args.slice(1));
					// A borrowed owner runs inside the append's own locked scope.
					if (planHeld || args[2] !== undefined || hashes[0] !== appendedHash) {
						return result;
					}
					planHeld = true;
					return (async () => {
						const plan = await result;
						planned.resolve();
						await planGate.promise;
						return plan;
					})();
				});
			// Hold its delivery until after the restore, so that the delivery
			// itself records nothing for the held owner.
			const delivering = pDefer<void>();
			const deliveryGate = pDefer<void>();
			const deliver = owner.log._appendDeliverToReplicators;
			sandbox
				.stub(owner.log, "_appendDeliverToReplicators")
				.callsFake(async function (this: unknown, ...args: any[]) {
					if (args[0]?.hash === appendedHash) {
						delivering.resolve();
						await deliveryGate.promise;
					}
					return deliver.apply(this, args);
				});

			const appended = appendEntry(owner, 1);
			let pushesBefore = 0;
			let repairsBefore = 0;
			try {
				await Promise.race([planned.promise, appended]);
				expect(
					planHeld,
					"hold: the append was planned before the removal",
				).to.equal(true);
				try {
					await depart(owner);
				} finally {
					planGate.resolve();
				}
				expect(owner.holds, "hold: the removal is held").to.deep.equal([
					"parked",
				]);
				await Promise.race([delivering.promise, appended]);
				pushesBefore = push.callCount;
				repairsBefore = owner.joinRepair.callCount;
				await returnUnchanged(owner);
			} finally {
				planGate.resolve();
				deliveryGate.resolve();
			}
			const entry = await appended;

			expect(
				owner.holds,
				"hold: the removal was held and restored",
			).to.deep.equal(["parked", "restored"]);
			expect(
				restorePushes(owner, push, pushesBefore, repairsBefore),
				"passes: the restore sent the entry planned before the removal",
			).to.deep.equal([[entry.hash]]);
		});

		it("records a delivery to the owner that is queued after its removal started", async () => {
			const owner = await openOwner();
			const push = sandbox.stub(owner.log, "pushEntryHashes").resolves();
			// Hold the append at its delivery: after its plan, which has the owner
			// as a recipient, and after the append's own recording points.
			const delivering = pDefer<void>();
			const gate = pDefer<void>();
			const deliver = owner.log._appendDeliverToReplicators;
			let held = false;
			sandbox
				.stub(owner.log, "_appendDeliverToReplicators")
				.callsFake(async function (this: unknown, ...args: unknown[]) {
					if (!held) {
						held = true;
						delivering.resolve();
						await gate.promise;
					}
					return deliver.apply(this, args);
				});

			const appended = appendEntry(owner, 1);
			try {
				await Promise.race([delivering.promise, appended]);
				expect(
					held,
					"hold: the append reached its delivery before the removal",
				).to.equal(true);
				await depart(owner);
				expect(owner.holds, "hold: the removal is held").to.deep.equal([
					"parked",
				]);
			} finally {
				gate.resolve();
			}
			const entry = await appended;
			expect(
				[
					...(owner.log._appendBackfillPendingByTarget
						.get(owner.remoteHash)
						?.keys() ?? []),
					...(owner.log._repairFrontierByMode
						.get("append-backfill")
						?.get(owner.remoteHash)
						?.keys() ?? []),
				],
				"hold: the append queued its delivery to the owner during the hold",
			).to.include(entry.hash);
			const pushesBefore = push.callCount;
			const repairsBefore = owner.joinRepair.callCount;
			await returnUnchanged(owner);

			expect(
				owner.holds,
				"hold: the removal was held and restored",
			).to.deep.equal(["parked", "restored"]);
			expect(
				restorePushes(owner, push, pushesBefore, repairsBefore),
				"passes: the restore sent the entry whose delivery was queued during the hold",
			).to.deep.equal([[entry.hash]]);
		});

		/**
		 * Batch appends that plan delivery for the whole batch before any of its
		 * entries is processed.
		 */
		const batchAppends = [
			{
				name: "independent entries",
				append: (owner: Owner, seeds: number[]) =>
					owner.log.appendLocallyPreparedManyIndependent(
						seeds.map((seed) => new Uint8Array([seed])),
					),
			},
			{
				name: "appendMany",
				// Explicit delivery keeps the chained batch off the coalesced path,
				// which delivers and records only its head.
				append: (owner: Owner, seeds: number[]) =>
					owner.log.appendMany(
						seeds.map((seed) => new Uint8Array([seed])),
						{ delivery: { reliability: "best-effort" } },
					),
			},
		];

		for (const batch of batchAppends) {
			it(`records a batch planned during the hold whose entries are processed after the restore (${batch.name})`, async () => {
				const owner = await openOwner();
				const push = sandbox.stub(owner.log, "pushEntryHashes").resolves();
				await depart(owner);
				expect(owner.holds, "hold: the removal is held").to.deep.equal([
					"parked",
				]);
				const planBatch = owner.log.planAndPersistNativeAppendEntries;
				let returned: Promise<unknown> | undefined;
				sandbox
					.stub(owner.log, "planAndPersistNativeAppendEntries")
					.callsFake(async function (this: unknown, ...args: unknown[]) {
						const plans = await planBatch.apply(this, args);
						// The owner returns once the batch is planned without it, before
						// any of its entries is processed.
						returned ??= returnUnchanged(owner);
						await returned;
						return plans;
					});
				const repairsBefore = owner.joinRepair.callCount;
				const result = await batch.append(owner, [1, 2, 3, 4]);

				expect(returned, "hold: the owner returned during the batch").to.exist;
				expect(
					owner.holds,
					"hold: the removal was held and restored",
				).to.deep.equal(["parked", "restored"]);
				expect(
					restorePushes(owner, push, 0, repairsBefore),
					"passes: the restore sent every entry of the batch",
				).to.deep.equal([
					result.entries.map((entry: { hash: string }) => entry.hash),
				]);
				expect(
					owner.deliveries,
					"passes: the restore reports what it sent",
				).to.deep.equal([{ peer: owner.remoteHash, pushed: 4, evicted: 0 }]);
			});

			it(`records a batch planned while the owner's removal started whose entries are processed after the restore (${batch.name})`, async () => {
				const owner = await openOwner();
				const push = sandbox.stub(owner.log, "pushEntryHashes").resolves();
				const planBatch = owner.log.planAndPersistNativeAppendEntries;
				let departed: Promise<unknown> | undefined;
				sandbox
					.stub(owner.log, "planAndPersistNativeAppendEntries")
					.callsFake(async function (this: unknown, ...args: unknown[]) {
						// The owner's removal starts, and is held, while the batch is
						// planned, so the plan leaves it out.
						departed ??= depart(owner);
						await departed;
						return planBatch.apply(this, args);
					});
				const processAppend = owner.log.processLocalAppend;
				let returned: Promise<unknown> | undefined;
				let pushesBefore = 0;
				let repairsBefore = 0;
				sandbox.stub(owner.log, "processLocalAppend").callsFake(async function (
					this: unknown,
					...args: unknown[]
				) {
					// The owner returns before any entry of the batch is processed.
					if (!returned) {
						pushesBefore = push.callCount;
						repairsBefore = owner.joinRepair.callCount;
						returned = returnUnchanged(owner);
					}
					await returned;
					return processAppend.apply(this, args);
				});
				const result = await batch.append(owner, [1, 2, 3, 4]);

				expect(departed, "hold: the owner left while the batch was planned").to
					.exist;
				expect(
					returned,
					"hold: the owner returned before the batch was processed",
				).to.exist;
				expect(
					owner.holds,
					"hold: the removal was held and restored",
				).to.deep.equal(["parked", "restored"]);
				expect(
					restorePushes(owner, push, pushesBefore, repairsBefore),
					"passes: the restore sent every entry of the batch",
				).to.deep.equal([
					result.entries.map((entry: { hash: string }) => entry.hash),
				]);
				expect(
					owner.deliveries,
					"passes: the restore reports what it sent",
				).to.deep.equal([{ peer: owner.remoteHash, pushed: 4, evicted: 0 }]);
			});
		}

		/** Commit an entry the way Documents commits a write with persisted delivery. */
		const commitWithoutDelivery = async (owner: Owner, seed: number) =>
			(
				await owner.log.append(new Uint8Array([seed]), {
					target: "none",
					delivery: false,
					replicate: false,
				})
			).entry as { hash: string };

		/**
		 * Deliver committed entries with persisted delivery, the way Documents
		 * does after such a commit. Resolves to the error it ends with, if any.
		 */
		const deliverPersisted = (
			owner: Owner,
			entries: unknown[],
			signal?: AbortSignal,
		): Promise<unknown> =>
			owner.log
				.deliverPersistedEntries(entries, {
					delivery: {
						reliability: "persisted",
						minAcks: 1,
						timeout: 10_000,
						signal,
					},
				})
				.then(
					() => undefined,
					(error: unknown) => error,
				);

		/** Hold the first leader plan of a persisted settlement until released. */
		const holdPersistedPlan = (owner: Owner) => {
			const planning = pDefer<void>();
			const gate = pDefer<void>();
			let held = false;
			const plan = owner.log.planPersistedDeliveryLeaders;
			sandbox
				.stub(owner.log, "planPersistedDeliveryLeaders")
				.callsFake(async function (this: unknown, ...args: unknown[]) {
					if (!held) {
						held = true;
						planning.resolve();
						await gate.promise;
					}
					return plan.apply(this, args);
				});
			return {
				planning: planning.promise,
				held: () => held,
				release: () => gate.resolve(),
			};
		};

		it("sends the owner an entry committed during the hold whose persisted delivery is still settling", async () => {
			const owner = await openOwner();
			const push = sandbox.stub(owner.log, "pushEntryHashes").resolves();
			await depart(owner);
			expect(owner.holds, "hold: the removal is held").to.deep.equal([
				"parked",
			]);
			const entry = await commitWithoutDelivery(owner, 1);
			const settlement = holdPersistedPlan(owner);
			const abort = new AbortController();
			const delivered = deliverPersisted(owner, [entry], abort.signal);
			let pushesBefore = 0;
			let repairsBefore = 0;
			try {
				await Promise.race([settlement.planning, delivered]);
				expect(
					settlement.held(),
					"hold: the delivery is settling when the owner returns",
				).to.equal(true);
				pushesBefore = push.callCount;
				repairsBefore = owner.joinRepair.callCount;
				await returnUnchanged(owner);
			} finally {
				// Otherwise the settlement waits for a receipt this owner cannot send.
				abort.abort();
				settlement.release();
			}
			expect(
				await delivered,
				"hold: the delivery ended when it was aborted",
			).to.be.instanceOf(PersistedDeliveryError);

			expect(
				owner.holds,
				"hold: the removal was held and restored",
			).to.deep.equal(["parked", "restored"]);
			expect(
				restorePushes(owner, push, pushesBefore, repairsBefore),
				"passes: the restore sent the entry whose delivery was settling",
			).to.deep.equal([[entry.hash]]);
			expect(
				owner.deliveries,
				"passes: the restore reports what it sent",
			).to.deep.equal([{ peer: owner.remoteHash, pushed: 1, evicted: 0 }]);
		});

		it("records a persisted delivery whose settlement was under way when the owner's removal started", async () => {
			const owner = await openOwner();
			const push = sandbox.stub(owner.log, "pushEntryHashes").resolves();
			const entry = await commitWithoutDelivery(owner, 1);
			const settlement = holdPersistedPlan(owner);
			const delivered = deliverPersisted(owner, [entry]);
			try {
				await Promise.race([settlement.planning, delivered]);
				expect(
					settlement.held(),
					"hold: the delivery started before the removal",
				).to.equal(true);
				await depart(owner);
				expect(owner.holds, "hold: the removal is held").to.deep.equal([
					"parked",
				]);
			} finally {
				settlement.release();
			}
			// With the owner held, the settlement's plan has no remote leader.
			expect(
				await delivered,
				"hold: the delivery failed without the held owner",
			).to.be.instanceOf(PersistedDeliveryError);
			expect(
				owner.holds,
				"hold: the delivery ended while the owner was held",
			).to.deep.equal(["parked"]);
			const pushesBefore = push.callCount;
			const repairsBefore = owner.joinRepair.callCount;
			await returnUnchanged(owner);

			expect(
				owner.holds,
				"hold: the removal was held and restored",
			).to.deep.equal(["parked", "restored"]);
			expect(
				restorePushes(owner, push, pushesBefore, repairsBefore),
				"passes: the restore sent the entry whose delivery ended while it was held",
			).to.deep.equal([[entry.hash]]);
			expect(
				owner.deliveries,
				"passes: the restore reports what it sent",
			).to.deep.equal([{ peer: owner.remoteHash, pushed: 1, evicted: 0 }]);
		});

		it("sends the owner its unconfirmed append deliveries from before it left", async () => {
			const owner = await openOwner();
			const push = sandbox.stub(owner.log, "pushEntryHashes").resolves();
			const first = await appendEntry(owner, 1);
			const second = await appendEntry(owner, 2);
			const unconfirmed = [
				...(owner.log._appendBackfillPendingByTarget
					.get(owner.remoteHash)
					?.keys() ?? []),
				...(owner.log._repairFrontierByMode
					.get("append-backfill")
					?.get(owner.remoteHash)
					?.keys() ?? []),
			];
			expect(
				unconfirmed,
				"hold: the first entry's delivery to the owner awaits its confirmation",
			).to.include(first.hash);
			// Stands in for the owner's confirmation of the second entry.
			owner.log.markEntriesKnownByPeer([second.hash], owner.remoteHash);
			await depart(owner);
			const pushesBefore = push.callCount;
			const repairsBefore = owner.joinRepair.callCount;
			await returnUnchanged(owner);

			expect(
				owner.holds,
				"hold: the removal was held and restored",
			).to.deep.equal(["parked", "restored"]);
			const repair = repairCallFor(owner, repairsBefore);
			const restorePushes = push
				.getCalls()
				.slice(pushesBefore)
				.filter(
					(call) =>
						call.args[0] === owner.remoteHash &&
						repair !== undefined &&
						call.calledBefore(repair),
				);
			expect(
				restorePushes,
				"passes: the restore pushed to the owner once before its repair",
			).to.have.length(1);
			expect(
				restorePushes[0].args[1],
				"passes: the restore sent the delivery the owner had not confirmed",
			).to.include(first.hash);
			expect(
				restorePushes[0].args[1],
				"passes: the restore left out the delivery the owner confirmed before it left",
			).not.to.include(second.hash);
		});

		it("keeps a separate record for each held owner until that owner returns", async () => {
			const first = await openOwner({ min: 3 });
			const second = await addRemoteOwner(first);
			const push = sandbox.stub(first.log, "pushEntryHashes").resolves();
			await depart(first);
			await depart(second);
			for (const owner of [first, second]) {
				expect(
					first.holdsByPeer.get(owner.remoteHash),
					"hold: both owners are held",
				).to.deep.equal(["parked"]);
			}
			const entry = await appendEntry(first, 1);

			await returnUnchanged(first);
			expect(
				pushesTo(push, first.remoteHash).map((call) => call.args[1]),
				"passes: the first owner's restore sent it the entry",
			).to.deep.equal([[entry.hash]]);
			expect(
				pushesTo(push, second.remoteHash),
				"passes: nothing is sent to the owner still held",
			).to.have.length(0);

			await returnUnchanged(second);
			expect(
				pushesTo(push, second.remoteHash).map((call) => call.args[1]),
				"passes: the second owner's restore sent it the entry",
			).to.deep.equal([[entry.hash]]);
			for (const owner of [first, second]) {
				expect(
					first.holdsByPeer.get(owner.remoteHash),
					"hold: each owner was held and restored",
				).to.deep.equal(["parked", "restored"]);
			}
		});

		it("records nothing for a departing peer that owns no rows", async () => {
			const owner = await openOwner();
			const stranger = (await Ed25519Keypair.create()).publicKey;
			const strangerHash = stranger.hashcode();
			// Keep the stranger's removal in flight while this replica appends.
			const draining = pDefer<void>();
			const gate = pDefer<void>();
			const drain = owner.log.drainPeerReceiveHandlers;
			sandbox
				.stub(owner.log, "drainPeerReceiveHandlers")
				.callsFake(async function (this: unknown, ...args: unknown[]) {
					if (args[0] === strangerHash) {
						draining.resolve();
						await gate.promise;
					}
					return drain.apply(this, args);
				});
			const removal = owner.log.removeReplicator(stranger, {
				transportLoss: true,
			});
			try {
				expect(
					owner.log._transportLossLedgers?.has(strangerHash) ?? false,
					"hold: the removal of a peer that owns no rows opens no ledger",
				).to.equal(false);
				await Promise.race([draining.promise, removal]);
				await appendEntry(owner, 1);
				expect(
					owner.log._transportLossLedgers?.size ?? 0,
					"hold: an append made while that removal runs is recorded nowhere",
				).to.equal(0);
			} finally {
				gate.resolve();
			}
			await removal;
			expect(owner.holds, "hold: nothing is held").to.deep.equal([]);
		});

		it("sends nothing at a restore when the owner leaves again while the restore reads its plan", async () => {
			const owner = await openOwner();
			expectHeldRemovalMethod(owner.log, "readTransportLossLeaderPlan");
			const push = sandbox.stub(owner.log, "pushEntryHashes").resolves();
			// The fixture's owner never subscribed: its session stays null
			// through this removal and the return below.
			await depart(owner);
			await appendEntry(owner, 1);
			const reading = pDefer<void>();
			const gate = pDefer<void>();
			const readPlan = owner.log.readTransportLossLeaderPlan;
			let reads = 0;
			sandbox
				.stub(owner.log, "readTransportLossLeaderPlan")
				.callsFake(async function (this: unknown, ...args: unknown[]) {
					if (reads++ === 0) {
						reading.resolve();
						await gate.promise;
					}
					return readPlan.apply(this, args);
				});
			const repairsBefore = owner.joinRepair.callCount;
			const returned = returnUnchanged(owner);
			let leftAgain: Promise<unknown> | undefined;
			try {
				await Promise.race([reading.promise, returned]);
				expect(reads, "hold: the restore reads its plan").to.equal(1);
				// Pubsub reports the owner unreachable again. Its session moves on
				// at once; the removal waits behind the restore.
				leftAgain = owner.log._onUnsubscription(
					new CustomEvent("unsubscribe", {
						detail: new UnsubcriptionEvent(
							owner.remote,
							[owner.log.topic],
							"peer-unreachable",
						),
					}),
				);
			} finally {
				gate.resolve();
			}
			await returned;
			await leftAgain;

			expect(
				owner.holds,
				"hold: the owner was restored, then held again",
			).to.deep.equal(["parked", "restored", "parked"]);
			expect(
				restorePushes(owner, push, 0, repairsBefore),
				"passes: the restore sent nothing into the session the owner left",
			).to.deep.equal([]);
			expect(
				owner.deliveries,
				"passes: the restore reports nothing sent",
			).to.deep.equal([{ peer: owner.remoteHash, pushed: 0, evicted: 0 }]);
		});

		it("sends nothing at a restore while repairs are suppressed after an assumed sync", async () => {
			const owner = await openOwner();
			const push = sandbox.stub(owner.log, "pushEntryHashes").resolves();
			await depart(owner);
			await appendEntry(owner, 1);
			// As an assumed sync sets it; the repair delivers once it ends.
			owner.log._assumeSyncedRepairSuppressedUntil = Date.now() + 5_000;
			const repairsBefore = owner.joinRepair.callCount;
			await returnUnchanged(owner);

			expect(
				owner.holds,
				"hold: the removal was held and restored",
			).to.deep.equal(["parked", "restored"]);
			expect(
				restorePushes(owner, push, 0, repairsBefore),
				"passes: nothing is sent while repairs are suppressed",
			).to.deep.equal([]);
			expect(
				owner.deliveries,
				"passes: the restore reports nothing sent",
			).to.deep.equal([{ peer: owner.remoteHash, pushed: 0, evicted: 0 }]);
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
