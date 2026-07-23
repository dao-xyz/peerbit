import { type PublicSignKey, randomBytes } from "@peerbit/crypto";
import { TestSession } from "@peerbit/test-utils";
import { delay, waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import pDefer from "p-defer";
import sinon from "sinon";
import { v4 as uuid } from "uuid";
import {
	ExchangeHeadsMessage,
	SyncCapabilitiesMessage,
} from "../src/exchange-heads.js";
import { ReplicationIntent } from "../src/ranges.js";
import {
	AddedReplicationSegmentMessage,
	AllReplicatingSegmentsMessage,
	StoppedReplicating,
} from "../src/replication.js";
import { EventStore } from "./utils/stores/index.js";

describe("events", () => {
	let session: TestSession;

	afterEach(async () => {
		await session.stop();
	});

	const openDisconnectedLog = async (peers = 2) => {
		session = await TestSession.disconnected(peers);
		const db = await session.peers[0].open(new EventStore(), {
			args: { replicate: false, timeUntilRoleMaturity: 0 },
		});
		return {
			db,
			log: db.log as any,
			replicationIndex: db.log.replicationIndex as any,
		};
	};

	const makeReplicationRange = (
		log: any,
		properties: {
			id: Uint8Array;
			ownerHash: string;
			offset: number;
			mode?: ReplicationIntent;
			timestamp?: bigint;
		},
	) =>
		new log.indexableDomain.constructorRange({
			id: properties.id,
			offset: log.indexableDomain.numbers.denormalize(properties.offset),
			width: log.indexableDomain.numbers.denormalize(0.2),
			publicKeyHash: properties.ownerHash,
			mode: properties.mode ?? ReplicationIntent.NonStrict,
			timestamp: properties.timestamp ?? 1n,
		});

	const observeRejectedReplicationMutation = (
		log: any,
		options?: { allowLifecycleClear?: boolean },
	) => {
		const emittedEvents: { type: string; hash: string }[] = [];
		const eventTypes = [
			"replication:change",
			"replicator:join",
			"replicator:mature",
			"replicator:leave",
		];
		const listeners = eventTypes.map((type) => {
			const listener = (event: any) => {
				emittedEvents.push({
					type,
					hash: event.detail.publicKey.hashcode(),
				});
			};
			log.events.addEventListener(type, listener);
			return { type, listener };
		});
		const debouncedChanges = sinon.spy(log.replicationChangeDebounceFn, "add");
		const membershipAdd = sinon.spy(log.uniqueReplicators, "add");
		const membershipDelete = sinon.spy(log.uniqueReplicators, "delete");
		const emittedMembershipAdd = sinon.spy(log._replicatorJoinEmitted, "add");
		const emittedMembershipDelete = sinon.spy(
			log._replicatorJoinEmitted,
			"delete",
		);
		const membershipBefore = [...log.uniqueReplicators].sort();
		const emittedMembershipBefore = [...log._replicatorJoinEmitted].sort();
		return {
			assertNoEffects: () => {
				expect(emittedEvents).to.deep.equal([]);
				expect(debouncedChanges.called).to.be.false;
				expect(membershipAdd.called).to.be.false;
				expect(membershipDelete.called).to.be.false;
				expect(emittedMembershipAdd.called).to.be.false;
				expect(emittedMembershipDelete.called).to.be.false;
				if (!options?.allowLifecycleClear) {
					expect([...log.uniqueReplicators].sort()).to.deep.equal(
						membershipBefore,
					);
					expect([...log._replicatorJoinEmitted].sort()).to.deep.equal(
						emittedMembershipBefore,
					);
				}
			},
			restore: () => {
				for (const { type, listener } of listeners) {
					log.events.removeEventListener(type, listener);
				}
				debouncedChanges.restore();
				membershipAdd.restore();
				membershipDelete.restore();
				emittedMembershipAdd.restore();
				emittedMembershipDelete.restore();
			},
		};
	};

	it("announces the authoritative empty snapshot after a reset put commits then throws", async () => {
		const { log, replicationIndex } = await openDisconnectedLog(1);
		const selfKey = session.peers[0].identity.publicKey;
		const selfHash = selfKey.hashcode();
		const numbers = log.indexableDomain.numbers;
		const makeRange = (offset: number, id: number) =>
			new log.indexableDomain.constructorRange({
				id: new Uint8Array([id]),
				offset: numbers.denormalize(offset),
				width: numbers.denormalize(0.2),
				publicKeyHash: selfHash,
				timestamp: BigInt(id),
			});
		const previous = makeRange(0.1, 1);
		const replacement = makeRange(0.6, 2);
		await log.addReplicationRange([previous], selfKey, {
			checkDuplicates: false,
			rebalance: false,
		});

		const durableFailure = new Error("forced reset put commit-then-throw");
		const originalPut = replicationIndex.put.bind(replicationIndex);
		const put = sinon.stub(replicationIndex, "put").callsFake((async (
			value: any,
			options?: any,
		) => {
			const result = await originalPut(value, options);
			if (value === replacement) {
				throw durableFailure;
			}
			return result;
		}) as any);
		const sent: unknown[] = [];
		const send = sinon.stub(log.rpc, "send").callsFake(async (message: any) => {
			sent.push(message);
			return [] as any;
		});

		try {
			await expect(
				log.startAnnounceReplicating([replacement], {
					reset: true,
					checkDuplicates: false,
					rebalance: false,
				}),
			).to.be.rejectedWith(durableFailure.message);

			expect(
				await replicationIndex.count({ query: { hash: selfHash } }),
			).to.equal(0);
			const snapshots = sent.filter(
				(message) => message instanceof AllReplicatingSegmentsMessage,
			) as AllReplicatingSegmentsMessage[];
			expect(snapshots).to.have.length(1);
			expect(snapshots[0].segments).to.deep.equal([]);
			expect(log._replicationRangeMutationFailure).to.be.undefined;
		} finally {
			put.restore();
			send.restore();
		}
	});

	it("emits leave before rejoin when a failed reset rollback confirms zero rows", async () => {
		const { log, replicationIndex } = await openDisconnectedLog(2);
		const ownerKey = session.peers[1].identity.publicKey;
		const ownerHash = ownerKey.hashcode();
		const lifecycle: string[] = [];
		log.events.addEventListener("replicator:join", () => {
			lifecycle.push("join");
		});
		log.events.addEventListener("replicator:leave", () => {
			lifecycle.push("leave");
		});
		const initial = makeReplicationRange(log, {
			id: randomBytes(32),
			ownerHash,
			offset: 0.1,
			timestamp: 1n,
		});
		await log.addReplicationRange([initial], ownerKey, {
			checkDuplicates: false,
			rebalance: false,
		});
		expect(lifecycle).to.deep.equal(["join"]);

		const replacement = makeReplicationRange(log, {
			id: randomBytes(32),
			ownerHash,
			offset: 0.5,
			timestamp: 2n,
		});
		const putFailure = new Error("forced reset replacement put failure");
		const put = sinon.stub(replicationIndex, "put").rejects(putFailure);
		try {
			await expect(
				log.addReplicationRange([replacement], ownerKey, {
					reset: true,
					checkDuplicates: false,
					rebalance: false,
				}),
			).to.be.rejectedWith(putFailure.message);
		} finally {
			put.restore();
		}

		expect(
			await replicationIndex.count({ query: { hash: ownerHash } }),
		).to.equal(0);
		expect(log.uniqueReplicators.has(ownerHash)).to.be.false;
		expect(log._replicatorJoinEmitted.has(ownerHash)).to.be.false;
		expect(lifecycle).to.deep.equal(["join", "leave"]);

		const rejoined = makeReplicationRange(log, {
			id: randomBytes(32),
			ownerHash,
			offset: 0.8,
			timestamp: 3n,
		});
		await log.addReplicationRange([rejoined], ownerKey, {
			checkDuplicates: false,
			rebalance: false,
		});
		expect(lifecycle).to.deep.equal(["join", "leave", "join"]);
	});

	it("emits leave only when an ambiguous reset deletion is confirmed empty", async () => {
		const { log, replicationIndex } = await openDisconnectedLog(3);
		const removedOwner = session.peers[1].identity.publicKey;
		const retainedOwner = session.peers[2].identity.publicKey;
		const makeOwnerRange = (
			ownerKey: PublicSignKey,
			offset: number,
			timestamp: bigint,
		) =>
			makeReplicationRange(log, {
				id: randomBytes(32),
				ownerHash: ownerKey.hashcode(),
				offset,
				timestamp,
			});
		await log.addReplicationRange(
			[makeOwnerRange(removedOwner, 0.1, 1n)],
			removedOwner,
			{ checkDuplicates: false, rebalance: false },
		);
		await log.addReplicationRange(
			[makeOwnerRange(retainedOwner, 0.4, 1n)],
			retainedOwner,
			{ checkDuplicates: false, rebalance: false },
		);
		const leaves: string[] = [];
		log.events.addEventListener("replicator:leave", (event: any) => {
			leaves.push(event.detail.publicKey.hashcode());
		});
		const originalDel = replicationIndex.del.bind(replicationIndex);
		const committedDeleteFailure = new Error(
			"forced committed reset deletion failure",
		);
		const committedDelete = sinon
			.stub(replicationIndex, "del")
			.callsFake((async (...args: any[]) => {
				await originalDel(...args);
				throw committedDeleteFailure;
			}) as any);
		try {
			await expect(
				log.addReplicationRange(
					[makeOwnerRange(removedOwner, 0.7, 2n)],
					removedOwner,
					{
						reset: true,
						checkDuplicates: false,
						rebalance: false,
					},
				),
			).to.be.rejectedWith(committedDeleteFailure.message);
		} finally {
			committedDelete.restore();
		}

		const retainedDeleteFailure = new Error(
			"forced retained reset deletion failure",
		);
		const retainedDelete = sinon
			.stub(replicationIndex, "del")
			.rejects(retainedDeleteFailure);
		try {
			await expect(
				log.addReplicationRange(
					[makeOwnerRange(retainedOwner, 0.8, 2n)],
					retainedOwner,
					{
						reset: true,
						checkDuplicates: false,
						rebalance: false,
					},
				),
			).to.be.rejectedWith(retainedDeleteFailure.message);
		} finally {
			retainedDelete.restore();
		}

		expect(
			await replicationIndex.count({
				query: { hash: removedOwner.hashcode() },
			}),
		).to.equal(0);
		expect(
			await replicationIndex.count({
				query: { hash: retainedOwner.hashcode() },
			}),
		).to.equal(1);
		expect(log.uniqueReplicators.has(removedOwner.hashcode())).to.be.false;
		expect(log.uniqueReplicators.has(retainedOwner.hashcode())).to.be.true;
		expect(leaves).to.deep.equal([removedOwner.hashcode()]);
	});

	it("rejects a non-reset range id collision without replacing the existing owner", async () => {
		const { log, replicationIndex } = await openDisconnectedLog(3);
		const victimKey = session.peers[1].identity.publicKey;
		const attackerKey = session.peers[2].identity.publicKey;
		const sharedId = randomBytes(32);
		const victimRange = makeReplicationRange(log, {
			id: sharedId,
			ownerHash: victimKey.hashcode(),
			offset: 0.1,
		});
		await log.addReplicationRange([victimRange], victimKey, {
			checkDuplicates: false,
			rebalance: false,
		});
		const effects = observeRejectedReplicationMutation(log);
		const collidingRange = makeReplicationRange(log, {
			id: sharedId,
			ownerHash: attackerKey.hashcode(),
			offset: 0.6,
			timestamp: 2n,
		});

		try {
			await expect(
				log.addReplicationRange([collidingRange], attackerKey, {
					checkDuplicates: false,
					rebalance: false,
				}),
			).to.be.rejectedWith(
				"Replication range id is already owned by another replicator",
			);

			const durable = await replicationIndex.iterate().all();
			expect(durable).to.have.length(1);
			expect(durable[0].value.rangeHash).to.equal(victimRange.rangeHash);
			expect(durable[0].value.hash).to.equal(victimKey.hashcode());
			expect(log.uniqueReplicators.has(victimKey.hashcode())).to.be.true;
			expect(log.uniqueReplicators.has(attackerKey.hashcode())).to.be.false;
			effects.assertNoEffects();
		} finally {
			effects.restore();
		}
	});

	it("rejects a reset range id collision before deleting the sender's prior ranges", async () => {
		const { log, replicationIndex } = await openDisconnectedLog(3);
		const victimKey = session.peers[1].identity.publicKey;
		const attackerKey = session.peers[2].identity.publicKey;
		const victimRange = makeReplicationRange(log, {
			id: randomBytes(32),
			ownerHash: victimKey.hashcode(),
			offset: 0.1,
		});
		const attackerPreviousRange = makeReplicationRange(log, {
			id: randomBytes(32),
			ownerHash: attackerKey.hashcode(),
			offset: 0.4,
		});
		await log.addReplicationRange([victimRange], victimKey, {
			checkDuplicates: false,
			rebalance: false,
		});
		await log.addReplicationRange([attackerPreviousRange], attackerKey, {
			checkDuplicates: false,
			rebalance: false,
		});
		const effects = observeRejectedReplicationMutation(log);
		const collidingRange = makeReplicationRange(log, {
			id: victimRange.id,
			ownerHash: attackerKey.hashcode(),
			offset: 0.7,
			timestamp: 2n,
		});

		try {
			await expect(
				log.addReplicationRange([collidingRange], attackerKey, {
					reset: true,
					checkDuplicates: false,
					rebalance: false,
				}),
			).to.be.rejectedWith(
				"Replication range id is already owned by another replicator",
			);

			const durable = (await replicationIndex.iterate().all()).map(
				(result: any) => result.value,
			);
			expect(durable.map((range: any) => range.rangeHash)).to.have.members([
				victimRange.rangeHash,
				attackerPreviousRange.rangeHash,
			]);
			expect(durable).to.have.length(2);
			expect(log.uniqueReplicators.has(victimKey.hashcode())).to.be.true;
			expect(log.uniqueReplicators.has(attackerKey.hashcode())).to.be.true;
			effects.assertNoEffects();
		} finally {
			effects.restore();
		}
	});

	it("rejects duplicate range ids before a reset can write or delete", async () => {
		const { log, replicationIndex } = await openDisconnectedLog(2);
		const ownerKey = session.peers[1].identity.publicKey;
		const previousRange = makeReplicationRange(log, {
			id: randomBytes(32),
			ownerHash: ownerKey.hashcode(),
			offset: 0.1,
		});
		await log.addReplicationRange([previousRange], ownerKey, {
			checkDuplicates: false,
			rebalance: false,
		});
		const effects = observeRejectedReplicationMutation(log);
		const duplicateId = randomBytes(32);
		const duplicateRanges = [0.4, 0.7].map((offset, index) =>
			makeReplicationRange(log, {
				id: duplicateId,
				ownerHash: ownerKey.hashcode(),
				offset,
				timestamp: BigInt(index + 2),
			}),
		);

		try {
			await expect(
				log.addReplicationRange(duplicateRanges, ownerKey, {
					reset: true,
					checkDuplicates: false,
					rebalance: false,
				}),
			).to.be.rejectedWith("Duplicate replication range id in announcement");

			const durable = await replicationIndex.iterate().all();
			expect(durable).to.have.length(1);
			expect(durable[0].value.rangeHash).to.equal(previousRange.rangeHash);
			effects.assertNoEffects();
		} finally {
			effects.restore();
		}
	});

	it("accepts only an ordered legacy replacement pair and applies its final range", async () => {
		const { db, log, replicationIndex } = await openDisconnectedLog(2);
		const ownerKey = session.peers[1].identity.publicKey;
		const ownerHash = ownerKey.hashcode();
		const id = randomBytes(32);
		const previous = makeReplicationRange(log, {
			id,
			ownerHash,
			offset: 0.1,
			timestamp: 1n,
		});
		const current = makeReplicationRange(log, {
			id,
			ownerHash,
			offset: 0.5,
			timestamp: 2n,
		});
		const unexpectedThird = makeReplicationRange(log, {
			id,
			ownerHash,
			offset: 0.8,
			timestamp: 3n,
		});
		const changes: string[] = [];
		log.events.addEventListener("replication:change", (event: any) => {
			changes.push(event.detail.publicKey.hashcode());
		});
		const receive = (segments: any[], timestamp: bigint) =>
			db.log.onMessage(new AddedReplicationSegmentMessage({ segments }), {
				from: ownerKey,
				message: { header: { timestamp } },
			} as any);
		const pair = [previous.toReplicationRange(), current.toReplicationRange()];

		await receive(pair, 10n);
		let durable = (
			await replicationIndex.iterate({ query: { hash: ownerHash } }).all()
		).map((result: any) => result.value);
		expect(durable).to.have.length(1);
		expect(durable[0].rangeHash).to.equal(current.rangeHash);
		expect(changes).to.deep.equal([ownerHash]);

		await receive(pair, 11n);
		durable = (
			await replicationIndex.iterate({ query: { hash: ownerHash } }).all()
		).map((result: any) => result.value);
		expect(durable).to.have.length(1);
		expect(durable[0].rangeHash).to.equal(current.rangeHash);
		expect(changes).to.deep.equal([ownerHash]);

		await receive([...pair, unexpectedThird.toReplicationRange()], 12n);
		durable = (
			await replicationIndex.iterate({ query: { hash: ownerHash } }).all()
		).map((result: any) => result.value);
		expect(durable).to.have.length(1);
		expect(durable[0].rangeHash).to.equal(current.rangeHash);
		expect(changes).to.deep.equal([ownerHash]);
	});

	it("rejects a forged range owner before a reset can write or delete", async () => {
		const { log, replicationIndex } = await openDisconnectedLog(3);
		const senderKey = session.peers[1].identity.publicKey;
		const forgedOwnerHash = session.peers[2].identity.publicKey.hashcode();
		const previousRange = makeReplicationRange(log, {
			id: randomBytes(32),
			ownerHash: senderKey.hashcode(),
			offset: 0.1,
		});
		await log.addReplicationRange([previousRange], senderKey, {
			checkDuplicates: false,
			rebalance: false,
		});
		const effects = observeRejectedReplicationMutation(log);
		const forgedRange = makeReplicationRange(log, {
			id: randomBytes(32),
			ownerHash: forgedOwnerHash,
			offset: 0.6,
			timestamp: 2n,
		});

		try {
			await expect(
				log.addReplicationRange([forgedRange], senderKey, {
					reset: true,
					checkDuplicates: false,
					rebalance: false,
				}),
			).to.be.rejectedWith("Replication range owner mismatch");

			const durable = await replicationIndex.iterate().all();
			expect(durable).to.have.length(1);
			expect(durable[0].value.rangeHash).to.equal(previousRange.rangeHash);
			expect(log.uniqueReplicators.has(senderKey.hashcode())).to.be.true;
			expect(log.uniqueReplicators.has(forgedOwnerHash)).to.be.false;
			effects.assertNoEffects();
		} finally {
			effects.restore();
		}
	});

	it("applies mode-only replacements but keeps timestamp-only announcements as no-ops", async () => {
		const { log, replicationIndex } = await openDisconnectedLog(2);
		const ownerKey = session.peers[1].identity.publicKey;
		const ownerHash = ownerKey.hashcode();
		const id = randomBytes(32);
		const makeModeRange = (mode: ReplicationIntent, timestamp: bigint) =>
			makeReplicationRange(log, {
				id,
				ownerHash,
				offset: 0.25,
				mode,
				timestamp,
			});
		const initial = makeModeRange(ReplicationIntent.NonStrict, 1n);
		await log.addReplicationRange([initial], ownerKey, {
			checkDuplicates: false,
			rebalance: false,
		});
		const replicationChanges: string[] = [];
		log.events.addEventListener("replication:change", (event: any) => {
			replicationChanges.push(event.detail.publicKey.hashcode());
		});

		const strict = makeModeRange(ReplicationIntent.Strict, 2n);
		const nonResetDiffs = await log.addReplicationRange([strict], ownerKey, {
			checkDuplicates: false,
			rebalance: false,
		});
		expect(nonResetDiffs.map((diff: any) => diff.type)).to.deep.equal([
			"replaced",
			"added",
		]);
		let durable = (await replicationIndex.iterate().all())[0].value;
		expect(durable.mode).to.equal(ReplicationIntent.Strict);
		expect(durable.timestamp).to.equal(2n);

		const nonStrict = makeModeRange(ReplicationIntent.NonStrict, 3n);
		const resetDiffs = await log.addReplicationRange([nonStrict], ownerKey, {
			reset: true,
			checkDuplicates: false,
			rebalance: false,
		});
		expect(resetDiffs.map((diff: any) => diff.type)).to.deep.equal([
			"removed",
			"added",
		]);
		durable = (await replicationIndex.iterate().all())[0].value;
		expect(durable.mode).to.equal(ReplicationIntent.NonStrict);
		expect(durable.timestamp).to.equal(3n);

		const timestampOnlyNonReset = makeModeRange(
			ReplicationIntent.NonStrict,
			4n,
		);
		expect(
			await log.addReplicationRange([timestampOnlyNonReset], ownerKey, {
				checkDuplicates: false,
				rebalance: false,
			}),
		).to.deep.equal([]);
		const timestampOnlyReset = makeModeRange(ReplicationIntent.NonStrict, 5n);
		expect(
			await log.addReplicationRange([timestampOnlyReset], ownerKey, {
				reset: true,
				checkDuplicates: false,
				rebalance: false,
			}),
		).to.deep.equal([]);
		durable = (await replicationIndex.iterate().all())[0].value;
		expect(durable.mode).to.equal(ReplicationIntent.NonStrict);
		expect(durable.timestamp).to.equal(3n);
		expect(replicationChanges).to.deep.equal([ownerHash, ownerHash]);
	});

	it("announces only the durable range after a non-reset replacement", async () => {
		const { log } = await openDisconnectedLog(1);
		const selfHash = session.peers[0].identity.publicKey.hashcode();
		const id = randomBytes(32);
		const initial = makeReplicationRange(log, {
			id,
			ownerHash: selfHash,
			offset: 0.1,
			timestamp: 1n,
		});
		const replacement = makeReplicationRange(log, {
			id,
			ownerHash: selfHash,
			offset: 0.6,
			timestamp: 2n,
		});
		const announced: unknown[] = [];
		const announce = (message: unknown) => {
			announced.push(message);
		};

		await log.startAnnounceReplicating([initial], {
			checkDuplicates: false,
			rebalance: false,
			announce,
		});
		announced.length = 0;

		await log.startAnnounceReplicating([replacement], {
			checkDuplicates: false,
			rebalance: false,
			announce,
		});

		expect(announced).to.have.length(1);
		expect(announced[0]).to.be.instanceOf(AddedReplicationSegmentMessage);
		const segments = (announced[0] as AddedReplicationSegmentMessage).segments;
		expect(segments).to.have.length(1);
		expect(segments[0].id).to.deep.equal(id);
		expect(segments[0].offset).to.equal(replacement.start1);
		expect(segments[0].factor).to.equal(replacement.width);
		expect(segments[0].timestamp).to.equal(replacement.timestamp);
	});

	it("recomputes the oldest timestamp once per durable reset phase before announcing", async () => {
		const { log, replicationIndex } = await openDisconnectedLog(1);
		const selfKey = session.peers[0].identity.publicKey;
		const selfHash = selfKey.hashcode();
		const previous = makeReplicationRange(log, {
			id: randomBytes(32),
			ownerHash: selfHash,
			offset: 0.1,
			timestamp: 1n,
		});
		const replacement = makeReplicationRange(log, {
			id: randomBytes(32),
			ownerHash: selfHash,
			offset: 0.6,
			timestamp: 2n,
		});
		await log.addReplicationRange([previous], selfKey, {
			checkDuplicates: false,
			rebalance: false,
		});
		const recomputeOldest = sinon.spy(log, "updateOldestTimestampFromIndex");
		const announced: unknown[] = [];

		try {
			await log.startAnnounceReplicating([replacement], {
				reset: true,
				checkDuplicates: false,
				rebalance: false,
				announce: (message: unknown) => announced.push(message),
			});

			expect(recomputeOldest.callCount).to.equal(2);
			expect(announced).to.have.length(1);
			expect(announced[0]).to.be.instanceOf(AllReplicatingSegmentsMessage);
			const snapshot = announced[0] as AllReplicatingSegmentsMessage;
			expect(snapshot.segments).to.have.length(1);
			expect(snapshot.segments[0].id).to.deep.equal(replacement.id);
			const durable = await replicationIndex
				.iterate({ query: { hash: selfHash } })
				.all();
			expect(durable).to.have.length(1);
			expect(durable[0].value.rangeHash).to.equal(replacement.rangeHash);
		} finally {
			recomputeOldest.restore();
		}
	});

	it("announces the authoritative snapshot when a merge removal precedes a failed replacement", async () => {
		const { log, replicationIndex } = await openDisconnectedLog(1);
		const selfKey = session.peers[0].identity.publicKey;
		const selfHash = selfKey.hashcode();
		const first = makeReplicationRange(log, {
			id: randomBytes(32),
			ownerHash: selfHash,
			offset: 0.1,
			timestamp: 1n,
		});
		const removed = makeReplicationRange(log, {
			id: randomBytes(32),
			ownerHash: selfHash,
			offset: 0.6,
			timestamp: 2n,
		});
		await log.addReplicationRange([first, removed], selfKey, {
			checkDuplicates: false,
			rebalance: false,
		});
		log.domain.canMerge = () => true;

		const durableFailure = new Error("forced merged replacement put failure");
		const originalPut = replicationIndex.put.bind(replicationIndex);
		let failedReplacement = false;
		const put = sinon.stub(replicationIndex, "put").callsFake((async (
			value: any,
			options?: any,
		) => {
			const result = await originalPut(value, options);
			if (!failedReplacement) {
				failedReplacement = true;
				throw durableFailure;
			}
			return result;
		}) as any);
		const sent: unknown[] = [];
		const send = sinon.stub(log.rpc, "send").callsFake(async (message: any) => {
			sent.push(message);
			return [] as any;
		});
		const rebalanceAdd = sinon.spy(log.replicationChangeDebounceFn, "add");

		try {
			await expect(
				log._replicate(
					{ offset: 0.3, factor: 0.2 },
					{ mergeSegments: true, rebalance: true },
				),
			).to.be.rejectedWith(durableFailure.message);

			expect(failedReplacement).to.be.true;
			const durable = (
				await replicationIndex.iterate({ query: { hash: selfHash } }).all()
			).map((result: any) => result.value);
			expect(durable).to.have.length(1);
			expect(durable[0].rangeHash).to.equal(first.rangeHash);
			expect(durable.some((range: any) => range.idString === removed.idString))
				.to.be.false;
			expect(
				(await log.getMyReplicationSegments()).map(
					(range: any) => range.rangeHash,
				),
			).to.deep.equal([first.rangeHash]);

			const snapshots = sent.filter(
				(message) => message instanceof AllReplicatingSegmentsMessage,
			) as AllReplicatingSegmentsMessage[];
			expect(snapshots).to.have.length(1);
			expect(snapshots[0].segments).to.have.length(1);
			expect(snapshots[0].segments[0].id).to.deep.equal(first.id);
			expect(sent.some((message) => message instanceof StoppedReplicating)).to
				.be.false;
			const removedDiffs = rebalanceAdd
				.getCalls()
				.map((call) => call.args[0])
				.filter((diff: any) => diff.type === "removed");
			expect(removedDiffs).to.have.length(1);
			expect(removedDiffs[0].range.rangeHash).to.equal(removed.rangeHash);
			expect(log._replicationRangeMutationFailure).to.be.undefined;
		} finally {
			rebalanceAdd.restore();
			put.restore();
			send.restore();
		}
	});

	it("queues a preliminary removal once when its delete commits then throws", async () => {
		const { log, replicationIndex } = await openDisconnectedLog(1);
		const selfKey = session.peers[0].identity.publicKey;
		const selfHash = selfKey.hashcode();
		const first = makeReplicationRange(log, {
			id: randomBytes(32),
			ownerHash: selfHash,
			offset: 0.1,
			timestamp: 1n,
		});
		const removed = makeReplicationRange(log, {
			id: randomBytes(32),
			ownerHash: selfHash,
			offset: 0.6,
			timestamp: 2n,
		});
		await log.addReplicationRange([first, removed], selfKey, {
			checkDuplicates: false,
			rebalance: false,
		});
		log.domain.canMerge = () => true;

		const deletionFailure = new Error(
			"forced committed preliminary removal failure",
		);
		const originalDel = replicationIndex.del.bind(replicationIndex);
		const del = sinon.stub(replicationIndex, "del").callsFake((async (
			...args: any[]
		) => {
			await originalDel(...args);
			throw deletionFailure;
		}) as any);
		const rebalanceAdd = sinon.spy(log.replicationChangeDebounceFn, "add");
		const sent: unknown[] = [];
		const send = sinon.stub(log.rpc, "send").callsFake(async (message: any) => {
			sent.push(message);
			return [] as any;
		});

		try {
			await expect(
				log._replicate(
					{ offset: 0.3, factor: 0.2 },
					{ mergeSegments: true, rebalance: true },
				),
			).to.be.rejectedWith(deletionFailure.message);

			const removedDiffs = rebalanceAdd
				.getCalls()
				.map((call) => call.args[0])
				.filter((diff: any) => diff.type === "removed");
			expect(removedDiffs).to.have.length(1);
			expect(removedDiffs[0].range.rangeHash).to.equal(removed.rangeHash);
			const snapshots = sent.filter(
				(message) => message instanceof AllReplicatingSegmentsMessage,
			) as AllReplicatingSegmentsMessage[];
			expect(snapshots).to.have.length(1);
			expect(snapshots[0].segments).to.have.length(1);
			expect(snapshots[0].segments[0].id).to.deep.equal(first.id);
		} finally {
			send.restore();
			rebalanceAdd.restore();
			del.restore();
		}
	});

	it("announces the current snapshot when post-write maturity bookkeeping fails", async () => {
		const { log, replicationIndex } = await openDisconnectedLog(1);
		const selfHash = session.peers[0].identity.publicKey.hashcode();
		const range = makeReplicationRange(log, {
			id: randomBytes(32),
			ownerHash: selfHash,
			offset: 0.35,
			timestamp: BigInt(Date.now()),
		});
		const minRoleAge = sinon.stub(log, "getDefaultMinRoleAge").resolves(60_000);
		const bookkeepingFailure = new Error(
			"forced post-write maturity bookkeeping failure",
		);
		const scheduleMaturity = sinon
			.stub(log, "schedulePendingMaturity")
			.throws(bookkeepingFailure);
		const sent: unknown[] = [];
		const send = sinon.stub(log.rpc, "send").callsFake(async (message: any) => {
			sent.push(message);
			return [] as any;
		});

		try {
			await expect(
				log.startAnnounceReplicating([range], {
					checkDuplicates: false,
					rebalance: false,
				}),
			).to.be.rejectedWith(bookkeepingFailure.message);

			expect(scheduleMaturity.calledOnce).to.be.true;
			const durable = await replicationIndex
				.iterate({ query: { hash: selfHash } })
				.all();
			expect(durable).to.have.length(1);
			expect(durable[0].value.rangeHash).to.equal(range.rangeHash);
			const snapshots = sent.filter(
				(message) => message instanceof AllReplicatingSegmentsMessage,
			) as AllReplicatingSegmentsMessage[];
			expect(snapshots).to.have.length(1);
			expect(snapshots[0].segments).to.have.length(1);
			expect(snapshots[0].segments[0].id).to.deep.equal(range.id);
			expect(
				sent.some(
					(message) => message instanceof AddedReplicationSegmentMessage,
				),
			).to.be.false;
		} finally {
			send.restore();
			scheduleMaturity.restore();
			minRoleAge.restore();
		}
	});

	it("poisons ownership instead of announcing an invalid persisted snapshot", async () => {
		const { log, replicationIndex } = await openDisconnectedLog(2);
		const selfKey = session.peers[0].identity.publicKey;
		const selfHash = selfKey.hashcode();
		const invalid = makeReplicationRange(log, {
			id: randomBytes(32),
			ownerHash: selfHash,
			offset: 0.2,
			mode: 255 as ReplicationIntent,
		});
		await replicationIndex.put(invalid);
		const send = sinon.spy(log.rpc, "send");
		const remoteKey = session.peers[1].identity.publicKey;
		const validMutation = makeReplicationRange(log, {
			id: randomBytes(32),
			ownerHash: selfHash,
			offset: 0.7,
		});

		try {
			await expect(
				log._onSubscription({
					detail: { from: remoteKey, topics: [log.topic] },
				} as any),
			).to.be.rejectedWith(
				"Persisted replication ownership is invalid and cannot be announced",
			);
			expect(send.called).to.be.false;
			expect(log._replicationRangeMutationFailure).to.be.instanceOf(Error);
			await expect(
				log._findLeaders([log.indexableDomain.numbers.denormalize(0.5)]),
			).to.be.rejectedWith("Replication ownership recovery is required");
			await expect(
				log.addReplicationRange([validMutation], selfKey, {
					checkDuplicates: false,
					rebalance: false,
				}),
			).to.be.rejectedWith("Replication ownership recovery is required");
		} finally {
			send.restore();
			log._replicationRangeMutationFailure = undefined;
		}
	});

	it("fences a reset after min-role-age planning crosses close", async () => {
		const { db, log, replicationIndex } = await openDisconnectedLog(2);
		const ownerKey = session.peers[1].identity.publicKey;
		const ownerHash = ownerKey.hashcode();
		const id = randomBytes(32);
		const previous = makeReplicationRange(log, {
			id,
			ownerHash,
			offset: 0.1,
		});
		const replacement = makeReplicationRange(log, {
			id,
			ownerHash,
			offset: 0.6,
			timestamp: 2n,
		});
		await log.addReplicationRange([previous], ownerKey, {
			checkDuplicates: false,
			rebalance: false,
		});

		const readStarted = pDefer<void>();
		const releaseRead = pDefer<void>();
		const minRoleAge = sinon
			.stub(log, "getDefaultMinRoleAge")
			.callsFake(async () => {
				readStarted.resolve();
				await releaseRead.promise;
				return 0;
			});
		const put = sinon.spy(replicationIndex, "put");
		const del = sinon.spy(replicationIndex, "del");
		const effects = observeRejectedReplicationMutation(log, {
			allowLifecycleClear: true,
		});

		try {
			const adding = log.addReplicationRange([replacement], ownerKey, {
				reset: true,
				checkDuplicates: false,
				rebalance: false,
			});
			await readStarted.promise;
			const rejected = expect(adding).to.be.rejectedWith(
				"Replication ownership lifecycle is no longer active",
			);
			const closing = db.close();
			releaseRead.resolve();
			await Promise.all([rejected, closing]);

			expect(put.called).to.be.false;
			expect(del.called).to.be.false;
			effects.assertNoEffects();
		} finally {
			releaseRead.resolve();
			minRoleAge.restore();
			put.restore();
			del.restore();
			effects.restore();
		}

		await session.peers[0].open(db, {
			args: { replicate: false, timeUntilRoleMaturity: 0 },
		});
		const durable = await replicationIndex.iterate().all();
		expect(durable).to.have.length(1);
		expect(durable[0].value.rangeHash).to.equal(previous.rangeHash);
	});

	it("fences a reset after its owner inventory read crosses close", async () => {
		const { db, log, replicationIndex } = await openDisconnectedLog(2);
		const ownerKey = session.peers[1].identity.publicKey;
		const ownerHash = ownerKey.hashcode();
		const id = randomBytes(32);
		const previous = makeReplicationRange(log, {
			id,
			ownerHash,
			offset: 0.1,
		});
		const replacement = makeReplicationRange(log, {
			id,
			ownerHash,
			offset: 0.6,
			timestamp: 2n,
		});
		await log.addReplicationRange([previous], ownerKey, {
			checkDuplicates: false,
			rebalance: false,
		});

		const readStarted = pDefer<void>();
		const releaseRead = pDefer<void>();
		const originalIterate = replicationIndex.iterate.bind(replicationIndex);
		let blockInventory = true;
		const iterate = sinon.stub(replicationIndex, "iterate").callsFake(((
			request?: any,
			options?: any,
		) => {
			const iterator = originalIterate(request, options);
			if (blockInventory && request?.query?.hash === ownerHash) {
				blockInventory = false;
				const originalAll = iterator.all.bind(iterator);
				iterator.all = async () => {
					readStarted.resolve();
					await releaseRead.promise;
					return originalAll();
				};
			}
			return iterator;
		}) as any);
		const put = sinon.spy(replicationIndex, "put");
		const del = sinon.spy(replicationIndex, "del");
		const effects = observeRejectedReplicationMutation(log, {
			allowLifecycleClear: true,
		});

		try {
			const adding = log.addReplicationRange([replacement], ownerKey, {
				reset: true,
				checkDuplicates: false,
				rebalance: false,
			});
			await readStarted.promise;
			const rejected = expect(adding).to.be.rejectedWith(
				"Replication ownership lifecycle is no longer active",
			);
			const closing = db.close();
			releaseRead.resolve();
			await Promise.all([rejected, closing]);

			expect(put.called).to.be.false;
			expect(del.called).to.be.false;
			effects.assertNoEffects();
		} finally {
			releaseRead.resolve();
			iterate.restore();
			put.restore();
			del.restore();
			effects.restore();
		}

		await session.peers[0].open(db, {
			args: { replicate: false, timeUntilRoleMaturity: 0 },
		});
		const durable = await replicationIndex.iterate().all();
		expect(durable).to.have.length(1);
		expect(durable[0].value.rangeHash).to.equal(previous.rangeHash);
	});

	it("fences a non-reset mutation after its id lookup crosses close", async () => {
		const { db, log, replicationIndex } = await openDisconnectedLog(2);
		const ownerKey = session.peers[1].identity.publicKey;
		const ownerHash = ownerKey.hashcode();
		const id = randomBytes(32);
		const previous = makeReplicationRange(log, {
			id,
			ownerHash,
			offset: 0.1,
		});
		const replacement = makeReplicationRange(log, {
			id,
			ownerHash,
			offset: 0.6,
			timestamp: 2n,
		});
		await log.addReplicationRange([previous], ownerKey, {
			checkDuplicates: false,
			rebalance: false,
		});

		const readStarted = pDefer<void>();
		const releaseRead = pDefer<void>();
		const originalIterate = replicationIndex.iterate.bind(replicationIndex);
		let blockLookup = true;
		const iterate = sinon.stub(replicationIndex, "iterate").callsFake(((
			request?: any,
			options?: any,
		) => {
			const iterator = originalIterate(request, options);
			if (blockLookup && Array.isArray(request?.query)) {
				blockLookup = false;
				const originalAll = iterator.all.bind(iterator);
				iterator.all = async () => {
					readStarted.resolve();
					await releaseRead.promise;
					return originalAll();
				};
			}
			return iterator;
		}) as any);
		const put = sinon.spy(replicationIndex, "put");
		const del = sinon.spy(replicationIndex, "del");
		const effects = observeRejectedReplicationMutation(log, {
			allowLifecycleClear: true,
		});

		try {
			const adding = log.addReplicationRange([replacement], ownerKey, {
				checkDuplicates: false,
				rebalance: false,
			});
			await readStarted.promise;
			const rejected = expect(adding).to.be.rejectedWith(
				"Replication ownership lifecycle is no longer active",
			);
			const closing = db.close();
			releaseRead.resolve();
			await Promise.all([rejected, closing]);

			expect(put.called).to.be.false;
			expect(del.called).to.be.false;
			effects.assertNoEffects();
		} finally {
			releaseRead.resolve();
			iterate.restore();
			put.restore();
			del.restore();
			effects.restore();
		}

		await session.peers[0].open(db, {
			args: { replicate: false, timeUntilRoleMaturity: 0 },
		});
		const durable = await replicationIndex.iterate().all();
		expect(durable).to.have.length(1);
		expect(durable[0].value.rangeHash).to.equal(previous.rangeHash);
	});

	it("fences a non-reset mutation after its owner count crosses close", async () => {
		const { db, log, replicationIndex } = await openDisconnectedLog(2);
		const ownerKey = session.peers[1].identity.publicKey;
		const incoming = makeReplicationRange(log, {
			id: randomBytes(32),
			ownerHash: ownerKey.hashcode(),
			offset: 0.4,
		});
		const readStarted = pDefer<void>();
		const releaseRead = pDefer<void>();
		const originalCount = replicationIndex.count.bind(replicationIndex);
		let blockOwnerCount = true;
		const count = sinon.stub(replicationIndex, "count").callsFake((async (
			request?: any,
			options?: any,
		) => {
			if (blockOwnerCount) {
				blockOwnerCount = false;
				readStarted.resolve();
				await releaseRead.promise;
			}
			return originalCount(request, options);
		}) as any);
		const put = sinon.spy(replicationIndex, "put");
		const del = sinon.spy(replicationIndex, "del");
		const effects = observeRejectedReplicationMutation(log, {
			allowLifecycleClear: true,
		});

		try {
			const adding = log.addReplicationRange([incoming], ownerKey, {
				checkDuplicates: false,
				rebalance: false,
			});
			await readStarted.promise;
			const rejected = expect(adding).to.be.rejectedWith(
				"Replication ownership lifecycle is no longer active",
			);
			const closing = db.close();
			releaseRead.resolve();
			await Promise.all([rejected, closing]);

			expect(put.called).to.be.false;
			expect(del.called).to.be.false;
			effects.assertNoEffects();
		} finally {
			releaseRead.resolve();
			count.restore();
			put.restore();
			del.restore();
			effects.restore();
		}

		await session.peers[0].open(db, {
			args: { replicate: false, timeUntilRoleMaturity: 0 },
		});
		expect(await replicationIndex.count()).to.equal(0);
	});

	it("fences duplicate filtering after its covering-range lookup crosses close", async () => {
		const { db, log, replicationIndex } = await openDisconnectedLog(2);
		const ownerKey = session.peers[1].identity.publicKey;
		const ownerHash = ownerKey.hashcode();
		const previous = makeReplicationRange(log, {
			id: randomBytes(32),
			ownerHash,
			offset: 0.1,
		});
		const incoming = makeReplicationRange(log, {
			id: randomBytes(32),
			ownerHash,
			offset: 0.7,
			timestamp: 2n,
		});
		await log.addReplicationRange([previous], ownerKey, {
			checkDuplicates: false,
			rebalance: false,
		});

		const readStarted = pDefer<void>();
		const releaseRead = pDefer<void>();
		const originalCount = replicationIndex.count.bind(replicationIndex);
		let blockCoveringLookup = true;
		const count = sinon.stub(replicationIndex, "count").callsFake((async (
			request?: any,
			options?: any,
		) => {
			if (blockCoveringLookup && Array.isArray(request?.query)) {
				blockCoveringLookup = false;
				readStarted.resolve();
				await releaseRead.promise;
			}
			return originalCount(request, options);
		}) as any);
		const put = sinon.spy(replicationIndex, "put");
		const del = sinon.spy(replicationIndex, "del");
		const effects = observeRejectedReplicationMutation(log, {
			allowLifecycleClear: true,
		});

		try {
			const adding = log.addReplicationRange([incoming], ownerKey, {
				checkDuplicates: true,
				rebalance: false,
			});
			await readStarted.promise;
			const rejected = expect(adding).to.be.rejectedWith(
				"Replication ownership lifecycle is no longer active",
			);
			const closing = db.close();
			releaseRead.resolve();
			await Promise.all([rejected, closing]);

			expect(put.called).to.be.false;
			expect(del.called).to.be.false;
			effects.assertNoEffects();
		} finally {
			releaseRead.resolve();
			count.restore();
			put.restore();
			del.restore();
			effects.restore();
		}

		await session.peers[0].open(db, {
			args: { replicate: false, timeUntilRoleMaturity: 0 },
		});
		const durable = await replicationIndex.iterate().all();
		expect(durable).to.have.length(1);
		expect(durable[0].value.rangeHash).to.equal(previous.rangeHash);
	});

	it("rejects an invalid replication mode before reads or reset mutation", async () => {
		const { log, replicationIndex } = await openDisconnectedLog(2);
		const ownerKey = session.peers[1].identity.publicKey;
		const ownerHash = ownerKey.hashcode();
		const previous = makeReplicationRange(log, {
			id: randomBytes(32),
			ownerHash,
			offset: 0.1,
		});
		await log.addReplicationRange([previous], ownerKey, {
			checkDuplicates: false,
			rebalance: false,
		});
		const forged = makeReplicationRange(log, {
			id: randomBytes(32),
			ownerHash,
			offset: 0.6,
			mode: 2 as ReplicationIntent,
			timestamp: 2n,
		});
		const originalTrustedReplicator = log._isTrustedReplicator;
		const authorization = sinon.stub().resolves(true);
		log._isTrustedReplicator = authorization;
		const mutationLane = sinon.spy(log, "withReplicationRangeMutationQueue");
		const iterate = sinon.spy(replicationIndex, "iterate");
		const count = sinon.spy(replicationIndex, "count");
		const put = sinon.spy(replicationIndex, "put");
		const del = sinon.spy(replicationIndex, "del");
		const effects = observeRejectedReplicationMutation(log);

		try {
			await expect(
				log.addReplicationRange([forged], ownerKey, {
					reset: true,
					checkDuplicates: false,
					rebalance: false,
				}),
			).to.be.rejectedWith("Invalid replication range mode at index 0: 2");

			expect(authorization.called).to.be.false;
			expect(mutationLane.called).to.be.false;
			expect(iterate.called).to.be.false;
			expect(count.called).to.be.false;
			expect(put.called).to.be.false;
			expect(del.called).to.be.false;
			effects.assertNoEffects();
		} finally {
			log._isTrustedReplicator = originalTrustedReplicator;
			mutationLane.restore();
			iterate.restore();
			count.restore();
			put.restore();
			del.restore();
			effects.restore();
		}

		const durable = await replicationIndex.iterate().all();
		expect(durable).to.have.length(1);
		expect(durable[0].value.rangeHash).to.equal(previous.rangeHash);
	});

	it("rejects over-limit range announcements before authorization or mutation admission", async () => {
		const { log, replicationIndex } = await openDisconnectedLog(2);
		const ownerKey = session.peers[1].identity.publicKey;
		const range = makeReplicationRange(log, {
			id: randomBytes(32),
			ownerHash: ownerKey.hashcode(),
			offset: 0.1,
		});
		const ranges = new Array(4097).fill(range);
		const originalTrustedReplicator = log._isTrustedReplicator;
		const authorization = sinon.stub().resolves(true);
		log._isTrustedReplicator = authorization;
		const mutationLane = sinon.spy(log, "withReplicationRangeMutationQueue");
		const iterate = sinon.spy(replicationIndex, "iterate");
		const count = sinon.spy(replicationIndex, "count");
		const put = sinon.spy(replicationIndex, "put");
		const del = sinon.spy(replicationIndex, "del");
		const effects = observeRejectedReplicationMutation(log);

		try {
			await expect(
				log.addReplicationRange(ranges, ownerKey, {
					reset: true,
					checkDuplicates: false,
					rebalance: false,
				}),
			).to.be.rejectedWith(
				"Replication range announcement exceeds the 4096-range limit",
			);

			expect(authorization.called).to.be.false;
			expect(mutationLane.called).to.be.false;
			expect(iterate.called).to.be.false;
			expect(count.called).to.be.false;
			expect(put.called).to.be.false;
			expect(del.called).to.be.false;
			effects.assertNoEffects();
		} finally {
			log._isTrustedReplicator = originalTrustedReplicator;
			mutationLane.restore();
			iterate.restore();
			count.restore();
			put.restore();
			del.restore();
			effects.restore();
		}

		expect(await replicationIndex.count()).to.equal(0);
	});

	it("keeps incremental owner state within the snapshot limit and reconnects at the limit", async () => {
		const { log, replicationIndex } = await openDisconnectedLog(2);
		const selfKey = session.peers[0].identity.publicKey;
		const selfHash = selfKey.hashcode();
		const ranges = Array.from({ length: 4096 }, (_, index) =>
			makeReplicationRange(log, {
				id: new Uint8Array([index >>> 24, index >>> 16, index >>> 8, index]),
				ownerHash: selfHash,
				offset: (index % 100) / 100,
				timestamp: BigInt(index + 1),
			}),
		);
		for (let index = 0; index < ranges.length; index += 64) {
			await Promise.all(
				ranges
					.slice(index, index + 64)
					.map((range) => replicationIndex.put(range)),
			);
		}

		const extra = makeReplicationRange(log, {
			id: new Uint8Array([0xff, 0xff, 0xff, 0xff]),
			ownerHash: selfHash,
			offset: 0.5,
			timestamp: 4097n,
		});
		const put = sinon.spy(replicationIndex, "put");
		const effects = observeRejectedReplicationMutation(log);
		try {
			await expect(
				log.addReplicationRange([extra], selfKey, {
					checkDuplicates: false,
					rebalance: false,
				}),
			).to.be.rejectedWith(
				"Replication range ownership exceeds the 4096-range limit",
			);
			expect(put.called).to.be.false;
			expect(
				await replicationIndex.count({ query: { hash: selfHash } }),
			).to.equal(4096);
			effects.assertNoEffects();
		} finally {
			put.restore();
			effects.restore();
		}

		const sent: unknown[] = [];
		const send = sinon
			.stub(log.rpc, "send")
			.callsFake(async (message: unknown) => {
				sent.push(message);
				return [] as any;
			});
		try {
			await log.handleSubscriptionChange(
				session.peers[1].identity.publicKey,
				[log.topic],
				true,
			);
			const snapshots = sent.filter(
				(message) => message instanceof AllReplicatingSegmentsMessage,
			) as AllReplicatingSegmentsMessage[];
			expect(snapshots).to.have.length(1);
			expect(snapshots[0].segments).to.have.length(4096);
			expect(log._replicationRangeMutationFailure).to.be.undefined;
		} finally {
			send.restore();
		}
	});

	it("rejects over-limit stopped announcements before liveness, queues, or queries", async () => {
		const { log, replicationIndex } = await openDisconnectedLog(2);
		const remoteKey = session.peers[1].identity.publicKey;
		const duplicateId = randomBytes(32);
		const markActivity = sinon.spy(log, "markReplicatorActivity");
		const applyQueue = sinon.spy(log, "withReplicationInfoApplyQueue");
		const mutationLane = sinon.spy(log, "withReplicationRangeMutationQueue");
		const resolveRanges = sinon.spy(
			log,
			"resolveReplicationRangesFromIdsAndKey",
		);
		const iterate = sinon.spy(replicationIndex, "iterate");
		const count = sinon.spy(replicationIndex, "count");
		const effects = observeRejectedReplicationMutation(log);

		try {
			await log.onMessage(
				new StoppedReplicating({
					segmentIds: new Array(4097).fill(duplicateId),
				}),
				{
					from: remoteKey,
					message: { header: { timestamp: 1n } },
				} as any,
			);

			expect(markActivity.called).to.be.false;
			expect(applyQueue.called).to.be.false;
			expect(mutationLane.called).to.be.false;
			expect(resolveRanges.called).to.be.false;
			expect(iterate.called).to.be.false;
			expect(count.called).to.be.false;
			effects.assertNoEffects();
		} finally {
			markActivity.restore();
			applyQueue.restore();
			mutationLane.restore();
			resolveRanges.restore();
			iterate.restore();
			count.restore();
			effects.restore();
		}
	});

	it("does not let an old prune debounce resume into the reopened coordinator", async () => {
		const { db, log } = await openDisconnectedLog(2);
		const callbackStarted = pDefer<void>();
		const releaseCallback = pDefer<void>();
		let blockFirstCall = true;
		const isReplicating = sinon
			.stub(log, "isReplicating")
			.callsFake(async () => {
				if (blockFirstCall) {
					blockFirstCall = false;
					callbackStarted.resolve();
					await releaseCallback.promise;
				}
				return false;
			});
		const prune = sinon.spy(log, "prune");
		const remoteHash = session.peers[1].identity.publicKey.hashcode();
		const staleEntry = { hash: "stale-prune-debounce" } as any;
		const oldDebounce = log.pruneDebouncedFn;

		try {
			const pendingOutcome = oldDebounce
				.add({
					key: staleEntry.hash,
					value: {
						entry: staleEntry,
						leaders: new Map([[remoteHash, { intersecting: true }]]),
					},
				})
				.then(
					() => ({ status: "fulfilled" as const }),
					(error: unknown) => ({ status: "rejected" as const, error }),
				);
			await callbackStarted.promise;
			const oldCoordinator = log._checkedPrune;

			log.poisonReplicationOwnership(new Error("forced ownership poison"));
			await db.close();
			await session.peers[0].open(db, {
				args: { replicate: false, timeUntilRoleMaturity: 0 },
			});
			const freshCoordinator = log._checkedPrune;
			expect(freshCoordinator).to.not.equal(oldCoordinator);
			prune.resetHistory();

			releaseCallback.resolve();
			expect((await pendingOutcome).status).to.equal("fulfilled");
			await delay(25);
			expect(prune.called).to.be.false;
			expect(freshCoordinator.hasActiveWork(staleEntry.hash)).to.be.false;
		} finally {
			releaseCallback.resolve();
			isReplicating.restore();
			prune.restore();
			log._replicationRangeMutationFailure = undefined;
		}
	});

	it("does not let an old adaptive rebalance continue after reopen", async () => {
		const { db, log } = await openDisconnectedLog(1);
		const memoryReadStarted = pDefer<void>();
		const releaseMemoryRead = pDefer<void>();
		log._isReplicating = true;
		log._isAdaptiveReplicating = true;
		const getMemoryUsage = sinon
			.stub(log, "getMemoryUsage")
			.callsFake(async () => {
				memoryReadStarted.resolve();
				await releaseMemoryRead.promise;
				return 0;
			});
		const getDynamicRange = sinon.stub(log, "getDynamicRange").resolves();

		try {
			const balancing = log.rebalanceParticipation();
			await memoryReadStarted.promise;

			log.poisonReplicationOwnership(new Error("forced ownership poison"));
			await db.close();
			await session.peers[0].open(db, {
				args: { replicate: false, timeUntilRoleMaturity: 0 },
			});

			releaseMemoryRead.resolve();
			expect(await balancing).to.equal(false);
			expect(getDynamicRange.called).to.be.false;
		} finally {
			releaseMemoryRead.resolve();
			getMemoryUsage.restore();
			getDynamicRange.restore();
			log._replicationRangeMutationFailure = undefined;
		}
	});

	it("captures append ownership before the lower append can cross reopen", async () => {
		const { db, log } = await openDisconnectedLog(1);
		const { entry } = await db.add("append-generation-seed");
		const lowerAppendStarted = pDefer<void>();
		const releaseLowerAppend = pDefer<void>();
		const lowerAppend = sinon.stub(log.log, "append").callsFake(async () => {
			lowerAppendStarted.resolve();
			await releaseLowerAppend.promise;
			return { entry, removed: [] };
		});
		const processLocalAppend = sinon.spy(log, "processLocalAppend");

		try {
			const appending = log.append("blocked-across-reopen", {
				target: "none",
				replicate: false,
				delivery: false,
			});
			await lowerAppendStarted.promise;

			log.poisonReplicationOwnership(new Error("forced ownership poison"));
			await db.close();
			await session.peers[0].open(db, {
				args: { replicate: false, timeUntilRoleMaturity: 0 },
			});
			processLocalAppend.resetHistory();

			releaseLowerAppend.resolve();
			await expect(appending).to.be.rejectedWith(
				"Replication ownership lifecycle is no longer active",
			);
			expect(processLocalAppend.called).to.be.false;
		} finally {
			releaseLowerAppend.resolve();
			lowerAppend.restore();
			processLocalAppend.restore();
			log._replicationRangeMutationFailure = undefined;
		}
	});

	it("captures appendMany ownership before the lower batch can cross reopen", async () => {
		const { db, log } = await openDisconnectedLog(1);
		const { entry } = await db.add("append-many-generation-seed");
		const lowerAppendStarted = pDefer<void>();
		const releaseLowerAppend = pDefer<void>();
		const lowerAppendMany = sinon
			.stub(log.log, "appendMany")
			.callsFake(async () => {
				lowerAppendStarted.resolve();
				await releaseLowerAppend.promise;
				return { entries: [entry], removed: [] };
			});
		const deleteCoordinates = sinon.spy(log, "deleteCoordinatesForHashes");
		const coalesced = sinon.spy(log, "processLocalAppendManyCoalesced");
		const planLocal = sinon.spy(log, "planNativeLocalAppendEntries");
		const planDelivery = sinon.spy(log, "planNativeAppendEntries");
		const processLocalAppend = sinon.spy(log, "processLocalAppend");

		try {
			const appending = log.appendMany(["blocked-batch-across-reopen"], {
				target: "none",
				replicate: false,
				delivery: false,
			});
			await lowerAppendStarted.promise;

			log.poisonReplicationOwnership(new Error("forced ownership poison"));
			await db.close();
			await session.peers[0].open(db, {
				args: { replicate: false, timeUntilRoleMaturity: 0 },
			});
			deleteCoordinates.resetHistory();
			coalesced.resetHistory();
			planLocal.resetHistory();
			planDelivery.resetHistory();
			processLocalAppend.resetHistory();

			releaseLowerAppend.resolve();
			await expect(appending).to.be.rejectedWith(
				"Replication ownership lifecycle is no longer active",
			);
			expect(deleteCoordinates.called).to.be.false;
			expect(coalesced.called).to.be.false;
			expect(planLocal.called).to.be.false;
			expect(planDelivery.called).to.be.false;
			expect(processLocalAppend.called).to.be.false;
		} finally {
			releaseLowerAppend.resolve();
			lowerAppendMany.restore();
			deleteCoordinates.restore();
			coalesced.restore();
			planLocal.restore();
			planDelivery.restore();
			processLocalAppend.restore();
			log._replicationRangeMutationFailure = undefined;
		}
	});

	it("captures validated append ownership before its lower append crosses reopen", async () => {
		const { db, log } = await openDisconnectedLog(1);
		const { entry } = await db.add("validated-generation-seed");
		const lowerAppendStarted = pDefer<void>();
		const releaseLowerAppend = pDefer<void>();
		const lowerAppend = sinon.stub(log.log, "append").callsFake(async () => {
			lowerAppendStarted.resolve();
			await releaseLowerAppend.promise;
			return { entry, removed: [] };
		});
		const onChange = sinon.spy(log, "onChange");
		const processLocalAppend = sinon.spy(log, "processLocalAppend");

		try {
			const appending = log.appendLocallyValidated(
				"validated-blocked-across-reopen",
				{
					target: "none",
					replicate: false,
					delivery: false,
				},
			);
			await lowerAppendStarted.promise;

			log.poisonReplicationOwnership(new Error("forced ownership poison"));
			await db.close();
			await session.peers[0].open(db, {
				args: { replicate: false, timeUntilRoleMaturity: 0 },
			});
			onChange.resetHistory();
			processLocalAppend.resetHistory();

			releaseLowerAppend.resolve();
			await expect(appending).to.be.rejectedWith(
				"Replication ownership lifecycle is no longer active",
			);
			expect(onChange.called).to.be.false;
			expect(processLocalAppend.called).to.be.false;
		} finally {
			releaseLowerAppend.resolve();
			lowerAppend.restore();
			onChange.restore();
			processLocalAppend.restore();
			log._replicationRangeMutationFailure = undefined;
		}
	});

	it("captures prepared append ownership before its trusted lower append crosses reopen", async () => {
		const { db, log } = await openDisconnectedLog(1);
		const lowerAppendStarted = pDefer<void>();
		const releaseLowerAppend = pDefer<void>();
		const lowerAppend = sinon
			.stub(log.log, "appendLocallyPrepared")
			.callsFake(async () => {
				lowerAppendStarted.resolve();
				await releaseLowerAppend.promise;
				return {} as any;
			});
		const processNative = sinon.spy(
			log,
			"processNativePreparedTargetNoneAppend",
		);
		const applyChange = sinon.spy(log, "applyChange");
		const processLocalAppend = sinon.spy(log, "processLocalAppend");

		try {
			const appending = log.appendLocallyPrepared(
				"prepared-blocked-across-reopen",
				{
					target: "none",
					replicate: false,
					delivery: false,
				},
			);
			await lowerAppendStarted.promise;

			log.poisonReplicationOwnership(new Error("forced ownership poison"));
			await db.close();
			await session.peers[0].open(db, {
				args: { replicate: false, timeUntilRoleMaturity: 0 },
			});
			processNative.resetHistory();
			applyChange.resetHistory();
			processLocalAppend.resetHistory();

			releaseLowerAppend.resolve();
			await expect(appending).to.be.rejectedWith(
				"Replication ownership lifecycle is no longer active",
			);
			expect(processNative.called).to.be.false;
			expect(applyChange.called).to.be.false;
			expect(processLocalAppend.called).to.be.false;
		} finally {
			releaseLowerAppend.resolve();
			lowerAppend.restore();
			processNative.restore();
			applyChange.restore();
			processLocalAppend.restore();
			log._replicationRangeMutationFailure = undefined;
		}
	});

	it("captures commit-only fallback ownership before its trusted lower append crosses reopen", async () => {
		const { db, log } = await openDisconnectedLog(1);
		const lowerAppendStarted = pDefer<void>();
		const releaseLowerAppend = pDefer<void>();
		const lowerAppend = sinon
			.stub(log.log, "appendLocallyPreparedCommitOnly")
			.callsFake(async () => {
				lowerAppendStarted.resolve();
				await releaseLowerAppend.promise;
				return {} as any;
			});
		const finishAppend = sinon.spy(
			log,
			"finishPreparedPayloadCommitOnlyAppend",
		);

		try {
			const appending = log.appendLocallyPreparedPayloadCommitOnly(
				new Uint8Array([1, 2, 3]),
				{
					target: "none",
					replicate: false,
					delivery: false,
				},
			);
			await lowerAppendStarted.promise;

			log.poisonReplicationOwnership(new Error("forced ownership poison"));
			await db.close();
			await session.peers[0].open(db, {
				args: { replicate: false, timeUntilRoleMaturity: 0 },
			});
			finishAppend.resetHistory();

			releaseLowerAppend.resolve();
			await expect(appending).to.be.rejectedWith(
				"Replication ownership lifecycle is no longer active",
			);
			expect(finishAppend.called).to.be.false;
		} finally {
			releaseLowerAppend.resolve();
			lowerAppend.restore();
			finishAppend.restore();
			log._replicationRangeMutationFailure = undefined;
		}
	});

	it("captures prepared-many fallback ownership before its trusted batch crosses reopen", async () => {
		const { db, log } = await openDisconnectedLog(1);
		const lowerAppendStarted = pDefer<void>();
		const releaseLowerAppend = pDefer<void>();
		const nativeBatch = sinon
			.stub(
				log,
				"appendLocallyPreparedPayloadsManyNativeBackboneDocumentIndexBatch",
			)
			.resolves(undefined);
		const lowerAppend = sinon
			.stub(log.log, "appendLocallyPreparedManyIndependent")
			.callsFake(async () => {
				lowerAppendStarted.resolve();
				await releaseLowerAppend.promise;
				return {} as any;
			});
		const applyChange = sinon.spy(log, "applyChange");
		const deleteCoordinates = sinon.spy(log, "deleteCoordinatesForHashes");
		const planLocal = sinon.spy(log, "planNativeLocalAppendEntries");
		const planDelivery = sinon.spy(log, "planNativeAppendEntries");
		const processLocalAppend = sinon.spy(log, "processLocalAppend");

		try {
			const appending = log.appendLocallyPreparedManyIndependent(
				["prepared-many-blocked-across-reopen"],
				{
					target: "none",
					replicate: false,
					delivery: false,
				},
			);
			await lowerAppendStarted.promise;

			log.poisonReplicationOwnership(new Error("forced ownership poison"));
			await db.close();
			await session.peers[0].open(db, {
				args: { replicate: false, timeUntilRoleMaturity: 0 },
			});
			applyChange.resetHistory();
			deleteCoordinates.resetHistory();
			planLocal.resetHistory();
			planDelivery.resetHistory();
			processLocalAppend.resetHistory();

			releaseLowerAppend.resolve();
			await expect(appending).to.be.rejectedWith(
				"Replication ownership lifecycle is no longer active",
			);
			expect(applyChange.called).to.be.false;
			expect(deleteCoordinates.called).to.be.false;
			expect(planLocal.called).to.be.false;
			expect(planDelivery.called).to.be.false;
			expect(processLocalAppend.called).to.be.false;
		} finally {
			releaseLowerAppend.resolve();
			nativeBatch.restore();
			lowerAppend.restore();
			applyChange.restore();
			deleteCoordinates.restore();
			planLocal.restore();
			planDelivery.restore();
			processLocalAppend.restore();
			log._replicationRangeMutationFailure = undefined;
		}
	});

	it("captures join ownership before the lower join can cross reopen", async () => {
		const { db, log } = await openDisconnectedLog(1);
		const { entry } = await db.add("join-generation-seed");
		const lowerJoinStarted = pDefer<void>();
		const releaseLowerJoin = pDefer<void>();
		const lowerJoin = sinon.stub(log.log, "join").callsFake(async () => {
			lowerJoinStarted.resolve();
			await releaseLowerJoin.promise;
		});

		try {
			const joining = log.join([entry]);
			await lowerJoinStarted.promise;

			log.poisonReplicationOwnership(new Error("forced ownership poison"));
			await db.close();
			await session.peers[0].open(db, {
				args: { replicate: false, timeUntilRoleMaturity: 0 },
			});

			releaseLowerJoin.resolve();
			await expect(joining).to.be.rejectedWith(
				"Replication ownership lifecycle is no longer active",
			);
		} finally {
			releaseLowerJoin.resolve();
			lowerJoin.restore();
			log._replicationRangeMutationFailure = undefined;
		}
	});

	it("continues bounded deletion after a failed batch and reconciles durable state", async () => {
		const { log, replicationIndex } = await openDisconnectedLog();
		const remoteKey = session.peers[1].identity.publicKey;
		const remoteHash = remoteKey.hashcode();
		const numbers = log.indexableDomain.numbers;
		const ranges = Array.from(
			{ length: 201 },
			(_, index) =>
				new log.indexableDomain.constructorRange({
					id: new Uint8Array([index >>> 8, index & 0xff, 1]),
					offset: numbers.denormalize((index % 100) / 100),
					width: numbers.denormalize(0.005),
					publicKeyHash: remoteHash,
					timestamp: BigInt(index + 1),
				}),
		);
		const idKey = (id: Uint8Array) => Array.from(id).join(",");
		const retainedById = new Map(
			ranges.slice(0, 100).map((range: any) => [idKey(range.id), range]),
		);
		const idMatcher = (request: any) =>
			request?.query?.and?.find((part: any) => Array.isArray(part?.or)) ??
			(Array.isArray(request?.query?.or) ? request.query : undefined);
		const iterateBatchSizes: number[] = [];
		const deleteBatchSizes: number[] = [];
		const durableFailure = new Error("forced first delete-batch failure");
		let deleteAttempt = 0;
		const del = sinon.stub(replicationIndex, "del").callsFake((async (
			request: any,
		) => {
			deleteAttempt += 1;
			deleteBatchSizes.push(idMatcher(request).or.length);
			if (deleteAttempt === 1) {
				throw durableFailure;
			}
		}) as any);
		const iterate = sinon.stub(replicationIndex, "iterate").callsFake(((
			request?: any,
		) => {
			const matcher = idMatcher(request);
			if (matcher) {
				iterateBatchSizes.push(matcher.or.length);
			}
			const current = matcher
				? matcher.or
						.map((part: any) => retainedById.get(idKey(part.value)))
						.filter((range: any) => range !== undefined)
						.map((value: any) => ({ value }))
				: [];
			return {
				all: async () => current,
				next: async () => [],
				close: async () => {},
			};
		}) as any);
		const count = sinon.stub(replicationIndex, "count").resolves(100);
		const putNative = sinon.stub(log, "putNativeReplicationRange");
		const deleteNative = sinon.stub(log, "deleteNativeReplicationRange");

		try {
			const resolved = await log.resolveReplicationRangesFromIdsAndKey(
				ranges.map((range: any) => range.id),
				remoteKey,
			);
			expect(resolved).to.have.length(100);
			expect(iterateBatchSizes).to.deep.equal([100, 100, 1]);
			iterateBatchSizes.length = 0;

			const outcome = await log.deleteReplicationRangesCoherently(
				ranges,
				remoteHash,
			);
			expect(outcome.error).to.equal(durableFailure);
			expect(outcome.retained).to.deep.equal(ranges.slice(0, 100));
			expect(outcome.removed).to.deep.equal(ranges.slice(100));
			expect(deleteBatchSizes).to.deep.equal([100, 100, 1]);
			expect(iterateBatchSizes).to.deep.equal([100, 100, 1]);
			expect(putNative.callCount).to.equal(100);
			expect(deleteNative.callCount).to.equal(101);
			expect(log.uniqueReplicators.has(remoteHash)).to.be.true;
		} finally {
			del.restore();
			iterate.restore();
			count.restore();
			putNative.restore();
			deleteNative.restore();
		}
	});

	it("serializes admitted range mutations and fences terminal admission", async () => {
		const { log, replicationIndex } = await openDisconnectedLog();
		const remoteKey = session.peers[1].identity.publicKey;
		const remoteHash = remoteKey.hashcode();
		const numbers = log.indexableDomain.numbers;
		const makeRange = (offset: number) =>
			new log.indexableDomain.constructorRange({
				id: randomBytes(32),
				offset: numbers.denormalize(offset),
				width: numbers.denormalize(0.2),
				publicKeyHash: remoteHash,
				timestamp: 1n,
			});
		const admittedRange = makeRange(0.1);
		const rejectedRange = makeRange(0.6);
		const rangePersisted = pDefer<void>();
		const releaseRangePut = pDefer<void>();
		const originalPut = replicationIndex.put.bind(replicationIndex);
		const put = sinon.stub(replicationIndex, "put").callsFake((async (
			value: any,
			options?: any,
		) => {
			const result = await originalPut(value, options);
			if (value === admittedRange) {
				rangePersisted.resolve();
				await releaseRangePut.promise;
			}
			return result;
		}) as any);

		try {
			const adding = log.addReplicationRange([admittedRange], remoteKey, {
				checkDuplicates: false,
				rebalance: false,
			});
			await rangePersisted.promise;

			let removalEntered = false;
			const removing = log.removeReplicationRanges([admittedRange], remoteKey, {
				shouldRemove: () => {
					removalEntered = true;
					return true;
				},
			});
			await Promise.resolve();
			expect(removalEntered).to.be.false;

			const terminalFence = log.acquireReplicationRangeMutationTerminalFence();
			await expect(
				log.addReplicationRange([rejectedRange], remoteKey, {
					checkDuplicates: false,
					rebalance: false,
				}),
			).to.be.rejectedWith("Replication range mutations are closing");

			releaseRangePut.resolve();
			const [changes, removed] = await Promise.all([adding, removing]);
			await terminalFence.drained;
			expect(changes).to.have.length(1);
			expect(removed).to.be.true;
			expect(removalEntered).to.be.true;
			expect(
				await replicationIndex.count({ query: { hash: remoteHash } }),
			).to.equal(0);
		} finally {
			releaseRangePut.resolve();
			put.restore();
		}
	});

	it("fences entry replication before the mutation lane across reopen", async () => {
		const { db, log, replicationIndex } = await openDisconnectedLog(1);
		const { entry } = await db.add("blocked-replicate");
		const fromEntryStarted = pDefer<void>();
		const releaseFromEntry = pDefer<void>();
		const originalFromEntry = log.domain.fromEntry.bind(log.domain);
		const fromEntry = sinon
			.stub(log.domain, "fromEntry")
			.callsFake(async (value: unknown) => {
				fromEntryStarted.resolve();
				await releaseFromEntry.promise;
				return originalFromEntry(value);
			});

		try {
			const replicating = db.log.replicate(entry);
			await fromEntryStarted.promise;
			const oldOwnershipLifecycle = log._repairLifecycleController;
			log.poisonReplicationOwnership(new Error("forced ownership poison"));
			await db.close();
			await session.peers[0].open(db, {
				args: { replicate: false, timeUntilRoleMaturity: 0 },
			});
			expect(log._repairLifecycleController).to.not.equal(
				oldOwnershipLifecycle,
			);

			releaseFromEntry.resolve();
			await expect(replicating).to.be.rejectedWith(
				"Replication ownership lifecycle is no longer active",
			);
			expect(
				await replicationIndex.count({
					query: {
						hash: session.peers[0].identity.publicKey.hashcode(),
					},
				}),
			).to.equal(0);
		} finally {
			releaseFromEntry.resolve();
			fromEntry.restore();
			log._replicationRangeMutationFailure = undefined;
		}
	});

	it("fences ID-based unreplication before the mutation lane across reopen", async () => {
		const { db, log, replicationIndex } = await openDisconnectedLog(1);
		const [range] = await db.log.replicate({
			factor: 0.2,
			offset: 0.1,
		});
		const resolutionStarted = pDefer<void>();
		const releaseResolution = pDefer<void>();
		const originalResolve = log.resolveReplicationRangesFromIdsAndKey.bind(log);
		let blockNextResolution = true;
		const resolve = sinon
			.stub(log, "resolveReplicationRangesFromIdsAndKey")
			.callsFake(async (...args: unknown[]) => {
				if (blockNextResolution) {
					blockNextResolution = false;
					resolutionStarted.resolve();
					await releaseResolution.promise;
				}
				return originalResolve(...args);
			});

		try {
			const unreplicating = db.log.unreplicate([{ id: range.id }]);
			await resolutionStarted.promise;
			log.poisonReplicationOwnership(new Error("forced ownership poison"));
			await db.close();
			await session.peers[0].open(db, {
				args: {
					replicate: {
						type: "resume",
						default: { factor: 0.2, offset: 0.1 },
					},
					timeUntilRoleMaturity: 0,
				},
			});

			releaseResolution.resolve();
			await expect(unreplicating).to.be.rejectedWith(
				"Replication ownership lifecycle is no longer active",
			);
			expect(
				await replicationIndex.count({
					query: {
						hash: session.peers[0].identity.publicKey.hashcode(),
					},
				}),
			).to.equal(1);
		} finally {
			releaseResolution.resolve();
			resolve.restore();
			log._replicationRangeMutationFailure = undefined;
		}
	});

	it("fences trusted-replicator authorization before lane admission", async () => {
		const { db, log, replicationIndex } = await openDisconnectedLog();
		const remoteKey = session.peers[1].identity.publicKey;
		const numbers = log.indexableDomain.numbers;
		const range = new log.indexableDomain.constructorRange({
			id: randomBytes(32),
			offset: numbers.denormalize(0.2),
			width: numbers.denormalize(0.3),
			publicKeyHash: remoteKey.hashcode(),
			timestamp: 1n,
		});
		const authorizationStarted = pDefer<void>();
		const releaseAuthorization = pDefer<void>();
		const originalTrustedReplicator = log._isTrustedReplicator;
		log._isTrustedReplicator = async () => {
			authorizationStarted.resolve();
			await releaseAuthorization.promise;
			return true;
		};

		try {
			const adding = log.addReplicationRange([range], remoteKey, {
				checkDuplicates: false,
				rebalance: false,
			});
			await authorizationStarted.promise;
			log.poisonReplicationOwnership(new Error("forced ownership poison"));
			await db.close();
			await session.peers[0].open(db, {
				args: { replicate: false, timeUntilRoleMaturity: 0 },
			});

			releaseAuthorization.resolve();
			await expect(adding).to.be.rejectedWith(
				"Replication ownership lifecycle is no longer active",
			);
			expect(
				await replicationIndex.count({
					query: { hash: remoteKey.hashcode() },
				}),
			).to.equal(0);
		} finally {
			releaseAuthorization.resolve();
			log._isTrustedReplicator = originalTrustedReplicator;
			log._replicationRangeMutationFailure = undefined;
		}
	});

	it("fences full-owner removal after its receive drain is poisoned", async () => {
		const { db, log, replicationIndex } = await openDisconnectedLog(1);
		await db.log.replicate({ factor: 0.2, offset: 0.1 });
		const drainStarted = pDefer<void>();
		const releaseDrain = pDefer<void>();
		const originalDrain = log.drainPeerReceiveHandlers.bind(log);
		let blockNextDrain = true;
		const drain = sinon
			.stub(log, "drainPeerReceiveHandlers")
			.callsFake(async (...args: unknown[]) => {
				if (blockNextDrain) {
					blockNextDrain = false;
					drainStarted.resolve();
					await releaseDrain.promise;
				}
				return originalDrain(args[0] as string);
			});

		try {
			const unreplicating = db.log.unreplicate();
			await drainStarted.promise;
			log.poisonReplicationOwnership(new Error("forced ownership poison"));
			releaseDrain.resolve();
			await expect(unreplicating).to.be.rejectedWith(
				"Replication ownership recovery is required",
			);

			await db.close();
			await session.peers[0].open(db, {
				args: {
					replicate: {
						type: "resume",
						default: { factor: 0.2, offset: 0.1 },
					},
					timeUntilRoleMaturity: 0,
				},
			});
			expect(
				await replicationIndex.count({
					query: {
						hash: session.peers[0].identity.publicKey.hashcode(),
					},
				}),
			).to.equal(1);
		} finally {
			releaseDrain.resolve();
			drain.restore();
			log._replicationRangeMutationFailure = undefined;
		}
	});

	it("rolls back every staged positive row after a commit-then-throw put", async () => {
		const { log, replicationIndex } = await openDisconnectedLog();
		const remoteKey = session.peers[1].identity.publicKey;
		const remoteHash = remoteKey.hashcode();
		const numbers = log.indexableDomain.numbers;
		const makeRange = (offset: number) =>
			new log.indexableDomain.constructorRange({
				id: randomBytes(32),
				offset: numbers.denormalize(offset),
				width: numbers.denormalize(0.2),
				publicKeyHash: remoteHash,
				timestamp: 1n,
			});
		const first = makeRange(0.1);
		const second = makeRange(0.6);
		const durableFailure = new Error("forced second positive put failure");
		const originalPut = replicationIndex.put.bind(replicationIndex);
		const put = sinon.stub(replicationIndex, "put").callsFake((async (
			value: any,
			options?: any,
		) => {
			const result = await originalPut(value, options);
			if (value === second) {
				throw durableFailure;
			}
			return result;
		}) as any);

		try {
			await expect(
				log.addReplicationRange([first, second], remoteKey, {
					checkDuplicates: false,
					rebalance: false,
				}),
			).to.be.rejectedWith(durableFailure.message);
			expect(
				await replicationIndex.count({ query: { hash: remoteHash } }),
			).to.equal(0);
			expect(log.uniqueReplicators.has(remoteHash)).to.be.false;
			expect(log._replicationRangeMutationFailure).to.be.undefined;
		} finally {
			put.restore();
		}
	});

	it("poisons mutation and planning when positive rollback cannot recover", async () => {
		const { log, replicationIndex } = await openDisconnectedLog();
		const remoteKey = session.peers[1].identity.publicKey;
		const remoteHash = remoteKey.hashcode();
		const numbers = log.indexableDomain.numbers;
		const range = new log.indexableDomain.constructorRange({
			id: randomBytes(32),
			offset: numbers.denormalize(0.2),
			width: numbers.denormalize(0.3),
			publicKeyHash: remoteHash,
			timestamp: 1n,
		});
		const publicationFailure = new Error("forced native publication failure");
		const rollbackFailure = new Error("forced positive rollback failure");
		const putNative = sinon
			.stub(log, "putNativeReplicationRange")
			.throws(publicationFailure);
		const del = sinon.stub(replicationIndex, "del").rejects(rollbackFailure);

		try {
			let observed: unknown;
			try {
				await log.addReplicationRange([range], remoteKey, {
					checkDuplicates: false,
					rebalance: false,
				});
			} catch (error) {
				observed = error;
			}
			expect(observed).to.be.instanceOf(AggregateError);
			expect((observed as AggregateError).errors).to.deep.equal([
				publicationFailure,
				rollbackFailure,
			]);
			expect(log._replicationRangeMutationFailure).to.equal(observed);
			expect(() => log.prune(new Map())).to.throw(
				"Replication ownership recovery is required",
			);
			await expect(
				log._findLeaders([numbers.denormalize(0.5)]),
			).to.be.rejectedWith("Replication ownership recovery is required");
		} finally {
			putNative.restore();
			del.restore();
			log._replicationRangeMutationFailure = undefined;
		}
	});

	it("rejects planning that was awaiting context when ownership poisons", async () => {
		const { log } = await openDisconnectedLog(1);
		const numbers = log.indexableDomain.numbers;
		const subscribersStarted = pDefer<void>();
		const releaseSubscribers = pDefer<void>();
		const subscribers = sinon
			.stub(log, "_getTopicSubscribers")
			.callsFake(async () => {
				subscribersStarted.resolve();
				await releaseSubscribers.promise;
				return [];
			});
		const planner = sinon.stub().returns(new Map());
		const originalBackbone = log._nativeBackbone;
		const originalPlanner = log._nativeRangePlanner;
		log._nativeBackbone = undefined;
		log._nativeRangePlanner = { findLeaders: planner };
		log.invalidateLeaderSelectionContextCache();

		try {
			const planning = log._findLeaders([numbers.denormalize(0.5)], {
				roleAge: 0,
			});
			await subscribersStarted.promise;
			log.poisonReplicationOwnership(new Error("forced ownership poison"));
			releaseSubscribers.resolve();
			await expect(planning).to.be.rejectedWith(
				"Replication ownership recovery is required",
			);
			expect(planner.called).to.be.false;
		} finally {
			releaseSubscribers.resolve();
			subscribers.restore();
			log._nativeBackbone = originalBackbone;
			log._nativeRangePlanner = originalPlanner;
			log._replicationRangeMutationFailure = undefined;
		}
	});

	it("rejects an old planner after reopen without caching fresh lifecycle state", async () => {
		const { db, log } = await openDisconnectedLog(1);
		const numbers = log.indexableDomain.numbers;
		const subscribersStarted = pDefer<void>();
		const releaseSubscribers = pDefer<void>();
		const originalGetSubscribers = log._getTopicSubscribers.bind(log);
		const subscribers = sinon
			.stub(log, "_getTopicSubscribers")
			.callsFake(async (...args: any[]) => {
				if (subscribers.callCount === 1) {
					subscribersStarted.resolve();
					await releaseSubscribers.promise;
					return [];
				}
				return originalGetSubscribers(...args);
			});
		const planner = sinon.stub().returns(new Map());
		const originalBackbone = log._nativeBackbone;
		const originalPlanner = log._nativeRangePlanner;
		log._nativeBackbone = undefined;
		log._nativeRangePlanner = { findLeaders: planner };
		log.invalidateLeaderSelectionContextCache();

		try {
			const planning = log._findLeaders([numbers.denormalize(0.5)], {
				roleAge: 0,
			});
			await subscribersStarted.promise;
			log.poisonReplicationOwnership(new Error("forced ownership poison"));
			await db.close();
			await session.peers[0].open(db, {
				args: { replicate: false, timeUntilRoleMaturity: 0 },
			});
			log._nativeBackbone = undefined;
			log._nativeRangePlanner = { findLeaders: planner };
			log.invalidateLeaderSelectionContextCache();
			planner.resetHistory();

			releaseSubscribers.resolve();
			await expect(planning).to.be.rejectedWith(
				"Replication ownership lifecycle is no longer active",
			);
			expect(planner.called).to.be.false;
			expect(log._leaderSelectionContextCache).to.be.undefined;
		} finally {
			releaseSubscribers.resolve();
			subscribers.restore();
			log._nativeBackbone = originalBackbone;
			log._nativeRangePlanner = originalPlanner;
			log._replicationRangeMutationFailure = undefined;
		}
	});

	it("fences native append planning across its second await and reopen", async () => {
		const { db, log } = await openDisconnectedLog(1);
		const { entry } = await db.add("native-append-planner-generation");
		const candidatesStarted = pDefer<void>();
		const releaseCandidates = pDefer<void>();
		const originalBackbone = log._nativeBackbone;
		const staleNativePlanner = {
			planAppendForGid: sinon.stub(),
		};
		const canPlan = sinon.stub(log, "canPlanNativeHashGid").returns(true);
		const context = sinon.stub(log, "createLeaderSelectionContext").resolves({
			roleAge: 0,
			selfHash: session.peers[0].identity.publicKey.hashcode(),
			selfReplicating: false,
			peerFilter: undefined,
		});
		const candidates = sinon
			.stub(log, "getFullReplicaRepairCandidates")
			.callsFake(async () => {
				candidatesStarted.resolve();
				await releaseCandidates.promise;
				return new Set<string>();
			});
		log._nativeBackbone = staleNativePlanner;
		let restored = false;
		const restoreAdmissionStubs = () => {
			if (restored) {
				return;
			}
			restored = true;
			canPlan.restore();
			context.restore();
			candidates.restore();
			if (log._nativeBackbone === staleNativePlanner) {
				log._nativeBackbone = originalBackbone;
			}
		};

		try {
			const planning = log.planNativeAppendEntry(entry, 1, false);
			await candidatesStarted.promise;
			restoreAdmissionStubs();

			log.poisonReplicationOwnership(new Error("forced ownership poison"));
			await db.close();
			await session.peers[0].open(db, {
				args: { replicate: false, timeUntilRoleMaturity: 0 },
			});

			releaseCandidates.resolve();
			await expect(planning).to.be.rejectedWith(
				"Replication ownership lifecycle is no longer active",
			);
			expect(staleNativePlanner.planAppendForGid.called).to.be.false;
		} finally {
			releaseCandidates.resolve();
			restoreAdmissionStubs();
			log._replicationRangeMutationFailure = undefined;
		}
	});

	it("fences the native document batch callback before a captured backbone commit after reopen", async () => {
		const { db, log } = await openDisconnectedLog(1);
		const transactionStarted = pDefer<void>();
		const releaseTransaction = pDefer<void>();
		const originalBackbone = log._nativeBackbone;
		const staleCompactCommit = sinon
			.stub()
			.throws(new Error("stale native document batch commit ran"));
		const staleLatestCommit = sinon
			.stub()
			.throws(new Error("stale native latest-document batch commit ran"));
		const staleBackbone = {
			blocks: {},
			preparePlainCommittedNoNextStorageAppendDocumentIndexCompactBatchTransaction:
				staleCompactCommit,
			preparePlainCommittedStorageAppendDocumentIndexLatestBatchTransaction:
				staleLatestCommit,
		};
		const canUseResidentState = sinon
			.stub(log, "canUseNativeBackboneResidentCoordinateState")
			.returns(true);
		const context = sinon.stub(log, "createLeaderSelectionContext").resolves({
			roleAge: 0,
			selfHash: session.peers[0].identity.publicKey.hashcode(),
			selfReplicating: false,
			peerFilter: undefined,
		});
		const snapshot = sinon
			.stub(log, "snapshotNativeBackboneDocument")
			.returns(undefined);
		const beginTransaction = sinon
			.stub(log, "beginNativeStrictDurableTransaction")
			.callsFake(async () => {
				transactionStarted.resolve();
				await releaseTransaction.promise;
				return undefined;
			});
		const appendBatch = sinon
			.stub(log.log, "appendLocallyPreparedNativeKnownNoNextCommitOnlyBatch")
			.callsFake(async (...args: unknown[]) => {
				const prepare = args[3] as (inputs: unknown[]) => Promise<unknown>;
				return prepare([{}]);
			});
		log._nativeBackbone = staleBackbone;
		let restored = false;
		const restoreAdmissionStubs = () => {
			if (restored) {
				return;
			}
			restored = true;
			canUseResidentState.restore();
			context.restore();
			snapshot.restore();
			beginTransaction.restore();
			appendBatch.restore();
			if (log._nativeBackbone === staleBackbone) {
				log._nativeBackbone = originalBackbone;
			}
		};

		try {
			const appending =
				log.appendLocallyPreparedPayloadsManyNativeBackboneDocumentIndexBatch(
					["native-document-batch"],
					{},
					{ target: "none", replicate: false, delivery: false },
					{
						payloadDatas: [new Uint8Array([1])],
						nativeBackboneDocumentIndexes: [{ projection: {} }],
					},
					1,
				);
			await transactionStarted.promise;
			restoreAdmissionStubs();

			log.poisonReplicationOwnership(new Error("forced ownership poison"));
			await db.close();
			await session.peers[0].open(db, {
				args: { replicate: false, timeUntilRoleMaturity: 0 },
			});

			releaseTransaction.resolve();
			await expect(appending).to.be.rejectedWith(
				"Replication ownership lifecycle is no longer active",
			);
			expect(staleCompactCommit.called).to.be.false;
			expect(staleLatestCommit.called).to.be.false;
		} finally {
			releaseTransaction.resolve();
			restoreAdmissionStubs();
			log._replicationRangeMutationFailure = undefined;
		}
	});

	it("fences blocked head-coordinate reconciliation from reopened indexes", async () => {
		const { db, log } = await openDisconnectedLog(1);
		const { entry } = await db.add("head-coordinate-generation");
		const iterateStarted = pDefer<void>();
		const releaseIterate = pDefer<void>();
		const originalBackbone = log._nativeBackbone;
		const originalNativeState = log._nativeSharedLogState;
		log._nativeBackbone = undefined;
		log._nativeSharedLogState = undefined;
		let nativeStateRestored = false;
		const coordinateIndex = log.entryCoordinatesIndex;
		const iterate = sinon.stub(coordinateIndex, "iterate").returns({
			all: async () => {
				iterateStarted.resolve();
				await releaseIterate.promise;
				return [{ value: { hash: "stale-old-coordinate" } }];
			},
		} as any);

		try {
			const reconciling = log.ensureCurrentHeadCoordinatesIndexed();
			await iterateStarted.promise;
			iterate.restore();
			log._nativeBackbone = originalBackbone;
			log._nativeSharedLogState = originalNativeState;
			nativeStateRestored = true;

			log.poisonReplicationOwnership(new Error("forced ownership poison"));
			await db.close();
			await session.peers[0].open(db, {
				args: { replicate: false, timeUntilRoleMaturity: 0 },
			});
			const deleteCoordinates = sinon.spy(log, "deleteCoordinatesForHashes");
			const planHeads = sinon.spy(log, "planEntryLeaderBatch");

			try {
				releaseIterate.resolve();
				await expect(reconciling).to.be.rejectedWith(
					"Replication ownership lifecycle is no longer active",
				);
				expect(deleteCoordinates.called).to.be.false;
				expect(planHeads.called).to.be.false;
				expect(entry.hash).to.not.equal("stale-old-coordinate");
			} finally {
				deleteCoordinates.restore();
				planHeads.restore();
			}
		} finally {
			releaseIterate.resolve();
			if ((iterate as any).wrappedMethod) {
				iterate.restore();
			}
			if (!nativeStateRestored) {
				log._nativeBackbone = originalBackbone;
				log._nativeSharedLogState = originalNativeState;
			}
			log._replicationRangeMutationFailure = undefined;
		}
	});

	it("fences a blocked replication announcement from reopened repair queues", async () => {
		const { db, log } = await openDisconnectedLog(1);
		const announcementStarted = pDefer<void>();
		const releaseAnnouncement = pDefer<void>();
		let announcementSignal: AbortSignal | undefined;
		const send = sinon
			.stub(log.rpc, "send")
			.callsFake(async (...args: unknown[]) => {
				const message = args[0];
				const options = args[1] as { signal?: AbortSignal } | undefined;
				if (message instanceof StoppedReplicating) {
					announcementSignal = options?.signal;
					announcementStarted.resolve();
					await releaseAnnouncement.promise;
				}
				return [] as any;
			});
		const queueRepair = sinon.spy(
			log,
			"queueCurrentReplicationStateAnnouncementRepair",
		);
		const queueRetry = sinon.spy(
			log,
			"queueCurrentReplicationStateAnnouncementRetry",
		);
		const ownershipLifecycleController = log._repairLifecycleController;

		try {
			const announcing = log.sendReplicationAnnouncement(
				new StoppedReplicating({ segmentIds: [] }),
			);
			await announcementStarted.promise;
			expect(announcementSignal).to.equal(ownershipLifecycleController.signal);

			log.poisonReplicationOwnership(new Error("forced ownership poison"));
			expect(announcementSignal?.aborted).to.be.true;
			await db.close();
			await session.peers[0].open(db, {
				args: { replicate: false, timeUntilRoleMaturity: 0 },
			});
			queueRepair.resetHistory();
			queueRetry.resetHistory();

			releaseAnnouncement.resolve();
			await expect(announcing).to.be.rejectedWith(
				"Replication ownership lifecycle is no longer active",
			);
			expect(queueRepair.called).to.be.false;
			expect(queueRetry.called).to.be.false;
		} finally {
			releaseAnnouncement.resolve();
			queueRepair.restore();
			queueRetry.restore();
			send.restore();
			log._replicationRangeMutationFailure = undefined;
		}
	});

	it("does not let a stale announcement retry poison reopened ownership", async () => {
		const { db, log } = await openDisconnectedLog(1);
		const snapshotReadStarted = pDefer<void>();
		const releaseSnapshotRead = pDefer<void>();
		const getMyReplicationSegments = sinon
			.stub(log, "getMyReplicationSegments")
			.callsFake(async () => {
				snapshotReadStarted.resolve();
				await releaseSnapshotRead.promise;
				return [
					{
						toReplicationRange: () => ({ mode: "invalid-stale-mode" }),
					},
				];
			});

		try {
			const retrying = log.retryCurrentReplicationStateAnnouncement();
			await snapshotReadStarted.promise;
			getMyReplicationSegments.restore();

			log.poisonReplicationOwnership(new Error("forced ownership poison"));
			await db.close();
			await session.peers[0].open(db, {
				args: { replicate: false, timeUntilRoleMaturity: 0 },
			});
			const reopenedLifecycle = log._repairLifecycleController;

			releaseSnapshotRead.resolve();
			await retrying;

			expect(log._replicationRangeMutationFailure).to.be.undefined;
			expect(log._repairLifecycleController).to.equal(reopenedLifecycle);
			expect(reopenedLifecycle.signal.aborted).to.be.false;
		} finally {
			releaseSnapshotRead.resolve();
			if ((getMyReplicationSegments as any).wrappedMethod) {
				getMyReplicationSegments.restore();
			}
			log._replicationRangeMutationFailure = undefined;
		}
	});

	it("does not let a stale announcement repair poison reopened ownership", async () => {
		const { db, log } = await openDisconnectedLog(1);
		const snapshotReadStarted = pDefer<void>();
		const releaseSnapshotRead = pDefer<void>();
		const getMyReplicationSegments = sinon
			.stub(log, "getMyReplicationSegments")
			.callsFake(async () => {
				snapshotReadStarted.resolve();
				await releaseSnapshotRead.promise;
				return [
					{
						toReplicationRange: () => ({ mode: "invalid-stale-mode" }),
					},
				];
			});
		log._replicationAnnouncementRepairPending = true;

		try {
			const repairing = log.runCurrentReplicationStateAnnouncementRepair();
			await snapshotReadStarted.promise;
			getMyReplicationSegments.restore();

			log.poisonReplicationOwnership(new Error("forced ownership poison"));
			await db.close();
			await session.peers[0].open(db, {
				args: { replicate: false, timeUntilRoleMaturity: 0 },
			});
			const reopenedLifecycle = log._repairLifecycleController;

			releaseSnapshotRead.resolve();
			await repairing;

			expect(log._replicationRangeMutationFailure).to.be.undefined;
			expect(log._repairLifecycleController).to.equal(reopenedLifecycle);
			expect(reopenedLifecycle.signal.aborted).to.be.false;
		} finally {
			releaseSnapshotRead.resolve();
			if ((getMyReplicationSegments as any).wrappedMethod) {
				getMyReplicationSegments.restore();
			}
			log._replicationRangeMutationFailure = undefined;
		}
	});

	it("fences a blocked repair runner from poison and reopened frontier state", async () => {
		const { db, log } = await openDisconnectedLog(1);
		const firstSendStarted = pDefer<void>();
		const secondSendStarted = pDefer<void>();
		const releaseFirstSend = pDefer<void>();
		const sentBatches: string[][] = [];
		const send = sinon
			.stub(log, "sendMaybeMissingEntriesNow")
			.callsFake(async (...args: unknown[]) => {
				const entries = args[1] as Map<string, unknown>;
				sentBatches.push([...entries.keys()]);
				if (sentBatches.length === 1) {
					firstSendStarted.resolve();
					await releaseFirstSend.promise;
				} else {
					secondSendStarted.resolve();
				}
			});

		try {
			log.queueRepairFrontierEntries(
				"churn",
				"target",
				new Map([["old", { hash: "old" }]]),
			);
			log.ensureRepairFrontierRunner("churn", "target", [0, 5]);
			await firstSendStarted.promise;

			const oldRepairLifecycle = log._repairLifecycleController;
			log.poisonReplicationOwnership(new Error("forced ownership poison"));
			expect(oldRepairLifecycle.signal.aborted).to.be.true;
			expect(log._repairRetryTimers.size).to.equal(0);

			await db.close();
			await session.peers[0].open(db, {
				args: { replicate: false, timeUntilRoleMaturity: 0 },
			});
			log.queueRepairFrontierEntries(
				"churn",
				"target",
				new Map([["new", { hash: "new" }]]),
			);

			releaseFirstSend.resolve();
			await delay(30);
			expect(sentBatches).to.deep.equal([["old"]]);
			expect(log._repairRetryTimers.size).to.equal(0);
			expect([
				...(log._repairFrontierByMode.get("churn")?.get("target")?.keys() ??
					[]),
			]).to.deep.equal(["new"]);

			log.ensureRepairFrontierRunner("churn", "target", [0, 1_000]);
			await secondSendStarted.promise;
			expect(sentBatches).to.deep.equal([["old"], ["new"]]);
		} finally {
			releaseFirstSend.resolve();
			log.poisonReplicationOwnership(new Error("test cleanup"));
			send.restore();
			log._replicationRangeMutationFailure = undefined;
		}
	});

	it("stops a fused repair send before its next publish after poison", async () => {
		const { log } = await openDisconnectedLog(1);
		const publishStarted = pDefer<void>();
		const abortError = new Error("fused repair publish aborted");
		let capturedSignal: AbortSignal | undefined;
		const pubsub = session.peers[0].services.pubsub as any;
		const publish = sinon
			.stub(pubsub, "publishPreEncodedData")
			.callsFake(async (...args: unknown[]) => {
				const options = args[2] as { signal?: AbortSignal } | undefined;
				capturedSignal = options?.signal;
				publishStarted.resolve();
				await new Promise<void>((_resolve, reject) => {
					const rejectForAbort = () => reject(abortError);
					if (capturedSignal?.aborted) {
						rejectForAbort();
					} else {
						capturedSignal?.addEventListener("abort", rejectForAbort, {
							once: true,
						});
					}
				});
				return undefined;
			});
		const originalBackbone = log._nativeBackbone;
		const profileEvents: any[] = [];
		log._logProperties.sync = {
			...(log._logProperties.sync ?? {}),
			profile: (event: any) => profileEvents.push(event),
		};
		let encoded = 0;
		log._nativeBackbone = {
			encodeRawExchangeSyncPayload: () => new Uint8Array([++encoded]),
			syncSendBlockByteLengths: () => [600_000, 600_000],
		};
		const ownershipLifecycleController = log._repairLifecycleController;

		try {
			const sending = log.sendFusedRawExchangeHeadsPlan(
				{
					hashes: ["first", "second"],
					gidRefrences: [[], []],
				},
				["target"],
				{ signal: ownershipLifecycleController.signal },
			);
			await publishStarted.promise;
			expect(capturedSignal).to.equal(ownershipLifecycleController.signal);
			log.poisonReplicationOwnership(new Error("forced ownership poison"));

			await expect(sending).to.be.rejectedWith(abortError.message);
			expect(publish.callCount).to.equal(1);
			expect(encoded).to.equal(2);
			const profile = profileEvents.find(
				(event) => event.name === "sharedLog.rawSend.fused",
			);
			expect(profile).to.include({
				entries: 0,
				bytes: 0,
				messages: 0,
			});
			expect(profile.details).to.include({
				attemptedMessages: 1,
				cancelled: true,
				plannedEntries: 2,
				plannedMessages: 2,
			});
		} finally {
			publish.restore();
			log._nativeBackbone = originalBackbone;
			log._replicationRangeMutationFailure = undefined;
		}
	});

	it("aborts an in-flight TS repair fallback send when ownership is poisoned", async () => {
		const { db, log } = await openDisconnectedLog(1);
		const { entry } = await db.add(uuid(), { meta: { next: [] } });
		const sendStarted = pDefer<void>();
		const releaseSend = pDefer<void>();
		const abortError = new Error("repair fallback send aborted");
		let capturedSignal: AbortSignal | undefined;
		const originalSend = log.rpc.send.bind(log.rpc);
		const send = sinon.stub(log.rpc, "send").callsFake((async (
			message: unknown,
			options?: { signal?: AbortSignal },
		) => {
			if (!(message instanceof ExchangeHeadsMessage)) {
				return originalSend(message, options);
			}
			capturedSignal = options?.signal;
			sendStarted.resolve();
			if (!capturedSignal) {
				await releaseSend.promise;
				return;
			}
			await new Promise<void>((_resolve, reject) => {
				const rejectForAbort = () => reject(abortError);
				if (capturedSignal?.aborted) {
					rejectForAbort();
				} else {
					capturedSignal?.addEventListener("abort", rejectForAbort, {
						once: true,
					});
				}
			});
		}) as any);
		const ownershipLifecycleController = log._repairLifecycleController;

		try {
			const sending = log.sendRepairEntriesWithTransport(
				"target",
				new Map([[entry.hash, { hash: entry.hash }]]),
				"simple",
				{
					bypassKnownPeers: true,
					bypassRecentKnownPeers: true,
					isStillCurrent: () =>
						log.isRepairLifecycleActive(ownershipLifecycleController),
					signal: ownershipLifecycleController.signal,
				},
			);
			await sendStarted.promise;
			expect(capturedSignal).to.equal(ownershipLifecycleController.signal);

			log.poisonReplicationOwnership(new Error("forced ownership poison"));
			await expect(sending).to.be.rejectedWith(abortError.message);
			expect(send.callCount).to.equal(1);
		} finally {
			releaseSend.resolve();
			send.restore();
			log._replicationRangeMutationFailure = undefined;
		}
	});

	it("fences a blocked replication-change callback from reopened state", async () => {
		const { db, log } = await openDisconnectedLog(2);
		const numbers = log.indexableDomain.numbers;
		const remoteKey = session.peers[1].identity.publicKey;
		const range = new log.indexableDomain.constructorRange({
			id: randomBytes(32),
			offset: numbers.denormalize(0.2),
			width: numbers.denormalize(0.2),
			publicKeyHash: remoteKey.hashcode(),
			timestamp: 1n,
		});
		const trimStarted = pDefer<void>();
		const releaseTrim = pDefer<void>();
		const originalTrim = log.log.trim.bind(log.log);
		const trim = sinon
			.stub(log.log, "trim")
			.callsFake(async (...args: any[]) => {
				if (trim.callCount === 1) {
					trimStarted.resolve();
					await releaseTrim.promise;
					return;
				}
				return originalTrim(...args);
			});

		try {
			const changing = log.onReplicationChange([
				{ range, type: "removed", timestamp: 2n },
			]);
			await trimStarted.promise;
			log.poisonReplicationOwnership(new Error("forced ownership poison"));
			await db.close();
			await session.peers[0].open(db, {
				args: { replicate: false, timeUntilRoleMaturity: 0 },
			});
			const freshCheckedPrune = log._checkedPrune;
			const freshRecentRepairDispatch = log._recentRepairDispatch;
			const freshRepairTimers = log._repairRetryTimers;
			const freshGidHistory = log._gidPeersHistory;
			const freshFrontier = log._repairFrontierByMode;

			releaseTrim.resolve();
			expect(await changing).to.equal(false);
			await delay(275);
			expect(log._checkedPrune).to.equal(freshCheckedPrune);
			expect(log._recentRepairDispatch).to.equal(freshRecentRepairDispatch);
			expect(log._repairRetryTimers).to.equal(freshRepairTimers);
			expect(log._gidPeersHistory).to.equal(freshGidHistory);
			expect(log._repairFrontierByMode).to.equal(freshFrontier);
			expect(freshRecentRepairDispatch.size).to.equal(0);
			expect(freshRepairTimers.size).to.equal(0);
			expect(freshGidHistory.size).to.equal(0);
			for (const targets of freshFrontier.values()) {
				expect(targets.size).to.equal(0);
			}
		} finally {
			releaseTrim.resolve();
			trim.restore();
			log._replicationRangeMutationFailure = undefined;
		}
	});

	it("cancels an admitted prune poisoned during ownership revalidation", async () => {
		session = await TestSession.disconnected(2);
		const db = await session.peers[0].open(new EventStore(), {
			args: { replicate: 1, timeUntilRoleMaturity: 0 },
		});
		const log = db.log as any;
		const { entry } = await db.add("prune-race");
		const remoteHash = session.peers[1].identity.publicKey.hashcode();
		const leaders = new Map([[remoteHash, { intersecting: true }]]);
		const plannerStarted = pDefer<void>();
		const releasePlanner = pDefer<void>();
		const waitForReplicators = sinon
			.stub(log, "_waitForEntryReplicators")
			.resolves(true);
		const getClampedReplicas = sinon
			.stub(log, "getClampedReplicas")
			.returns({ getValue: () => 1 });
		const send = sinon.stub(log.rpc, "send").resolves();
		const findLeaders = sinon
			.stub(log, "findLeadersFromEntry")
			.callsFake(async () => {
				plannerStarted.resolve();
				await releasePlanner.promise;
				return new Map();
			});
		const remove = sinon.stub(log.log, "remove").resolves();

		try {
			const [pruning] = log.prune(new Map([[entry.hash, { entry, leaders }]]), {
				timeout: 2_000,
			});
			const pending = log._checkedPrune.getPendingDelete(entry.hash);
			expect(pending).to.exist;
			await pending.resolve(remoteHash);
			await plannerStarted.promise;

			log.poisonReplicationOwnership(new Error("forced ownership poison"));
			releasePlanner.resolve();

			await expect(pruning).to.be.rejectedWith(
				"Replication ownership recovery is required",
			);
			expect(remove.called).to.be.false;
			expect(log._checkedPrune.getPendingDelete(entry.hash)).to.be.undefined;
		} finally {
			releasePlanner.resolve();
			waitForReplicators.restore();
			getClampedReplicas.restore();
			send.restore();
			findLeaders.restore();
			remove.restore();
			log._replicationRangeMutationFailure = undefined;
		}
	});

	it("keeps a stale checked prune out of the reopened coordinator", async () => {
		session = await TestSession.disconnected(2);
		const db = await session.peers[0].open(new EventStore(), {
			args: { replicate: 1, timeUntilRoleMaturity: 0 },
		});
		const log = db.log as any;
		const { entry } = await db.add("prune-reopen-race");
		const remoteHash = session.peers[1].identity.publicKey.hashcode();
		const leaders = new Map([[remoteHash, { intersecting: true }]]);
		const plannerStarted = pDefer<void>();
		const releasePlanner = pDefer<void>();
		const waitForReplicators = sinon
			.stub(log, "_waitForEntryReplicators")
			.resolves(true);
		const getClampedReplicas = sinon
			.stub(log, "getClampedReplicas")
			.returns({ getValue: () => 1 });
		const send = sinon.stub(log.rpc, "send").resolves();
		const findLeaders = sinon
			.stub(log, "findLeadersFromEntry")
			.callsFake(async () => {
				if (findLeaders.callCount === 1) {
					plannerStarted.resolve();
					await releasePlanner.promise;
				}
				return new Map();
			});
		const remove = sinon.stub(log.log, "remove").resolves();
		let freshMarkRemoving: any;

		try {
			const [pruning] = log.prune(new Map([[entry.hash, { entry, leaders }]]), {
				timeout: 2_000,
			});
			const pruningOutcome = pruning.then(
				() => ({ status: "fulfilled" as const }),
				(error: unknown) => ({ status: "rejected" as const, error }),
			);
			const oldCoordinator = log._checkedPrune;
			const pending = oldCoordinator.getPendingDelete(entry.hash);
			expect(pending).to.exist;
			await pending.resolve(remoteHash);
			await plannerStarted.promise;

			log.poisonReplicationOwnership(new Error("forced ownership poison"));
			await db.close();
			await session.peers[0].open(db, {
				args: { replicate: false, timeUntilRoleMaturity: 0 },
			});
			const freshCoordinator = log._checkedPrune;
			expect(freshCoordinator).to.not.equal(oldCoordinator);
			const freshPendingBeforeRelease = freshCoordinator.getPendingDelete(
				entry.hash,
			);
			freshMarkRemoving = sinon.spy(freshCoordinator, "markRemoving");

			releasePlanner.resolve();
			const outcome = await pruningOutcome;
			// Terminal close deliberately settles old pending prunes so callers do
			// not hang; the important boundary is that the resumed callback cannot
			// perform the delete or touch the newly opened coordinator.
			expect(outcome.status).to.equal("fulfilled");
			await delay(25);
			expect(remove.called).to.be.false;
			expect(freshMarkRemoving.called).to.be.false;
			expect(freshCoordinator.getPendingDelete(entry.hash)).to.equal(
				freshPendingBeforeRelease,
			);
		} finally {
			releasePlanner.resolve();
			waitForReplicators.restore();
			getClampedReplicas.restore();
			send.restore();
			findLeaders.restore();
			remove.restore();
			freshMarkRemoving?.restore();
			log._replicationRangeMutationFailure = undefined;
		}
	});

	it("settles a successful admitted prune without a post-remove poison rejection", async () => {
		session = await TestSession.disconnected(2);
		const db = await session.peers[0].open(new EventStore(), {
			args: { replicate: 1, timeUntilRoleMaturity: 0 },
		});
		const log = db.log as any;
		const { entry } = await db.add("prune-post-remove-race");
		const remoteHash = session.peers[1].identity.publicKey.hashcode();
		const leaders = new Map([[remoteHash, { intersecting: true }]]);
		const removeStarted = pDefer<void>();
		const releaseRemove = pDefer<void>();
		const waitForReplicators = sinon
			.stub(log, "_waitForEntryReplicators")
			.resolves(true);
		const getClampedReplicas = sinon
			.stub(log, "getClampedReplicas")
			.returns({ getValue: () => 1 });
		const send = sinon.stub(log.rpc, "send").resolves();
		const findLeaders = sinon
			.stub(log, "findLeadersFromEntry")
			.resolves(leaders);
		const remove = sinon.stub(log.log, "remove").callsFake(async () => {
			removeStarted.resolve();
			await releaseRemove.promise;
		});
		const unhandledRejections: unknown[] = [];
		const onUnhandledRejection = (reason: unknown) => {
			unhandledRejections.push(reason);
		};
		process.on("unhandledRejection", onUnhandledRejection);

		try {
			const [pruning] = log.prune(new Map([[entry.hash, { entry, leaders }]]), {
				timeout: 2_000,
			});
			const pending = log._checkedPrune.getPendingDelete(entry.hash);
			expect(pending).to.exist;
			await pending.resolve(remoteHash);
			await removeStarted.promise;

			log.poisonReplicationOwnership(new Error("forced ownership poison"));
			releaseRemove.resolve();

			await pruning;
			await delay(25);
			expect(remove.calledOnce).to.be.true;
			expect(log._checkedPrune.getPendingDelete(entry.hash)).to.be.undefined;
			expect(unhandledRejections).to.deep.equal([]);
		} finally {
			process.removeListener("unhandledRejection", onUnhandledRejection);
			releaseRemove.resolve();
			waitForReplicators.restore();
			getClampedReplicas.restore();
			send.restore();
			findLeaders.restore();
			remove.restore();
			log._replicationRangeMutationFailure = undefined;
		}
	});

	for (const unchecked of [false, true]) {
		it(`drains an admitted ${unchecked ? "unchecked" : "checked"} lower-log remove before reopen`, async () => {
			session = await TestSession.disconnected(2);
			const db = await session.peers[0].open(new EventStore(), {
				args: { replicate: 1, timeUntilRoleMaturity: 0 },
			});
			const log = db.log as any;
			const { entry } = await db.add(
				unchecked ? "unchecked-prune-drain" : "checked-prune-drain",
			);
			const remoteHash = session.peers[1].identity.publicKey.hashcode();
			const leaders = new Map([[remoteHash, { intersecting: true }]]);
			const removeStarted = pDefer<void>();
			const releaseRemove = pDefer<void>();
			const waitForReplicators = sinon
				.stub(log, "_waitForEntryReplicators")
				.resolves(true);
			const getClampedReplicas = sinon
				.stub(log, "getClampedReplicas")
				.returns({ getValue: () => 1 });
			const send = sinon.stub(log.rpc, "send").resolves();
			const findLeaders = sinon
				.stub(log, "findLeadersFromEntry")
				.resolves(leaders);
			const remove = sinon.stub(log.log, "remove").callsFake(async () => {
				removeStarted.resolve();
				await releaseRemove.promise;
			});

			try {
				const [pruning] = log.prune(
					new Map([[entry.hash, { entry, leaders }]]),
					{ timeout: 2_000, unchecked },
				);
				if (!unchecked) {
					const pending = log._checkedPrune.getPendingDelete(entry.hash);
					expect(pending).to.exist;
					await pending.resolve(remoteHash);
				}
				await removeStarted.promise;
				expect(log._admittedPruneRemoves.size).to.equal(1);

				log.poisonReplicationOwnership(new Error("forced ownership poison"));
				let closeSettled = false;
				const closing = db.close().finally(() => {
					closeSettled = true;
				});
				await delay(25);
				expect(closeSettled).to.be.false;

				releaseRemove.resolve();
				await Promise.all([pruning, closing]);
				expect(log._admittedPruneRemoves.size).to.equal(0);
				await session.peers[0].open(db, {
					args: { replicate: false, timeUntilRoleMaturity: 0 },
				});
				const callsAtReopen = remove.callCount;
				await delay(25);

				expect(callsAtReopen).to.equal(1);
				expect(remove.callCount).to.equal(callsAtReopen);
				expect(log._admittedPruneRemoves.size).to.equal(0);
			} finally {
				releaseRemove.resolve();
				waitForReplicators.restore();
				getClampedReplicas.restore();
				send.restore();
				findLeaders.restore();
				remove.restore();
				log._replicationRangeMutationFailure = undefined;
			}
		});
	}

	it("cancels pending maturity without emitting after ownership poison", async () => {
		const { log } = await openDisconnectedLog();
		const remoteKey = session.peers[1].identity.publicKey;
		const numbers = log.indexableDomain.numbers;
		const range = new log.indexableDomain.constructorRange({
			id: randomBytes(32),
			offset: numbers.denormalize(0.2),
			width: numbers.denormalize(0.3),
			publicKeyHash: remoteKey.hashcode(),
			timestamp: BigInt(Date.now()),
		});
		let matureEvents = 0;
		log.events.addEventListener("replicator:mature", () => {
			matureEvents += 1;
		});
		const rebalance = sinon.spy(log.replicationChangeDebounceFn, "add");

		try {
			log.schedulePendingMaturity(
				{ type: "added", range, timestamp: range.timestamp },
				remoteKey,
				{ rebalance: true, waitMs: 20 },
			);
			expect(log.pendingMaturity.size).to.equal(1);

			log.poisonReplicationOwnership(new Error("forced ownership poison"));
			expect(log.pendingMaturity.size).to.equal(0);
			await delay(50);

			expect(matureEvents).to.equal(0);
			expect(rebalance.called).to.be.false;
		} finally {
			rebalance.restore();
			log._replicationRangeMutationFailure = undefined;
		}
	});

	it("suppresses pending maturity while terminal close is still in progress", async () => {
		const { db, log } = await openDisconnectedLog();
		const remoteKey = session.peers[1].identity.publicKey;
		const numbers = log.indexableDomain.numbers;
		const range = new log.indexableDomain.constructorRange({
			id: randomBytes(32),
			offset: numbers.denormalize(0.2),
			width: numbers.denormalize(0.3),
			publicKeyHash: remoteKey.hashcode(),
			timestamp: BigInt(Date.now()),
		});
		const closeAnnouncementStarted = pDefer<void>();
		const releaseCloseAnnouncement = pDefer<void>();
		let matureEvents = 0;
		log.events.addEventListener("replicator:mature", () => {
			matureEvents += 1;
		});
		const rebalance = sinon.spy(log.replicationChangeDebounceFn, "add");
		const send = sinon
			.stub(log.rpc, "send")
			.callsFake(async (message: unknown) => {
				if (
					message instanceof AllReplicatingSegmentsMessage &&
					message.segments.length === 0
				) {
					closeAnnouncementStarted.resolve();
					await releaseCloseAnnouncement.promise;
				}
				return [] as any;
			});
		const ownershipLifecycleController = log._repairLifecycleController;

		try {
			log.schedulePendingMaturity(
				{ type: "added", range, timestamp: range.timestamp },
				remoteKey,
				{ rebalance: true, waitMs: 20 },
			);
			expect(log.pendingMaturity.size).to.equal(1);

			const closing = db.close();
			await closeAnnouncementStarted.promise;
			expect(ownershipLifecycleController.signal.aborted).to.be.true;
			await delay(50);

			expect(matureEvents).to.equal(0);
			expect(rebalance.called).to.be.false;
			expect(log.pendingMaturity.size).to.equal(0);

			releaseCloseAnnouncement.resolve();
			await closing;
		} finally {
			releaseCloseAnnouncement.resolve();
			rebalance.restore();
			send.restore();
			log._replicationRangeMutationFailure = undefined;
		}
	});

	it("reconciles committed timestamp rows, poisons, and rejects a queued follower", async () => {
		const { log, replicationIndex } = await openDisconnectedLog(1);
		const numbers = log.indexableDomain.numbers;
		const selfKey = session.peers[0].identity.publicKey;
		const ranges = [0.1, 0.6].map(
			(offset, index) =>
				new log.indexableDomain.constructorRange({
					id: new Uint8Array([index + 1, 42]),
					offset: numbers.denormalize(offset),
					width: numbers.denormalize(0.2),
					publicKeyHash: selfKey.hashcode(),
					timestamp: 1n,
				}),
		);
		for (const range of ranges) {
			await replicationIndex.put(range);
		}
		const durableFailure = new Error("forced timestamp commit-then-throw");
		const originalPut = replicationIndex.put.bind(replicationIndex);
		const put = sinon.stub(replicationIndex, "put").callsFake((async (
			value: any,
			options?: any,
		) => {
			const result = await originalPut(value, options);
			if (value.idString === ranges[1].idString && value.timestamp === 9n) {
				throw durableFailure;
			}
			return result;
		}) as any);
		const nativePut = sinon.stub(log, "putNativeReplicationRange");

		try {
			const first = log.updateTimestampOfOwnedReplicationRanges(9);
			const follower = log.updateTimestampOfOwnedReplicationRanges(10);
			const [firstResult, followerResult] = await Promise.allSettled([
				first,
				follower,
			]);
			expect(firstResult.status).to.equal("rejected");
			expect(followerResult.status).to.equal("rejected");
			expect((firstResult as PromiseRejectedResult).reason).to.be.instanceOf(
				AggregateError,
			);
			expect((firstResult as PromiseRejectedResult).reason.errors).to.include(
				durableFailure,
			);
			expect(
				(followerResult as PromiseRejectedResult).reason.message,
			).to.contain("Replication ownership recovery is required");
			const durable = await replicationIndex
				.iterate({ query: { hash: selfKey.hashcode() } })
				.all();
			expect(
				durable.map((result: any) => result.value.timestamp),
			).to.deep.equal([9n, 9n]);
			expect(
				nativePut.calledWithMatch(
					sinon.match.has("idString", ranges[0].idString),
				),
			).to.be.true;
			expect(
				nativePut.calledWithMatch(
					sinon.match.has("idString", ranges[1].idString),
				),
			).to.be.true;
			expect(log._replicationRangeMutationFailure).to.equal(
				(firstResult as PromiseRejectedResult).reason,
			);
		} finally {
			put.restore();
			nativePut.restore();
			log._replicationRangeMutationFailure = undefined;
		}
	});

	it("poisons and reconciles timestamp state after a native publication failure", async () => {
		const { log, replicationIndex } = await openDisconnectedLog(1);
		const numbers = log.indexableDomain.numbers;
		const selfKey = session.peers[0].identity.publicKey;
		const range = new log.indexableDomain.constructorRange({
			id: new Uint8Array([7, 42]),
			offset: numbers.denormalize(0.25),
			width: numbers.denormalize(0.2),
			publicKeyHash: selfKey.hashcode(),
			timestamp: 1n,
		});
		await replicationIndex.put(range);
		const nativeFailure = new Error("forced timestamp native failure");
		const nativePut = sinon
			.stub(log, "putNativeReplicationRange")
			.onFirstCall()
			.throws(nativeFailure);

		try {
			let observed: any;
			try {
				await log.updateTimestampOfOwnedReplicationRanges(12);
			} catch (error) {
				observed = error;
			}
			expect(observed).to.be.instanceOf(AggregateError);
			expect(observed.errors).to.include(nativeFailure);
			expect(log._replicationRangeMutationFailure).to.equal(observed);
			expect(nativePut.callCount).to.equal(2);
			const durable = await replicationIndex
				.iterate({ query: { hash: selfKey.hashcode() } })
				.all();
			expect(durable[0].value.timestamp).to.equal(12n);
		} finally {
			nativePut.restore();
			log._replicationRangeMutationFailure = undefined;
		}
	});

	it("cancels delayed owned-range maturity after ownership poison", async () => {
		const { log, replicationIndex } = await openDisconnectedLog(1);
		const numbers = log.indexableDomain.numbers;
		const selfKey = session.peers[0].identity.publicKey;
		const range = new log.indexableDomain.constructorRange({
			id: new Uint8Array([8, 42]),
			offset: numbers.denormalize(0.35),
			width: numbers.denormalize(0.2),
			publicKeyHash: selfKey.hashcode(),
			timestamp: 1n,
		});
		await replicationIndex.put(range);
		const minRoleAge = sinon.stub(log, "getDefaultMinRoleAge").resolves(30);
		let matureEvents = 0;
		log.events.addEventListener("replicator:mature", () => {
			matureEvents += 1;
		});

		try {
			await log.updateTimestampOfOwnedReplicationRanges(13);
			expect(log._repairRetryTimers.size).to.equal(1);
			log.poisonReplicationOwnership(new Error("forced ownership poison"));
			expect(log._repairRetryTimers.size).to.equal(0);
			await delay(60);
			expect(matureEvents).to.equal(0);
		} finally {
			minRoleAge.restore();
			log._replicationRangeMutationFailure = undefined;
		}
	});

	it("observes an in-flight replication debounce rejection after poison", async () => {
		const { log } = await openDisconnectedLog();
		const remoteKey = session.peers[1].identity.publicKey;
		const numbers = log.indexableDomain.numbers;
		const range = new log.indexableDomain.constructorRange({
			id: randomBytes(32),
			offset: numbers.denormalize(0.2),
			width: numbers.denormalize(0.3),
			publicKeyHash: remoteKey.hashcode(),
			timestamp: 1n,
		});
		const callbackStarted = pDefer<void>();
		const releaseCallback = pDefer<void>();
		const callbackFailure = new Error("forced debounced callback failure");
		const onReplicationChange = sinon
			.stub(log, "onReplicationChange")
			.callsFake(async () => {
				callbackStarted.resolve();
				await releaseCallback.promise;
				throw callbackFailure;
			});
		const unhandledRejections: unknown[] = [];
		const onUnhandledRejection = (reason: unknown) => {
			unhandledRejections.push(reason);
		};
		process.on("unhandledRejection", onUnhandledRejection);

		try {
			void log.replicationChangeDebounceFn.add({
				range,
				type: "removed",
				timestamp: 1n,
			});
			await callbackStarted.promise;
			log.poisonReplicationOwnership(new Error("forced ownership poison"));
			releaseCallback.resolve();
			await delay(25);

			expect(onReplicationChange.calledOnce).to.be.true;
			expect(unhandledRejections).to.deep.equal([]);
		} finally {
			process.removeListener("unhandledRejection", onUnhandledRejection);
			releaseCallback.resolve();
			onReplicationChange.restore();
			log._replicationRangeMutationFailure = undefined;
		}
	});

	it("replicator:(join|leave)", async () => {
		// Joining now includes a targeted replication-info handshake which can race on
		// slower CI machines. Use waitForResolved instead of a fixed delay.
		session = await TestSession.connected(2);

		let db1JoinEvents: string[] = [];
		let db1LeaveEvents: string[] = [];

		const db1a = await session.peers[0].open(new EventStore(), {
			args: { replicate: 1 },
		});
		db1a.log.events.addEventListener("replicator:join", (event) => {
			db1JoinEvents.push(event.detail.publicKey.hashcode());
		});

		db1a.log.events.addEventListener("replicator:leave", (event) => {
			db1LeaveEvents.push(event.detail.publicKey.hashcode());
		});

		const db1b = await session.peers[0].open(new EventStore(), {
			args: { replicate: 1 },
		});

		const db2a = await session.peers[1].open(db1a.clone(), {
			args: { replicate: 0.6 },
		});

		const db2b = await session.peers[1].open(db1b.clone(), {
			args: { replicate: 0.4 },
		});
		await waitForResolved(
			() =>
				expect(db1JoinEvents).to.have.members([
					session.peers[1].identity.publicKey.hashcode(),
				]),
			{ timeout: 20_000 },
		);

		await db2a.close();
		await db2b.close();

		// try open another db and make sure it does not trigger join event to db1
		await waitForResolved(
			() =>
				expect(db1LeaveEvents).to.have.members([
					session.peers[1].identity.publicKey.hashcode(),
				]),
			{ timeout: 20_000 },
		);
		expect(db1JoinEvents).to.have.length(1); // no new join event
	});

	it("cleans prune response tracking on unsubscribe", async () => {
		session = await TestSession.connected(2);

		const db1 = await session.peers[0].open(new EventStore(), {
			args: { replicate: 1 },
		});

		const disconnectedPublicKey = session.peers[1].identity.publicKey;
		const disconnectedPeerHash = disconnectedPublicKey.hashcode();
		const entryHash = uuid();

		const responseMap = (db1.log as any)[
			"_requestIPruneResponseReplicatorSet"
		] as Map<string, Set<string>>;
		responseMap.set(entryHash, new Set([disconnectedPeerHash]));

		await db1.log.handleSubscriptionChange(
			disconnectedPublicKey,
			[db1.log.topic],
			false,
		);

		expect(responseMap.get(entryHash)).to.be.undefined;
	});

	it("accepts structural public keys when removing replicators", async () => {
		session = await TestSession.connected(2);

		const db1 = await session.peers[0].open(new EventStore(), {
			args: { replicate: 1 },
		});
		const peerKey = session.peers[1].identity.publicKey as PublicSignKey & {
			publicKey?: Uint8Array;
		};
		const foreignKey = {
			publicKey: peerKey.publicKey,
			hashcode: () => peerKey.hashcode(),
			toString: () => peerKey.toString(),
			get bytes() {
				return peerKey.bytes;
			},
		} as unknown as PublicSignKey;
		const log = db1.log as any;
		const numbers = log.indexableDomain.numbers;
		await log.replicationIndex.put(
			new log.indexableDomain.constructorRange({
				id: randomBytes(32),
				offset: numbers.denormalize(0.1),
				width: numbers.denormalize(0.2),
				publicKeyHash: peerKey.hashcode(),
				timestamp: 1n,
			}),
		);
		log.uniqueReplicators.add(peerKey.hashcode());

		const changes: string[] = [];
		db1.log.events.addEventListener("replication:change", (event) => {
			changes.push(event.detail.publicKey.hashcode());
		});

		await (
			db1.log as unknown as {
				removeReplicator(key: PublicSignKey): Promise<void>;
			}
		).removeReplicator(foreignKey);

		expect(changes).to.deep.equal([peerKey.hashcode()]);
	});

	it("does not publish deletion or leave effects for retained owner ranges", async () => {
		const { log, replicationIndex } = await openDisconnectedLog();
		const remoteKey = session.peers[1].identity.publicKey;
		const remoteHash = remoteKey.hashcode();
		const numbers = log.indexableDomain.numbers;
		const ranges = [0.1, 0.6].map(
			(offset) =>
				new log.indexableDomain.constructorRange({
					id: randomBytes(32),
					offset: numbers.denormalize(offset),
					width: numbers.denormalize(0.2),
					publicKeyHash: remoteHash,
					timestamp: 1n,
				}),
		);
		for (const range of ranges) {
			await replicationIndex.put(range);
		}
		log.uniqueReplicators.add(remoteHash);
		const deletionFailure = new Error("forced retained deletion");
		const deletion = sinon
			.stub(log, "deleteReplicationRangesCoherently")
			.resolves({
				removed: [],
				retained: ranges,
				ownerHasRanges: true,
				error: deletionFailure,
			});
		const rebalance = sinon
			.stub(log.replicationChangeDebounceFn, "add")
			.resolves();
		const onRemoved = sinon.spy();
		const changes: string[] = [];
		log.events.addEventListener("replication:change", (event: any) => {
			changes.push(event.detail.publicKey.hashcode());
		});

		try {
			await expect(
				log.removeReplicator(remoteKey, { onRemoved }),
			).to.be.rejectedWith(deletionFailure.message);
			expect(changes).to.deep.equal([]);
			expect(rebalance.called).to.be.false;
			expect(onRemoved.called).to.be.false;
		} finally {
			deletion.restore();
			rebalance.restore();
		}
	});

	it("publishes only confirmed partial deletions without a full-owner leave", async () => {
		const { log, replicationIndex } = await openDisconnectedLog();
		const remoteKey = session.peers[1].identity.publicKey;
		const remoteHash = remoteKey.hashcode();
		const numbers = log.indexableDomain.numbers;
		const ranges = [0.1, 0.6].map(
			(offset) =>
				new log.indexableDomain.constructorRange({
					id: randomBytes(32),
					offset: numbers.denormalize(offset),
					width: numbers.denormalize(0.2),
					publicKeyHash: remoteHash,
					timestamp: 1n,
				}),
		);
		for (const range of ranges) {
			await replicationIndex.put(range);
		}
		log.uniqueReplicators.add(remoteHash);
		const deletionFailure = new Error("forced partial deletion");
		const deletion = sinon
			.stub(log, "deleteReplicationRangesCoherently")
			.resolves({
				removed: [ranges[0]],
				retained: [ranges[1]],
				ownerHasRanges: true,
				error: deletionFailure,
			});
		const rebalance = sinon
			.stub(log.replicationChangeDebounceFn, "add")
			.resolves();
		const onRemoved = sinon.spy();
		const changes: string[] = [];
		log.events.addEventListener("replication:change", (event: any) => {
			changes.push(event.detail.publicKey.hashcode());
		});

		try {
			await expect(
				log.removeReplicator(remoteKey, { onRemoved }),
			).to.be.rejectedWith(deletionFailure.message);
			expect(changes).to.deep.equal([remoteHash]);
			expect(rebalance.calledOnce).to.be.true;
			expect(rebalance.firstCall.args[0]).to.include({
				range: ranges[0],
				type: "removed",
			});
			expect(onRemoved.called).to.be.false;
		} finally {
			deletion.restore();
			rebalance.restore();
		}
	});

	it("fences a reconnect until an in-flight unsubscribe removal is coherent", async () => {
		session = await TestSession.connected(2);

		const db1 = await session.peers[0].open(new EventStore(), {
			args: { replicate: 1, timeUntilRoleMaturity: 0 },
		});
		await session.peers[1].open(db1.clone(), {
			args: { replicate: 1, timeUntilRoleMaturity: 0 },
		});

		const remoteKey = session.peers[1].identity.publicKey;
		const remoteHash = remoteKey.hashcode();
		const log = db1.log as any;
		const replicationIndex = db1.log.replicationIndex as any;
		await waitForResolved(async () => {
			expect(
				await replicationIndex
					.iterate({ query: { hash: remoteHash } })
					.all(),
			).to.have.length.greaterThan(0);
			expect(db1.log.uniqueReplicators.has(remoteHash)).to.be.true;
		});

		const deleteStarted = pDefer<void>();
		const releaseDelete = pDefer<void>();
		const originalDel = replicationIndex.del.bind(replicationIndex);
		let blockNextRemovalDelete = true;
		const del = sinon.stub(replicationIndex, "del").callsFake((async (
			query: any,
			options?: any,
		) => {
			const ownerScopedDelete =
				query?.query?.hash === remoteHash ||
				query?.query?.and?.some(
					(part: any) =>
						part?.value === remoteHash && part?.key?.includes("hash"),
				);
			if (blockNextRemovalDelete && ownerScopedDelete) {
				blockNextRemovalDelete = false;
				deleteStarted.resolve();
				await releaseDelete.promise;
			}
			return originalDel(query, options);
		}) as any);
		const disconnected = sinon.spy(log.syncronizer, "onPeerDisconnected");
		const leaves: string[] = [];
		db1.log.events.addEventListener("replicator:leave", (event) => {
			leaves.push(event.detail.publicKey.hashcode());
		});

		try {
			const unsubscribeEvent = {
				detail: { from: remoteKey, topics: [db1.log.topic] },
			} as any;
			const subscribeEvent = {
				detail: { from: remoteKey, topics: [db1.log.topic] },
			} as any;

			const oldUnsubscribe = log._onUnsubscription(unsubscribeEvent);
			await deleteStarted.promise;

			// The old removal has crossed into its destructive queue item. Reconnect
			// must stay blocked behind it instead of creating state midway through.
			let reconnectSettled = false;
			const reconnect = log._onSubscription(subscribeEvent).finally(() => {
				reconnectSettled = true;
			});
			await Promise.resolve();
			expect(reconnectSettled).to.be.false;
			expect(log._replicationInfoBlockedPeers.has(remoteHash)).to.be.true;

			releaseDelete.resolve();
			await Promise.all([oldUnsubscribe, reconnect]);
			await waitForResolved(async () => {
				expect(
					await replicationIndex
						.iterate({ query: { hash: remoteHash } })
						.all(),
				).to.have.length.greaterThan(0);
				expect(log.uniqueReplicators.has(remoteHash)).to.be.true;
				expect(log._replicatorJoinEmitted.has(remoteHash)).to.be.true;
			});

			const gid = "reconnected-generation";
			log._peerSyncCapabilities.set(remoteHash, 7);
			log._gidPeersHistory.set(gid, new Set([remoteHash]));

			expect(log._peerSyncCapabilities.get(remoteHash)).to.equal(7);
			expect(log._replicatorLastActivityAt.has(remoteHash)).to.be.true;
			expect(log._gidPeersHistory.get(gid)?.has(remoteHash)).to.be.true;
			expect(log._replicationInfoBlockedPeers.has(remoteHash)).to.be.false;
			expect(disconnected.calledOnceWith(remoteHash)).to.be.true;
			expect(leaves).to.deep.equal([]);

			// A later unsubscribe still owns the current epoch and must perform the
			// complete removal, including sync-related cleanup and one leave event.
			await log._onUnsubscription(unsubscribeEvent);
			expect(
				await replicationIndex
					.iterate({ query: { hash: remoteHash } })
					.all(),
			).to.have.length(0);
			expect(log.uniqueReplicators.has(remoteHash)).to.be.false;
			expect(log._replicatorJoinEmitted.has(remoteHash)).to.be.false;
			expect(log._peerSyncCapabilities.has(remoteHash)).to.be.false;
			expect(log._replicatorLastActivityAt.has(remoteHash)).to.be.false;
			expect(log._gidPeersHistory.has(gid)).to.be.false;
			expect(disconnected.callCount).to.equal(2);
			expect(disconnected.alwaysCalledWith(remoteHash)).to.be.true;
			expect(leaves).to.deep.equal([remoteHash]);
		} finally {
			del.restore();
			releaseDelete.resolve();
			disconnected.restore();
		}
	});

	it("does not let a superseded reconnect reopen a peer", async () => {
		session = await TestSession.connected(2);

		const db1 = await session.peers[0].open(new EventStore(), {
			args: { replicate: 1, timeUntilRoleMaturity: 0 },
		});
		await session.peers[1].open(db1.clone(), {
			args: { replicate: 1, timeUntilRoleMaturity: 0 },
		});

		const remoteKey = session.peers[1].identity.publicKey;
		const remoteHash = remoteKey.hashcode();
		const log = db1.log as any;
		const replicationIndex = db1.log.replicationIndex as any;
		await waitForResolved(async () => {
			expect(
				await replicationIndex
					.iterate({ query: { hash: remoteHash } })
					.all(),
			).to.have.length.greaterThan(0);
			expect(db1.log.uniqueReplicators.has(remoteHash)).to.be.true;
		});

		const deleteStarted = pDefer<void>();
		const releaseDelete = pDefer<void>();
		const originalDel = replicationIndex.del.bind(replicationIndex);
		let blockNextRemovalDelete = true;
		const del = sinon.stub(replicationIndex, "del").callsFake((async (
			query: any,
			options?: any,
		) => {
			const ownerScopedDelete =
				query?.query?.hash === remoteHash ||
				query?.query?.and?.some(
					(part: any) =>
						part?.value === remoteHash && part?.key?.includes("hash"),
				);
			if (blockNextRemovalDelete && ownerScopedDelete) {
				blockNextRemovalDelete = false;
				deleteStarted.resolve();
				await releaseDelete.promise;
			}
			return originalDel(query, options);
		}) as any);
		const unblock = sinon.spy(log._replicationInfoBlockedPeers, "delete");
		const scheduleRequests = sinon.spy(log, "scheduleReplicationInfoRequests");
		const disconnected = sinon.spy(log.syncronizer, "onPeerDisconnected");
		const leaves: string[] = [];
		db1.log.events.addEventListener("replicator:leave", (event) => {
			leaves.push(event.detail.publicKey.hashcode());
		});

		const unsubscribeEvent = {
			detail: { from: remoteKey, topics: [db1.log.topic] },
		} as any;
		const subscribeEvent = {
			detail: { from: remoteKey, topics: [db1.log.topic] },
		} as any;

		try {
			const firstUnsubscribe = log._onUnsubscription(unsubscribeEvent);
			await deleteStarted.promise;
			const supersededSubscribe = log._onSubscription(subscribeEvent);
			await Promise.resolve();
			const winningUnsubscribe = log._onUnsubscription(unsubscribeEvent);
			await Promise.resolve();
			expect(log._replicationInfoBlockedPeers.has(remoteHash)).to.be.true;

			releaseDelete.resolve();
			await Promise.all([
				firstUnsubscribe,
				supersededSubscribe,
				winningUnsubscribe,
			]);

			expect(
				await replicationIndex
					.iterate({ query: { hash: remoteHash } })
					.all(),
			).to.have.length(0);
			expect(log.uniqueReplicators.has(remoteHash)).to.be.false;
			expect(log._replicatorJoinEmitted.has(remoteHash)).to.be.false;
			expect(log._replicationInfoBlockedPeers.has(remoteHash)).to.be.true;
			expect(unblock.neverCalledWith(remoteHash)).to.be.true;
			expect(scheduleRequests.notCalled).to.be.true;
			expect(disconnected.callCount).to.equal(2);
			expect(disconnected.alwaysCalledWith(remoteHash)).to.be.true;
			expect(leaves).to.deep.equal([remoteHash]);
		} finally {
			del.restore();
			unblock.restore();
			scheduleRequests.restore();
			releaseDelete.resolve();
			disconnected.restore();
		}
	});

	it("cleans old sync state when reconnect supersedes a queued removal", async () => {
		session = await TestSession.connected(2);

		const db1 = await session.peers[0].open(new EventStore(), {
			args: { replicate: 1, timeUntilRoleMaturity: 0 },
		});
		await session.peers[1].open(db1.clone(), {
			args: { replicate: 1, timeUntilRoleMaturity: 0 },
		});

		const remoteKey = session.peers[1].identity.publicKey;
		const remoteHash = remoteKey.hashcode();
		const log = db1.log as any;
		const replicationIndex = db1.log.replicationIndex as any;
		await waitForResolved(async () => {
			expect(
				await replicationIndex.count({ query: { hash: remoteHash } }),
			).to.be.greaterThan(0);
		});

		const blockerStarted = pDefer<void>();
		const releaseBlocker = pDefer<void>();
		const blocker = log.withReplicationInfoApplyQueue(
			remoteHash,
			async () => {
				blockerStarted.resolve();
				await releaseBlocker.promise;
			},
		);
		await blockerStarted.promise;

		const cleanupStarted = pDefer<void>();
		const releaseCleanup = pDefer<void>();
		const originalDisconnect =
			log.syncronizer.onPeerDisconnected.bind(log.syncronizer);
		const disconnected = sinon
			.stub(log.syncronizer, "onPeerDisconnected")
			.callsFake(async (...args: unknown[]) => {
				const peerHash = args[0] as string;
				cleanupStarted.resolve();
				await releaseCleanup.promise;
				return originalDisconnect(peerHash);
			});
		const leaves: string[] = [];
		db1.log.events.addEventListener("replicator:leave", (event) => {
			leaves.push(event.detail.publicKey.hashcode());
		});
		const gid = "stale-before-reconnect";
		log._peerSyncCapabilities.set(remoteHash, 7);
		log._gidPeersHistory.set(gid, new Set([remoteHash]));

		try {
			const unsubscribe = log._onUnsubscription({
				detail: { from: remoteKey, topics: [db1.log.topic] },
			});
			let reconnectSettled = false;
			const reconnect = log
				._onSubscription({
					detail: { from: remoteKey, topics: [db1.log.topic] },
				})
					.finally(() => {
						reconnectSettled = true;
					});
			releaseBlocker.resolve();
			await cleanupStarted.promise;
			expect(log._receiveCleanupGateByPeer.has(remoteHash)).to.be.true;
			await db1.log.onMessage(new SyncCapabilitiesMessage(), {
				from: remoteKey,
			} as any);
			expect(
				log._openingSyncCapabilitiesByPeer.get(remoteHash)?.capabilities,
			).to.equal(1);
			await Promise.resolve();
			expect(reconnectSettled).to.be.false;
			releaseCleanup.resolve();
			await Promise.all([blocker, unsubscribe, reconnect]);

			// The stale removal must preserve current membership, but its ordered
			// disconnect cleanup must run before reconnect starts using the lane.
			expect(
				await replicationIndex.count({ query: { hash: remoteHash } }),
			).to.be.greaterThan(0);
			expect(db1.log.uniqueReplicators.has(remoteHash)).to.be.true;
			expect(log._peerSyncCapabilities.get(remoteHash)).to.equal(1);
			expect(log._openingSyncCapabilitiesByPeer.has(remoteHash)).to.be.false;
			expect(log._gidPeersHistory.has(gid)).to.be.false;
			expect(disconnected.calledOnceWith(remoteHash)).to.be.true;
			expect(leaves).to.deep.equal([]);
		} finally {
			releaseBlocker.resolve();
			releaseCleanup.resolve();
			disconnected.restore();
		}
	});

	it("does not apply a replication message superseded while the synchronizer yields", async () => {
		session = await TestSession.connected(2);

		const db1 = await session.peers[0].open(new EventStore(), {
			args: { replicate: 1, timeUntilRoleMaturity: 0 },
		});
		await session.peers[1].open(db1.clone(), {
			args: { replicate: 1, timeUntilRoleMaturity: 0 },
		});

		const remoteKey = session.peers[1].identity.publicKey;
		const remoteHash = remoteKey.hashcode();
		const log = db1.log as any;
		const replicationIndex = db1.log.replicationIndex as any;
		let remoteRange: any;
		await waitForResolved(async () => {
			const ranges = await replicationIndex
				.iterate({ query: { hash: remoteHash } })
				.all();
			expect(ranges).to.have.length.greaterThan(0);
			remoteRange = ranges[0].value;
		});

		await log.removeReplicator(remoteKey, { noEvent: true });
		expect(
			await replicationIndex.count({ query: { hash: remoteHash } }),
		).to.equal(0);

		const delayedMessage = new AllReplicatingSegmentsMessage({
			segments: [remoteRange.toReplicationRange()],
		});
		const synchronizerEntered = pDefer<void>();
		const releaseSynchronizer = pDefer<void>();
		const originalSynchronizerOnMessage =
			log.syncronizer.onMessage.bind(log.syncronizer);
		const synchronizer = sinon
			.stub(log.syncronizer, "onMessage")
			.callsFake(async (message: unknown, context: unknown) => {
				if (message === delayedMessage) {
					synchronizerEntered.resolve();
					await releaseSynchronizer.promise;
					return false;
				}
				return originalSynchronizerOnMessage(message, context);
			});
		const scheduleRequests = sinon
			.stub(log, "scheduleReplicationInfoRequests")
			.callsFake(() => {});

		try {
			const delayedReceive = db1.log.onMessage(delayedMessage, {
				from: remoteKey,
				message: { header: { timestamp: BigInt(Date.now()) } },
			} as any);
			await synchronizerEntered.promise;

			const unsubscribe = log._onUnsubscription({
				detail: { from: remoteKey, topics: [db1.log.topic] },
			});
			releaseSynchronizer.resolve();
			await Promise.all([delayedReceive, unsubscribe]);
			await log._onSubscription({
				detail: { from: remoteKey, topics: [db1.log.topic] },
			});
			expect(
				await replicationIndex.count({ query: { hash: remoteHash } }),
			).to.equal(0);
		} finally {
			releaseSynchronizer.resolve();
			synchronizer.restore();
			scheduleRequests.restore();
		}
	});

	it("scopes replication timestamps to a reconnect generation", async () => {
		session = await TestSession.connected(2);

		const db1 = await session.peers[0].open(new EventStore(), {
			args: { replicate: 1, timeUntilRoleMaturity: 0 },
		});
		await session.peers[1].open(db1.clone(), {
			args: { replicate: 1, timeUntilRoleMaturity: 0 },
		});

		const remoteKey = session.peers[1].identity.publicKey;
		const remoteHash = remoteKey.hashcode();
		const log = db1.log as any;
		const replicationIndex = db1.log.replicationIndex as any;
		let remoteRange: any;
		await waitForResolved(async () => {
			const ranges = await replicationIndex
				.iterate({ query: { hash: remoteHash } })
				.all();
			expect(ranges).to.have.length.greaterThan(0);
			remoteRange = ranges[0].value;
		});
		const scheduleRequests = sinon
			.stub(log, "scheduleReplicationInfoRequests")
			.callsFake(() => {});

		try {
			await log._onUnsubscription({
				detail: { from: remoteKey, topics: [db1.log.topic] },
			});
			// Successful cleanup retires both the old receive generation and its
			// sender-clock watermark; the unsubscribe fence rejects late traffic.
			expect(log.latestReplicationInfoMessage.has(remoteHash)).to.be.false;

			await log._onSubscription({
				detail: { from: remoteKey, topics: [db1.log.topic] },
			});
			expect(log.latestReplicationInfoMessage.has(remoteHash)).to.be.false;

			await db1.log.onMessage(
				new AllReplicatingSegmentsMessage({
					segments: [remoteRange.toReplicationRange()],
				}),
				{
					from: remoteKey,
					// Simulate a sender whose wall clock trails this receiver.
					message: { header: { timestamp: 1n } },
				} as any,
			);

			expect(
				await replicationIndex.count({ query: { hash: remoteHash } }),
			).to.be.greaterThan(0);
			expect(log.latestReplicationInfoMessage.get(remoteHash)).to.equal(1n);
		} finally {
			scheduleRequests.restore();
		}
	});

	it("ignores a stopped-segment message older than the latest snapshot", async () => {
		session = await TestSession.connected(2);

		const db1 = await session.peers[0].open(new EventStore(), {
			args: { replicate: 1, timeUntilRoleMaturity: 0 },
		});
		await session.peers[1].open(db1.clone(), {
			args: { replicate: 1, timeUntilRoleMaturity: 0 },
		});

		const remoteKey = session.peers[1].identity.publicKey;
		const remoteHash = remoteKey.hashcode();
		const log = db1.log as any;
		const replicationIndex = db1.log.replicationIndex as any;
		let remoteRange: any;
		await waitForResolved(async () => {
			const ranges = await replicationIndex
				.iterate({ query: { hash: remoteHash } })
				.all();
			expect(ranges).to.have.length.greaterThan(0);
			remoteRange = ranges[0].value;
		});

		log.latestReplicationInfoMessage.delete(remoteHash);
		const newerTimestamp = BigInt(Date.now() + 1_000);
		await db1.log.onMessage(
			new AllReplicatingSegmentsMessage({
				segments: [remoteRange.toReplicationRange()],
			}),
			{
				from: remoteKey,
				message: { header: { timestamp: newerTimestamp } },
			} as any,
		);
		await db1.log.onMessage(
			new StoppedReplicating({ segmentIds: [remoteRange.id] }),
			{
				from: remoteKey,
				message: { header: { timestamp: newerTimestamp - 1n } },
			} as any,
		);

		expect(
			await replicationIndex.count({ query: { hash: remoteHash } }),
		).to.be.greaterThan(0);
		expect(log.latestReplicationInfoMessage.get(remoteHash)).to.equal(
			newerTimestamp,
		);
	});

	it("drains admitted replication mutations before close and reopen", async () => {
		session = await TestSession.connected(1);
		const db = await session.peers[0].open(new EventStore(), {
			args: { replicate: 1, timeUntilRoleMaturity: 0 },
		});
		const log = db.log as any;
		const mutationStarted = pDefer<void>();
		const releaseMutation = pDefer<void>();
		const mutation = log.withReplicationInfoApplyQueue(
			"synthetic-remote-peer",
			async () => {
				mutationStarted.resolve();
				await releaseMutation.promise;
			},
		);
		await mutationStarted.promise;

		let closeSettled = false;
		const close = db.close().finally(() => {
			closeSettled = true;
		});
		await delay(25);
		expect(closeSettled).to.be.false;

		releaseMutation.resolve();
		await Promise.all([mutation, close]);
		await session.peers[0].open(db);
		expect(log._replicationInfoApplyQueueByPeer.size).to.equal(0);
	});

	it("replicate:join not emitted on update", async () => {
		session = await TestSession.connected(2);

		const store = new EventStore();
		let db1JoinEvents: string[] = [];
		const store1 = await session.peers[0].open(store, {
			args: {
				replicate: { factor: 1 },
			},
		});
		store1.log.events.addEventListener("replicator:join", (event) => {
			db1JoinEvents.push(event.detail.publicKey.hashcode());
		});

		const store2 = await session.peers[1].open(store.clone(), {
			args: {
				replicate: { factor: 1 },
			},
		});
		await waitForResolved(() =>
			expect(db1JoinEvents).to.have.members([
				session.peers[1].identity.publicKey.hashcode(),
			]),
		);

		await store2.log.replicate({ factor: 0.5 }, { reset: true });

		await waitForResolved(async () => {
			const store2Role = await store1.log.replicationIndex
				.iterate({ query: { hash: store2.node.identity.publicKey.hashcode() } })
				.all();
			expect(store2Role).to.have.length(1);
			expect(store2Role[0].value.widthNormalized).to.be.closeTo(0.5, 0.01);
		});

		expect(db1JoinEvents).to.have.members([
			session.peers[1].identity.publicKey.hashcode(),
		]); // no new join events
	});

	it("replicator:mature not emitted more than once on update same same range id", async () => {
		session = await TestSession.connected(2);

		const store = new EventStore();
		let db1JoinEvents: string[] = [];
		let timeUntilRoleMaturity = 1e3;
		const store1 = await session.peers[0].open(store, {
			args: {
				replicate: { factor: 1 },
				timeUntilRoleMaturity,
			},
		});
		store1.log.events.addEventListener("replicator:mature", (event) => {
			db1JoinEvents.push(event.detail.publicKey.hashcode());
		});

		let rangeId = randomBytes(32);
		const store2 = await session.peers[1].open(store.clone(), {
			args: {
				replicate: { id: rangeId, factor: 1 },
				timeUntilRoleMaturity,
			},
		});
		await waitForResolved(() =>
			expect(db1JoinEvents).to.have.members([
				session.peers[0].identity.publicKey.hashcode(),
				session.peers[1].identity.publicKey.hashcode(),
			]),
		);

		// reset: true means we will re-initalize hence we expect a maturity event
		await store2.log.replicate({ id: rangeId, factor: 0.5 }, { reset: true });

		await waitForResolved(async () => {
			const store2Role = await store1.log.replicationIndex
				.iterate({ query: { hash: store2.node.identity.publicKey.hashcode() } })
				.all();
			expect(store2Role).to.have.length(1);
			expect(store2Role[0].value.widthNormalized).to.be.closeTo(0.5, 0.01);
		});
		expect(store.log.pendingMaturity.size).to.be.eq(0);

		await delay(timeUntilRoleMaturity * 2); // wait a little bit more
		expect(db1JoinEvents).to.have.members([
			session.peers[0].identity.publicKey.hashcode(),
			session.peers[1].identity.publicKey.hashcode(),
		]); // no new join events

		expect(store.log.pendingMaturity.size).to.eq(0);
	});

	it("replicator:mature emit twice on update reset", async () => {
		session = await TestSession.connected(2);

		const store = new EventStore();
		let db1JoinEvents: string[] = [];
		let timeUntilRoleMaturity = 1e3;
		const store1 = await session.peers[0].open(store, {
			args: {
				replicate: { factor: 1 },
				timeUntilRoleMaturity,
			},
		});
		store1.log.events.addEventListener("replicator:mature", (event) => {
			db1JoinEvents.push(event.detail.publicKey.hashcode());
		});

		const store2 = await session.peers[1].open(store.clone(), {
			args: {
				replicate: { factor: 1 },
				timeUntilRoleMaturity,
			},
		});
		await waitForResolved(() =>
			expect(db1JoinEvents).to.have.members([
				session.peers[0].identity.publicKey.hashcode(),
				session.peers[1].identity.publicKey.hashcode(),
			]),
		);

		// reset: true means we will re-initalize hence we expect a maturity event
		await store2.log.replicate({ factor: 0.5 }, { reset: true });

		await waitForResolved(async () => {
			const store2Role = await store1.log.replicationIndex
				.iterate({ query: { hash: store2.node.identity.publicKey.hashcode() } })
				.all();
			expect(store2Role).to.have.length(1);
			expect(store2Role[0].value.widthNormalized).to.be.closeTo(0.5, 0.01);
		});
		expect(store.log.pendingMaturity.size).to.be.greaterThan(0);

		await waitForResolved(() =>
			expect(db1JoinEvents).to.have.members([
				session.peers[0].identity.publicKey.hashcode(),
				session.peers[1].identity.publicKey.hashcode(),
				session.peers[1].identity.publicKey.hashcode(),
			]),
		);

		expect(store.log.pendingMaturity.size).to.eq(0);
	});

	describe("waitForReplicators", async () => {
		it("resolves immediately is offline and replicating and mature", async () => {
			session = await TestSession.connected(1);
			const store = new EventStore();
			const store1 = await session.peers[0].open(store, {
				args: {
					replicate: {
						factor: 1,
					},
					timeUntilRoleMaturity: 0,
				},
			});
			let timeout = 1e4;
			let t0 = Date.now();
			await store1.log.waitForReplicators({
				timeout,
			});
			let t1 = Date.now();
			expect(t1 - t0).to.be.lessThan(1e3); // "immediately"
		});

		it("times out after mature when is offline and replicating and unmature", async () => {
			session = await TestSession.connected(1);
			const store = new EventStore();

			let t0 = Date.now();
			const store1 = await session.peers[0].open(store, {
				args: {
					replicate: {
						factor: 1,
					},
					timeUntilRoleMaturity: 6e3, // > 3e3
				},
			});

			let timeout = 1e4;

			await store1.log.waitForReplicators({
				timeout,
			});

			let t1 = Date.now();
			expect(t1 - t0).to.be.greaterThan(3e3); // should wait for maturity
		});

		it("resolves when replication starts after wait is pending", async () => {
			session = await TestSession.connected(1);
			const store = new EventStore();
			const store1 = await session.peers[0].open(store, {
				args: {
					replicate: false,
					timeUntilRoleMaturity: 1e3,
				},
			});

			const waitPromise = store1.log.waitForReplicators({
				timeout: 10e3,
				waitForNewPeers: true,
			});

			await delay(100);
			await store1.log.replicate({ factor: 1 });

			await waitPromise;
		});

		it("resolves even if maturity timers are cleared", async () => {
			session = await TestSession.connected(1);
			const store = new EventStore();
			const timeUntilRoleMaturity = 3e3;
			const store1 = await session.peers[0].open(store, {
				args: {
					replicate: {
						factor: 1,
					},
					timeUntilRoleMaturity,
				},
			});

			const hash = session.peers[0].identity.publicKey.hashcode();
			// @ts-ignore accessing internal state for test purposes
			const pending = store1.log.pendingMaturity.get(hash);
			expect(pending, "expected pending maturity timers").to.exist;
			if (pending) {
				for (const [_key, value] of pending) {
					clearTimeout(value.timeout);
				}
				pending.clear();
			}

			await store1.log.waitForReplicators({
				timeout: 10e3,
			});
		});

		it("times out after timeout if online", async () => {
			session = await TestSession.connected(2);
			const store = new EventStore();
			const store1 = await session.peers[0].open(store, {
				args: {
					replicate: false,
				},
			});

			const store2 = await session.peers[1].open(store.clone(), {
				args: {
					replicate: false,
				},
			});

			await store1.log.waitFor(store2.log.node.identity.publicKey);

			let timeout = 3e3;
			let t0 = Date.now();
			await expect(
				store1.log.waitForReplicators({
					timeout,
				}),
			).to.be.eventually.rejectedWith("Timeout");
			let t1 = Date.now();
			// Allow small timer jitter on busy CI runners.
			expect(t1 - t0).to.be.greaterThanOrEqual(timeout - 25);
		});

		it("will wait for role age", async () => {
			session = await TestSession.connected(1);

			const store = new EventStore();
			const store1 = await session.peers[0].open(store, {
				args: {
					replicate: { factor: 1 },
				},
			});

			let waitForRoleAge = 2e3;
			let t0 = Date.now();
			await store1.log.waitForReplicators({
				roleAge: waitForRoleAge,
				waitForNewPeers: true, // prevent waitForReplicators from resolving immediately
			});
			let t1 = Date.now();
			// Allow some timer jitter across environments/CI
			expect(t1 - t0).to.be.greaterThanOrEqual(waitForRoleAge - 250);
		});

		it("will wait for warmup when restarting", async () => {
			session = await TestSession.connected(1, {
				directory:
					"./tmp/shared-log/waitForReplicators/wait-for-warmup/" + uuid(),
			});

			const store = new EventStore();
			let store1 = await session.peers[0].open(store, {
				args: {
					replicate: { factor: 1 },
				},
			});

			await delay(3e3);
			await store1.close();
			let waitForRoleAge = 3e3;
			let t0 = Date.now();
			store1 = await session.peers[0].open(store1, {
				args: {
					replicate: {
						type: "resume",
						default: {
							factor: 0.5,
						},
					},
				},
			});

			await store1.log.waitForReplicators({
				roleAge: waitForRoleAge,
				timeout: 1e4,
				waitForNewPeers: true, // prevent waitForReplicators from resolving immediately
			});
			let t1 = Date.now();
			// Restart warmup starts during reopen, before waitForReplicators is called.
			expect(t1 - t0).to.be.greaterThanOrEqual(waitForRoleAge - 250);
			expect(t1 - t0).to.be.lessThan(waitForRoleAge + 5e3);
		});

		it("will wait joining replicator role age", async () => {
			session = await TestSession.connected(2);
			const store = new EventStore();
			await session.peers[1].open(store.clone(), {
				args: {
					replicate: { factor: 1 },
				},
			});

			await delay(3e3);

			const store2 = await session.peers[0].open(store, {
				args: {
					replicate: false,
				},
			});

			let waitForRoleAge = 3e3;
			let t0 = Date.now();
			await store2.log.waitForReplicators({
				roleAge: waitForRoleAge,
				waitForNewPeers: true, // prevent waitForReplicators from resolving immediately
			});

			let t1 = Date.now();
			expect(t1 - t0).to.be.lessThanOrEqual(waitForRoleAge); // because store1
		});
	});
});
