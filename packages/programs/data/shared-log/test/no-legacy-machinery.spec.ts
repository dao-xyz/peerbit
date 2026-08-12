import { readFileSync, readdirSync } from "fs";
import path from "path";
import { TestSession } from "@peerbit/test-utils";
import { expect } from "chai";
import sinon from "sinon";
import {
	AddedReplicationSegmentMessage,
	AllReplicatingSegmentsMessage,
	FullReplicationInfoV2Message,
	RequestReplicationInfoMessage,
	ResponseRoleMessage,
	StoppedReplicating,
} from "../src/replication.js";
import { Observer } from "../src/role.js";
import { EventStore } from "./utils/stores/index.js";

// B12 stage 3: the legacy outbound announcement machinery (primary broadcast
// tail, retry/repair workers, startup sends, request polling) is deleted.
// This ratchet supersedes the dormant-by-default retry pin: instead of
// pinning that the retired machinery stays idle, it pins that the machinery
// cannot be constructed or scheduled at all, and that no legacy frame can
// egress at the reachable seams (strengthening the migration-8-9 default
// no-legacy-egress pin at the subscription-change seam).
// B12 stage 4 extends the ratchet inbound: the legacy dispatch arms, apply
// handlers and the ordering watermark are deleted, so a received legacy
// frame must die at the unconditional B1 drop with zero side effects, and
// no inbound apply-path symbol may reappear in src.
describe("no legacy machinery", () => {
	let session: TestSession | undefined;

	afterEach(async () => {
		sinon.restore();
		if (session) {
			await session.stop();
			session = undefined;
		}
	});

	const isLegacyFrame = (message: unknown) =>
		message instanceof RequestReplicationInfoMessage ||
		message instanceof ResponseRoleMessage ||
		message instanceof AllReplicatingSegmentsMessage ||
		message instanceof AddedReplicationSegmentMessage ||
		message instanceof StoppedReplicating;

	it("cannot construct or schedule the deleted announcement retry/repair machinery", async () => {
		session = await TestSession.disconnected(1);
		const store = await session.peers[0].open(new EventStore<string, any>(), {
			args: { replicate: false, timeUntilRoleMaturity: 0 },
		});
		const log = store.log as any;
		const coordinator = log._announcements;
		// Compile-level guarantee probed at runtime: the retry/repair worker
		// members no longer exist on the coordinator, so nothing can re-arm
		// them without re-adding the machinery itself.
		for (const member of [
			"setupReplicationAnnouncementRetryFunction",
			"setupReplicationAnnouncementRepairFunction",
			"queueCurrentReplicationStateAnnouncementRetry",
			"queueCurrentReplicationStateAnnouncementRepair",
			"retryCurrentReplicationStateAnnouncement",
			"repairCurrentReplicationStateAnnouncement",
			"runCurrentReplicationStateAnnouncementRepair",
			"cancelCurrentReplicationStateAnnouncementRetry",
			"cancelCurrentReplicationStateAnnouncementRepair",
		]) {
			expect(coordinator[member], member).to.equal(undefined);
		}
		// No retry timer or repair worker state can exist either.
		for (const field of [
			"_replicationAnnouncementRetryPending",
			"_replicationAnnouncementRetryController",
			"replicationAnnouncementRetryDebounced",
			"_announcementRepairBinding",
			"_replicationAnnouncementRepairFairCursorHash",
			"_replicationAnnouncementRepairMaxAttempts",
			"_replicationAnnouncementRepairController",
			"replicationAnnouncementRepairDebounced",
		]) {
			expect(field in coordinator, field).to.be.false;
		}
		// The V2 mutation feed survives: a committed local mutation reaches the
		// V2 sender and this path never touches rpc directly.
		const enqueueV2 = sinon.spy(log._v2Send, "enqueue");
		const send = sinon.spy(log.rpc, "send");
		await coordinator.sendReplicationAnnouncement({ added: { segments: [] } });
		expect(enqueueV2.calledOnce).to.be.true;
		expect(send.notCalled).to.be.true;
	});

	it("emits zero legacy frames across a default-mode subscription change", async () => {
		session = await TestSession.disconnected(2);
		const db1 = await session.peers[0].open(new EventStore<string, any>(), {
			args: { replicate: { factor: 1 }, timeUntilRoleMaturity: 0 },
		});
		const log1 = db1.log as any;
		const sent1: unknown[] = [];
		const originalSend1 = log1.rpc.send.bind(log1.rpc);
		sinon.stub(log1.rpc, "send").callsFake(((message: any, options: any) => {
			sent1.push(message);
			return originalSend1(message, options);
		}) as any);

		const db2 = await session.peers[1].open(db1.clone(), {
			args: { replicate: false, timeUntilRoleMaturity: 0 },
		});
		const log2 = db2.log as any;
		const sent2: unknown[] = [];
		const originalSend2 = log2.rpc.send.bind(log2.rpc);
		sinon.stub(log2.rpc, "send").callsFake(((message: any, options: any) => {
			sent2.push(message);
			return originalSend2(message, options);
		}) as any);

		await session.connect([[session.peers[0], session.peers[1]]]);
		// The observer must learn the replicator's ranges through V2 alone.
		await db2.log.waitForReplicator(session.peers[0].identity.publicKey);

		// Positive control: the subscription-change seam announced the
		// replicator's state through the V2 lane...
		expect(sent1.some((message) => message instanceof FullReplicationInfoV2Message))
			.to.be.true;
		// ...and no legacy frame crossed the wire from either side.
		expect(sent1.filter(isLegacyFrame)).to.have.length(0);
		expect(sent2.filter(isLegacyFrame)).to.have.length(0);
	});

	it("applies nothing when a default-mode node receives each legacy frame", async () => {
		// Runtime inbound ratchet: the five legacy replication-info frames die
		// at the unconditional B1 drop, ahead of every side effect. The sender
		// gets a live open session first, so the no-effect outcome cannot be
		// explained by missing membership; every observation below is
		// symbol-free (the deleted watermark, handlers and cutover probe are
		// additionally pinned absent by name).
		session = await TestSession.disconnected(2);
		const db = await session.peers[0].open(new EventStore<string, any>(), {
			args: { replicate: false, timeUntilRoleMaturity: 0 },
		});
		const log = db.log as any;
		const remote = session.peers[1].identity.publicKey;
		const remoteHash = remote.hashcode();
		const peerSession = log._peerSessions.rotate(remoteHash, "opening");
		log._peerSessions.unblockReplicationInfo(remoteHash);
		log._peerSessions.markOpen(remoteHash, peerSession);

		const synchronizer = sinon
			.stub(log.syncronizer, "onMessage")
			.resolves(false);
		const activity = sinon.spy(log._liveness, "markReplicatorActivity");
		const add = sinon.spy(log, "addReplicationRange");
		const remove = sinon.spy(log, "removeReplicationRanges");
		const send = sinon.spy(log.rpc, "send");
		const acquireLease = sinon.spy(log, "acquirePeerReceiveLease");
		const messages = [
			new RequestReplicationInfoMessage(),
			new ResponseRoleMessage({ role: new Observer() }),
			new AllReplicatingSegmentsMessage({ segments: [] }),
			new AddedReplicationSegmentMessage({ segments: [] }),
			new StoppedReplicating({ segmentIds: [] }),
		];
		for (const [index, message] of messages.entries()) {
			await db.log.onMessage(message, {
				from: remote,
				message: { header: { session: 1n, timestamp: BigInt(index + 1) } },
			} as any);
		}
		// No lease, no synchronizer work, no liveness, no mutation, no reply,
		// no apply-lane row and no per-peer V2 receive state: nothing applied.
		expect(acquireLease.notCalled).to.be.true;
		expect(synchronizer.notCalled).to.be.true;
		expect(activity.notCalled).to.be.true;
		expect(add.notCalled).to.be.true;
		expect(remove.notCalled).to.be.true;
		expect(send.notCalled).to.be.true;
		expect(log._replicationInfoApplyQueueByPeer.has(remoteHash)).to.be.false;
		expect(log._v2Receive._receiveStates.has(remoteHash)).to.be.false;
		expect(
			await db.log.replicationIndex.count({ query: { hash: remoteHash } }),
		).to.equal(0);
		// The session installed above is untouched — the drop happened before
		// any session, epoch or blocked-set transition.
		expect(log._peerSessions.current(remoteHash)).to.equal(peerSession);
		// The deleted inbound machinery cannot silently return: the watermark
		// field, the apply handlers and the cutover probe stay deleted.
		for (const member of [
			"latestReplicationInfoMessage",
			"handleReplicationInfoAnnouncement",
			"handleStoppedReplicating",
			"handleRequestReplicationInfo",
		]) {
			expect(log[member], member).to.equal(undefined);
		}
		expect((log._v2Receive as any).isLegacyCutover).to.equal(undefined);
	});

	it("has zero legacy-frame construction sites on the outbound source paths", () => {
		// Source-level outbound ratchet: constructing a legacy announcement
		// class is a prerequisite for sending one. After B12 stage 4 the only
		// sanctioned constructions in all of src live in
		// replication-info-v2-receive.ts (byte-stable fingerprint
		// canonicalization, whitelisted by decision Q4) — that single file is
		// the whitelist; every other source file must stay clean in every
		// open mode, including replication.ts now that the ResponseRole
		// tombstone's decode conversion is deleted.
		const forbidden =
			/new\s+(RequestReplicationInfoMessage|ResponseRoleMessage|AllReplicatingSegmentsMessage|AddedReplicationSegmentMessage|StoppedReplicating)\s*\(/g;
		const whitelist = new Set(["src/replication-info-v2-receive.ts"]);
		for (const file of listSourceFiles()) {
			if (whitelist.has(file)) {
				continue;
			}
			const source = readFileSync(path.join(process.cwd(), file), "utf8");
			const matches = [...source.matchAll(forbidden)].map((m) => m[0]);
			expect(matches, file).to.deep.equal([]);
		}
	});

	it("has zero legacy inbound apply-path symbols in src", () => {
		// Source-level inbound ratchet: the legacy inbound dispatch arms, the
		// All/Added/Stopped apply handlers, the request handler, the ordering
		// watermark, the tombstone decode conversion and the legacy-cutover
		// probe are deleted (B12 stage 4). No source file may reference their
		// names again — the retained legacy remnants in
		// replication-info-v2-receive.ts (the local legacy union, fingerprint
		// canonicalization and the noteLegacyAnnouncement sidecar) do not use
		// these names, so this leg needs no whitelist.
		const forbidden =
			/latestReplicationInfoMessage|handleReplicationInfoAnnouncement|handleStoppedReplicating|handleRequestReplicationInfo|toReplicationInfoMessage|isLegacyCutover/g;
		for (const file of listSourceFiles()) {
			const source = readFileSync(path.join(process.cwd(), file), "utf8");
			const matches = [...source.matchAll(forbidden)].map((m) => m[0]);
			expect(matches, file).to.deep.equal([]);
		}
	});
});

const listSourceFiles = (): string[] =>
	readdirSync(path.join(process.cwd(), "src"), { recursive: true })
		.map((entry) => String(entry))
		.filter((entry) => entry.endsWith(".ts"))
		.map((entry) => path.join("src", entry));
