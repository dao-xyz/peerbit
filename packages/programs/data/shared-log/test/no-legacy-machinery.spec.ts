import { readFileSync } from "fs";
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
import { EventStore } from "./utils/stores/index.js";

// B12 stage 3: the legacy outbound announcement machinery (primary broadcast
// tail, retry/repair workers, startup sends, request polling) is deleted.
// This ratchet supersedes the dormant-by-default retry pin: instead of
// pinning that the retired machinery stays idle, it pins that the machinery
// cannot be constructed or scheduled at all, and that no legacy frame can
// egress at the reachable seams (strengthening the migration-8-9 default
// no-legacy-egress pin at the subscription-change seam).
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

	it("has zero legacy-frame construction sites on the outbound source paths", () => {
		// Source-level outbound ratchet: constructing a legacy announcement
		// class is a prerequisite for sending one. After B12 stage 3 the only
		// sanctioned constructions live in replication.ts (the ResponseRole
		// tombstone's decode conversion) and replication-info-v2-receive.ts
		// (byte-stable fingerprint canonicalization) — both inbound-only and
		// scheduled for stage 4. Everything the sender stack can reach must
		// stay clean in every open mode.
		const forbidden =
			/new\s+(RequestReplicationInfoMessage|ResponseRoleMessage|AllReplicatingSegmentsMessage|AddedReplicationSegmentMessage|StoppedReplicating)\s*\(/g;
		for (const file of [
			"src/index.ts",
			"src/replication-announcement.ts",
			"src/replication-info-v2-send.ts",
		]) {
			const source = readFileSync(path.join(process.cwd(), file), "utf8");
			const matches = [...source.matchAll(forbidden)].map((m) => m[0]);
			expect(matches, file).to.deep.equal([]);
		}
	});
});
