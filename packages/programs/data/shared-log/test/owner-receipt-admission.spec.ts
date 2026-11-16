import { TestSession } from "@peerbit/test-utils";
import { expect } from "chai";
import sinon from "sinon";
import { RequestPersistedEntriesV1 } from "../src/sync/simple.js";
import { EventStore } from "./utils/stores/index.js";

describe("append delivery options — receive ownership admission", () => {
	let session: TestSession;
	let store: EventStore<string, any>;
	let log: any;
	let sandbox: sinon.SinonSandbox;
	let releases: (() => void)[];

	beforeEach(async () => {
		sandbox = sinon.createSandbox();
		releases = [];
		session = await TestSession.disconnected(1);
		store = await session.peers[0].open(new EventStore<string, any>(), {
			args: { replicate: 1, timeUntilRoleMaturity: 0 },
		});
		log = store.log;
	});

	afterEach(async () => {
		for (const release of releases) release();
		sandbox.restore();
		await session?.stop();
	});

	const admit = () => {
		const release = log.beginReceiveOwnershipMutationAdmission();
		releases.push(release);
		return release;
	};

	it("returns an empty receipt before lower flush while completion is pending", async () => {
		const { entry } = await store.add("held", { target: "none" });
		// Exercise the storage/ownership boundary, not session negotiation or fsync.
		sandbox.stub(log, "isPersistedReceiptRequestSessionCurrent").returns(true);
		const storage = sandbox
			.stub(log, "resolvePersistedReceiptStorage")
			.returns({});
		const flushed = new Error("lower flush reached");
		const flush = sandbox
			.stub(log.log.entryIndex, "flushPendingWrites")
			.rejects(flushed);
		const coordinateFlush = sandbox.spy(
			log._coordinates,
			"flushNativeBackboneCoordinateJournal",
		);
		const request = new RequestPersistedEntriesV1({
			hashes: [entry.hash],
			expectedReceiverSession: 1n,
		});
		const context = { message: { header: { session: 1n } } };
		const lane = {
			fromHash: "receipt-admission-peer",
			ownershipLifecycleController: log.captureReplicationOwnershipLifecycle(),
		};
		const release = admit();
		const receipt = await log.handleRequestPersistedEntriesV1(
			request,
			context,
			lane,
		);
		expect(receipt.hashes).to.deep.equal([]);
		expect(flush.notCalled).to.equal(true);
		expect(coordinateFlush.notCalled).to.equal(true);
		expect(storage.notCalled).to.equal(true);
		release();
		await expect(
			log.handleRequestPersistedEntriesV1(request, context, lane),
		).to.be.rejectedWith(flushed);
		expect(flush.calledOnce).to.equal(true);
	});

	it("releases only once and invalidates the current ownership snapshot", () => {
		const lifecycle = log._instanceLifecycle;
		const originalRevision = lifecycle._receiveOwnershipRevision;
		const releaseA = admit();
		const releaseB = admit();
		expect(lifecycle._receiveOwnershipMutationAdmissions).to.equal(2);
		expect(log.isReceiveOwnershipSnapshotStable(originalRevision)).to.equal(
			false,
		);
		const admittedRevision = lifecycle._receiveOwnershipRevision;
		releaseA();
		releaseA();
		expect(lifecycle._receiveOwnershipMutationAdmissions).to.equal(1);
		expect(lifecycle._receiveOwnershipRevision).to.equal(admittedRevision + 1);
		releaseB();
		expect(lifecycle._receiveOwnershipMutationAdmissions).to.equal(0);
		expect(log.isReceiveOwnershipSnapshotStable(admittedRevision)).to.equal(
			false,
		);
		expect(
			log.isReceiveOwnershipSnapshotStable(lifecycle._receiveOwnershipRevision),
		).to.equal(true);
	});

	it("does not release or invalidate a replacement generation", async () => {
		const old = log._instanceLifecycle;
		const releaseOld = admit();
		await store.close();
		await session.peers[0].open(store, {
			args: { replicate: 1, timeUntilRoleMaturity: 0 },
		});
		const current = log._instanceLifecycle;
		expect(current).to.not.equal(old);
		const releaseCurrent = admit();
		const revision = current._receiveOwnershipRevision;
		const invalidate = sandbox.spy(
			log,
			"invalidateLeaderSelectionContextCache",
		);
		releaseOld();
		releaseOld();
		expect(old._receiveOwnershipMutationAdmissions).to.equal(0);
		expect(current._receiveOwnershipMutationAdmissions).to.equal(1);
		expect(current._receiveOwnershipRevision).to.equal(revision);
		expect(invalidate.notCalled).to.equal(true);
		releaseCurrent();
		expect(current._receiveOwnershipMutationAdmissions).to.equal(0);
	});

	it("allows a completion callback to await replicate without owning the range tail", async () => {
		const tail = log._replicationRangeMutationTail;
		const release = admit();
		expect(log._replicationRangeMutationTail).to.equal(tail);
		await log.replicate({ factor: 0.5 });
		expect(log._instanceLifecycle._receiveOwnershipMutationAdmissions).to.equal(
			1,
		);
		release();
		expect(log._instanceLifecycle._receiveOwnershipMutationAdmissions).to.equal(
			0,
		);
	});

	it("keeps ordinary queued receive mutations admitted through callback settlement", async () => {
		const lifecycle = log._instanceLifecycle;
		const failure = new Error("callback failed");
		const mutation = log.withReceiveOwnershipMutationQueue(async () => {
			expect(lifecycle._receiveOwnershipMutationAdmissions).to.equal(1);
			throw failure;
		});
		expect(lifecycle._receiveOwnershipMutationAdmissions).to.equal(1);
		await expect(mutation).to.be.rejectedWith(failure);
		expect(lifecycle._receiveOwnershipMutationAdmissions).to.equal(0);
		await log.withReceiveOwnershipMutationQueue(async () => undefined);
		expect(lifecycle._receiveOwnershipMutationAdmissions).to.equal(0);
	});

	it("rejects closed or stale admission before changing the current counters", () => {
		const lifecycle = log._instanceLifecycle;
		const revision = lifecycle._receiveOwnershipRevision;
		expect(() =>
			log.beginReceiveOwnershipMutationAdmission(new AbortController()),
		).to.throw();
		log._replicationRangeMutationsClosing = true;
		try {
			expect(() => admit()).to.throw("Replication range mutations are closing");
		} finally {
			log._replicationRangeMutationsClosing = false;
		}
		expect(lifecycle._receiveOwnershipMutationAdmissions).to.equal(0);
		expect(lifecycle._receiveOwnershipRevision).to.equal(revision);
	});
});
