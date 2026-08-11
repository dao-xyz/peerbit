import { Program } from "@peerbit/program";
import { TestSession } from "@peerbit/test-utils";
import { expect } from "chai";
import sinon from "sinon";
import { Documents } from "../src/index.js";
import { Document, TestStore } from "./data.js";

/**
 * DocumentIndex.waitFor readiness gating: a document index and its SharedLog
 * use separate RPC topics, and `waitFor` must establish BOTH before a peer is
 * reported ready. These tests pin the raw dual-topic state, the hash
 * intersection under partial admission, the one-shot iterator materialization
 * and the sibling-abort cleanup on rejection.
 */
describe("waitFor dual-topic readiness", () => {
	let session: TestSession;
	let store: TestStore;

	afterEach(async () => {
		sinon.restore();
		if (store && store.closed === false) {
			await store.drop();
		}
		await session?.stop();
	});

	it("establishes both document-index and shared-log topic readiness", async () => {
		session = await TestSession.connected(2);
		store = new TestStore({ docs: new Documents<Document>() });
		await session.peers[0].open(store, {
			args: { replicate: { factor: 1 }, timeUntilRoleMaturity: 0 },
		});
		const observer = await session.peers[1].open(store.clone(), {
			args: { replicate: false, timeUntilRoleMaturity: 0 },
		});
		const remote = session.peers[0].identity.publicKey;
		const remoteHash = remote.hashcode();

		// A one-shot iterator must be materialized once and shared safely by the
		// two parallel topic waits.
		const oneShot = new Set([remote]).values();
		const hashes = await observer.docs.index.waitFor(oneShot);
		expect(hashes).to.deep.equal([remoteHash]);

		// Raw readiness on BOTH topic sets, not just absence of a throw.
		const indexTopics = (
			observer.docs.index as any
		).getAllTopicsIncludingThis() as string[];
		const logTopics = (
			observer.docs.log as any
		).getAllTopicsIncludingThis() as string[];
		expect(indexTopics.length).to.be.greaterThan(0);
		expect(logTopics.length).to.be.greaterThan(0);
		for (const topic of [...indexTopics, ...logTopics]) {
			const subscribers =
				(await session.peers[1].services.pubsub.getSubscribers(topic)) ?? [];
			expect(
				subscribers.find((key) => key.equals(remote)),
				`remote must be a known subscriber of ${topic}`,
			).to.exist;
		}
		// The SharedLog replication-info session opened and the trailing
		// replication-index gate really observed the remote's ranges.
		expect(
			(observer.docs.log as any)._peerSessions.current(remoteHash)?.phase,
		).to.equal("open");
		expect(
			await observer.docs.log.replicationIndex.count({
				query: { hash: remoteHash },
			}),
		).to.be.greaterThan(0);
	});

	it("filters index-ready peers that lack shared-log readiness", async () => {
		session = await TestSession.connected(1);
		store = new TestStore({ docs: new Documents<Document>() });
		await session.peers[0].open(store, {
			args: { replicate: false, timeUntilRoleMaturity: 0 },
		});
		const index = store.docs.index as any;
		// Partial admission (seek: "present"): the log topic admits only "a"
		// while the index topic admits "a" and "b". Only the intersection is
		// ready — a peer ready for queries but with no replication-info session
		// must not be reported.
		const logWait = sinon.stub(index._log, "waitFor").resolves(["a"]);
		const superWait = sinon
			.stub(Program.prototype, "waitFor")
			.resolves(["a", "b"]);
		const count = sinon.stub(index._log.replicationIndex, "count").resolves(1);

		const result = await index.waitFor([], { seek: "present" });
		expect(result).to.deep.equal(["a"]);
		expect(logWait.calledOnce).to.be.true;
		expect(superWait.calledOnce).to.be.true;
		// The trailing replication gate polls only the intersected peer.
		expect(
			count.getCalls().every((call) => call.args[0]?.query?.hash === "a"),
		).to.be.true;
	});

	it("aborts the sibling topic wait when the log topic wait rejects", async () => {
		session = await TestSession.connected(1);
		store = new TestStore({ docs: new Documents<Document>() });
		await session.peers[0].open(store, {
			args: { replicate: false, timeUntilRoleMaturity: 0 },
		});
		const index = store.docs.index as any;
		const failure = new Error("log-topic-wait-failed");
		sinon.stub(index._log, "waitFor").rejects(failure);
		let capturedSignal: AbortSignal | undefined;
		sinon
			.stub(Program.prototype, "waitFor")
			.callsFake(function (_other: unknown, options?: { signal?: AbortSignal }) {
				capturedSignal = options?.signal;
				// Behave like the real wait: pend until the abort signal retires the
				// listeners/poll loop.
				return new Promise<string[]>((_resolve, reject) => {
					options?.signal?.addEventListener(
						"abort",
						() => reject(new Error("sibling wait aborted")),
						{ once: true },
					);
				});
			});

		await expect(index.waitFor([])).to.be.rejectedWith("log-topic-wait-failed");
		// Raw cleanup state: the sibling's wait received the combined signal and
		// that signal is aborted, so its subscribe listener and poll timer are
		// retired instead of leaking until their own timeout.
		expect(capturedSignal).to.exist;
		expect(capturedSignal!.aborted).to.be.true;
	});
});
