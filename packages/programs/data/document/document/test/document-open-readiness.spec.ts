import { sha256Base64Sync } from "@peerbit/crypto";
import { Program } from "@peerbit/program";
import { TestSession } from "@peerbit/test-utils";
import { expect } from "chai";
import sinon from "sinon";
import { Documents } from "../src/index.js";
import { Document, TestStore } from "./data.js";

const deferred = () => {
	let resolve!: () => void;
	const promise = new Promise<void>((done) => (resolve = done));
	return { promise, resolve };
};

const args = {
	mode: "compat" as const,
	replicate: { factor: 1 },
	nativeGraph: false as const,
	nativeBackbone: false as const,
	nativeRangePlanner: false as const,
	timeUntilRoleMaturity: 0,
	index: { prefetch: false as const },
};

describe("document open query readiness", function () {
	this.timeout(30_000);
	let session: TestSession;

	beforeEach(async () => {
		session = await TestSession.connected(2);
	});

	afterEach(async () => {
		sinon.restore();
		await session?.stop();
	});

	const holdLogOpen = (
		store: TestStore,
		rejectWith?: Error,
		lateRecovery = false,
	) => {
		const entered = deferred();
		const release = deferred();
		const query = store.docs.index._query;
		const queryOpen = sinon.spy(query, "open");
		const subscribe = sinon.spy(session.peers[0].services.pubsub, "subscribe");
		const topic = sha256Base64Sync(
			new Uint8Array([
				...store.docs.log.log.id,
				...new TextEncoder().encode("/document"),
			]),
		);
		const block = async () => {
			entered.resolve();
			await release.promise;
			if (rejectWith) throw rejectWith;
		};
		let logOpen: sinon.SinonStub;
		if (lateRecovery) {
			const trusted = store.docs.log as unknown as {
				finishNativeStrictDurableDocumentRecovery(): Promise<void>;
			};
			const original =
				trusted.finishNativeStrictDurableDocumentRecovery.bind(trusted);
			logOpen = sinon
				.stub(trusted, "finishNativeStrictDurableDocumentRecovery")
				.callsFake(async () => {
					await block();
					return original();
				});
		} else {
			const original = store.docs.log.open.bind(store.docs.log);
			logOpen = sinon
				.stub(store.docs.log, "open")
				.callsFake(async (options) => {
					await block();
					return original(options);
				});
		}
		let parentOpened = 0;
		const opening = session.peers[0].open(store, {
			args,
			onOpen: (program) => {
				if (program !== store) return;
				parentOpened++;
				// Root onOpen runs before child afterOpen callbacks. Query activation
				// must already be complete for callers that use the store here.
				expect(queryOpen.calledOnce).equal(true);
				expect((query as any)._subscribed).equal(true);
				expect((query as any)._listenerAttached).equal(true);
			},
		});
		// Attach rejection handling before yielding to the blocked lifecycle.
		const outcome = opening.then(
			(value) => ({ value, error: undefined }),
			(error: unknown) => ({ value: undefined, error }),
		);
		return {
			entered: entered.promise,
			release: release.resolve,
			outcome,
			assertNotServing: () => {
				expect(parentOpened).equal(0);
				expect(
					queryOpen.called,
					"query RPC must not open before its owner",
				).equal(false);
				expect(
					subscribe.calledWith(topic),
					"query topic must not be advertised",
				).equal(false);
				// RPC.open installs the response handler and data listener together.
				// With no listener attached, directed queries cannot be answered either.
				expect((query as any)._subscribed === true).equal(false);
				expect((query as any)._listenerAttached === true).equal(false);
			},
			assertServing: () => {
				expect(parentOpened).equal(1);
				expect(queryOpen.calledOnce).equal(true);
				expect(subscribe.calledWith(topic)).equal(true);
				expect((query as any)._subscribed).equal(true);
				expect((query as any)._listenerAttached).equal(true);
			},
			restore: () => {
				logOpen.restore();
				queryOpen.restore();
				subscribe.restore();
			},
		};
	};

	it("starts queries only after log readiness, including instance and hydrated-address reopen", async () => {
		const original = new TestStore({ docs: new Documents<Document>() });
		let donor = original;
		let observer: TestStore | undefined;
		for (let cycle = 0; cycle < 3; cycle++) {
			if (cycle === 2) {
				const loaded = await Program.load<TestStore>(
					original.address,
					session.peers[0].services.blocks,
				);
				expect(loaded).instanceOf(TestStore);
				expect(loaded).not.equal(original);
				donor = loaded!;
			}
			const held = holdLogOpen(donor);
			try {
				await held.entered;
				held.assertNotServing();
				held.release();
				const outcome = await held.outcome;
				if (outcome.error) throw outcome.error;
				expect(outcome.value).equal(donor);
				held.assertServing();
			} finally {
				held.release();
				await held.outcome;
				held.restore();
			}
			observer ??= await session.peers[1].open(donor.clone(), {
				args: { ...args, replicate: false },
			});
			await observer.docs.index.waitFor(session.peers[0].identity.publicKey, {
				timeout: 5_000,
			});
			const id = `ready-${cycle}`;
			await donor.docs.put(new Document({ id, name: "ready" }), {
				target: "none",
			});
			expect(
				await donor.docs.index.get(id, { local: true, remote: false }),
			).to.have.property("name", "ready");
			const remote = await observer.docs.index.search(
				{},
				{
					local: false,
					remote: {
						from: [session.peers[0].identity.publicKey.hashcode()],
						replicate: false,
						timeout: 5_000,
						throwOnMissing: true,
						retryMissingResponses: false,
					},
				},
			);
			expect(remote.some((document) => document.id === id)).equal(true);
			await donor.close();
			expect((donor.docs.index._query as any)._subscribed).equal(false);
			expect((donor.docs.index._query as any)._listenerAttached).equal(false);
		}
	});

	it("never starts the query service when owning log open rejects", async () => {
		const store = new TestStore({ docs: new Documents<Document>() });
		const failure = new Error("Blocked log failed to initialize");
		const held = holdLogOpen(store, failure);
		try {
			await held.entered;
			held.assertNotServing();
			held.release();
			const outcome = await held.outcome;
			expect(outcome.error).equal(failure);
			held.assertNotServing();
		} finally {
			held.release();
			await held.outcome;
			held.restore();
		}
	});

	it("keeps queries inactive through late local recovery and recovery failure", async () => {
		for (const fail of [false, true]) {
			const store = new TestStore({ docs: new Documents<Document>() });
			const failure = fail
				? new Error("Late local recovery failed")
				: undefined;
			const openedLog = sinon.spy(store.docs.log, "open");
			const held = holdLogOpen(store, failure, true);
			try {
				await held.entered;
				expect(openedLog.calledOnce).equal(true);
				await openedLog.returnValues[0];
				held.assertNotServing();
				held.release();
				const outcome = await held.outcome;
				if (fail) {
					expect(outcome.error).equal(failure);
					held.assertNotServing();
				} else {
					if (outcome.error) throw outcome.error;
					expect(outcome.value).equal(store);
					held.assertServing();
				}
			} finally {
				held.release();
				await held.outcome;
				held.restore();
				openedLog.restore();
			}
			if (!fail) await store.close();
		}
	});

	it("preserves an injected index open override and resets query deferral for standalone reopen", async () => {
		const index = new Documents<Document>().index;
		const originalOpen = index.open.bind(index);
		let captured: Parameters<typeof index.open>[0] | undefined;
		const openOverride = sinon
			.stub(index, "open")
			.callsFake(async (properties) => {
				captured = properties;
				return originalOpen({ ...properties, includeIndexed: true });
			});
		const store = new TestStore({ docs: new Documents<Document>({ index }) });
		const held = holdLogOpen(store);
		try {
			await held.entered;
			expect(openOverride.calledOnce).equal(true);
			expect((index as any).includeIndexed).equal(true);
			held.assertNotServing();
			held.release();
			const outcome = await held.outcome;
			if (outcome.error) throw outcome.error;
			held.assertServing();
		} finally {
			held.release();
			await held.outcome;
			held.restore();
			openOverride.restore();
		}
		await store.close();
		const queryOpen = sinon.spy(index._query, "open");
		await session.peers[0].open(index, {
			args: { ...captured!, includeIndexed: false },
		});
		expect(queryOpen.calledOnce).equal(true);
		expect((index as any).includeIndexed).equal(false);
		expect((index._query as any)._subscribed).equal(true);
		expect((index._query as any)._listenerAttached).equal(true);
		await index.close();
	});
});
