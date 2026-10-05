import { InMemorySession } from "@peerbit/libp2p-test-utils/inmemory-libp2p.js";
import { AbortError, waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import pDefer from "p-defer";
import sinon from "sinon";
import { MSG_PROVIDER_QUERY } from "../src/fanout-tree-codec.js";
import {
	type FanoutProviderCandidate,
	FanoutTree,
} from "../src/fanout-tree.js";

describe("fanout provider query lifecycle", () => {
	for (const ending of [
		"caller abort",
		"timeout",
		"service stop",
		"send failure",
	] as const) {
		it(`retires pending requests on ${ending} while query signing is held`, async function () {
			this.timeout(20_000);
			const trackerCount = ending === "send failure" ? 2 : 1;
			const session = await InMemorySession.disconnected<{
				fanout: FanoutTree;
			}>(trackerCount + 1, {
				basePort: 47_500,
				services: {
					fanout: (c) => new FanoutTree(c, { connectionManager: false }),
				},
			});
			const sandbox = sinon.createSandbox();
			const consumer = session.peers[trackerCount]!.services.fanout;
			const internals = consumer as any;
			const abort = new AbortController();
			const sentinel = new Error("provider query send failed");
			const gates = Array.from({ length: trackerCount }, () => pDefer<void>());
			const signed = pDefer<void>();
			const realSetTimeout = globalThis.setTimeout;
			const realClearTimeout = globalThis.clearTimeout;
			let clock: sinon.SinonFakeTimers | undefined;
			const requests: Array<{
				id: number;
				pending: Map<number, unknown>;
				record: unknown;
			}> = [];
			const messages: unknown[] = [];
			let signerReturns = 0;
			let pendingAtSettlement: unknown[] | undefined;
			let outcome:
				| { value: FanoutProviderCandidate[] }
				| { error: unknown }
				| undefined;
			let settled: Promise<void> | undefined;
			let stopping: Promise<void> | undefined;

			try {
				consumer.setBootstraps(
					session.peers
						.slice(0, trackerCount)
						.flatMap((peer) => peer.getMultiaddrs()),
				);
				// Establish real tracker streams before installing the signing gate.
				await consumer.announceProvider("provider-query-lifecycle-setup");
				for (const peer of session.peers.slice(0, trackerCount)) {
					expect(
						internals.peers.has(peer.services.fanout.publicKeyHash),
					).to.equal(true);
				}
				const namespace = `provider-query-lifecycle-${ending}`;
				const { suffixKey } = internals.getProviderNamespaceId(namespace);
				const originalCreate = internals.createMessage.bind(consumer);
				sandbox
					.stub(internals, "createMessage")
					.callsFake(async (...args: any[]) => {
						const bytes = args[0];
						if (
							!(bytes instanceof Uint8Array) ||
							bytes[0] !== MSG_PROVIDER_QUERY
						) {
							return originalCreate(...args);
						}
						const { reqId } = internals.codec.decodeProviderQuery(bytes);
						const index = requests.length;
						const pending =
							internals.pendingProviderQueryBySuffixKey.get(suffixKey);
						requests.push({ id: reqId, pending, record: pending.get(reqId) });
						const message = await originalCreate(...args);
						messages.push(message);
						if (messages.length === trackerCount) signed.resolve();
						await gates[index]!.promise;
						signerReturns++;
						if (ending === "send failure" && index === 0) throw sentinel;
						return message;
					});
				const publish = sandbox.spy(internals, "publishMessageMaybe");
				if (ending === "timeout") {
					clock = sandbox.useFakeTimers({
						toFake: ["setTimeout", "clearTimeout"],
					});
				}
				settled = consumer
					.queryProviderCandidates(namespace, {
						want: 4,
						cacheTtlMs: 0,
						signal: abort.signal,
					})
					.then(
						(value) => {
							pendingAtSettlement = requests.map((request) =>
								request.pending.get(request.id),
							);
							outcome = { value };
						},
						(error: unknown) => {
							pendingAtSettlement = requests.map((request) =>
								request.pending.get(request.id),
							);
							outcome = { error };
						},
					);
				let barrierTimer: ReturnType<typeof setTimeout> | undefined;
				try {
					await Promise.race([
						signed.promise,
						new Promise<never>((_, reject) => {
							barrierTimer = realSetTimeout(
								() => reject(new Error("query signing barrier not reached")),
								2_000,
							);
						}),
					]);
				} finally {
					if (barrierTimer) realClearTimeout(barrierTimer);
				}
				expect(requests).to.have.length(trackerCount);
				expect(messages).to.have.length(trackerCount);
				expect(new Set(requests.map((request) => request.id)).size).to.equal(
					trackerCount,
				);
				for (const request of requests) expect(request.record).to.exist;
				expect(signerReturns).to.equal(0);

				if (ending === "caller abort") abort.abort();
				if (ending === "service stop") stopping = consumer.stop();
				if (ending === "send failure") gates[0]!.resolve();
				if (ending === "timeout") {
					// Expire the unchanged 1s request deadline, then release signing in
					// the same turn: cancellation cannot wait for a later continuation.
					clock!.tick(1_000);
					gates[0]!.resolve();
					clock!.restore();
					clock = undefined;
				}
				await waitForResolved(() => expect(outcome).not.to.equal(undefined), {
					timeout: 2_000,
				});
				if (ending === "timeout") {
					expect(outcome).to.deep.equal({ value: [] });
				} else if (ending === "send failure") {
					expect(outcome).to.have.property("error", sentinel);
				} else {
					expect(outcome)
						.to.have.property("error")
						.that.is.instanceOf(AbortError);
				}
				// Abort, stop, and sibling failure must settle with signing still held.
				expect(signerReturns).to.equal(
					ending === "send failure" || ending === "timeout" ? 1 : 0,
				);
				expect(
					pendingAtSettlement,
					"requests retired at public settlement",
				).to.deep.equal(
					Array.from({ length: trackerCount }, (): undefined => undefined),
				);
				for (const request of requests) {
					expect(
						request.pending.get(request.id),
						"owned request retired",
					).to.equal(undefined);
				}

				for (const gate of gates) gate.resolve();
				await waitForResolved(() =>
					expect(signerReturns).to.equal(trackerCount),
				);
				// Drain the resumed send continuations, not a transport timing window.
				await new Promise<void>((resolve) => setImmediate(resolve));
				expect(
					publish.getCalls().filter((call) => messages.includes(call.args[1])),
					"cancelled query is never published after signing finishes",
				).to.have.length(0);
			} finally {
				clock?.restore();
				abort.abort();
				for (const gate of gates) gate.resolve();
				try {
					await settled;
					await stopping;
					await new Promise<void>((resolve) => setImmediate(resolve));
				} finally {
					sandbox.restore();
					await session.stop();
				}
			}
		});
	}
});
