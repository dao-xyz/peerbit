import { AnyBlockStore } from "@peerbit/blocks";
import { Ed25519Keypair } from "@peerbit/crypto";
import { Log } from "@peerbit/log";
import { expect } from "chai";
import pDefer from "p-defer";
import sinon from "sinon";
import { SharedLog } from "../src/index.js";

describe("sync-repair-session stale held offers", () => {
	for (const nativeGraph of [false, true]) {
		describe(nativeGraph ? "native" : "JS", () => {
			const methods = SharedLog.prototype as any;
			let blocks: AnyBlockStore;
			let lower: Log<Uint8Array>;
			let releases: (() => void)[];
			let pending: Promise<unknown>[];

			beforeEach(async () => {
				releases = [];
				pending = [];
				blocks = new AnyBlockStore();
				await blocks.start();
				lower = new Log<Uint8Array>();
				await lower.open(blocks, await Ed25519Keypair.create(), {
					nativeGraph,
					// SharedLog installs this completion hook for both storage paths; it
					// makes lower mutations participate in the coupled generation fence.
					__peerbitOnDeleteCommitted: async () => {},
				} as any);
				expect(!!lower.entryIndex.properties.nativeGraph).to.equal(nativeGraph);
			});

			afterEach(async () => {
				for (const release of releases) release();
				await Promise.allSettled(pending);
				sinon.restore();
				await lower.close();
				await blocks.stop();
			});

			const append = async () =>
				(await lower.append(new Uint8Array([1]), { meta: { next: [] } })).entry;

			const harness = (hashes: string[]) => {
				const state = { session: {}, epoch: {}, current: true };
				const entries = new Map(hashes.map((hash) => [hash, { hash }]));
				const frontier = new Map(entries);
				const sent: string[][] = [];
				const record = (entries: Map<string, unknown>) => {
					sent.push([...entries.keys()]);
				};
				const host = {
					log: lower,
					_peerSessions: {
						current: () => state.session,
						receiveEpoch: () => state.epoch,
					},
					_instanceLifecycle: {
						ownershipLifecycleController: new AbortController(),
					},
					_repairFrontierByMode: new Map([
						["churn", new Map([["peer", frontier]])],
					]),
					_repairFrontierBypassKnownPeersByMode: new Map(),
					isRepairLifecycleActive: () => true,
					isEntryRecentlyKnownByPeer: () => false,
					isEntryKnownByPeer: () => false,
					clearRepairFrontierHashes: methods.clearRepairFrontierHashes,
					markEntriesKnownByPeer: sinon.spy(),
					pushRepairEntries: async (
						_peer: string,
						entries: Map<string, unknown>,
					) => record(entries),
					syncronizer: {
						onMaybeMissingEntries: async (options: {
							entries: Map<string, unknown>;
						}) => record(options.entries),
						onEntryAddedHash: sinon.spy(),
					},
				};
				const send = (transport = "rateless") => {
					const operation = methods.sendRepairEntriesWithTransport.call(
						host,
						"peer",
						entries,
						transport,
						{
							isStillCurrent: () => state.current,
							signal:
								host._instanceLifecycle.ownershipLifecycleController.signal,
						},
						"churn",
					) as Promise<void>;
					pending.push(operation);
					return operation;
				};
				return { state, host, entries, frontier, sent, send };
			};

			for (const transport of ["rateless", "simple"]) {
				it(`drops a removed entry from offers and the local frontier (${transport})`, async () => {
					const removed = await append();
					const kept = await append();
					const h = harness([removed.hash, kept.hash]);
					await lower.delete(removed.hash);
					expect(await lower.has(removed.hash)).to.equal(false);

					await h.send(transport);

					expect(h.sent).to.deep.equal([[kept.hash]]);
					expect([...h.frontier.keys()]).to.deep.equal([kept.hash]);
					expect(h.host.markEntriesKnownByPeer.called).to.equal(false);
					expect(h.host.syncronizer.onEntryAddedHash.called).to.equal(false);
					// A stale caller-owned snapshot stays reusable; it is not rewritten as
					// remote presence, completion, or a persisted receipt.
					expect([...h.entries.keys()]).to.deep.equal([
						removed.hash,
						kept.hash,
					]);
				});
			}

			it("does not send or clear a successor's frontier after a delayed presence read", async () => {
				const entry = await append();
				const h = harness([entry.hash]);
				await lower.delete(entry.hash);
				const gate = pDefer<void>();
				releases.push(() => gate.resolve());
				const presence = sinon.stub(lower, "hasMany").callsFake(async () => {
					await gate.promise;
					return new Set<string>();
				});
				const sending = h.send();
				await Promise.resolve();
				expect(presence.called).to.equal(true);
				h.state.session = {};
				gate.resolve();
				await sending;
				expect(h.sent).to.deep.equal([]);
				expect([...h.frontier.keys()]).to.deep.equal([entry.hash]);
			});

			for (const boundary of [
				"mutation",
				"receive epoch",
				"lifecycle",
				"cancellation",
			]) {
				it(`does not trust a presence read across a changed ${boundary}`, async () => {
					const entry = await append();
					const h = harness([entry.hash]);
					const gate = pDefer<void>();
					releases.push(() => gate.resolve());
					const presence = sinon.stub(lower, "hasMany").callsFake(async () => {
						await gate.promise;
						return new Set<string>();
					});
					const sending = h.send();
					await Promise.resolve();
					expect(presence.called).to.equal(true);
					if (boundary === "mutation") await append();
					else if (boundary === "receive epoch") h.state.epoch = {};
					else if (boundary === "cancellation")
						h.host._instanceLifecycle.ownershipLifecycleController.abort();
					else h.state.current = false;
					gate.resolve();
					await sending;
					expect(h.sent).to.deep.equal([]);
					expect([...h.frontier.keys()]).to.deep.equal([entry.hash]);
				});
			}

			it("allows a removed hash to be offered again after an authorized local rejoin", async () => {
				const entry = await append();
				const h = harness([entry.hash]);
				const profile = sinon.spy();
				Object.assign(h.host, { _logProperties: { sync: { profile } } });
				await lower.delete(entry.hash);
				await h.send();
				expect(h.sent).to.deep.equal([]);
				expect(h.frontier.size).to.equal(0);
				expect(profile.firstCall.args[0].details.outcome).to.equal("not-held");
				await lower.join([entry]);
				await h.send();
				expect(h.sent).to.deep.equal([[entry.hash]]);
				expect(profile.secondCall.args[0].details.outcome).to.equal(
					"dispatched",
				);
			});

			it("bounds presence reads without turning an index failure into absence", async () => {
				const hashes = Array.from({ length: 2_001 }, (_, i) => `entry-${i}`);
				const h = harness(hashes);
				const presence = sinon
					.stub(lower, "hasMany")
					.callsFake(async (hashes) => new Set(hashes));
				await h.send();
				expect(
					presence.getCalls().map((call) => [...call.args[0]].length),
				).to.deep.equal([1_000, 1_000, 1]);
				expect(h.sent).to.deep.equal([hashes]);
				expect(h.frontier.size).to.equal(hashes.length);

				const failure = new Error("presence index failed");
				presence.rejects(failure);
				let caught: unknown;
				try {
					await h.send();
				} catch (error) {
					caught = error;
				}
				expect(caught).to.equal(failure);
				expect(h.sent).to.deep.equal([hashes]);
				expect(h.frontier.size).to.equal(hashes.length);
			});

			it("leaves a frontier intact while lower mutation admission is occupied", async () => {
				const entry = await append();
				const h = harness([entry.hash]);
				const owner = await lower.entryIndex.acquireHashMutationLocks([
					entry.hash,
				]);
				try {
					await h.send();
					expect(h.sent).to.deep.equal([]);
					expect([...h.frontier.keys()]).to.deep.equal([entry.hash]);
				} finally {
					lower.entryIndex.releaseHashMutationLocks(owner);
				}
				await h.send();
				expect(h.sent).to.deep.equal([[entry.hash]]);
			});

			it("retries append backfill after a concurrent lower mutation releases", async () => {
				const entry = await append();
				const h = harness([entry.hash]);
				const controller =
					h.host._instanceLifecycle.ownershipLifecycleController;
				const sleeping = pDefer<void>();
				const resume = pDefer<void>();
				const retried = pDefer<void>();
				releases.push(() => {
					controller.abort();
					resume.resolve();
				});
				const mode = "append-backfill";
				h.host._repairFrontierByMode = new Map([
					[mode, new Map([["peer", h.frontier]])],
				]);
				let sleeps = 0;
				Object.assign(h.host, {
					_repairFrontierActiveTargetsByMode: new Map([[mode, new Map()]]),
					_recentRepairDispatch: new Map(),
					_repairMetrics: {
						[mode]: {
							dispatches: 0,
							entries: 0,
							simpleFallbackPasses: 0,
							ratelessFirstPasses: 0,
						},
					},
					isFrontierTrackedRepairMode: methods.isFrontierTrackedRepairMode,
					shouldBypassKnownPeerHints: methods.shouldBypassKnownPeerHints,
					sendRepairEntriesWithTransport:
						methods.sendRepairEntriesWithTransport,
					sendMaybeMissingEntriesNow: methods.sendMaybeMissingEntriesNow,
					isRepairLifecycleActive: () => !controller.signal.aborted,
					sleepTracked: async () => {
						if (sleeps++ === 0) {
							sleeping.resolve();
							await resume.promise;
							return true;
						}
						controller.abort();
						retried.resolve();
						return false;
					},
				});
				const owner = await lower.entryIndex.acquireHashMutationLocks([
					entry.hash,
				]);
				try {
					methods.ensureRepairFrontierRunner.call(
						h.host,
						mode,
						"peer",
						[0, 1],
						controller,
					);
					await sleeping.promise;
					expect(h.sent).to.deep.equal([]);
					expect([...h.frontier.keys()]).to.deep.equal([entry.hash]);
				} finally {
					lower.entryIndex.releaseHashMutationLocks(owner);
				}
				resume.resolve();
				await retried.promise;
				expect(h.sent).to.deep.equal([[entry.hash]]);
				// Dispatch is not an acknowledgement: the still-held obligation remains.
				expect([...h.frontier.keys()]).to.deep.equal([entry.hash]);
				expect(h.host.markEntriesKnownByPeer.called).to.equal(false);
			});
		});
	}
});
