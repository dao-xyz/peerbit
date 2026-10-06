import { keys } from "@libp2p/crypto";
import { createStore } from "@peerbit/any-store";
import { toId } from "@peerbit/indexer-interface";
import { create as createSQLiteIndices } from "@peerbit/indexer-sqlite3";
import { NativeBackboneNodeCoordinatePersistence } from "@peerbit/native-backbone";
import { TestSession } from "@peerbit/test-utils";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import fs from "fs/promises";
import os from "os";
import pDefer from "p-defer";
import path from "path";
import { Peerbit } from "peerbit";
import { createRustPeerbitOptions } from "peerbit/rust";
import sinon from "sinon";
import { EntryWithRefs, ExchangeHeadsMessage } from "../src/exchange-heads.js";
import { createReplicationDomainHash } from "../src/replication-domain-hash.js";
import { SimpleSyncronizer } from "../src/sync/simple.js";
import { EventStore } from "./utils/stores/event-store.js";

describe("receive admission coordinate recovery", function () {
	this.timeout(30_000);

	for (const native of [false, true]) {
		it(`preserves only the admitted prefix through terminal close and fresh offline reopen (${native ? "native" : "SQLite"})`, async () => {
			const directory = await fs.mkdtemp(
				path.join(os.tmpdir(), "receive-coordinate-reopen-"),
			);
			const createOptions = native
				? createRustPeerbitOptions({ network: false })
				: {
						storage: {
							storeFactory: (storeDirectory?: string) =>
								createStore(storeDirectory),
						},
						indexer: (indexDirectory?: string) =>
							createSQLiteIndices(indexDirectory),
					};
			const diskOptions = {
				...createOptions,
				directory,
				libp2p: { privateKey: await keys.generateKeyPair("Ed25519") },
			};
			const args = {
				replicate: false as const,
				keep: () => true,
				timeUntilRoleMaturity: 0,
				nativeGraph: native,
				nativeBackbone: native ? { optional: false } : (false as const),
				setup: {
					domain: createReplicationDomainHash("u32"),
					type: "u32" as const,
					syncronizer: SimpleSyncronizer,
					name: "receive-coordinate-reopen",
				},
			};
			const diskOpenArgs = () => ({
				...args,
				nativeBackbone: native
					? {
							optional: false,
							coordinatePersistence:
								new NativeBackboneNodeCoordinatePersistence(
									path.join(directory, "coordinate-wal"),
									{ flushOnAppend: true },
								),
						}
					: (false as const),
			});
			const assertCoordinateAuthority = (store: EventStore<string, any>) => {
				const shared = store.log as any;
				if (native) {
					expect(shared._nativeBackbone, "required native backbone").to.exist;
					expect(
						shared.log.entryIndex.properties.nativeGraph,
						"required native graph",
					).to.exist;
					expect(
						shared._nativeBackboneCoordinatePersistence,
						"required durable coordinate journal",
					).to.exist;
				} else {
					expect(
						shared._nativeBackbone,
						"SQLite coordinate authority",
					).to.equal(undefined);
				}
			};
			let session: TestSession | undefined;
			let reopened: Peerbit | undefined;
			const sandbox = sinon.createSandbox();
			const gate = pDefer<void>();
			const pending: Promise<unknown>[] = [];
			const track = <T>(promise: Promise<T>) => {
				void promise.catch(() => {});
				pending.push(promise);
				return promise;
			};
			try {
				session = await TestSession.disconnected(2, [
					createOptions,
					diskOptions,
				]);
				const source = await session.peers[0].open(
					new EventStore<string, any>(),
					{ args },
				);
				const first = (
					await source.add("admitted", { target: "none", meta: { next: [] } })
				).entry;
				const second = (
					await source.add("not admitted", {
						target: "none",
						meta: { next: [] },
					})
				).entry;
				const observed: string[] = [];
				const target = await session.peers[1].open(source.clone(), {
					args: {
						...diskOpenArgs(),
						onChange: async (change) => {
							for (const { entry } of change.added) observed.push(entry.hash);
							if (change.added.some(({ entry }) => entry.hash === first.hash)) {
								await gate.promise;
							}
						},
					},
				});
				assertCoordinateAuthority(target);
				const targetKey = session.peers[1].identity.publicKey;
				const clone = source.clone();
				const lowerJoin = sandbox.spy(target.log.log, "join");
				const receiving = track(
					target.log.onMessage(
						new ExchangeHeadsMessage({
							heads: [first, second].map(
								(entry) => new EntryWithRefs({ entry, gidRefrences: [] }),
							),
						}),
						{ from: source.node.identity.publicKey } as any,
					),
				);
				await waitForResolved(() => expect(observed).to.include(first.hash), {
					timeout: 5_000,
				});
				expect(await target.log.log.has(first.hash)).to.equal(true);
				expect(await target.log.log.has(second.hash)).to.equal(false);
				const receiveSignal = lowerJoin.firstCall.args[1]?.signal;
				expect(receiveSignal).to.be.instanceOf(AbortSignal);
				const lowerOutcome = track(
					Promise.resolve(lowerJoin.firstCall.returnValue).then(
						() => undefined,
						(error: unknown) => error,
					),
				);
				let closed = false;
				const closing = track(
					target.close().then(() => {
						closed = true;
					}),
				);
				await waitForResolved(
					() => expect(receiveSignal!.aborted).to.equal(true),
					{
						timeout: 1_000,
					},
				);
				expect(closed).to.equal(false);
				gate.resolve();
				await Promise.all([receiving, closing]);
				expect(await lowerOutcome).to.equal(receiveSignal!.reason);
				expect(observed).to.deep.equal([first.hash]);
				expect((target.log as any)._activeReceiveHandlersByPeer.size).to.equal(
					0,
				);
				await session.stop();
				session = undefined;

				// A new peer and index connection must recover the prefix without a donor.
				reopened = await Peerbit.create(diskOptions);
				expect(reopened.identity.publicKey.equals(targetKey)).to.equal(true);
				expect(reopened.libp2p.getConnections()).to.have.length(0);
				const restored = await reopened.open(clone, { args: diskOpenArgs() });
				assertCoordinateAuthority(restored);
				expect(restored.log.log.length).to.equal(1);
				expect(await restored.log.log.has(first.hash)).to.equal(true);
				expect(await restored.log.log.blocks.has(first.hash)).to.equal(true);
				const coordinates = (restored.log as any)._coordinates;
				expect(
					(
						await coordinates.getAuthoritativeCoordinateEntryForReceipt(
							first.hash,
						)
					)?.hash,
				).to.equal(first.hash);
				expect(await restored.log.log.has(second.hash)).to.equal(false);
				expect(await restored.log.log.blocks.has(second.hash)).to.equal(false);
				expect(
					await coordinates.getAuthoritativeCoordinateEntryForReceipt(
						second.hash,
					),
				).to.equal(undefined);
				if (native) {
					expect(
						(restored.log as any)._nativeBackbone.getEntryCoordinateHashes(),
					).to.deep.equal([first.hash]);
				} else {
					expect(
						(await restored.log.entryCoordinatesIndex.get(toId(first.hash)))
							?.value.hash,
					).to.equal(first.hash);
					expect(
						await restored.log.entryCoordinatesIndex.get(toId(second.hash)),
					).to.equal(undefined);
					expect(
						(await restored.log.entryCoordinatesIndex.iterate({}).all()).map(
							({ value }) => value.hash,
						),
					).to.deep.equal([first.hash]);
				}
				expect(reopened.libp2p.getConnections()).to.have.length(0);
			} finally {
				gate.resolve();
				await Promise.allSettled(pending);
				sandbox.restore();
				try {
					await reopened?.stop();
				} finally {
					try {
						await session?.stop();
					} finally {
						await fs.rm(directory, { recursive: true, force: true });
					}
				}
			}
		});
	}
});
