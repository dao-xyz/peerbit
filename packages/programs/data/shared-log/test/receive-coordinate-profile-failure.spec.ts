import { create as createRustIndexer } from "@peerbit/indexer-rust";
import {
	NativeBackboneCoordinatePersistence,
	NativeBackboneMemoryCoordinatePersistenceStore,
} from "@peerbit/native-backbone";
import { TestSession } from "@peerbit/test-utils";
import { expect } from "chai";
import {
	RawEntryWithRefs,
	RawExchangeHeadsMessage,
} from "../src/exchange-heads.js";
import { createReplicationDomainHash } from "../src/replication-domain-hash.js";
import { SimpleSyncronizer } from "../src/sync/simple.js";
import { EventStore } from "./utils/stores/event-store.js";

describe("receive coordinate post-commit profile failure", () => {
	for (const nativeBackbone of [false, true]) {
		const phase = nativeBackbone
			? "log.joinPreparedFacts.nativePreparedCommit"
			: "log.joinPreparedFacts.entryIndex";
		it(`completes metadata or fails closed when ${phase} diagnostics throw`, async () => {
			const session = await TestSession.disconnected(2, {
				indexer: createRustIndexer,
			});
			try {
				const args = {
					replicate: false as const,
					keep: () => true,
					timeUntilRoleMaturity: 0,
					nativeGraph: true,
					setup: {
						domain: createReplicationDomainHash("u32"),
						type: "u32" as const,
						syncronizer: SimpleSyncronizer,
						name: "receive-coordinate-profile-failure",
					},
				};
				const source = await session.peers[0].open(
					new EventStore<string, any>(),
					{ args },
				);
				const failure = new Error("post-commit profiling failed");
				let injected = 0;
				const target = await session.peers[1].open(source.clone(), {
					args: {
						...args,
						...(nativeBackbone
							? {
									nativeBackbone: {
										optional: false,
										coordinatePersistence:
											new NativeBackboneCoordinatePersistence(
												new NativeBackboneMemoryCoordinatePersistenceStore(),
											),
									},
								}
							: {}),
						sync: {
							profile: (event) => {
								if (event.name === phase) {
									injected++;
									throw failure;
								}
							},
						},
					},
				});
				const entries = [];
				for (const value of ["first", "second"]) {
					entries.push((await source.add(value, { meta: { next: [] } })).entry);
				}
				const hashes = entries.map((entry) => entry.hash);
				const message = new RawExchangeHeadsMessage({
					heads: await Promise.all(
						entries.map(async (entry) => {
							const bytes = await source.log.log.blocks.get(entry.hash);
							expect(bytes).to.be.instanceOf(Uint8Array);
							return new RawEntryWithRefs({
								hash: entry.hash,
								bytes: bytes!,
								gidRefrences: [],
							});
						}),
					),
				});
				// Either successful completion or the original failure may surface. The
				// safety contract is metadata completion or rejection of later work.
				const captureError = async (operation: () => unknown) =>
					Promise.resolve()
						.then(operation)
						.then(
							() => undefined,
							(error: unknown) => error,
						);
				const assertCause = (error: unknown) => {
					expect(error).to.be.instanceOf(Error);
					const causes: Error[] = [];
					for (
						let current = error;
						current instanceof Error && !causes.includes(current);
						current = current.cause
					) {
						causes.push(current);
					}
					expect(causes).to.include(failure);
				};
				const receiveFailure = await captureError(() =>
					target.log.onMessage(message, {
						from: source.node.identity.publicKey,
					} as any),
				);
				if (receiveFailure !== undefined) assertCause(receiveFailure);
				expect(injected).to.equal(1);
				for (const hash of hashes) {
					expect(await target.log.log.has(hash)).to.equal(true);
					expect(await target.log.log.blocks.has(hash)).to.equal(true);
				}
				const shared = target.log as any;
				if (nativeBackbone) {
					// The fused native mutation happened before this diagnostic callback;
					// its mandatory coordinates cannot be selectively forgotten on error.
					expect(
						shared._nativeBackbone.getEntryCoordinateHashes(),
					).to.have.members(hashes);
				}
				const recovery = await captureError(() =>
					shared.throwIfNativeDurableCommitFailed(),
				);
				if (recovery === undefined) {
					for (const hash of hashes) {
						expect(
							await shared._coordinates.getAuthoritativeCoordinateEntryForReceipt(
								hash,
							),
						).to.have.property("hash", hash);
					}
				} else {
					assertCause(recovery);
					assertCause(
						await captureError(() =>
							target.add("after-profile-failure", { target: "none" }),
						),
					);
				}
			} finally {
				await session.stop();
			}
		});
	}
});
