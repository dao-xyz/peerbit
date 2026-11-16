import { createStore } from "@peerbit/any-store";
import { AnyBlockStore } from "@peerbit/blocks";
import { Ed25519Keypair } from "@peerbit/crypto";
import { create as createRust } from "@peerbit/indexer-rust";
import { create as createSQLite } from "@peerbit/indexer-sqlite3";
import { EntryType, Log } from "@peerbit/log";
import { expect } from "chai";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import sinon from "sinon";

// Persistent candidate lookup only: a CUT row is NOT authenticated proof or
// retirement authority. No Documents format, admission or retention is changed.
describe("replicate proof marker persistent candidate lookup (private foundation)", () => {
	for (const backend of [
		"sqlite-js",
		"sqlite-native",
		"rust-native",
	] as const) {
		it(`pages nonhead exact-target CUT candidates after deletion and offline reopen (${backend})`, async () => {
			const directory = await mkdtemp(join(tmpdir(), "peerbit-proof-index-"));
			const identity = await Ed25519Keypair.create();
			const nativeGraph = backend !== "sqlite-js";
			const indexerFactory =
				backend === "rust-native" ? createRust : createSQLite;
			let store = new AnyBlockStore(createStore(join(directory, "blocks")));
			let log = new Log<Uint8Array>();
			const open = async () => {
				await store.start();
				await log.open(store, identity, {
					indexer: await indexerFactory(join(directory, "index")),
					nativeGraph,
					appendDurability: "strict",
				});
				expect(log.entryIndex.properties.index.constructor.name).equal(
					backend === "rust-native" ? "RustIndex" : "SQLiteIndex",
				);
				expect(Boolean(log.entryIndex.properties.nativeGraph)).equal(
					nativeGraph,
				);
			};
			try {
				await open();
				const victim = (
					await log.append(Uint8Array.of(1), { meta: { next: [] } })
				).entry;
				// Preserve the victim during ordinary legacy CUT construction; its
				// eventual removal below is explicitly exact, not recursive.
				const survivor = (
					await log.append(Uint8Array.of(2), { meta: { next: [victim] } })
				).entry;
				const markers = [];
				for (let i = 0; i < 6; i++) {
					markers.push(
						(
							await log.append(Uint8Array.of(3, i), {
								meta: { type: EntryType.CUT, next: [victim] },
							})
						).entry,
					);
				}
				// A lookup restricted to heads would now miss every marker.
				await log.append(Uint8Array.of(4), { meta: { next: markers } });
				const unrelated = (
					await log.append(Uint8Array.of(5), { meta: { next: [] } })
				).entry;
				const unrelatedCut = (
					await log.append(Uint8Array.of(6), {
						meta: { type: EntryType.CUT, next: [unrelated] },
					})
				).entry;
				await log.remove(victim, { recursively: false });
				const markerHashes = markers.map((entry) => entry.hash).sort();
				const victimHash = victim.hash;
				const survivorHash = survivor.hash;
				const logId = new Uint8Array(log.id);

				const check = async () => {
					expect(await log.has(victimHash)).equal(false);
					expect(await store.has(victimHash)).equal(false);
					expect(await log.has(survivorHash)).equal(true);
					const get = sinon.spy(store, "get");
					const getMany = sinon.spy(store, "getMany");
					const allChildren = sinon
						.stub(log.entryIndex, "getJoinChildren")
						.throws(new Error("Unbounded children lookup is forbidden"));
					const candidateIterator = () =>
						log.entryIndex.getHasNext(victimHash, {
							type: "shape",
							shape: {
								hash: true,
								head: true,
								meta: { type: true, next: true },
							},
						});
					try {
						const limited = candidateIterator();
						try {
							expect(await limited.next(2)).to.have.length(2);
							expect(limited.done()).equal(false);
						} finally {
							await limited.close();
						}
						expect(await limited.next(2)).to.deep.equal([]);

						const candidates = candidateIterator();
						const found: string[] = [];
						let examined = 0;
						try {
							// The fixture has seven children. Keep both page and total
							// work finite; budget exhaustion must not mean "no proof".
							for (let pageNumber = 0; pageNumber < 4; pageNumber++) {
								const page = await candidates.next(2);
								expect(page.length).at.most(2);
								examined += page.length;
								for (const row of page) {
									expect(row.meta.next).to.include(victimHash);
									expect(row.hash).not.equal(unrelatedCut.hash);
									if (row.meta.type === EntryType.CUT) {
										expect(row.head).equal(false);
										found.push(row.hash);
									} else {
										expect(row.hash).equal(survivorHash);
									}
								}
							}
							expect(candidates.done()).equal(true);
						} finally {
							await candidates.close();
						}
						expect(examined).equal(7);
						expect(found.sort()).to.deep.equal(markerHashes);
						expect(get.callCount).equal(0);
						expect(getMany.callCount).equal(0);
					} finally {
						get.restore();
						getMany.restore();
						allChildren.restore();
					}
				};
				await check();
				await log.close();
				await store.stop();
				// New log, indexer and block-store objects: no retained in-memory
				// reverse edges, no remote source and no proof payload reads.
				log = new Log({ id: logId });
				store = new AnyBlockStore(createStore(join(directory, "blocks")));
				await open();
				await check();
			} finally {
				await log.close();
				await store.stop();
				await rm(directory, { recursive: true, force: true });
			}
		});
	}
});
