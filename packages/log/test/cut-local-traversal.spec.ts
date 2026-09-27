import { AnyBlockStore } from "@peerbit/blocks";
import { Ed25519Keypair } from "@peerbit/crypto";
import { HashmapIndices } from "@peerbit/indexer-simple";
import { expect } from "chai";
import sinon from "sinon";
import { EntryType } from "../src/entry-type.js";
import { Log } from "../src/log.js";

describe("recursive CUT cleanup stays in the admitted graph", () => {
	for (const nativeGraph of [false, true]) {
		it(`deletes an indexed chain without loading entry blocks (nativeGraph=${nativeGraph})`, async () => {
			const store = new AnyBlockStore();
			const log = new Log<Uint8Array>();
			const sandbox = sinon.createSandbox();
			await store.start();
			try {
				await log.open(store, await Ed25519Keypair.create(), {
					indexer: new HashmapIndices(),
					nativeGraph,
				});
				const { entry: a } = await log.append(Uint8Array.of(1));
				const { entry: b } = await log.append(Uint8Array.of(2));
				const { entry: c } = await log.append(Uint8Array.of(3));
				(log.entryIndex as any).cache.clear();
				const get = sandbox.spy(store, "get");
				const getMany = sandbox.spy(store, "getMany");
				const { entry: cut } = await log.append(Uint8Array.of(4), {
					meta: { next: [c], type: EntryType.CUT },
				});

				expect(get.callCount).equal(0);
				expect(getMany.callCount).equal(0);
				expect(log.length).equal(1);
				expect(
					(await log.getHeads().all()).map((entry) => entry.hash),
				).deep.equal([cut.hash]);
				for (const entry of [a, b, c]) {
					expect(await log.has(entry.hash)).equal(false);
					expect(await store.has(entry.hash)).equal(false);
				}
			} finally {
				sandbox.restore();
				await log.close();
				await store.stop();
			}
		});

		for (const prefixPresent of [false, true]) {
			it(`does not resolve an unindexed predecessor (nativeGraph=${nativeGraph}, prefixPresent=${prefixPresent})`, async () => {
				const sourceStore = new AnyBlockStore();
				const targetStore = new AnyBlockStore();
				const source = new Log<Uint8Array>();
				const target = new Log<Uint8Array>();
				const sandbox = sinon.createSandbox();
				await sourceStore.start();
				await targetStore.start();
				try {
					const identity = await Ed25519Keypair.create();
					for (const [log, store] of [
						[source, sourceStore],
						[target, targetStore],
					] as const) {
						await log.open(store, identity, {
							indexer: new HashmapIndices(),
							nativeGraph,
						});
					}
					const { entry: prefix } = await source.append(Uint8Array.of(1));
					const { entry: boundary } = await source.append(Uint8Array.of(2));
					if (prefixPresent) {
						await targetStore.put((await sourceStore.get(prefix.hash))!);
					}
					// Fixture for a partial local graph: original signed links are kept.
					// This low-level seed is not an authenticated checkpoint importer.
					await target.entryIndex.put(boundary, {
						unique: true,
						isHead: true,
						toMultiHash: true,
					});
					(target.entryIndex as any).cache.clear();
					const requested: string[] = [];
					const getMany = sandbox.spy(targetStore, "getMany");
					const get = targetStore.get.bind(targetStore);
					sandbox.stub(targetStore, "get").callsFake(async (hash, options) => {
						requested.push(hash);
						if (hash === prefix.hash)
							throw new Error("Unindexed prefix lookup");
						return get(hash, options);
					});
					await target.append(Uint8Array.of(3), {
						meta: { next: [boundary], type: EntryType.CUT },
					});

					expect(requested).deep.equal([]);
					expect(getMany.callCount).equal(0);
					expect(await target.has(boundary.hash)).equal(false);
					expect(await target.has(prefix.hash)).equal(false);
					expect(await targetStore.has(prefix.hash)).equal(prefixPresent);
					expect(target.length).equal(1);
				} finally {
					sandbox.restore();
					await target.close();
					await source.close();
					await targetStore.stop();
					await sourceStore.stop();
				}
			});
		}

		it(`does not cross an unindexed gap to delete an indexed ancestor (nativeGraph=${nativeGraph})`, async () => {
			const store = new AnyBlockStore();
			const log = new Log<Uint8Array>();
			const sandbox = sinon.createSandbox();
			await store.start();
			try {
				await log.open(store, await Ed25519Keypair.create(), {
					indexer: new HashmapIndices(),
					nativeGraph,
				});
				const { entry: ancestor } = await log.append(Uint8Array.of(1));
				const { entry: bridge } = await log.append(Uint8Array.of(2));
				const bridgeBytes = (await store.get(bridge.hash))!;
				await log.delete(bridge.hash);
				expect(await store.put(bridgeBytes)).equal(bridge.hash);
				(log.entryIndex as any).cache.clear();
				const get = sandbox.spy(store, "get");
				const getMany = sandbox.spy(store, "getMany");
				const { entry: cut } = await log.append(Uint8Array.of(3), {
					meta: { next: [bridge], type: EntryType.CUT },
				});

				expect(get.callCount).equal(0);
				expect(getMany.callCount).equal(0);
				expect(await log.has(ancestor.hash)).equal(true);
				expect(await store.has(ancestor.hash)).equal(true);
				expect(await log.has(bridge.hash)).equal(false);
				expect(await store.has(bridge.hash)).equal(true);
				expect(
					(await log.toArray()).map((entry) => entry.hash).sort(),
				).deep.equal([ancestor.hash, cut.hash].sort());
			} finally {
				sandbox.restore();
				await log.close();
				await store.stop();
			}
		});
	}
});
