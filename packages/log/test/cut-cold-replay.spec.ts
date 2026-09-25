import { AnyBlockStore } from "@peerbit/blocks";
import { Ed25519Keypair } from "@peerbit/crypto";
import { HashmapIndices } from "@peerbit/indexer-simple";
import { expect } from "chai";
import { EntryType } from "../src/entry-type.js";
import { Log } from "../src/log.js";

describe("cold CUT replay", () => {
	for (const nativeGraph of [false, true]) {
		it(`rejects a first-seen victim after reopen (nativeGraph=${nativeGraph})`, async () => {
			const sourceStore = new AnyBlockStore();
			const targetStore = new AnyBlockStore();
			const source = new Log<Uint8Array>();
			let target = new Log<Uint8Array>();
			await sourceStore.start();
			await targetStore.start();
			try {
				await source.open(sourceStore, await Ed25519Keypair.create(), {
					indexer: new HashmapIndices(),
					nativeGraph,
				});
				const targetKey = await Ed25519Keypair.create();
				const targetIndexer = new HashmapIndices();
				await target.open(targetStore, targetKey, {
					indexer: targetIndexer,
					nativeGraph,
				});

				const { entry: victim } = await source.append(new Uint8Array([1]), {
					meta: { next: [] },
				});
				const { entry: sibling } = await source.append(new Uint8Array([2]), {
					meta: { next: [victim] },
				});
				const { entry: cut } = await source.append(new Uint8Array([3]), {
					meta: { type: EntryType.CUT, next: [victim] },
				});

				expect(await targetStore.has(victim.hash)).to.equal(false);
				await target.join([cut], { verifySignatures: true });
				expect(target.length).to.equal(1);
				expect(await target.has(victim.hash)).to.equal(false);
				expect(await targetStore.has(victim.hash)).to.equal(false);

				const targetId = target.id;
				await target.close();
				target = new Log<Uint8Array>({ id: targetId });
				await target.open(targetStore, targetKey, {
					indexer: targetIndexer,
					nativeGraph,
				});
				expect(
					(await target.getHeads().all()).map((entry) => entry.hash),
				).to.deep.equal([cut.hash]);
				expect(await targetStore.has(victim.hash)).to.equal(false);

				let replayChanges = 0;
				await target.join([victim], {
					verifySignatures: true,
					onChange: () => {
						replayChanges++;
					},
				});
				expect(replayChanges).to.equal(0);
				expect(target.length).to.equal(1);
				expect(await target.has(victim.hash)).to.equal(false);
				expect(await targetStore.has(victim.hash)).to.equal(false);
				expect(
					(await target.getHeads().all()).map((entry) => entry.hash),
				).to.deep.equal([cut.hash]);

				await target.join([{ entry: sibling, references: [victim] }], {
					verifySignatures: true,
				});
				expect(await target.has(sibling.hash)).to.equal(true);
				expect(await target.has(victim.hash)).to.equal(false);
				expect(
					(await target.getHeads().all()).map((entry) => entry.hash).sort(),
				).to.deep.equal([cut.hash, sibling.hash].sort());
			} finally {
				await target.close();
				await source.close();
				await targetStore.stop();
				await sourceStore.stop();
			}
		});

		it(`keeps the ancestry of a surviving concurrent branch (nativeGraph=${nativeGraph})`, async () => {
			const sourceStore = new AnyBlockStore();
			const targetStore = new AnyBlockStore();
			const source = new Log<Uint8Array>();
			let target = new Log<Uint8Array>();
			await sourceStore.start();
			await targetStore.start();
			try {
				await source.open(sourceStore, await Ed25519Keypair.create(), {
					indexer: new HashmapIndices(),
					nativeGraph,
				});
				const targetKey = await Ed25519Keypair.create();
				const targetIndexer = new HashmapIndices();
				await target.open(targetStore, targetKey, {
					indexer: targetIndexer,
					nativeGraph,
				});

				const { entry: ancestor } = await source.append(Uint8Array.of(1), {
					meta: { next: [] },
				});
				const { entry: victim } = await source.append(Uint8Array.of(2), {
					meta: { next: [ancestor] },
				});
				const { entry: sibling } = await source.append(Uint8Array.of(3), {
					meta: { next: [ancestor] },
				});
				const { entry: cut } = await source.append(Uint8Array.of(4), {
					meta: { type: EntryType.CUT, next: [victim] },
				});
				for (const entry of [ancestor, victim, sibling]) {
					expect(entry.meta.gid).to.equal(cut.meta.gid);
					expect(
						entry.meta.clock.timestamp.compare(cut.meta.clock.timestamp),
					).to.be.lessThan(0);
				}
				expect(await source.has(ancestor.hash)).to.equal(true);
				expect(await source.has(victim.hash)).to.equal(false);

				await target.join([cut], { verifySignatures: true });
				expect(await targetStore.has(ancestor.hash)).to.equal(false);
				const targetId = target.id;
				await target.close();
				target = new Log<Uint8Array>({ id: targetId });
				await target.open(targetStore, targetKey, {
					indexer: targetIndexer,
					nativeGraph,
				});

				// Both branches share a gid and predate the CUT. Neither fact is
				// sufficient to discard the surviving branch or its ancestor.
				await target.join([victim], { verifySignatures: true });
				expect(await target.has(victim.hash)).to.equal(false);
				expect(await target.has(ancestor.hash)).to.equal(false);
				await target.join([{ entry: sibling, references: [ancestor] }], {
					verifySignatures: true,
				});
				await target.join([victim], { verifySignatures: true });
				expect(await target.has(ancestor.hash)).to.equal(true);
				expect(await target.has(sibling.hash)).to.equal(true);
				expect(await target.has(victim.hash)).to.equal(false);
				expect(target.length).to.equal(3);
				expect(
					(await target.getHeads().all()).map((entry) => entry.hash).sort(),
				).to.deep.equal([cut.hash, sibling.hash].sort());
			} finally {
				await target.close();
				await source.close();
				await targetStore.stop();
				await sourceStore.stop();
			}
		});
	}
});
