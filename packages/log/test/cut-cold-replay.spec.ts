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
	}
});
