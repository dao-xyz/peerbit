import { AnyBlockStore } from "@peerbit/blocks";
import { Ed25519Keypair } from "@peerbit/crypto";
import { expect } from "chai";
import { Timestamp } from "../src/clock.js";
import { Log } from "../src/log.js";

describe("timestamp trust boundary", () => {
	it("inherits an admitted author's future wall time despite valid signatures", async () => {
		const sourceStore = new AnyBlockStore();
		const receiverStore = new AnyBlockStore();
		const source = new Log<Uint8Array>();
		const receiver = new Log<Uint8Array>();
		try {
			await Promise.all([sourceStore.start(), receiverStore.start()]);
			await source.open(sourceStore, await Ed25519Keypair.create());
			const receiverKey = await Ed25519Keypair.create();
			await receiver.open(receiverStore, receiverKey);
			const { entry: before } = await receiver.append(Uint8Array.of(0));
			const future = new Timestamp({
				wallTime:
					before.meta.clock.timestamp.wallTime +
					10n * 365n * 24n * 60n * 60n * 1_000_000_000n,
				logical: 7,
			});
			const { entry } = await source.append(Uint8Array.of(1), {
				meta: { timestamp: future },
			});
			expect(entry.meta.clock.timestamp).to.deep.equal(future);

			// Transfer bytes, not the source's Entry or signature-verification cache.
			const bytes = await sourceStore.get(entry.hash, { remote: false });
			expect(bytes).not.to.equal(undefined);
			expect(await receiverStore.put(bytes!)).to.equal(entry.hash);
			expect(await receiver.has(entry.hash)).to.equal(false);
			await receiver.join([entry.hash], { verifySignatures: true });
			const joined = await receiver.get(entry.hash);
			expect(joined).not.to.equal(undefined);
			expect(joined!.meta.clock.timestamp).to.deep.equal(future);
			expect(await joined!.verifySignatures()).to.equal(true);
			expect(receiver.length).to.equal(2);

			const { entry: after } = await receiver.append(Uint8Array.of(2));
			expect(after.meta.clock.id).to.deep.equal(receiverKey.publicKey.bytes);
			expect(after.meta.clock.timestamp.wallTime).to.equal(future.wallTime);
			expect(after.meta.clock.timestamp.compare(future)).to.equal(1);
			expect(after.meta.next).to.have.members([before.hash, entry.hash]);
			expect(await after.verifySignatures()).to.equal(true);
		} finally {
			try {
				await Promise.all([receiver.close(), source.close()]);
			} finally {
				await Promise.all([receiverStore.stop(), sourceStore.stop()]);
			}
		}
	});
});
