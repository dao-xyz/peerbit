import { Log } from "../src/log.js";

import { type BlockStore, AnyBlockStore } from "@peerbit/blocks";
import { signKey } from "./fixtures/privateKey.js";
import { JSON_ENCODING } from "./utils/encoding.js";
import { expect } from "chai";

describe("Log - Nexts", function () {
	let store: BlockStore;

	before(async () => {
		store = new AnyBlockStore();
		await store.start();
	});

	after(async () => {
		await store.stop();
	});
	describe("Custom next", () => {
		it("can fork explicitly", async () => {
			const log1 = new Log();
			await log1.open(store, signKey, { encoding: JSON_ENCODING });
			const { entry: e0 } = await log1.append("0", { meta: { next: [] } });
			const { entry: e1 } = await log1.append("1", { meta: { next: [e0] } });

			const { entry: e2a } = await log1.append("2a", {
				meta: { next: await log1.getHeads(true).all() }
			});
			expect((await log1.toArray())[0].next?.length).equal(0);
			expect((await log1.toArray())[1].next).to.deep.equal([e0.hash]);
			expect((await log1.toArray())[2].next).to.deep.equal([e1.hash]);
			expect((await log1.getHeads().all()).map((h) => h.hash)).to.have.members([
				e2a.hash
			]);
			/*    expect([...log1._nextsIndexToHead[e0.hash]]).to.deep.equal([e1.hash]); */

			// fork at root
			const { entry: e2ForkAtRoot } = await log1.append("2b", {
				meta: { next: [] }
			});
			expect((await log1.toArray())[3].hash).equal(e2ForkAtRoot.hash); // Due to clock  // If we only use logical clok then it should be index 1 since clock is reset as this is a root "fork"
			expect((await log1.toArray())[2].hash).equal(e2a.hash);
			expect((await log1.getHeads().all()).map((h) => h.hash)).to.have.members([
				e2a.hash,
				e2ForkAtRoot.hash
			]);

			// fork at 0
			const { entry: e2ForkAt0 } = await log1.append("2c", {
				meta: { next: [e0] }
			});
			expect((await log1.toArray())[4].next).to.deep.equal([e0.hash]);
			expect((await log1.getHeads().all()).map((h) => h.hash)).to.have.members([
				e2a.hash,
				e2ForkAtRoot.hash,
				e2ForkAt0.hash
			]);

			// fork at 1
			const { entry: e2ForkAt1 } = await log1.append("2d", {
				meta: { next: [e1] }
			});
			expect((await log1.toArray())[5].next).to.deep.equal([e1.hash]);
			expect((await log1.getHeads().all()).map((h) => h.hash)).to.have.members([
				e2a.hash,
				e2ForkAtRoot.hash,
				e2ForkAt0.hash,
				e2ForkAt1.hash
			]);
		});
	});
});
