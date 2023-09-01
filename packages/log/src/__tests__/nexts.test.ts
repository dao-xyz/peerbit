import rmrf from "rimraf";
import { Log } from "../log.js";

import { BlockStore, MemoryLevelBlockStore } from "@peerbit/blocks";
import { signKey } from "./fixtures/privateKey.js";
import { JSON_ENCODING } from "./utils/encoding.js";

describe("Log - Nexts", function () {
	let store: BlockStore;

	beforeAll(async () => {
		store = new MemoryLevelBlockStore();
		await store.start();
	});

	afterAll(async () => {
		await store.stop();
	});
	describe("Custom next", () => {
		it("can fork explicitly", async () => {
			const log1 = new Log();
			await log1.open(store, signKey, { encoding: JSON_ENCODING });
			const { entry: e0 } = await log1.append("0", { meta: { next: [] } });
			const { entry: e1 } = await log1.append("1", { meta: { next: [e0] } });

			const { entry: e2a } = await log1.append("2a", {
				meta: { next: await log1.getHeads() }
			});
			expect((await log1.toArray())[0].next?.length).toEqual(0);
			expect((await log1.toArray())[1].next).toEqual([e0.hash]);
			expect((await log1.toArray())[2].next).toEqual([e1.hash]);
			expect((await log1.getHeads()).map((h) => h.hash)).toContainAllValues([
				e2a.hash
			]);
			/*    expect([...log1._nextsIndexToHead[e0.hash]]).toEqual([e1.hash]); */

			// fork at root
			const { entry: e2ForkAtRoot } = await log1.append("2b", {
				meta: { next: [] }
			});
			expect((await log1.toArray())[3].hash).toEqual(e2ForkAtRoot.hash); // Due to clock  // If we only use logical clok then it should be index 1 since clock is reset as this is a root "fork"
			expect((await log1.toArray())[2].hash).toEqual(e2a.hash);
			expect((await log1.getHeads()).map((h) => h.hash)).toContainAllValues([
				e2a.hash,
				e2ForkAtRoot.hash
			]);

			// fork at 0
			const { entry: e2ForkAt0 } = await log1.append("2c", {
				meta: { next: [e0] }
			});
			expect((await log1.toArray())[4].next).toEqual([e0.hash]);
			expect((await log1.getHeads()).map((h) => h.hash)).toContainAllValues([
				e2a.hash,
				e2ForkAtRoot.hash,
				e2ForkAt0.hash
			]);

			// fork at 1
			const { entry: e2ForkAt1 } = await log1.append("2d", {
				meta: { next: [e1] }
			});
			expect((await log1.toArray())[5].next).toEqual([e1.hash]);
			expect((await log1.getHeads()).map((h) => h.hash)).toContainAllValues([
				e2a.hash,
				e2ForkAtRoot.hash,
				e2ForkAt0.hash,
				e2ForkAt1.hash
			]);
		});
	});
});
