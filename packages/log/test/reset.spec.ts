import { AnyBlockStore, type BlockStore } from "@peerbit/blocks";
import { expect } from "chai";
import type { Entry, ShallowOrFullEntry } from "../src/entry.js";
import { Log } from "../src/log.js";
import { signKey } from "./fixtures/privateKey.js";
import { JSON_ENCODING } from "./utils/encoding.js";

describe("reset", function () {
	let store: BlockStore;

	beforeEach(async () => {
		store = new AnyBlockStore();
		await store.start();
	});

	afterEach(async () => {
		await store.stop();
	});

	it("will emit events on reset", async () => {
		const log = new Log();
		let deleted: ShallowOrFullEntry<any>[] = [];
		let added: Entry<any>[] = [];
		await log.open(store, signKey, {
			encoding: JSON_ENCODING,
			onChange: (change) => {
				for (const eleent of change.added) {
					added.push(eleent);
				}

				for (const element of change.removed) {
					deleted.push(element);
				}
			},
		});

		await log.append(new Uint8Array([0]), { meta: { next: [] } });
		await log.append(new Uint8Array([1]), { meta: { next: [] } });
		await log.append(new Uint8Array([2]), { meta: { next: [] } });

		expect(added.length).equal(3);
		expect(deleted.length).equal(0);

		await log.load({ reset: true, heads: await log.getHeads(true).all() });

		expect(added.length).equal(6);
		expect(deleted.length).equal(3);
	});
});
