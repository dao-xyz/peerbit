import { AnyBlockStore } from "@peerbit/blocks";
import { Ed25519Keypair } from "@peerbit/crypto";
import { Log } from "@peerbit/log";
import { expect } from "chai";
import sinon from "sinon";
import { createExchangeHeadsMessages } from "../src/exchange-heads.js";

describe("exchange heads", () => {
	let store: AnyBlockStore;
	let signKey: Ed25519Keypair;

	before(async () => {
		store = new AnyBlockStore();
		signKey = await Ed25519Keypair.create();
		await store.start();
	});

	after(async () => {
		await store.stop();
	});

	it("uses native graph reference gids for single-head messages", async () => {
		const log = new Log<Uint8Array>();
		await log.open(store, signKey, {
			appendDurability: "strict",
			nativeGraph: true,
		});

		const { entry: left } = await log.append(new Uint8Array([1]), {
			meta: { next: [], gidSeed: new Uint8Array([1]) },
		});
		const { entry: right } = await log.append(new Uint8Array([2]), {
			meta: { next: [], gidSeed: new Uint8Array([2]) },
		});
		const { entry: head } = await log.append(new Uint8Array([3]), {
			meta: { next: [left, right] },
		});
		const expectedReferenceGids = [left, right]
			.filter((entry) => entry.meta.gid !== head.meta.gid)
			.map((entry) => entry.meta.gid);

		const nativeGraph = log.entryIndex.properties.nativeGraph!.graph;
		const uniqueReferenceGidsSpy = sinon.spy(
			nativeGraph,
			"uniqueReferenceGids",
		);
		const getShallowSpy = sinon.spy(log.entryIndex, "getShallow");
		try {
			const messages = [];
			for await (const message of createExchangeHeadsMessages(log, [head])) {
				messages.push(message);
			}

			expect(messages).to.have.length(1);
			expect(messages[0]!.heads).to.have.length(1);
			expect(messages[0]!.heads[0]!.entry.hash).equal(head.hash);
			expect(messages[0]!.heads[0]!.gidRefrences).to.deep.equal(
				expectedReferenceGids,
			);
			expect(uniqueReferenceGidsSpy.calledOnceWithExactly(head.hash)).to.be
				.true;
			expect(getShallowSpy.callCount).equal(0);
		} finally {
			getShallowSpy.restore();
			uniqueReferenceGidsSpy.restore();
			await log.close();
		}
	});
});
