import { type AnyStore, createStore } from "@peerbit/any-store";
import { TestSession } from "@peerbit/libp2p-test-utils";
import { expect } from "chai";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import sinon from "sinon";
import { DirectBlock } from "../src/libp2p.js";

describe("DirectBlock crash-safe durability capability", () => {
	let session: TestSession<{ blocks: DirectBlock }> | undefined;
	let directory: string | undefined;

	afterEach(async () => {
		sinon.restore();
		await session?.stop();
		session = undefined;
		if (directory) await fs.rm(directory, { recursive: true, force: true });
		directory = undefined;
	});

	it("does not synthesize a barrier from a persisted claim", async () => {
		const local: AnyStore = createStore();
		// Durability is an explicit capability, not an inference from persisted().
		local.persisted = () => true;
		expect(local.crashSafeDurability).equal(undefined);
		session = await TestSession.disconnected(1, {
			services: {
				blocks: (components) =>
					new DirectBlock(components, { localStore: local, rustCore: false }),
			},
		});
		const blocks = session.peers[0].services.blocks;
		expect(await blocks.persisted()).equal(true);
		expect(blocks.crashSafeDurability).equal(undefined);
	});

	it("forwards the exact disk capability and its barrier failures", async () => {
		directory = await fs.mkdtemp(
			path.join(os.tmpdir(), "peerbit-block-durability-"),
		);
		const local: AnyStore = createStore(directory);
		const durability = local.crashSafeDurability;
		expect(durability?.crashSafe).equal(true);
		session = await TestSession.disconnected(1, {
			services: {
				blocks: (components) =>
					new DirectBlock(components, { localStore: local, rustCore: false }),
			},
		});
		const blocks = session.peers[0].services.blocks;
		expect(blocks.crashSafeDurability).equal(durability);
		const data = new Uint8Array([1, 2, 3]);
		const cid = await blocks.put(data);
		const barrier = sinon.spy(durability!, "barrier");
		await blocks.crashSafeDurability!.barrier();
		expect(barrier.calledOnce).equal(true);
		expect(await blocks.get(cid, { remote: false })).deep.equal(data);
		barrier.restore();
		const failure = new Error("injected local durability barrier failure");
		sinon.stub(durability!, "barrier").rejects(failure);
		await expect(blocks.crashSafeDurability!.barrier()).rejectedWith(
			failure.message,
		);
	});
});
