import { BlockResponse } from "@peerbit/blocks";
import { TestSession } from "@peerbit/test-utils";
import { SilentDelivery } from "@peerbit/stream-interface";
import { expect } from "chai";
import { BlocksMessage } from "../src/blocks.js";
import { EventStore } from "./utils/stores/event-store.js";

describe("shared-log block publish adapter", () => {
	let session: TestSession;
	let db: EventStore<string, any> | undefined;

	afterEach(async () => {
		if (db && db.closed === false) {
			await db.drop();
		}
		await session?.stop();
	});

	it("converts bare target lists into silent delivery for RPC block responses", async () => {
		session = await TestSession.connected(2);
		db = await session.peers[0].open(new EventStore<string, any>());

		const sent: { message: any; options: any }[] = [];
		db.log.rpc.send = async (message: any, options: any) => {
			sent.push({ message, options });
		};

		const remoteBlocks = (db.log as any).remoteBlocks;
		await remoteBlocks.options.publish(
			new BlockResponse("cid", new Uint8Array([1, 2, 3])),
			{ to: [session.peers[1].identity.publicKey] } as any,
		);

		expect(sent).to.have.length(1);
		expect(sent[0].message).to.be.instanceOf(BlocksMessage);
		expect(sent[0].options.mode).to.be.instanceOf(SilentDelivery);
		expect(sent[0].options.mode.to).to.deep.equal([
			session.peers[1].identity.publicKey.hashcode(),
		]);
		expect(sent[0].options.mode.redundancy).to.equal(1);
	});
});
