import { BlockResponse } from "@peerbit/blocks";
import { TestSession } from "@peerbit/test-utils";
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

	it("passes bare target lists through to RPC.send for block responses", async () => {
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
		expect(sent[0].options.to).to.deep.equal([
			session.peers[1].identity.publicKey,
		]);
		expect(sent[0].options.mode).to.equal(undefined);
	});
});
