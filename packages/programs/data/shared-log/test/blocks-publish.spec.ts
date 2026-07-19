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

	it("does not retain unsolicited blocks unless eager mode is explicit", async () => {
		session = await TestSession.connected(1);
		db = await session.peers[0].open(new EventStore<string, any>());
		expect(db.log.getEagerBlockCacheTelemetry()).to.equal(undefined);
		const runtime = db.log.getRuntimeSnapshot();
		expect(runtime.nativeGraph.active).to.be.a("boolean");
		expect(runtime.nativeGraph.useHeads).to.equal(
			runtime.nativeGraph.active &&
				db.log.log.entryIndex.properties.nativeGraph?.useHeads === true,
		);
		expect(Object.isFrozen(runtime)).to.equal(true);
		expect(Object.isFrozen(runtime.nativeGraph)).to.equal(true);
	});

	it("keeps explicit eager mode available with bounded defaults", async () => {
		session = await TestSession.connected(1);
		db = await session.peers[0].open(new EventStore<string, any>(), {
			args: { eagerBlocks: true },
		});
		const telemetry = db.log.getEagerBlockCacheTelemetry()!;
		expect(telemetry.limits).to.include({
			maxEntries: 1_000,
			maxBytes: 32 * 1024 * 1024,
			maxBlockBytes: 10 * 1024 * 1024,
		});
	});
});
