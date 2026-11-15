import { BlockRequest } from "@peerbit/blocks";
import { TestSession } from "@peerbit/test-utils";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import sinon from "sinon";
import { EntryWithRefs, ExchangeHeadsMessage } from "../src/exchange-heads.js";
import { EventStore } from "./utils/stores/event-store.js";

describe("receive admission parent-fetch cancellation", () => {
	for (const terminal of ["close", "peer stop"] as const) {
		it(`drains a missing-parent receive during ${terminal} without its block timeout`, async () => {
			const session = await TestSession.disconnected(2);
			let receiving: Promise<void> | undefined;
			let closing: Promise<unknown> | undefined;
			let sharedLog: any;
			try {
				const source = await session.peers[0].open(
					new EventStore<string, any>(),
					{
						args: { replicate: false },
					},
				);
				const target = await session.peers[1].open(source.clone(), {
					args: { replicate: false, keep: () => true },
				});
				const parent = (
					await source.add("missing parent", {
						target: "none",
						meta: { next: [] },
					})
				).entry;
				const child = (
					await source.add("received child", {
						target: "none",
						meta: { next: [parent] },
					})
				).entry;
				sharedLog = target.log as any;
				const remoteBlocks = sharedLog.remoteBlocks;
				const sourceKey = source.node.identity.publicKey;
				sinon
					.stub(remoteBlocks, "resolveRemoteProviders")
					.resolves([sourceKey.hashcode()]);
				const requests: BlockRequest[] = [];
				sinon
					.stub(remoteBlocks.options, "publish")
					.callsFake(async (message: unknown) => {
						if (message instanceof BlockRequest) requests.push(message);
					});
				const get = sinon.spy(remoteBlocks, "get");
				receiving = target.log.onMessage(
					new ExchangeHeadsMessage({
						heads: [new EntryWithRefs({ entry: child, gidRefrences: [] })],
					}),
					{ from: sourceKey } as any,
				);
				await waitForResolved(
					() => expect(requests.length).to.be.greaterThan(0),
					{ timeout: 1_000 },
				);
				const fetch = get
					.getCalls()
					.find((call) => call.args[0] === parent.hash && call.args[1]?.remote);
				expect(fetch).not.to.equal(undefined);
				expect((fetch!.args[1]!.remote as any).timeout).to.equal(undefined);
				expect(await target.log.log.has(child.hash)).to.equal(false);
				expect(sharedLog._activeReceiveHandlersByPeer.size).to.equal(1);
				let closed = false;
				closing = (
					terminal === "close" ? target.close() : session.peers[1].stop()
				).then(() => {
					closed = true;
				});
				await waitForResolved(() => expect(closed).to.equal(true), {
					timeout: 1_000,
				});
				await receiving;
				expect(sharedLog._activeReceiveHandlersByPeer.size).to.equal(0);
				expect(remoteBlocks._readFromPeersPromises.size).to.equal(0);
			} finally {
				// Also releases the real 30s block read on the deliberately unfixed
				// baseline, so its failure never inflates the fixture's cleanup time.
				await sharedLog?.remoteBlocks.stop();
				await receiving;
				await closing;
				sinon.restore();
				await session.stop();
			}
		});
	}
});
