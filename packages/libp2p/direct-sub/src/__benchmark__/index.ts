import B from "benchmark";
import { LSession } from "@dao-xyz/libp2p-test-utils";
import { DirectSub } from "../index.js";
import crypto from "crypto";
import { waitForPeers } from "@dao-xyz/libp2p-direct-stream";

// Run with "node --loader ts-node/esm ./src/__benchmark__/index.ts"
// size: 1kb x 1,852 ops/sec ±1.44% (86 runs sampled)
// size: 1000kb x 116 ops/sec ±1.98% (80 runs sampled)

const session: LSession = await LSession.disconnected(4);
await session.connect([
	[session.peers[0], session.peers[1]],
	[session.peers[1], session.peers[2]],
	[session.peers[2], session.peers[3]],
]);

/* 
┌─┐
│1│
└┬┘
┌▽┐
│2│
└┬┘
┌▽┐
│3│
└┬┘
┌▽┐
│4│
└─┘

 */

const streams: DirectSub[] = await Promise.all(
	session.peers.map(async (peer) => {
		const stream = new DirectSub(peer, {
			canRelayMessage: true,
			emitSelf: true,
		});
		await stream.start();
		return stream;
	})
);
const TOPIC = "world";
streams[streams.length - 1].subscribe(TOPIC);
await waitForPeers(streams[0], streams[1]);
await waitForPeers(streams[1], streams[2]);
await waitForPeers(streams[2], streams[3]);
let suite = new B.Suite();
let listener: ((msg: any) => any) | undefined = undefined;
const msgMap: Map<string, { resolve: () => any }> = new Map();
const msgIdFn = (msg: Uint8Array) =>
	crypto.createHash("sha1").update(msg.subarray(0, 20)).digest("base64");

const sizes = [1e3, 1e6];
for (const size of sizes) {
	suite = suite.add("size: " + size / 1e3 + "kb", {
		defer: true,
		fn: (deferred) => {
			const small = crypto.randomBytes(size); // 1kb
			msgMap.set(msgIdFn(small), deferred);
			streams[0].publish(small, { topics: [TOPIC] });
		},
		setup: () => {
			listener = (msg) => {
				msgMap.get(msgIdFn(msg.detail.data))!.resolve();
			};

			streams[streams.length - 1].addEventListener("data", listener);
			msgMap.clear();
		},
		teardown: () => {
			streams[streams.length - 1].removeEventListener("data", listener);
		},
	});
}
suite
	.on("cycle", (event: any) => {
		console.log(String(event.target));
	})
	.on("error", (e) => {
		console.error(e);
		throw e;
	})
	.on("complete", function (this: any, ...args: any[]) {
		streams.forEach((stream) => stream.stop());
		session.stop();
	})
	.run({ async: true });
