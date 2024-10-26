import { tcp } from "@libp2p/tcp";
import { TestSession } from "@peerbit/libp2p-test-utils";
import { DataEvent } from "@peerbit/pubsub-interface";
import { waitForNeighbour } from "@peerbit/stream";
import B from "benchmark";
import crypto from "crypto";
import { DirectSub } from "../src/index.js";

// Run with "node --loader ts-node/esm ./benchmark/index.ts"
// size: 1kb x 1,722 ops/sec ±1.89% (82 runs sampled)
// size: 1000kb x 107 ops/sec ±2.02% (85 runs sampled)

const session = await TestSession.disconnected(4, {
	transports: [tcp()],
	services: {
		pubsub: (c: any) =>
			new DirectSub(c, {
				canRelayMessage: true,
				connectionManager: false,
			}),
	},
});

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

const TOPIC = "world";
session.peers[session.peers.length - 1].services.pubsub.subscribe(TOPIC);
await waitForNeighbour(
	session.peers[0].services.pubsub,
	session.peers[1].services.pubsub,
);
await waitForNeighbour(
	session.peers[1].services.pubsub,
	session.peers[2].services.pubsub,
);
await waitForNeighbour(
	session.peers[2].services.pubsub,
	session.peers[3].services.pubsub,
);
let suite = new B.Suite();
let listener: ((msg: any) => any) | undefined = undefined;
const msgMap: Map<string, { resolve: () => any }> = new Map();
const msgIdFn = (msg: Uint8Array) => {
	return crypto.createHash("sha1").update(msg.subarray(0, 20)).digest("base64");
};

const sizes = [1e3, 1e6];
for (const size of sizes) {
	suite = suite.add("size: " + size / 1e3 + "kb", {
		defer: true,
		fn: (deferred: any) => {
			const small = crypto.randomBytes(size); // 1kb
			msgMap.set(msgIdFn(small), deferred);
			session.peers[0].services.pubsub.publish(small, { topics: [TOPIC] });
		},
		setup: () => {
			listener = (msg: CustomEvent<DataEvent>) => {
				msgMap.get(msgIdFn(msg.detail.data.data))!.resolve();
			};

			session.peers[session.peers.length - 1].services.pubsub.addEventListener(
				"data",
				listener,
			);
			msgMap.clear();
		},
		teardown: () => {
			session.peers[
				session.peers.length - 1
			].services.pubsub.removeEventListener("data", listener);
		},
	});
}
suite
	.on("cycle", (event: any) => {
		console.log(String(event.target));
	})
	.on("error", (e: any) => {
		console.error(e);
		throw e;
	})
	.on("complete", function (this: any, ...args: any[]) {
		session.stop();
	})
	.run({ async: true });
