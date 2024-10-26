import { tcp } from "@libp2p/tcp";
import { TestSession } from "@peerbit/libp2p-test-utils";
import { SeekDelivery } from "@peerbit/stream-interface";
import { waitForResolved } from "@peerbit/time";
import B from "benchmark";
import crypto from "crypto";
import {
	DirectStream,
	type DirectStreamComponents,
	waitForNeighbour,
} from "../src/index.js";

// Run with "node --loader ts-node/esm ./benchmark/transfer.ts"

// size: 100byte x 1,727 ops/sec ±2.61% (83 runs sampled)
// size: 1kb x 1,727 ops/sec ±2.61% (83 runs sampled)
// size: 1000kb x 104 ops/sec ±2.46% (83 runs sampled)

class TestStreamImpl extends DirectStream {
	constructor(c: DirectStreamComponents) {
		super(c, ["bench/0.0.0"], {
			canRelayMessage: true,
			connectionManager: false,
		});
	}
}
const session = await TestSession.disconnected(4, {
	transports: [tcp()],
	services: { directstream: (c: any) => new TestStreamImpl(c) },
});

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
await session.connect([
	[session.peers[0], session.peers[1]],
	[session.peers[1], session.peers[2]],
	[session.peers[2], session.peers[3]],
]);

const stream = (i: number): TestStreamImpl =>
	session.peers[i].services.directstream;

await waitForNeighbour(stream(0), stream(1));
await waitForNeighbour(stream(1), stream(2));
await waitForNeighbour(stream(2), stream(3));

stream(0).publish(new Uint8Array([123]), {
	mode: new SeekDelivery({
		redundancy: 1,
		to: [stream(session.peers.length - 1).publicKey],
	}),
});
await waitForResolved(() =>
	stream(0).routes.isReachable(
		stream(0).publicKeyHash,
		stream(3).publicKeyHash,
	),
);

let suite = new B.Suite();

let listener: ((msg: any) => any) | undefined = undefined;
const msgMap: Map<string, { resolve: () => any }> = new Map();
const msgIdFn = (msg: Uint8Array) =>
	crypto.createHash("sha1").update(msg.subarray(0, 20)).digest("base64");

const sizes = [100, 1e3, 1e6];
for (const size of sizes) {
	suite = suite.add("size: " + size / 1e3 + "kb", {
		defer: true,
		fn: (deferred: any) => {
			const small = crypto.randomBytes(size); // 1kb
			msgMap.set(msgIdFn(small), deferred);
			stream(0).publish(small, {
				to: [stream(session.peers.length - 1).publicKey],
			});
		},
		setup: () => {
			listener = (msg) => {
				msgMap.get(msgIdFn(msg.detail.data))!.resolve();
			};

			stream(session.peers.length - 1).addEventListener("data", listener);
			msgMap.clear();
		},
		teardown: () => {
			stream(session.peers.length - 1).removeEventListener("data", listener);
		},
	});
}
suite
	.on("cycle", (event: any) => {
		console.log(String(event.target));
	})
	.on("complete", function (this: any, ...args: any[]) {
		session.stop();
	})
	.run({ async: true });
