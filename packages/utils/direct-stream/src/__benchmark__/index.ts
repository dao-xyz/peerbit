import B from 'benchmark'
import { LSession } from '@dao-xyz/peerbit-test-utils';
import { Libp2p } from 'libp2p';
import { DirectStream } from '../index.js';
import { waitForPeers } from '../__tests__/utils.js'
import { delay } from '@dao-xyz/peerbit-time';
import { Message } from '../encoding.js';
import crypto from 'crypto';

// Run with "node --loader ts-node/esm ./src/__benchmark__/index.ts"

let session: LSession = await LSession.disconnected(4);;

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
await session.connect([[session.peers[0], session.peers[1]], [session.peers[1], session.peers[2]], [session.peers[2], session.peers[3]]])


class TestStreamImpl extends DirectStream {

	constructor(libp2p: Libp2p) {
		super(libp2p, ['bench/0.0.0'], { heartbeatInterval: 5000, canRelayMessage: true, emitSelf: true })
	}

}
const streams: TestStreamImpl[] = await Promise.all(session.peers.map(async (peer) => {
	const stream = new TestStreamImpl(peer);
	await stream.start();
	return stream;
}))

await waitForPeers(streams[0], streams[1]);
await waitForPeers(streams[1], streams[2]);
await waitForPeers(streams[2], streams[3]);
await delay(6000);

const suite = new B.Suite()
const small = crypto.randomBytes(1000); // 1kb
const large = crypto.randomBytes(1e6); //  1mb

suite.add("small", () => {
	streams.map(stream => { stream.removeEventListener('data'); })
	const recieved: Uint8Array[] = [];

	let count = 1000;
	for (let i = 0; i < count; i++) {
		streams[0].publish(small)
	}
	return new Promise((rs, rj) => {
		streams[streams.length - 1].addEventListener('data', (msg) => {
			recieved.push(msg.detail.data);
			if (recieved.length == count) {
				rs(true);
			}
		})
	})
}).add("large", () => {
	streams.map(stream => { stream.removeEventListener('data'); })
	const recieved: Uint8Array[] = [];

	let count = 100;
	for (let i = 0; i < count; i++) {
		streams[0].publish(large)
	}
	return new Promise((rs, rj) => {
		streams[streams.length - 1].addEventListener('data', (msg) => {
			recieved.push(msg.detail.data);
			if (recieved.length == count) {
				rs(true);
			}
		})
	})
}).on('cycle', (event: any) => {
	console.log(String(event.target));
}).on('complete', function (this: any, ...args: any[]) {
	streams.forEach((stream) => stream.stop())
	session.stop();
}).run(({ async: true }))

