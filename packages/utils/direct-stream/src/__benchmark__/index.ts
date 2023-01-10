import B from 'benchmark'
import { LSession } from '@dao-xyz/peerbit-test-utils';
import { Libp2p } from 'libp2p';
import { DirectStream, waitForPeers } from '../index.js';
import { delay } from '@dao-xyz/peerbit-time';
import { Message } from '../encoding.js';
import crypto from 'crypto';
import { equals } from 'uint8arrays'

// Run with "node --loader ts-node/esm ./src/__benchmark__/index.ts"

const session: LSession = await LSession.disconnected(4);

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
		super(libp2p, ['bench/0.0.0'], { canRelayMessage: true, emitSelf: true })
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

const large = crypto.randomBytes(1e6); //  1mb

suite.add("small", {
	defer: true, fn: (deferred) => {
		const small = crypto.randomBytes(1e3); // 1kb
		const published = crypto.createHash('sha1').update(small).digest('hex');
		const listener = (msg) => {
			if (crypto.createHash('sha1').update(msg.detail.dataBytes).digest('hex') === published) {
				streams[streams.length - 1].removeEventListener('data', listener);
				deferred.resolve()
			}
		}
		streams[streams.length - 1].addEventListener('data', listener)
		streams[0].publish(small)

	}
}).add("large", {
	defer: true, fn: (deferred) => {
		const small = crypto.randomBytes(1e6); // 1mb
		const published = crypto.createHash('sha1').update(small).digest('hex');
		const listener = (msg) => {
			if (crypto.createHash('sha1').update(msg.detail.dataBytes).digest('hex') === published) {
				streams[streams.length - 1].removeEventListener('data', listener);
				deferred.resolve()
			}
		}
		streams[streams.length - 1].addEventListener('data', listener)
		streams[0].publish(small)
	}
}).on('cycle', (event: any) => {
	console.log(String(event.target));
}).on('complete', function (this: any, ...args: any[]) {
	streams.forEach((stream) => stream.stop())
	session.stop();
}).run(({ async: true }))

