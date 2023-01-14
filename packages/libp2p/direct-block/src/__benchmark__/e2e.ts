import B from 'benchmark'
import { LSession } from '@dao-xyz/peerbit-test-utils';
import { Libp2p } from 'libp2p';
import { waitForPeers } from '../__tests__/utils.js'
import { delay } from '@dao-xyz/peerbit-time';
import crypto from 'crypto';
import { Blocks, LibP2PBlockStore, MemoryLevelBlockStore, stringifyCid } from '../index.js';

// Run with "node --loader ts-node/esm ./src/__benchmark__/e2e.ts"

const session: LSession = await LSession.disconnected(2);

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
await session.connect([[session.peers[0], session.peers[1]]/* , [session.peers[1], session.peers[2]], [session.peers[2], session.peers[3]] */])

const stores: Blocks[] = await Promise.all(session.peers.map(async (peer) => {
	const stream = new Blocks(new LibP2PBlockStore(peer, new MemoryLevelBlockStore()));
	await stream.open();
	return stream;
}))

await session.connect();
await waitForPeers(stores[0], stores[1]);
/* await waitForPeers(stores[1], stores[2]);
await waitForPeers(stores[2], stores[3]); */
await delay(11000)

const suite = new B.Suite('_', { minSamples: 1, initCount: 1, maxTime: 5 })

const largeRandom: Uint8Array[] = [];
for (let i = 0; i < 100; i++) {
	largeRandom.push(crypto.randomBytes(1e6));
}

const smallRandom: Uint8Array[] = [];
const t1 = +new Date();
for (let i = 0; i < 1000; i++) {
	smallRandom.push(crypto.randomBytes(1e3));
}


suite.add("small", async () => {
	const rng = crypto.randomBytes(1e3);
	const cid = await stores[0].put(rng, "raw", { pin: true });
	const readData = await stores[stores.length - 1].get<Uint8Array>(stringifyCid(cid));
	if (readData?.length !== rng.length) {
		console.log('err!', readData?.length);

		throw Error("Unexpected")
	}
}).on('cycle', (event: any) => {
	console.log(String(event.target));
}).on('complete', function (this: any, ...args: any[]) {
	stores.forEach((stream) => stream.close())
	session.stop();
}).run(({ async: true }))

