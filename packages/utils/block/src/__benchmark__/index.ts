import B from 'benchmark'
import { LSession } from '@dao-xyz/peerbit-test-utils';
import { Libp2p } from 'libp2p';
import { waitForPeers } from '../__tests__/utils.js'
import { delay } from '@dao-xyz/peerbit-time';
import crypto from 'crypto';
import { Blocks, LibP2PBlockStore, MemoryLevelBlockStore, stringifyCid } from '../index.js';

// Run with "node --loader ts-node/esm ./src/__benchmark__/index.ts"

let session: LSession = await LSession.disconnected(2);;

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
const reset = () => {
	stores.forEach((store) => {
		(store._store as LibP2PBlockStore)._gossipCache?.clear();
		(store._store as LibP2PBlockStore)._blockSub.seenCache?.clear();
		((store._store as LibP2PBlockStore)._localStore as MemoryLevelBlockStore)._tempStore.clear();
		((store._store as LibP2PBlockStore)._localStore as MemoryLevelBlockStore)._level.clear();
	})

}

suite.add("small", async () => {
	return new Promise((rs, rj) => {
		stores[0].put(crypto.randomBytes(1e3), "raw", { pin: true }).then((cid) => {
			stores[stores.length - 1].get<Uint8Array>(stringifyCid(cid)).then((result) => {
				if (result) {
					console.log('got data!')
					rs(result);
				}
				else { rj(new Error('Missing data')) }
			}).catch((error) => rj(error))
		}).catch((err) => {
			rj(err)
		})
	})
	/* for (let i = 0; i < 10; i++) {
		cids.push(await stores[0].put(smallRandom[i], "raw", { pin: true }));
	}

	for (const [i, cid] of cids.entries()) {
		console.log(i)
		const readData = await stores[stores.length - 1].get<Uint8Array>(stringifyCid(cid));
		if (readData?.length !== smallRandom[i].length) {
			console.log('err!', readData?.length);

			throw Error("Unexpected")
		}
		else {
			console.log('ok!')
		}
	}
 */
})/* .add("large", async () => {
	const cids: string[] = [];
	for (let i = 0; i < 10; i++) {
		cids.push(await stores[0].put(largeRandom[i], "raw", { pin: true }));
	}

	for (const [i, cid] of cids.entries()) {
		const readData = await stores[stores.length - 1].get<Uint8Array>(stringifyCid(cid));
		if (readData?.length !== largeRandom[i].length) {
			throw Error("Unexpected")
		}
	}
}, {
	onReset: () => {
		reset()
	}
}) */.on('cycle', (event: any) => {
	console.log('cycle!')
	reset();
}).on('complete', function (this: any, ...args: any[]) {
	stores.forEach((stream) => stream.close())
	session.stop();
}).run(({ async: true, minSamples: 1, initCount: 1, maxTime: 5 }))

