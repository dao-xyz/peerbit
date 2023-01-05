import B from 'benchmark'
import { LSession } from '@dao-xyz/peerbit-test-utils';
import { Libp2p } from 'libp2p';
import crypto from 'crypto';
import { BlockMessage, BlockRequest, BlockResponse, Blocks, BlockStream, LibP2PBlockStore, MemoryLevelBlockStore, stringifyCid } from '../index.js';
import { deserialize, serialize } from '@dao-xyz/borsh';
import * as Block from "multiformats/block";
import { checkDecodeBlock, cidifyString, defaultHasher } from '../block.js';

// Run with "node --loader ts-node/esm ./src/__benchmark__/index.ts"


import { waitFor, delay } from "@dao-xyz/peerbit-time";

export const waitForPeers = async (...libs: BlockStream[]) => {
	for (let i = 0; i < libs.length; i++) {
		await waitFor(() => {

			for (let j = 0; j < libs.length; j++) {
				if (i === j) {
					continue;
				}
				if (!libs[i].peers.has(libs[j].libp2p.peerId)) {
					return false;
				}
			}
			return true;
		});
		const peers = libs[i].peers;
		for (const peer of peers.values()) {
			await waitFor(() => peer.isReadable && peer.isWritable)
		}
	}
}



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

const stores: { stream: BlockStream, store: Map<string, Block.Block<any, any, any, any>> }[] = await Promise.all(session.peers.map(async (peer) => {
	const stream = new BlockStream(peer, { canRelayMessage: true });
	await stream.start();
	const localStore: Map<string, Block.Block<any, any, any, any>> = new Map()
	stream.addEventListener('data', async (evt) => {
		const message = evt.detail;
		try {
			const decoded = deserialize(
				message.data,
				BlockMessage
			);
			if (
				decoded instanceof BlockRequest
			) {
				const cid = stringifyCid(decoded.cid);
				const block = localStore.get(cid);
				if (!block) {
					return;
				}
				const response = serialize(
					new BlockResponse(cid, block.bytes)
				);
				await stream.publish(
					response
				);
			} else if (
				decoded instanceof BlockResponse
			) {
				// TODO make sure we are not storing too much bytes in ram (like filter large blocks)
				const cidObject = cidifyString(decoded.cid);
				const block = await checkDecodeBlock(cidObject, decoded.bytes, {});

				localStore.set(decoded.cid, block)
			}
		} catch (error) {
			console.error(
				"Got error for libp2p block transport: ",
				error
			);
			return; // timeout o r invalid cid
		}
	})
	return { stream, store: localStore };
}))

await session.connect();
await waitForPeers(stores[0].stream, stores[1].stream);
/* await waitForPeers(stores[1], stores[2]);
await waitForPeers(stores[2], stores[3]); */
await delay(11000)

const suite = new B.Suite('_')

const largeRandom: Uint8Array[] = [];
for (let i = 0; i < 100; i++) {
	largeRandom.push(crypto.randomBytes(1e6));
}

const smallRandom: Uint8Array[] = [];
const t1 = +new Date();
for (let i = 0; i < 1000; i++) {
	smallRandom.push(crypto.randomBytes(1e3));
}
/*
const reset = () => {
	stores.forEach((store) => {
		(store._store as LibP2PBlockStore)._gossipCache?.clear();
		((store._store as LibP2PBlockStore)._localStore as MemoryLevelBlockStore)._tempStore?.clear();
		((store._store as LibP2PBlockStore)._localStore as MemoryLevelBlockStore)._level?.clear();
	})
} */

suite.add("small", async () => {
	const rng = crypto.randomBytes(1e3);
	const block = await Blocks.block(rng, "raw");
	const cidString = stringifyCid(block.cid);
	stores[0].store.set(cidString, block)
	await stores[stores.length - 1].stream.publish(serialize(new BlockRequest(cidString)));
	const readData = await waitFor(() => stores[stores.length - 1].store.get(cidString), { timeout: 60 * 1000, delayInterval: 100 })
	if (readData?.bytes.length !== rng.length) {
		console.log('err!', readData?.bytes.length);
		throw Error("Unexpected")
	}
	else {
	}

}).on('cycle', (event: any) => {
	console.log(String(event.target));
}).on('complete', function (this: any, ...args: any[]) {
	stores.forEach((stream) => stream.stream.stop())
	session.stop();
}).run(({ async: true }))

