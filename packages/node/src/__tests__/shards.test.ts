
import { ShardedDB } from '../index';
import * as ipfs from 'ipfs';
import fs from 'fs';
import Identities from 'orbit-db-identity-provider';
import { Keypair } from '@solana/web3.js';
import { SolanaIdentityProvider } from '../identity-providers/solana-identity-provider';

class StringShardedDB extends ShardedDB {
    constructor() {
        super();
    }

    async addData(shardChain: string, data: string): Promise<string> {
        let shard = await this.getShardChain(shardChain).getWritableShard();
        return shard.addBlock(data, data.length, this)
    }
}

const getPeers = async (amount: number = 1, shardSize: number, replicationTopic: string): Promise<StringShardedDB[]> => {
    let idx = Array(amount).fill(0).map((_, idx) => idx);
    Identities.addIdentityProvider(SolanaIdentityProvider)
    let keypair = Keypair.generate();
    const rootIdentity = await Identities.createIdentity({ type: 'solana', wallet: keypair.publicKey, keypair: keypair })
    fs.rmSync('./ipfs', { recursive: true, force: true });
    fs.rmSync('./orbitdb', { recursive: true, force: true });
    fs.rmSync('./orbit-db-stores', { recursive: true, force: true });

    let nodeRoots = idx.map(x => './ipfs/' + x);

    const peers = await Promise.all(nodeRoots.map(async (root) => {
        const peer = new StringShardedDB();
        await peer.create({ shardingTopic: replicationTopic, local: false, repo: root, identity: rootIdentity });
        return peer;
    }));

    return peers;
}

const disconnectPeers = async (peers: ShardedDB[]): Promise<void> => {
    await Promise.all(peers.map(peer => peer.disconnect()));
}

describe('cluster', () => {

    test('sharding if overflow', async () => {
        let replicationTopic = 'repl';
        let peers = await getPeers(2, 1, replicationTopic);
        let peer = peers[0];
        let peer2 = peers[1];
        // await peer.node.swarm.connect((await peer2.node.id()).addresses[0]);
        await peer.node.swarm.connect((await peer2.node.id()).addresses[0]);
        await peer2.subscribeForReplication(replicationTopic, 1);
        await delay(5000);

        await peer.sendMessage(replicationTopic, "hello");
        await delay(25000);
        const t = 1;
        /*  let hash = await peer.addData("a")

        // Should trigger new shard
        let hash2 = await peer.addData("b")
        const x = 2; */

    });

    test('test chain', async () => {
        let replicationTopic = 'repl';
        let peers = await getPeers(3, 1, replicationTopic);
        let peer = peers[0];
        let peer2 = peers[1];
        let peer3 = peers[2];
        await delay(5000);
        // await peer.node.swarm.connect((await peer2.node.id()).addresses[0]);
        await peer.node.swarm.connect((await peer2.node.id()).addresses[0]);
        await peer2.node.swarm.connect((await peer3.node.id()).addresses[0]);
        await peer.node.swarm.connect((await peer3.node.id()).addresses[0]);

        await peer3.subscribeForReplication(replicationTopic, 1);
        await delay(5000);

        await peer.sendMessage(replicationTopic, "hello");
        await delay(25000);
        const t = 1;
        /*  let hash = await peer.addData("a")

        // Should trigger new shard
        let hash2 = await peer.addData("b")
        const x = 2; */

    });

    test('test chain 2', async () => {
        let replicationTopic = 'repl';
        let peers = await getPeers(2, 1, replicationTopic);
        let peer = peers[0];
        let peer2 = peers[1];
        await peer.node.swarm.connect((await peer2.node.id()).addresses[0]);

        await delay(5000);
        // await peer.node.swarm.connect((await peer2.node.id()).addresses[0]);
        await peer.getShardChain("test").addPeerToShards(0, 1, 1);

        let shardFromPeer2 = await peer2.getShardChain("test").getWritableShard();
        await delay(5000);
        await shardFromPeer2.peers.load();
        await delay(25000);
        const t = 1;
        /*  let hash = await peer.addData("a")

        // Should trigger new shard
        let hash2 = await peer.addData("b")
        const x = 2; */

    });




    test('working withou swarm connect', async () => {
        let replicationTopic = 'repl';
        let peers = await getPeers(2, 1, replicationTopic);
        let peer = peers[0];
        let peer2 = peers[1];
        delay(5000);
        //   await peer.node.swarm.connect((await peer2.node.id()).addresses[0]);
        let shard = undefined;// await peer.addPeerToShards('root', 0, 1, 1)[0];

        try {
            let shards = await peer.getShardChain("test").addPeerToShards(0, 1, 1);
            shard = shards[0];
        }
        catch (error) {
        }
        //  let shard = await peer.getWritableShard();
        /*         await shard.loadPeers(peer.orbitDB);
                await shard.peers.set("xyz", "hello"); */
        await delay(15000);
        let other = await peer2.getShardChain("test").getWritableShard();
        await other.peers.load();
        await delay(15000);

        let t = 123;
        /*   let cid = await shard.peers.set("xyz", new Uint8Array([1]));
          await delay(25000);
          const x = 123; */
        /* 
        await delay(5000);
        await peer2.addPeerToShards("root", 0, 1, 1);
     

        // Should be first shard
        let hash = await peer.addData(Uint8Array.from([1]))

        // Should trigger new shard
        let hash2 = await peer.addData(Uint8Array.from([2]))
        */

        /*       
        await peer.addPeerToShards({
                capacity: 
            }, 0, 1, 1, 1); */

    });


    test('splix', async () => {
        let replicationTopic = 'repl';
        let peers = await getPeers(2, 1, replicationTopic);
        let peer = peers[0];
        let peer2 = peers[1];
        let shard = (await peer.getShardChain("test").addPeerToShards(0, 1, 1))[0];
        let other = await peer2.getShardChain("test").getWritableShard();
        await other.peers.load();

        let t = 123;


    });


    test('request shard', async () => {

        // Check whether we can request a replication
        // Check if shard counter is updated

        let replicationTopic = 'repl';
        let peers = await getPeers(2, 1, replicationTopic);
        let peer = peers[0];
        let peer2 = peers[1];
        await peer2.subscribeForReplication(replicationTopic, 1);
        let chainFromPeer1 = peer.getShardChain("test");
        let shard = await chainFromPeer1.getWritableShard();
        let chainFromPeer2 = peer2.getShardChain("test");
        let counter = await chainFromPeer2.getShardCounter();
        expect(counter.value).toEqual(1);
        await disconnectPeers(peers);

    });


    test('auto sharding on new data', async () => {

        // Check whether we can request a replication
        // Check if shard counter is updated

        let replicationTopic = 'repl';
        let peers = await getPeers(2, 1, replicationTopic);
        let peer = peers[0];
        let peer2 = peers[1];
        await peer2.subscribeForReplication(replicationTopic, 1);
        let chainFromPeer1 = peer.getShardChain("test");
        let shard = await chainFromPeer1.getWritableShard();
        let chainFromPeer2 = peer2.getShardChain("test");
        let counter = await chainFromPeer2.getShardCounter();
        expect(counter.value).toEqual(1);
        await disconnectPeers(peers);

    });

    test('splixxx', async () => {
        let replicationTopic = 'repl';
        let peers = await getPeers(2, 1, replicationTopic);
        let peer = peers[0];
        let peer2 = peers[1];
        // await peer.node.swarm.connect((await peer2.node.id()).addresses[0]);

        await peer2.node.pubsub.subscribe('xyz', (msg) => {
            console.log('got message!', msg)
        });
        await delay(5000);

        await peer.node.pubsub.publish('xyz', new Uint8Array([1, 2, 3]));

        await delay(15000);
        let t = 123;


    });


});

// Test grantee can grante another grantee

// 
const delay = ms => new Promise(res => setTimeout(res, ms));

