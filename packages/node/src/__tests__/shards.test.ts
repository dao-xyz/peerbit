
import { ShardedDB } from '../index';
import fs from 'fs';
import Identities from 'orbit-db-identity-provider';
import { Keypair } from '@solana/web3.js';
import { SolanaIdentityProvider } from '../identity-providers/solana-identity-provider';
import { ShardChain, TypedBehaviours } from '../shard';
import FeedStore from 'orbit-db-feedstore';
import { BinaryDocumentStore } from '@dao-xyz/orbit-db-bdocstore';
import { BinaryDocumentStoreOptions, FeedStoreOptions } from '../stores';
import { Constructor, field, variant } from '@dao-xyz/borsh';
import { BN } from 'bn.js';
import { delay } from '../utils';



const testBehaviours: TypedBehaviours = {

    typeMap: {}
}


const getPeers = async (amount: number = 1, peerCapacity: number, behaviours: TypedBehaviours = testBehaviours): Promise<ShardedDB[]> => {


    let idx = Array(amount).fill(0).map((_, idx) => idx);
    Identities.addIdentityProvider(SolanaIdentityProvider)
    let keypair = Keypair.generate();
    const rootIdentity = await Identities.createIdentity({ type: 'solana', wallet: keypair.publicKey, keypair: keypair })
    fs.rmSync('./ipfs', { recursive: true, force: true });
    fs.rmSync('./orbitdb', { recursive: true, force: true });
    fs.rmSync('./orbit-db-stores', { recursive: true, force: true });

    let nodeRoots = idx.map(x => './ipfs/' + x);

    const peers = await Promise.all(nodeRoots.map(async (root) => {
        const peer = new ShardedDB();
        await peer.create({ rootAddress: 'root', local: false, repo: root, identity: rootIdentity, behaviours, replicationCapacity: peerCapacity });
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
        let peers = await getPeers(2, 1);
        let peer = peers[0];
        let peer2 = peers[1];
        // await peer.node.swarm.connect((await peer2.node.id()).addresses[0]);
        await peer.node.swarm.connect((await peer2.node.id()).addresses[0]);
        await peer2.subscribeForReplication();
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
        let peers = await getPeers(3, 1);
        let peer = peers[0];
        let peer2 = peers[1];
        let peer3 = peers[2];
        await delay(5000);
        // await peer.node.swarm.connect((await peer2.node.id()).addresses[0]);
        await peer.node.swarm.connect((await peer2.node.id()).addresses[0]);
        await peer2.node.swarm.connect((await peer3.node.id()).addresses[0]);
        await peer.node.swarm.connect((await peer3.node.id()).addresses[0]);

        await peer3.subscribeForReplication();
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
        let peers = await getPeers(2, 1);
        let peer = peers[0];
        let peer2 = peers[1];
        await peer.node.swarm.connect((await peer2.node.id()).addresses[0]);

        await delay(5000);
        // await peer.node.swarm.connect((await peer2.node.id()).addresses[0]);
        let feedStoreOptions = new FeedStoreOptions();
        await (await peer.loadShardChain("test", feedStoreOptions)).addPeerToShards();
        let shardFromPeer2 = await (await peer2.loadShardChain("test", feedStoreOptions)).getWritableShard();

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
        let peers = await getPeers(2, 1);
        let peer = peers[0];
        let peer2 = peers[1];
        delay(5000);
        //   await peer.node.swarm.connect((await peer2.node.id()).addresses[0]);
        let shard = undefined;// await peer.addPeerToShards('root', 0, 1, 1)[0];
        let feedStoreOptions = new FeedStoreOptions();
        try {
            let shards = await (await peer.loadShardChain("test", feedStoreOptions)).addPeerToShards();
            shard = shards[0];
        }
        catch (error) {
        }
        //  let shard = await peer.getWritableShard();
        /*         await shard.loadPeers(peer.orbitDB);
                await shard.peers.set("xyz", "hello"); */
        await delay(15000);
        let other = await (await peer2.loadShardChain("test", feedStoreOptions)).getWritableShard();
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




    describe('presharding', () => {
        test('root shard -> feed store shard', async () => {
            let replicationTopic = 'repl';
            let peers = await getPeers(2, 1);
            let peer = peers[0];
            let peer2 = peers[1];

            //  --- Create
            let rootChains = peer.shardChainChain;

            // Create Root shard
            await rootChains.addPeerToShards();

            expect(rootChains.shardCounter.value).toEqual(1);

            // Create Feed store
            let feedStoreOptions = new FeedStoreOptions();
            let feedStoreChain = await peer.loadShardChain("test", feedStoreOptions);
            await feedStoreChain.addPeerToShards(
                {
                    peersLimit: 1,
                    startIndex: 0,
                    supportAmountOfShards: 1
                }
            );

            // --- Load assert

            // The can be seen as a root folder DB
            let rootShard = await peer2.shardChainChain.loadShard(0, {
                expectedBlockReplicationEvents: 1,
                expectedPeerReplicationEvents: 1
            });

            // Assert root shard is replicated
            expect(await rootShard.isSupported());

            // Assert root shard contains our feed store shard chain
            expect(Object.keys(rootShard.blocks._index._index).length).toEqual(1);
            await disconnectPeers(peers);
        });
    })

    describe('sharding on demand', () => {
        test('root shard -> feed store shard', async () => {
            let peers = await getPeers(2, 1);
            let peer = peers[0];
            let peer2 = peers[1];
            await peer2.subscribeForReplication();

            //  --- Create
            let rootChains = peer.shardChainChain;

            // Create Root shard
            await rootChains.addPeerToShards();
            expect(rootChains.shardCounter.value).toEqual(1);

            // Create Feed store
            let feedStoreOptions = new FeedStoreOptions();
            let feedStoreChain = await peer.loadShardChain("test", feedStoreOptions);
            await feedStoreChain.addPeerToShards();

            // --- Load assert

            // The can be seen as a root folder DB
            let rootShard = await peer2.shardChainChain.loadShard(0, {
                expectedBlockReplicationEvents: 1,
                expectedPeerReplicationEvents: 1
            });

            // Assert root shard is replicated
            expect(await rootShard.isSupported());

            // Assert root shard contains our feed store shard chain
            expect(Object.keys(rootShard.blocks._index._index).length).toEqual(1);
            await disconnectPeers(peers);
        });
    })

    test('request shard', async () => {

        // Check whether we can request a replication
        // Check if shard counter is updated

        let replicationTopic = 'repl';
        let peers = await getPeers(2, 1);
        let peer = peers[0];
        let peer2 = peers[1];
        await peer2.subscribeForReplication();
        let feedStoreOptions = new FeedStoreOptions();
        let chainFromPeer1 = await peer.loadShardChain("test", feedStoreOptions);
        let shard = await chainFromPeer1.getWritableShard();
        let sameChainFromPeer2 = await peer2.loadShardChain("test", feedStoreOptions);
        let counter = await sameChainFromPeer2.getShardCounter();
        expect(counter.value).toEqual(1);
        await disconnectPeers(peers);

    });


    test('auto sharding on new data', async () => {

        // Check whether we can request a replication
        // Check if shard counter is updated

        let replicationTopic = 'repl';
        let peers = await getPeers(2, 1);
        let peer = peers[0];
        let peer2 = peers[1];
        let feedStoreOptions = new FeedStoreOptions();
        await peer2.subscribeForReplication();
        let chainFromPeer1 = await peer.loadShardChain("test", feedStoreOptions);
        let shard = await chainFromPeer1.getWritableShard();
        let chainFromPeer2 = await peer2.loadShardChain("test", feedStoreOptions);
        let counter = await chainFromPeer2.getShardCounter();
        expect(counter.value).toEqual(1);
        await disconnectPeers(peers);

    });

    test('splixxx', async () => {
        let replicationTopic = 'repl';
        let peers = await getPeers(2, 1);
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

