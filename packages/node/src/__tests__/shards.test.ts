
import { ShardedDB } from '../index';
import fs from 'fs';
import Identities, { Identity } from 'orbit-db-identity-provider';
import { Keypair } from '@solana/web3.js';
import { SolanaIdentityProvider } from '../identity-providers/solana-identity-provider';
import { ShardChain, TypedBehaviours } from '../shard';
import FeedStore from 'orbit-db-feedstore';
import { BinaryDocumentStore } from '@dao-xyz/orbit-db-bdocstore';
import { BinaryDocumentStoreOptions, FeedStoreOptions } from '../stores';
import { Constructor, field, variant } from '@dao-xyz/borsh';
import { BN } from 'bn.js';
import { delay } from '../utils';
import { clean } from './utils';
import { generateUUID } from '../id';
import { CONTRACT_ACCESS_CONTROLLER } from '../acl';



const testBehaviours: TypedBehaviours = {

    typeMap: {}
}



const getPeersSameIdentity = async (amount: number = 1, peerCapacity: number, behaviours: TypedBehaviours = testBehaviours): Promise<ShardedDB[]> => {
    let ids = Array(amount).fill(0).map((_) => generateUUID());
    for (const id in ids) {
        await clean(id);
    }

    Identities.addIdentityProvider(SolanaIdentityProvider)
    let keypair = Keypair.generate();
    const rootIdentity = await Identities.createIdentity({ type: 'solana', wallet: keypair.publicKey, keypair: keypair })
    let nodeRoots = ids.map(x => './ipfs/' + x);
    const peers = await Promise.all(nodeRoots.map(async (root) => {
        const peer = new ShardedDB();
        await peer.create({ rootAddress: 'root', local: false, repo: root, identity: rootIdentity, behaviours, replicationCapacity: peerCapacity });
        return peer;
    }));

    return peers;
}

const getPeer = async (rootAddress: string = 'root', peerCapacity: number = 50 * 1000, behaviours: TypedBehaviours = testBehaviours, identity?: Identity): Promise<ShardedDB> => {
    let id = generateUUID();
    await clean(id);
    let nodeRoot = './ipfs/' + id;
    const peer = new ShardedDB();
    await peer.create({ rootAddress, local: false, repo: nodeRoot, identity, behaviours, replicationCapacity: peerCapacity });
    return peer;
}



const disconnectPeers = async (peers: ShardedDB[]): Promise<void> => {
    await Promise.all(peers.map(peer => peer.disconnect()));
}


describe('cluster', () => {


    test('sharding if overflow', async () => {
        let replicationTopic = 'repl';
        let peers = await getPeersSameIdentity(2, 1);
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
        let peers = await getPeersSameIdentity(3, 1);
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
        let peers = await getPeersSameIdentity(2, 1);
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
        let peers = await getPeersSameIdentity(2, 1);
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


    describe('resiliance', () => {

        test('connect to remote', async () => {
            //THEN FIX THIS

            let peer1 = await getPeer();
            let rootAddress = '/orbitdb/' + peer1.orbitDB.id;
            let peer2 = await getPeer();

            let qqq = await peer1.orbitDB.determineAddress("hello", "docstore", {
                accessController: {
                    //write: [this.orbitDB.identity.id],
                    type: CONTRACT_ACCESS_CONTROLLER
                } as any,

                replicate: true
            })
            let qqq2 = await peer2.orbitDB.determineAddress("hello", "docstore", {
                replicate: true
            })
            let qqq3 = await peer2.orbitDB.determineAddress("hello", "docstore")
            let qqq4 = await peer1.orbitDB.determineAddress("hello", "docstore")
            let qqq5 = await peer1.orbitDB.determineAddress("xyz", "docstore")
            let qqq6 = await peer1.orbitDB.determineAddress("xyz", "docstore")

            let addddd = qqq.toString();
            let addddd2 = qqq2.toString();
            let addddd3 = qqq3.toString();
            let addddd4 = qqq4.toString();
            let addddd5 = qqq5.toString();
            let addddd6 = qqq6.toString();



            //  --- Create
            let rootChains = peer1.shardChainChain;

            // Create Root shard
            await rootChains.addPeerToShards();
            expect(rootChains.shardCounter.value).toEqual(1);

            // Create Feed store
            let feedStoreOptions = new FeedStoreOptions();
            let feedStoreChain = await peer1.createShardChain("test", feedStoreOptions, new BN(500000));
            await feedStoreChain.addPeerToShards();


            // --- Load assert

            // The can be seen as a root folder DB
            let rootShard = await peer2.shardChainChain.loadShard(0, { expectedBlockReplicationEvents: 1 });

            // Assert root shard is replicated
            expect(await rootShard.isSupported());

            // Assert root shard contains our feed store shard chain
            expect(Object.keys(rootShard.blocks._index._index).length).toEqual(1);
            await disconnectPeers([peer1, peer2]);

        })

        test('remote drop, still alive', async => {

        })
    })

    describe('presharding', () => {


        test('root shard -> feed store shard', async () => {


            let peers = await getPeersSameIdentity(2, 100000000);
            let peer = peers[0];
            let peer2 = peers[1];

            //  --- Create
            let rootChains = peer.shardChainChain;

            // Create Root shard
            await rootChains.addPeerToShards();

            // Create Feed store
            let feedStoreOptions = new FeedStoreOptions();
            let feedStoreChain = await peer.createShardChain("test", feedStoreOptions, new BN(500000));
            await feedStoreChain.addPeerToShards();
            (await (await feedStoreChain.getWritableShard()).loadBlocks()).add("abc")
            // --- Load assert

            expect(rootChains.shardCounter.value).toEqual(1);
            let l0 = await rootChains.getShard(0);
            await l0.loadBlocks();
            expect(Object.keys(l0.blocks._index._index).length).toEqual(1);


            // The can be seen as a root folder DB
            let l0b = await peer2.shardChainChain.loadShard(0, { expectedBlockReplicationEvents: 1 });



            // Assert root shard is replicated
            expect(await l0b.isSupported());

            // Assert root shard contains our feed store shard chain
            /*   let qqq = await l0b.chain.db.orbitDB.determineAddress("hello", "docstore", {
                  accessController: {
                      //write: [this.orbitDB.identity.id],
                      type: CONTRACT_ACCESS_CONTROLLER
                  } as any,
                  replicate: true
              })
              let qqq2 = await l0b.chain.db.orbitDB.determineAddress("hello", "docstore", {
                  replicate: true
              })
              let qqq3 = await l0b.chain.db.orbitDB.determineAddress("hello", "docstore")
              let qqq4 = await l0.chain.db.orbitDB.determineAddress("hello", "docstore")
  
              let addddd = qqq.toString();
              let addddd2 = qqq2.toString();
              let addddd3 = qqq3.toString();
              let addddd4 = qqq4.toString(); */

            expect(Object.keys(l0b.blocks._index._index).length).toEqual(1);
            await disconnectPeers(peers);

        });
    })

    describe('sharding on demand', () => {

        test('xyz', async () => {

            let peers = await getPeersSameIdentity(2, 1);
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
            let feedStoreChain = await peer.createShardChain("test", feedStoreOptions);
            await feedStoreChain.addPeerToShards();

            // --- Load assert

            // The can be seen as a root folder DB
            let rootShard = await peer2.shardChainChain.loadShard(0);

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
        let peers = await getPeersSameIdentity(2, 1);
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
        let peers = await getPeersSameIdentity(2, 1);
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
        let peers = await getPeersSameIdentity(2, 1);
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

