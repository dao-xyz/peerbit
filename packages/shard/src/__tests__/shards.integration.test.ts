
import { Shard } from '../shard';
import { AnyPeer } from '../peer';
import { BinaryFeedStoreInterface, DocumentStoreInterface, Document, documentStoreShard, getPeer, shardStoreShard, getConnectedPeers } from './utils';
import { connectPeers, disconnectPeers } from '@dao-xyz/peer-test-utils';
import { delay, waitFor, waitForAsync } from '@dao-xyz/time';
import { P2PTrust } from '@dao-xyz/orbit-db-trust-web'
import { MemoryLimitExceededError } from '../errors';
import { AccessError } from '@dao-xyz/ipfs-log';
import v8 from 'v8';

const isInSwarm = async (from: AnyPeer, swarmSource: AnyPeer) => {

    let peerAddressesSet = (await from.node.id()).addresses.map(x => x.toString());
    let peerAddresses = new Set(peerAddressesSet);

    const results = await swarmSource.node.swarm.addrs();
    for (const result of results) {
        for (const addr of result.addrs) {
            if (peerAddresses.has(addr.toString())) {
                return true;
            }
        }
    }
    return false;
}


/* const disconnectPeers = async (peers: AnyPeer[]): Promise<void> => {
    await Promise.all(peers.map(peer => peer.disconnect()));
} */


describe('cluster', () => {
    describe('trust', () => {
        test('add trustee', async () => {

            let [peer, peer2] = await getConnectedPeers(2);
            let l0a = await documentStoreShard();
            await l0a.init(peer);
            expect(l0a.cid).toBeDefined();
            expect(l0a.trust).toBeInstanceOf(P2PTrust);
            expect((l0a.trust as P2PTrust).rootTrust).toBeDefined();
            expect((l0a.trust as P2PTrust).rootTrust.id === peer.orbitDB.identity.id)


            let newTrustee = peer2.orbitDB.identity;
            await l0a.trust.addTrust(newTrustee);
            const l0b = await Shard.loadFromCID(l0a.cid, peer2.node);
            await l0b.init(peer2);
            await l0b.trust.load(1);
            expect(l0b.trust.db.size).toEqual(1)
            await disconnectPeers([peer, peer2]);


        })
    })

    describe('manifest', () => {
        test('save load', async () => {

            let [peer, peer2] = await getConnectedPeers(2);
            let l1 = (await peer.node.id()).addresses[0];
            await peer2.node.swarm.connect(l1)

            let l0 = await documentStoreShard();
            await l0.init(peer);
            expect(l0.cid).toBeDefined();
            let loadedShard = await Shard.loadFromCID<BinaryFeedStoreInterface>(l0.cid, peer2.node);
            expect(loadedShard.interface.db.address).toEqual(l0.interface.db.address);
            await disconnectPeers([peer, peer2]);
        })
    })

    describe('recursive shard', () => {
        test('peer backward connect', async () => {

            let [peer, peer2] = await getConnectedPeers(2)

            // Create Root shard
            let l0 = await shardStoreShard();
            await l0.replicate(peer);
            // Create Feed store

            expect(await isInSwarm(peer, peer2)).toBeFalsy();
            let feedStore = await (await documentStoreShard()).init(peer2, l0.cid); // <-- This should trigger a swarm connection from peer to peer2
            await feedStore.replicate(peer2);
            expect(feedStore.interface.address.endsWith(l0.cid + '-documents'));
            await waitForAsync(async () => await isInSwarm(peer, peer2))
            disconnectPeers([peer, peer2]);
        })


        test('backward connect filter unique', async () => {

            let [peer, peer2] = await getConnectedPeers(2)

            // Create Root shard
            let l0 = await shardStoreShard();
            await l0.replicate(peer);
            // Create Feed store

            expect(await isInSwarm(peer, peer2)).toBeFalsy();
            let feedStore1 = await (await documentStoreShard()).init(peer2, l0.cid); // <-- This should trigger a swarm connection from peer to peer2
            await feedStore1.replicate(peer2);
            let feedStore2 = await (await documentStoreShard()).init(peer2, l0.cid); // <-- This should trigger a swarm connection from peer to peer2
            await feedStore2.replicate(peer2);

            await waitForAsync(async () => await isInSwarm(peer, peer2))
            expect(peer2.supportJobs.size).toEqual(2)
            expect([...peer2.supportJobs.values()].filter(x => x.connectingToParentShardCID)).toHaveLength(1);
            expect([...peer2.supportJobs.values()].filter(x => x.connectingToParentShardCID)[0].connectingToParentShardCID).toEqual(l0.cid);
            disconnectPeers([peer, peer2]);
        })

        test('backward connect no job is same peer', async () => {

            let [peer] = await getConnectedPeers(2)

            // Create Root shard
            let l0 = await shardStoreShard();
            await l0.replicate(peer);
            // Create Feed store

            let feedStore1 = await (await documentStoreShard()).init(peer, l0.cid); // <-- This should trigger a swarm connection from peer to peer2
            await feedStore1.replicate(peer);
            let feedStore2 = await (await documentStoreShard()).init(peer, l0.cid); // <-- This should trigger a swarm connection from peer to peer2
            await feedStore2.replicate(peer);

            expect(peer.supportJobs.size).toEqual(3)
            expect([...peer.supportJobs.values()].filter(x => x.connectingToParentShardCID)).toHaveLength(0);
            disconnectPeers([peer]);
        })
    })

    describe('resiliance', () => {
        test('connect to remote', async () => {

            let [peer, peer2] = await getConnectedPeers(2);

            // Create Root shard
            let l0 = await shardStoreShard<DocumentStoreInterface>();
            await l0.replicate(peer);

            // Create Feed store
            let documentStore = await (await documentStoreShard()).init(peer, l0.cid);
            await documentStore.replicate(peer);
            await documentStore.interface.db.put(new Document({ id: 'hello' }));
            await l0.interface.db.put(documentStore);


            // --- Load assert, from another peer
            await l0.init(peer2)
            await l0.interface.load(1);
            expect(Object.keys(l0.interface.db.index._index).length).toEqual(1);
            let feedStoreLoaded = await l0.interface.loadShard(documentStore.cid, peer2);
            await feedStoreLoaded.interface.load(1);
            await waitFor(() => Object.keys(feedStoreLoaded.interface.db.index._index).length == 1)
            await disconnectPeers([peer, peer2]);

        })

        test('first peer drop, data still alive because 2nd peer is up', async () => {
            let [peer, peer2, peer3] = await getConnectedPeers(3);

            // Create Root shard
            let l0a = await shardStoreShard();
            await l0a.replicate(peer);

            // Create Feed store
            let l1 = await (await documentStoreShard()).init(peer, l0a.cid);
            await l0a.interface.load();
            await l0a.interface.db.put(l1);

            // --- Load assert, from another peer
            const l0b = await Shard.loadFromCID<DocumentStoreInterface>(l0a.cid, peer2.node);
            await l0b.replicate(peer2);
            await l0b.interface.load(1);
            expect(Object.keys(l0b.interface.db.index._index).length).toEqual(1);


            // Drop 1 peer and make sure a third peer can access data
            await disconnectPeers([peer]);
            const l0c = await Shard.loadFromCID<DocumentStoreInterface>(l0a.cid, peer3.node);
            await l0c.init(peer3)
            await l0c.interface.load(1);
            expect(Object.keys(l0c.interface.db.index._index).length).toEqual(1);
            await disconnectPeers([peer2, peer3]);
        })
    })


    /* describe('presharding', () => {

        test('nested block store', async () => {


            let peers = await getPeersSameIdentity(2, 100000000);
            let peer = peers[0];
            let peer2 = peers[1];

            // Create Root shard
            let l0 = await shardStoreShard<FeedStoreInterface>();
            await l0.replicate(peer);

            // Create Feed store
            let feedStore = await (await feedStoreShard()).init(peer, l0);
            await feedStore.replicate();
            let feedStoreLoaded = await l0.interface.loadShard(0);
            await feedStoreLoaded.interface.db.add("xxx");
            await l0.interface.db.put(feedStoreLoaded);



            // --- Load assert
            let l0b = await Shard.loadFromCID<FeedStoreInterface>(l0.cid, peer2.node);
            await l0b.init(peer2)
            await l0b.interface.load(1);
            expect(Object.keys(l0.interface.db._index._index).length).toEqual(1);
            feedStoreLoaded = await l0b.interface.loadShard(0)
            expect(Object.keys(feedStoreLoaded.interface.db._index._index).length).toEqual(1);
            await disconnectPeers(peers);
        });
    }) */

    /*    HOW TO WE WORK WITH REPLICATION TOPICS ???
   
           WHILE PARNET
       SUBSCRICE ?
    */
    describe('peer', () => {

        test('peer counter from 1 replicator', async () => {
            let [peer, peer2] = await getConnectedPeers(2);
            let l0a = await shardStoreShard();
            await l0a.replicate(peer);
            let l0b = await Shard.loadFromCID(l0a.cid, peer2.node);
            await l0b.init(peer2);
            await delay(1000);
            expect(await l0b.shardPeerInfo.getPeers()).toHaveLength(1)
            await disconnectPeers([peer, peer2]);
        })

        test('peer counter from 2 replicators', async () => {
            let [peer, peer2, peer3] = await getConnectedPeers(3);
            let l0a = await shardStoreShard();
            await l0a.replicate(peer);

            let l0b = await Shard.loadFromCID(l0a.cid, peer2.node);
            await l0b.replicate(peer2);
            let l0c = await Shard.loadFromCID(l0a.cid, peer3.node);
            await l0c.init(peer3);
            await delay(1000);
            expect(await l0c.shardPeerInfo.getPeers()).toHaveLength(2)
            await disconnectPeers([peer, peer2, peer3]);
        })

        test('peer counter from 2 replicators, but one is offline', async () => {
            let [peer, peer2, peer3] = await getConnectedPeers(3);
            let l0a = await shardStoreShard();
            await l0a.replicate(peer);

            let l0b = await Shard.loadFromCID(l0a.cid, peer2.node);
            await l0b.replicate(peer2);

            let l0c = await Shard.loadFromCID(l0a.cid, peer3.node);
            await l0c.init(peer3);
            await delay(5000);
            waitForAsync(async () => (await l0c.shardPeerInfo.getPeers()).length == 2);
            await disconnectPeers([peer2]);
            waitForAsync(async () => (await l0c.shardPeerInfo.getPeers()).length == 1);
            await disconnectPeers([peer, peer3]);
        })


        test('request replicate', async () => {
            let peer = await getPeer();
            let peer2 = await getPeer(undefined, false);
            await connectPeers(peer, peer2);

            let l0a = await shardStoreShard();
            await l0a.init(peer);
            await l0a.trust.load();
            await l0a.trust.addTrust(peer2.orbitDB.identity);
            await waitFor(() => l0a.trust.db.size == 1)// add some delay because trust db is not synchronous

            let l0b = await Shard.loadFromCID(l0a.cid, peer2.node);
            await l0b.init(peer2)

            expect(await l0a.shardPeerInfo.getPeers()).toHaveLength(0)
            expect(await l0b.shardPeerInfo.getPeers()).toHaveLength(0)

            expect(l0a.trust.rootTrust.id === l0b.trust.rootTrust.id);            // Replication step
            let replicationCallback = false
            await Shard.subscribeForReplication(peer, l0a.trust, () => { replicationCallback = true });
            await delay(5000); // Pubsub is flaky, wait some time before requesting shard
            await l0b.requestReplicate();
            //  --------------
            expect(await l0b.shardPeerInfo.getPeers()).toHaveLength(1)
            expect(replicationCallback);
            // add some delay because replication might take some time and is not synchronous
            await disconnectPeers([peer, peer2]);
        })

        test('trust web reuse closing shards', async () => {
            let peer = await getPeer();
            let l0a = await shardStoreShard();
            await l0a.init(peer);
            await l0a.trust.load();
            let l0b = await shardStoreShard(l0a.trust);
            await l0b.init(peer);
            expect(peer.trustWebs.size).toEqual(1);
            const hashCode = l0a.trust.hashCode();
            expect(peer.trustWebs.get(hashCode).shards).toHaveLength(2);
            await l0b.close();
            expect(peer.trustWebs.get(hashCode).shards).toHaveLength(1);
            await l0a.close();
            expect(peer.trustWebs.has(hashCode)).toBeFalsy();
            expect(l0a.trust).toEqual(l0b.trust);
            await disconnectPeers([peer])
        })

    })

    describe('peer', () => {
        describe('options', () => {
            test('isServer=false no subscriptions on idle on shardStoreShard', async () => {
                let peerNonServer = await getPeer(undefined, false);
                let l0 = await shardStoreShard();
                await l0.init(peerNonServer);
                const subscriptions = await peerNonServer.node.pubsub.ls();
                expect(subscriptions.length).toEqual(0);  // non server should not have any subscriptions idle
                await disconnectPeers([peerNonServer]);
            })

            test('isServer=false no subscriptions on idle on documentStoreShard', async () => {

                let peerNonServer = await getPeer(undefined, false);
                let l0 = await documentStoreShard();
                await l0.init(peerNonServer);
                const subscriptions = await peerNonServer.node.pubsub.ls();
                expect(subscriptions.length).toEqual(0); // non server should not have any subscriptions idle
                await disconnectPeers([peerNonServer]);
            })


            test('isServer=false can write', async () => {

                let peerServer = await getPeer(undefined, true);
                let peerNonServer = await getPeer(undefined, false);
                await connectPeers(peerServer, peerNonServer)

                let l0 = await documentStoreShard();
                await l0.replicate(peerServer);

                await l0.trust.addTrust(peerNonServer.orbitDB.identity);

                let l0Write = await Shard.loadFromCID<DocumentStoreInterface>(l0.cid, peerNonServer.node);
                await l0Write.init(peerNonServer);

                //await peerNonServer.orbitDB["_pubsub"].subscribe(l0Write.interface.db.address.toString(), peerNonServer.orbitDB["_onMessage"].bind(peerNonServer.orbitDB), peerNonServer.orbitDB["_onPeerConnected"].bind(peerNonServer.orbitDB))
                let subscriptions = await peerNonServer.node.pubsub.ls();
                expect(subscriptions.length).toEqual(0); // non server should not have any subscriptions idle
                await delay(3000)
                await l0Write.interface.load();
                await l0Write.interface.write((x) => l0Write.interface.db.put(x), new Document({
                    id: 'hello'
                }))

                await l0.interface.load();
                await waitFor(() => l0.interface.db.size > 0);
                subscriptions = await peerNonServer.node.pubsub.ls();
                expect(subscriptions.length).toEqual(0);  // non server should not have any subscriptions after write
                await disconnectPeers([peerServer, peerNonServer]);
            })
        });
        describe('leader', () => {
            test('no leader, since no peers', async () => {

                let [peer] = await getConnectedPeers(1)

                // Create Root shard

                let l0 = await shardStoreShard();
                await l0.init(peer)

                let isLeader = await l0.shardPeerInfo.isLeader(0);
                expect(isLeader).toBeFalsy();
                disconnectPeers([peer]);
            })

            test('always leader, since 1 peer', async () => {

                let [peer] = await getConnectedPeers(1)

                // Create Root shard
                let l0 = await shardStoreShard();
                await l0.replicate(peer);

                for (let time = 0; time < 3; time++) {
                    let isLeader = await l0.shardPeerInfo.isLeader(time);
                    expect(isLeader).toBeTruthy();
                }
                await disconnectPeers([peer]);
            })

            test('1 leader if two peers', async () => {

                let [peer, peer2] = await getConnectedPeers(2)

                // Create Root shard
                let l0a = await shardStoreShard();
                await l0a.replicate(peer);

                let l0b = await Shard.loadFromCID(l0a.cid, peer2.node);
                await l0b.replicate(peer2);
                let isLeaderA = await l0a.shardPeerInfo.isLeader(123);
                let isLeaderB = await l0b.shardPeerInfo.isLeader(123);

                expect(typeof isLeaderA).toEqual('boolean');
                expect(typeof isLeaderB).toEqual('boolean');
                expect(isLeaderA).toEqual(!isLeaderB);

                await disconnectPeers([peer, peer2]);
            })

        })


    })

    describe('query', () => {
        test('query subscription are combined', async () => {

            let [peer] = await getConnectedPeers(1)

            // Create Root shard

            let l0 = await shardStoreShard();
            await l0.replicate(peer);
            // Create 2 feed stores
            let feedStore1 = await (await documentStoreShard()).init(peer, l0.cid); // <-- This should trigger a swarm connection from peer to peer2
            await feedStore1.replicate(peer);
            let feedStore2 = await (await documentStoreShard()).init(peer, l0.cid); // <-- This should trigger a swarm connection from peer to peer2
            await feedStore2.replicate(peer);
            const subscriptions = await peer.node.pubsub.ls();
            expect(subscriptions.filter(x => x.endsWith("/query"))).toHaveLength(1);
            disconnectPeers([peer]);
        })
    })

    // TODO: Autosharding on new data
    // TODO: Sharding if overflow

    describe('sharding', () => {

        test('memory left peer info', async () => {
            let peer = await getPeer()

            // Create Root shard
            let l0 = await shardStoreShard();
            await l0.replicate(peer);

            // Create 1 feed store
            let statusA = await l0.shardPeerInfo.getShardPeerInfo();
            let feedStore1 = await (await documentStoreShard()).init(peer, l0.cid);
            await feedStore1.replicate(peer);

            let statusB = await l0.shardPeerInfo.getShardPeerInfo();

            expect(statusA.memoryLeft).toBeGreaterThan(statusB.memoryLeft)
            disconnectPeers([peer]);
        })

        test('memory runs out, prevent replicating', async () => {
            let peer = await getPeer()

            // Create Root shard
            let l0 = await shardStoreShard();
            await l0.replicate(peer);

            // Create store
            let feedStore1 = await (await documentStoreShard()).init(peer, l0.cid);

            // Introduce memory limit
            const usedHeap = v8.getHeapStatistics().used_heap_size;
            peer.options.heapSizeLimit = usedHeap + 1;

            await expect(feedStore1.replicate(peer)).rejects.toBeInstanceOf(MemoryLimitExceededError);
            await disconnectPeers([peer]);
        })

        //  replication happens when someone runs out of memory, but shard does not excceed max memory to be allocated
        //  sharding happens maximum memory is allocated, and new data wants to be written (! we can not measure memory consumption by db interfaces
        //  without spawn sub processes

        // alternative solution 
        // shard everything as soon as a peer runs out of memory, to let the peer still be alive
        // replication is only something that is invoked when a peer goes down or a new shard is created and reducancy is to be built
        test('memory runs out, will request sharding', async () => {
            let [peerLowMemory, peerSupporting, peerNew] = await getConnectedPeers(3)

            // Create Root shard
            let l0 = await shardStoreShard();
            await l0.replicate(peerLowMemory);
            await l0.trust.addTrust(peerSupporting.orbitDB.identity);
            await l0.trust.addTrust(peerNew.orbitDB.identity);

            // Subscribe for replication for peer2
            await Shard.subscribeForReplication(peerSupporting, l0.trust);

            // Create store
            let docStoreA = await (await documentStoreShard(l0.trust)).init(peerLowMemory, l0.cid);
            await docStoreA.interface.load();

            // add documents to docStore to trigger sharding
            expect(peerSupporting.supportJobs.size).toEqual(0);
            peerLowMemory.options.heapSizeLimit = v8.getHeapStatistics().used_heap_size + 100;
            /*   await expect(async () => {
                 
              }).rejects.toBeInstanceOf(AccessError); */
            await expect(async () => {
                for (let i = 0; i <= 1000; i++) {
                    await docStoreA.interface.db.put(new Document({ id: i.toString() })) // This will eventually fail
                }
            }).rejects.toBeInstanceOf(AccessError);

            // Check that peer2 started supporting a shard (indexed 1)
            await waitFor(() => peerSupporting.supportJobs.size == 1);
            expect(peerSupporting.supportJobs.values().next().value.shard.shardIndex).toEqual(1);
            expect(peerSupporting.supportJobs.values().next().value.shard.parentShardCID).toEqual(l0.cid);
            expect(peerSupporting.supportJobs.values().next().value.shard.trust).toEqual(l0.trust);

            // try write some, and see shard picks up
            let docStoreAWritable = await docStoreA.createShardWithIndex(1, peerNew);
            await docStoreAWritable.load();
            await waitFor(() => docStoreAWritable.trust.isTrusted(peerNew.orbitDB.identity));
            await docStoreAWritable.interface.db.put(new Document({ id: 'new' }));
            await waitFor(() => Object.keys((peerSupporting.supportJobs.values().next().value.shard.interface as DocumentStoreInterface).db.index._index).length == 1);
            await disconnectPeers([peerLowMemory, peerSupporting, peerNew]);
        })


    })
});
