
import { NoResourceRequirements, Shard } from '../shard';
import { AnyPeer } from '../peer';
import {  getPeer, getConnectedPeers } from './utils';
import { connectPeers, disconnectPeers } from '@dao-xyz/peer-test-utils';
import { delay, waitFor, waitForAsync } from '@dao-xyz/time';
import { RegionAccessController } from '@dao-xyz/orbit-db-trust-web'
import { MemoryLimitExceededError } from '../errors';
import v8 from 'v8';
import { AccessError } from '@dao-xyz/encryption-utils';
import { v4 as uuid } from 'uuid';
import { BinaryDocumentStore } from '@dao-xyz/orbit-db-bdocstore';
import { field, variant } from '@dao-xyz/borsh';
import { DynamicAccessController } from '@dao-xyz/orbit-db-dynamic-access-controller';
import { PublicKey } from '@dao-xyz/identity';
import { Address } from '@dao-xyz/orbit-db-store';

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




@variant(0)
class Document  {

    @field({ type: 'string' })
    id: string;

    constructor(opts?: { id: string }) {
        if (opts) {
            this.id = opts.id;
        }

    }
}


@variant(1)
class ShardDocument {

    @field({ type: 'string' })
    id: string;

    @field({type: Shard})
    shard: Shard<any>

    constructor(opts?: { id: string, shard: Shard<any> }) {
        if (opts) {
            this.shard = opts.shard;
            this.id = opts.id;
        }

    }
}


export const documentStoreShard = async (properties: {rootTrust?: PublicKey, regionAccessController?: RegionAccessController, parentAddress?: Address} ) => new Shard({
    id: uuid(),
    cluster: 'x',
    resourceRequirements: new NoResourceRequirements(),
    store: new BinaryDocumentStore({
        name: 'documents',
        indexBy: 'id',
        objectType: Document.name,
        accessController: new DynamicAccessController({
            regionAccessController: properties.regionAccessController,
            rootTrust: properties.rootTrust
        }),
        clazz: Document
    }),
    trust: properties.regionAccessController,
    parentAddress: properties.parentAddress
})


export const shardStoreShard = async <T>(properties: {rootTrust?: PublicKey, regionAccessController?: RegionAccessController, parentAddress?: Address}) => new Shard<BinaryDocumentStore<ShardDocument>>({
    id: uuid(),
    cluster: 'x',
    resourceRequirements: new NoResourceRequirements(),
    store: new BinaryDocumentStore({
        indexBy: 'id',
        accessController:  new DynamicAccessController({
            regionAccessController: properties.regionAccessController,
            rootTrust: properties.rootTrust
        }),
        objectType: ShardDocument.name,
        clazz: ShardDocument,
        name: 'shard',
    }),
    trust: properties.regionAccessController,
    parentAddress: properties.parentAddress
})


describe('cluster', () => {
    describe('trust', () => {
        it('add trustee', async () => {
            let [peer, peer2] = await getConnectedPeers(2);
            let l0a = await documentStoreShard({rootTrust: peer.orbitDB.identity});
            await peer.orbitDB.open(l0a);
            expect(l0a.address).toBeDefined();
            expect(l0a.trust).toBeInstanceOf(RegionAccessController);
            expect((l0a.trust as RegionAccessController).rootTrust).toBeDefined();
            expect((l0a.trust as RegionAccessController).rootTrust.equals(peer.orbitDB.identity))

            let newTrustee = peer2.orbitDB.identity;
            await l0a.trust.addTrust(newTrustee);
            const l0b = await Shard.load(peer2.node,l0a.address);
            await peer2.orbitDB.open(l0b);
            await l0b.trust.trustGraph.load(1);
            expect(l0b.trust.trustGraph.size).toEqual(1)
            await disconnectPeers([peer, peer2]);
        })
    })

    describe('manifest', () => {
        it('save load', async () => {

            let [peer, peer2] = await getConnectedPeers(2);
            let l1 = (await peer.node.id()).addresses[0];
            await peer2.node.swarm.connect(l1)

            let l0 = await documentStoreShard({rootTrust: peer.orbitDB.identity});
            await peer.orbitDB.open(l0);
            expect(l0.address).toBeDefined();
            let loadedShard = await Shard.load(peer2.node,l0.address);
            expect(loadedShard.store.address).toEqual(l0.store.address);
            await disconnectPeers([peer, peer2]);
        })
    })

    describe('recursive shard', () => {
        it('peer backward connect', async () => {

            let [peer, peer2] = await getConnectedPeers(2)

            // Create Root shard
            let l0 = await shardStoreShard({rootTrust: peer.orbitDB.identity});
            await l0.support(peer);
            // Create Feed store

            expect(await isInSwarm(peer, peer2)).toBeFalsy();
            let feedStore = await peer2.orbitDB.open(await documentStoreShard({parentAddress: l0.address})); // <-- This should trigger a swarm connection from peer to peer2
            await feedStore.support(peer2);
            expect(feedStore.store.address.toString().endsWith(l0.address + '-documents'));
            await waitForAsync(async () => await isInSwarm(peer, peer2))
            disconnectPeers([peer, peer2]);
        })


        it('backward connect filter unique', async () => {

            let [peer, peer2] = await getConnectedPeers(2)

            // Create Root shard
            let l0 = await shardStoreShard({rootTrust: peer.orbitDB.identity});
            await l0.support(peer);
            // Create Feed store

            expect(await isInSwarm(peer, peer2)).toBeFalsy();
            let feedStore1 = await peer2.orbitDB.open(await documentStoreShard({parentAddress: l0.address})); // <-- This should trigger a swarm connection from peer to peer2
            await feedStore1.support(peer2);
            let feedStore2 = await peer2.orbitDB.open(await documentStoreShard({parentAddress: l0.address})); // <-- This should trigger a swarm connection from peer to peer2
            await feedStore2.support(peer2);

            await waitForAsync(async () => await isInSwarm(peer, peer2))
            expect(peer2.supportJobs.size).toEqual(2)
            expect([...peer2.supportJobs.values()].filter(x => x.connectingToParentShardCID)).toHaveLength(1);
            expect([...peer2.supportJobs.values()].filter(x => x.connectingToParentShardCID)[0].connectingToParentShardCID).toEqual(l0.address);
            disconnectPeers([peer, peer2]);
        })

        it('backward connect no job is same peer', async () => {

            let [peer] = await getConnectedPeers(2)

            // Create Root shard
            let l0 = await shardStoreShard({rootTrust: peer.orbitDB.identity});
            await l0.support(peer);
            // Create Feed store

            let feedStore1 = await peer.orbitDB.open(await documentStoreShard({parentAddress:l0.address})); // <-- This should trigger a swarm connection from peer to peer2
            await feedStore1.support(peer);
            let feedStore2 = await peer.orbitDB.open(await documentStoreShard({parentAddress:l0.address})); // <-- This should trigger a swarm connection from peer to peer2
            await feedStore2.support(peer);

            expect(peer.supportJobs.size).toEqual(3)
            expect([...peer.supportJobs.values()].filter(x => x.connectingToParentShardCID)).toHaveLength(0);
            disconnectPeers([peer]);
        })
    })

    /* describe('resiliance', () => {
        it('connect to remote', async () => {

            let [peer, peer2] = await getConnectedPeers(2);

            // Create Root shard
            let l0 = await shardStoreShard();
            await l0.support(peer);

            // Create Feed store
            let documentStore = await (await documentStoreShard({parentAddress: l0.cid})).open(peer);
            await documentStore.support(peer);
            await documentStore.store.put(new Document({ id: 'hello' }));
            await l0.store.put(documentStore);


            // --- Load assert, from another peer
            const l0b = await Shard.loadFromCID(l0.cid, peer2.orbitDB._ipfs);
            await peer2.orbitDB.open(l0b)
            await (l0b.store).load();
            await waitFor(() => Object.keys((l0b.store as BinaryDocumentStore<ShardDocument>)._index._index).length === 1);
            let feedStoreLoaded = await (l0b.store as BinaryDocumentStore<ShardDocument>).get(documentStore.cid), peer2);
            await feedStoreLoaded.interface.load();
            await waitFor(() => Object.keys(feedStoreLoaded.interface.db.index._index).length === 1);
            await disconnectPeers([peer, peer2]);
        })

        it('first peer drop, data still alive because 2nd peer is up', async () => {
            let [peer, peer2, peer3] = await getConnectedPeers(3);

            // Create Root shard
            let l0a = await shardStoreShard();
            await l0a.support(peer);

            // Create Feed store
            let l1 = await (await documentStoreShard({parentAddress: l0a.cid})).open(peer);
            await l0a.store.load();
            await l0a.store.put(l1);

            // --- Load assert, from another peer
            const l0b = await Shard.loadFromCID<BinaryDocumentStore<ShardDocument>>(l0a.cid, peer2.node);
            await l0b.support(peer2);
            await l0b.store.load(1);
            await waitFor(() => Object.keys(l0b.store._index._index).length === 1);


            // Drop 1 peer and make sure a third peer can access data
            await disconnectPeers([peer]);
            const l0c = await Shard.loadFromCID<BinaryDocumentStore<ShardDocument>>(l0a.cid, peer3.node);
            await peer3.orbitDB.open(l0c)
            await l0c.store.load(1);
            await waitFor(() => Object.keys(l0c.store._index._index).length === 1);
            await disconnectPeers([peer2, peer3]);
        })
    })




    describe('peer', () => {

        it('peer counter from 1 replicator', async () => {
            let [peer, peer2] = await getConnectedPeers(2);
            let l0a = await shardStoreShard();
            await l0a.support(peer);
            let l0b = await Shard.loadFromCID(l0a.cid, peer2.node);
            await peer2.orbitDB.open(l0b);
            await delay(1000);
            expect(await l0b.shardPeerInfo.getPeers()).toHaveLength(1)
            await disconnectPeers([peer, peer2]);
        })

        it('peer counter from 2 replicators', async () => {
            let [peer, peer2, peer3] = await getConnectedPeers(3);
            let l0a = await shardStoreShard();
            await l0a.support(peer);

            let l0b = await Shard.loadFromCID(l0a.cid, peer2.node);
            await l0b.support(peer2);
            let l0c = await Shard.loadFromCID(l0a.cid, peer3.node);
            await peer3.orbitDB.open(l0c);
            await delay(1000);
            expect(await l0c.shardPeerInfo.getPeers()).toHaveLength(2)
            await disconnectPeers([peer, peer2, peer3]);
        })

        it('peer counter from 2 replicators, but one is offline', async () => {
            let [peer, peer2, peer3] = await getConnectedPeers(3);
            let l0a = await shardStoreShard();
            await l0a.support(peer);

            let l0b = await Shard.loadFromCID(l0a.cid, peer2.node);
            await l0b.support(peer2);

            let l0c = await Shard.loadFromCID(l0a.cid, peer3.node);
            await peer3.orbitDB.open(l0c);
            await delay(5000);
            await waitForAsync(async () => (await l0c.shardPeerInfo.getPeers()).length == 2);
            await disconnectPeers([peer2]);
            await waitForAsync(async () => (await l0c.shardPeerInfo.getPeers()).length == 1);
            await disconnectPeers([peer, peer3]);
        })


        it('request replicate', async () => {
            let peer = await getPeer();
            let peer2 = await getPeer(undefined, false);
            await connectPeers(peer, peer2);

            let l0a = await shardStoreShard();
            await peer.orbitDB.open(l0a);
            await l0a.trust.load();
            await l0a.trust.addTrust(peer2.orbitDB.identity);
            await waitFor(() => l0a.trust.trustGraph.size == 1)// add some delay because trust db is not synchronous

            let l0b = await Shard.loadFromCID(l0a.cid, peer2.node);
            await peer2.orbitDB.open(l0b)

            expect(await l0a.shardPeerInfo.getPeers()).toHaveLength(0)

            expect(l0a.trust.rootTrust.equals(l0b.trust.rootTrust));            // Replication step
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

        it('trust web reuse closing shards', async () => {
            let peer = await getPeer();
            let l0a = await shardStoreShard();
            await peer.orbitDB.open(l0a);
            await l0a.trust.load();
            let l0b = await shardStoreShard(l0a.trust);
            await peer.orbitDB.open(l0b);
            expect(peer.trustWebs.size).toEqual(1);
            const hashCode = l0a.trust.hashCode();
            expect(peer.trustWebs.get(hashCode).shards.size).toEqual(2);
            await l0b.close();
            expect(peer.trustWebs.get(hashCode).shards.size).toEqual(1);
            await l0a.close();
            expect(peer.trustWebs.has(hashCode)).toBeFalsy();
            expect(l0a.trust).toEqual(l0b.trust);
            await disconnectPeers([peer])
        })

    })

    describe('peer', () => {
        describe('options', () => {
            it('isServer=false no subscriptions on idle on shardStoreShard', async () => {
                let peerNonServer = await getPeer(undefined, false);
                let l0 = await shardStoreShard();
                await l0.open(peerNonServer);
                const subscriptions = await peerNonServer.node.pubsub.ls();
                expect(subscriptions.length).toEqual(0);  // non server should not have any subscriptions idle
                await disconnectPeers([peerNonServer]);
            })

            it('isServer=false no subscriptions on idle on documentStoreShard', async () => {

                let peerNonServer = await getPeer(undefined, false);
                let l0 = await documentStoreShard();
                await l0.open(peerNonServer);
                const subscriptions = await peerNonServer.node.pubsub.ls();
                expect(subscriptions.length).toEqual(0); // non server should not have any subscriptions idle
                await disconnectPeers([peerNonServer]);
            })


            it('isServer=false can write', async () => {

                let peerServer = await getPeer(undefined, true);
                let peerNonServer = await getPeer(undefined, false);
                await connectPeers(peerServer, peerNonServer)

                let l0 = await documentStoreShard();
                await l0.support(peerServer);

                await l0.trust.addTrust(peerNonServer.orbitDB.identity);

                let l0Write = await Shard.loadFromCID<BinaryDocumentStore<ShardDocument>>(l0.cid, peerNonServer.node);
                await l0Write.open(peerNonServer);

                //await peerNonServer.orbitDB["_pubsub"].subscribe(l0Write.interface.db.address.toString(), peerNonServer.orbitDB["_onMessage"].bind(peerNonServer.orbitDB), peerNonServer.orbitDB["_onPeerConnected"].bind(peerNonServer.orbitDB))
                let subscriptions = await peerNonServer.node.pubsub.ls();
                expect(subscriptions.length).toEqual(0); // non server should not have any subscriptions idle
                await delay(3000)
                await l0Write.store.load();
                await l0Write.store.put(new Document({ id: 'hello' }))
                await waitFor(() => l0.store.size > 0);
                await l0Write.close();
                subscriptions = await peerNonServer.node.pubsub.ls();
                expect(subscriptions.length).toEqual(0);  // non server should not have any subscriptions after write
                await disconnectPeers([peerServer, peerNonServer]);
            })
        });
        describe('leader', () => {
            it('no leader, since no peers', async () => {

                let [peer] = await getConnectedPeers(1)

                // Create Root shard

                let l0 = await shardStoreShard();
                await peer.orbitDB.open(l0)

                let isLeader = await l0.shardPeerInfo.isLeader(0);
                expect(isLeader).toBeFalsy();
                disconnectPeers([peer]);
            })

            it('always leader, since 1 peer', async () => {

                let [peer] = await getConnectedPeers(1)

                // Create Root shard
                let l0 = await shardStoreShard();
                await l0.support(peer);

                for (let time = 0; time < 3; time++) {
                    let isLeader = await l0.shardPeerInfo.isLeader(time);
                    expect(isLeader).toBeTruthy();
                }
                await disconnectPeers([peer]);
            })

            it('1 leader if two peers', async () => {

                let [peer, peer2] = await getConnectedPeers(2)

                // Create Root shard
                let l0a = await shardStoreShard();
                await l0a.support(peer);

                let l0b = await Shard.loadFromCID(l0a.cid, peer2.node);
                await l0b.support(peer2);
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
        it('query subscription are combined', async () => {

            let [peer] = await getConnectedPeers(1)

            // Create Root shard
            let l0 = await shardStoreShard();
            await l0.support(peer);

            // Create 2 feed stores
            let feedStore1 = await (await documentStoreShard({parentAddress: l0.cid})).open(peer);
            await feedStore1.support(peer);
            let feedStore2 = await (await documentStoreShard({parentAddress: l0.cid})).open(peer);
            await feedStore2.support(peer);

            const subscriptions = await peer.node.pubsub.ls();
            expect(subscriptions).toHaveLength(2); // 1 channel for queries, 1 channel for replication
            await disconnectPeers([peer]);
        })
    })

    
    describe('trigger', () => {

        it('memory left peer info', async () => {
            let peer = await getPeer()

            // Create Root shard
            let l0 = await shardStoreShard();
            await l0.support(peer);

            // Create 1 feed store
            let statusA = await l0.shardPeerInfo.getShardPeerInfo();
            let feedStore1 = await (await documentStoreShard({parentAddress: l0.cid})).open(peer);
            await feedStore1.support(peer);

            let statusB = await l0.shardPeerInfo.getShardPeerInfo();

            expect(statusA.memoryLeft).toBeGreaterThan(statusB.memoryLeft)
            disconnectPeers([peer]);
        })

        it('memory runs out, prevent replicating', async () => {
            let peer = await getPeer()

            // Create Root shard
            let l0 = await shardStoreShard();
            await l0.support(peer);

            // Create store
            let feedStore1 = await (await documentStoreShard({parentAddress: l0.cid})).open(peer);

            // Introduce memory limit
            const usedHeap = v8.getHeapStatistics().used_heap_size;
            peer.options.heapSizeLimit = usedHeap + 1;

            await expect(feedStore1.support(peer)).rejects.toBeInstanceOf(MemoryLimitExceededError);
            await disconnectPeers([peer]);
        })

        //  replication happens when someone runs out of memory, but shard does not excceed max memory to be allocated
        //  sharding happens maximum memory is allocated, and new data wants to be written (! we can not measure memory consumption by db interfaces
        //  without spawn sub processes

        // alternative solution 
        // shard everything as soon as a peer runs out of memory, to let the peer still be alive
        // replication is only something that is invoked when a peer goes down or a new shard is created and reducancy is to be built
        it('memory runs out, will request sharding', async () => {
            let [peerLowMemory, peerSupporting, peerNew] = await getConnectedPeers(3)

            // Create Root shard
            let l0 = await shardStoreShard();
            await l0.support(peerLowMemory);
            await l0.trust.addTrust(peerSupporting.orbitDB.identity);
            await l0.trust.addTrust(peerNew.orbitDB.identity);

            // Subscribe for replication for peer2
            await Shard.subscribeForReplication(peerSupporting, l0.trust);

            // Create store
            let docStoreA = await (await documentStoreShard({trust: l0.trust, parentAddress: l0.cid})).open(peerLowMemory);
            await docStoreA.store.load();

            // add documents to docStore to trigger sharding
            expect(peerSupporting.supportJobs.size).toEqual(0);
            peerLowMemory.options.heapSizeLimit = v8.getHeapStatistics().used_heap_size + 100;

            await expect(async () => {
                for (let i = 0; i <= 1000; i++) {
                    await docStoreA.store.put(new Document({ id: i.toString() })) // This will eventually fail
                }
            }).rejects.toBeInstanceOf(AccessError);

            // Check that peer2 started supporting a shard (indexed 1)
            await waitFor(() => peerSupporting.supportJobs.size == 1);
            expect(peerSupporting.supportJobs.values().next().value.shard.shardIndex).toEqual(1n);
            expect(peerSupporting.supportJobs.values().next().value.shard.parentAddress).toEqual(l0.cid);
            expect(peerSupporting.supportJobs.values().next().value.shard.trust).toEqual(l0.trust);

            // try write some, and see shard picks up
            let docStoreAWritable = await docStoreA.createShardWithIndex(1n, peerNew);
            await docStoreAWritable.load();
            await waitFor(() => docStoreAWritable.trust.isTrusted(peerNew.orbitDB.identity));
            await docStoreAWritable.store.put(new Document({ id: 'new' }));
            await waitFor(() => Object.keys((peerSupporting.supportJobs.values().next().value.shard.store as BinaryDocumentStore<any>)._index._index).length == 1);
            await disconnectPeers([peerLowMemory, peerSupporting, peerNew]);
        })
    }) */

})
 





    /* describe('presharding', () => {

        it('nested block store', async () => {


            let peers = await getPeersSameIdentity(2, 100000000);
            let peer = peers[0];
            let peer2 = peers[1];

            // Create Root shard
            let l0 = await shardStoreShard<FeedStoreInterface>();
            await l0.support(peer);

            // Create Feed store
            let feedStore = await (await feedStoreShard()).init(peer, l0);
            await feedStore.support();
            let feedStoreLoaded = await l0.interface.loadShard(0);
            await feedStoreLoaded.interface.db.add("xxx");
            await l0.interface.db.put(feedStoreLoaded);



            // --- Load assert
            let l0b = await Shard.loadFromCID<FeedStoreInterface>(l0.cid, peer2.node);
            await peer2.orbitDB.open(l0b)
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


       // TODO: Autosharding on new data
    // TODO: Sharding if overflow

    /* describe('sharding', () => {

        describe('encryption', () => {
            it('keys are shared', async () => {

                TODO FIX TEST
                let peer = await getPeer();
                let peer2 = await getPeer(undefined, false);
                await connectPeers(peer, peer2);

                let l0a = await shardStoreShard();
                await peer.orbitDB.open(l0a);
                await l0a.trust.load();
                await l0a.trust.addTrust(peer2.orbitDB.identity);
                await waitFor(() => l0a.trust.db.size == 1)// add some delay because trust db is not synchronous

                let l0b = await Shard.loadFromCID(l0a.cid, peer2.node);
                await peer2.orbitDB.open(l0b)

                expect(await l0a.shardPeerInfo.getPeers()).toHaveLength(0)
                expect(l0a.trust.rootTrust.id === l0b.trust.rootTrust.id);

                // Replication step
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
        })
    }) */
