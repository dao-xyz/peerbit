
import { Shard } from '../shard';
import { AnyPeer, PeerOptions } from '../node';
import { BinaryFeedStoreInterface, DocumentStoreInterface, Document, documentStoreShard, getPeer, shardStoreShard, getConnectedPeers } from './utils';
import { P2PTrust } from '../trust';
import { PublicKey } from '../key';
import { connectPeers, disconnectPeers } from '@dao-xyz/peer-test-utils';
import { delay, waitFor, waitForAsync } from '@dao-xyz/time';

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
            let l0 = await documentStoreShard(Document);
            await l0.init(peer);
            expect(l0.cid).toBeDefined();
            expect(l0.trust).toBeInstanceOf(P2PTrust);
            expect((l0.trust as P2PTrust).rootTrust).toBeDefined();
            expect((l0.trust as P2PTrust).rootTrust).toEqual(PublicKey.from(peer.orbitDB.identity))


            let newTrustee = PublicKey.from(peer2.orbitDB.identity);
            await l0.trust.addTrust(newTrustee);

            await l0.init(peer2);
            await l0.trust.load(1);
            expect(l0.trust.db.db.size).toEqual(1)
            await disconnectPeers([peer, peer2]);


        })

        describe('isTrusted', () => {

            test('trusted by chain', async () => {

                let [peer, peer2, peer3] = await getConnectedPeers(3);

                let l0a = await documentStoreShard(Document);
                await l0a.init(peer);

                let peer2Key = PublicKey.from(peer2.orbitDB.identity);
                await l0a.trust.load();
                await l0a.trust.addTrust(peer2Key);

                let l0b = await Shard.loadFromCID(l0a.cid, peer2.node);
                await l0b.init(peer2);
                let peer3Key = PublicKey.from(peer3.orbitDB.identity);
                await l0b.trust.load();
                await l0b.trust.addTrust(peer3Key);

                // now check if peer3 is trusted from peer perspective
                await l0a.trust.load(2);
                expect(l0a.trust.isTrusted(peer3Key));

                await disconnectPeers([peer, peer2, peer3]);

            })

            test('untrusteed by chain', async () => {

                let [peer, peer2, peer3] = await getConnectedPeers(3);

                let l0a = await documentStoreShard(Document);
                await l0a.init(peer);

                let l0b = await Shard.loadFromCID(l0a.cid, peer2.node);
                await l0b.init(peer2);
                let peer3Key = PublicKey.from(peer3.orbitDB.identity);
                await l0b.trust.addTrust(peer3Key);

                // now check if peer3 is trusted from peer perspective
                // which it will not be since peer never trusted peer2 (which is required for peer3 to be trusted)
                await l0b.trust.load(1);
                expect(l0b.trust.rootTrust.address).toEqual(peer.orbitDB.identity.publicKey);
                expect(l0b.trust.isTrusted(peer3Key)).toBeFalsy();
                await disconnectPeers([peer, peer2, peer3]);

            })
        })



    })
    describe('manifest', () => {
        test('save load', async () => {

            let [peer, peer2] = await getConnectedPeers(2);
            let l1 = (await peer.node.id()).addresses[0];
            await peer2.node.swarm.connect(l1)

            let l0 = await documentStoreShard(Document);
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
            await l0.init(peer)
            await l0.replicate();
            // Create Feed store

            expect(await isInSwarm(peer, peer2)).toBeFalsy();
            let feedStore = await (await documentStoreShard(Document)).init(peer2, l0.cid); // <-- This should trigger a swarm connection from peer to peer2
            await feedStore.replicate();
            expect(feedStore.interface.db.address.endsWith(l0.cid + '-documents'));
            await waitForAsync(async () => await isInSwarm(peer, peer2))
            disconnectPeers([peer, peer2]);
        })


        test('backward connect filter unique', async () => {

            let [peer, peer2] = await getConnectedPeers(2)

            // Create Root shard
            let l0 = await shardStoreShard();
            await l0.init(peer)
            await l0.replicate();
            // Create Feed store

            expect(await isInSwarm(peer, peer2)).toBeFalsy();
            let feedStore1 = await (await documentStoreShard(Document)).init(peer2, l0.cid); // <-- This should trigger a swarm connection from peer to peer2
            await feedStore1.replicate();
            let feedStore2 = await (await documentStoreShard(Document)).init(peer2, l0.cid); // <-- This should trigger a swarm connection from peer to peer2
            await feedStore2.replicate();

            await waitForAsync(async () => await isInSwarm(peer, peer2))
            expect(peer2.supportJobs).toHaveLength(2);
            expect(peer2.supportJobs.filter(x => x.connectingToParentShardCID)).toHaveLength(1);
            expect(peer2.supportJobs.filter(x => x.connectingToParentShardCID)[0].connectingToParentShardCID).toEqual(l0.cid);
            disconnectPeers([peer, peer2]);
        })

        test('backward connect no job is same peer', async () => {

            let [peer] = await getConnectedPeers(2)

            // Create Root shard
            let l0 = await shardStoreShard();
            await l0.init(peer)
            await l0.replicate();
            // Create Feed store

            let feedStore1 = await (await documentStoreShard(Document)).init(peer, l0.cid); // <-- This should trigger a swarm connection from peer to peer2
            await feedStore1.replicate();
            let feedStore2 = await (await documentStoreShard(Document)).init(peer, l0.cid); // <-- This should trigger a swarm connection from peer to peer2
            await feedStore2.replicate();

            expect(peer.supportJobs).toHaveLength(3);
            expect(peer.supportJobs.filter(x => x.connectingToParentShardCID)).toHaveLength(0);
            disconnectPeers([peer]);
        })
    })

    describe('resiliance', () => {
        test('connect to remote', async () => {

            let [peer, peer2] = await getConnectedPeers(2);

            // Create Root shard
            let l0 = await shardStoreShard<DocumentStoreInterface<Document>>();
            await l0.init(peer)
            await l0.replicate();

            // Create Feed store
            let documentStore = await (await documentStoreShard(Document)).init(peer, l0.cid);
            await documentStore.replicate();
            await documentStore.interface.db.db.put(new Document({ id: 'hello' }));
            await l0.interface.db.db.put(documentStore);


            // --- Load assert, from another peer
            await l0.init(peer2)
            await l0.interface.db.load(1);
            expect(Object.keys(l0.interface.db.db.index._index).length).toEqual(1);
            let feedStoreLoaded = await l0.interface.loadShard(documentStore.cid);
            await feedStoreLoaded.interface.db.load(1);
            await waitFor(() => Object.keys(feedStoreLoaded.interface.db.db.index._index).length == 1)
            await disconnectPeers([peer, peer2]);

        })

        test('first peer drop, data still alive because 2nd peer is up', async () => {
            let [peer, peer2, peer3] = await getConnectedPeers(3);

            // Create Root shard
            let l0a = await shardStoreShard();
            await l0a.init(peer)
            await l0a.replicate();

            // Create Feed store
            let l1 = await (await documentStoreShard(Document)).init(peer, l0a.cid);
            await l0a.interface.db.load();
            await l0a.interface.db.db.put(l1);

            // --- Load assert, from another peer
            const l0b = await Shard.loadFromCID<DocumentStoreInterface<Document>>(l0a.cid, peer2.node);
            await l0b.init(peer2)
            await l0b.interface.db.load(1);
            await l0b.replicate();
            expect(Object.keys(l0b.interface.db.db.index._index).length).toEqual(1);


            // Drop 1 peer and make sure a third peer can access data
            await disconnectPeers([peer]);
            const l0c = await Shard.loadFromCID<DocumentStoreInterface<Document>>(l0a.cid, peer3.node);
            await l0c.init(peer3)
            await l0c.interface.db.load(1);
            expect(Object.keys(l0c.interface.db.db.index._index).length).toEqual(1);
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
            await l0.init(peer)
            await l0.replicate();

            // Create Feed store
            let feedStore = await (await feedStoreShard()).init(peer, l0);
            await feedStore.replicate();
            let feedStoreLoaded = await l0.interface.loadShard(0);
            await feedStoreLoaded.interface.db.db.add("xxx");
            await l0.interface.db.db.put(feedStoreLoaded);



            // --- Load assert
            let l0b = await Shard.loadFromCID<FeedStoreInterface>(l0.cid, peer2.node);
            await l0b.init(peer2)
            await l0b.interface.db.load(1);
            expect(Object.keys(l0.interface.db.db._index._index).length).toEqual(1);
            feedStoreLoaded = await l0b.interface.loadShard(0)
            expect(Object.keys(feedStoreLoaded.interface.db.db._index._index).length).toEqual(1);
            await disconnectPeers(peers);
        });
    }) */

    /*    HOW TO WE WORK WITH REPLICATION TOPICS ???
   
           WHILE PARNET
       SUBSCRICE ?
    */
    describe('ondemand-sharding', () => {
        /*  test('subscribe, request', async () => {
             let peer = await getPeer();
             let peer2 = await getPeer();
             let l0a = await shardStoreShard();
 
             await l0a.init(peer);
 
             await l0a.trust.addTrust(PublicKey.from(peer2.orbitDB.identity));
 
             let l0b = await Shard.loadFromCID(l0a.cid, peer2.node);
             await l0b.init(peer2)
 
             expect(await l0a.getRemotePeersSize()).toEqual(0)
             await waitFor(() => l0b.trust.db.db.size == 1)// add some delay because trust db is not synchronous
 
             // Replication step
             await peer2.subscribeForReplication(l0b.trust);
             await delay(1000); // Pubsub is flaky, wait some time before requesting shard
             await l0a.requestReplicate();
             //  --------------
 
             await waitFor(() => l0a.peers.db.size == 1) // add some delay because replication might take some time and is not synchronous
             expect(await l0a.getRemotePeersSize()).toEqual(1);
             expect(l0a.peers.db.size).toEqual(1);
             await disconnectPeers([peer, peer2]);
 
 
         }) */

        /*    test('recycle peer db max sized', async () => {
   
               // test that peers db does not grow infinitly
               let [peer] = await getConnectedPeers(1);
               peer.options.peersRecycle.maxOplogLength = 2;
               peer.options.peersRecycle.cutOplogToLength = 2;
               let l0a = await shardStoreShard();
               await l0a.init(peer);
               await l0a.replicate();
               await delay(peer.options.peerHealtcheckInterval * 4); // wait for fill up peer pings, but it will cut back to 3 because of the peer settings
               expect(l0a.peers.db["_oplog"].values.length).toEqual(3);
           }) */

        /*  test('peer healthcheck interval option', async () => {
 
             // test that peers db does not grow infinitly
             let [peer] = await getConnectedPeers(1);
             let l0a = await shardStoreShard();
             await l0a.init(peer);
             await l0a.replicate();
             let start = l0a.peers.db["_oplog"].values.length;
             await delay(peer.options.peerHealtcheckInterval * 2 + 100); // wait for at least 2 healthchecks + a little extra for margin
             let end = l0a.peers.db["_oplog"].values.length;
             expect(end - start).toEqual(2);
         }) */

        /*  test('peer remote counter', async () => {
             let [peer, peer2] = await getConnectedPeers(2);
             let l0a = await shardStoreShard();
             await l0a.init(peer);
             let thisPeer = new Peer({
                 key: PublicKey.from(peer.orbitDB.identity),
                 addresses: (await peer.node.id()).addresses.map(x => x.toString()),
                 timestamp: new BN(+new Date),
                 memoryBudget: new BN(1000)
             });
             await l0a.peers.load();
             await l0a.peers.db.put(thisPeer);
             let l0b = await Shard.loadFromCID(l0a.cid, peer2.node);
             await l0b.init(peer2);
             expect(await l0b.getRemotePeersSize(true)).toEqual(1);
             await disconnectPeers([peer, peer2]);
         }) */

        test('peer counter from 1 replicator', async () => {
            let [peer, peer2] = await getConnectedPeers(2);
            let l0a = await shardStoreShard();
            await l0a.init(peer);
            await l0a.replicate();
            let l0b = await Shard.loadFromCID(l0a.cid, peer2.node);
            await l0b.init(peer2);
            await delay(1000);
            expect(await l0b.shardPeerInfo.getPeers()).toHaveLength(1)
            await disconnectPeers([peer, peer2]);
        })

        test('peer counter from 2 replicators', async () => {
            let [peer, peer2, peer3] = await getConnectedPeers(3);
            let l0a = await shardStoreShard();
            await l0a.init(peer);
            await l0a.replicate();

            let l0b = await Shard.loadFromCID(l0a.cid, peer2.node);
            await l0b.init(peer2);
            await l0b.replicate();
            let l0c = await Shard.loadFromCID(l0a.cid, peer3.node);
            await l0c.init(peer3);
            await delay(1000);
            expect(await l0c.shardPeerInfo.getPeers()).toHaveLength(2)
            await disconnectPeers([peer, peer2, peer3]);
        })

        test('peer counter from 2 replicators, but one is offline', async () => {
            let [peer, peer2, peer3] = await getConnectedPeers(3);
            let l0a = await shardStoreShard();
            await l0a.init(peer);
            await l0a.replicate();

            let l0b = await Shard.loadFromCID(l0a.cid, peer2.node);
            await l0b.init(peer2);
            await l0b.replicate();

            let l0c = await Shard.loadFromCID(l0a.cid, peer3.node);
            await l0c.init(peer3);
            await delay(5000);
            waitForAsync(async () => (await l0c.shardPeerInfo.getPeers()).length == 2);
            await disconnectPeers([peer2]);
            waitForAsync(async () => (await l0c.shardPeerInfo.getPeers()).length == 1);
            await disconnectPeers([peer, peer3]);
        })



        test('subscribe, request', async () => {
            let peer = await getPeer();
            let peer2 = await getPeer(undefined, false);
            await connectPeers(peer, peer2);

            let l0a = await shardStoreShard();
            await l0a.init(peer);
            await l0a.trust.load();
            await l0a.trust.addTrust(PublicKey.from(peer2.orbitDB.identity));
            await waitFor(() => l0a.trust.db.db.size == 1)// add some delay because trust db is not synchronous

            let l0b = await Shard.loadFromCID(l0a.cid, peer2.node);
            await l0b.init(peer2)

            expect(await l0a.shardPeerInfo.getPeers()).toHaveLength(0)
            expect(await l0b.shardPeerInfo.getPeers()).toHaveLength(0)

            expect(l0a.trust.rootTrust.equals(l0b.trust.rootTrust));            // Replication step
            await peer.subscribeForReplication(l0a.trust);
            await delay(5000); // Pubsub is flaky, wait some time before requesting shard
            await l0b.requestReplicate();
            //  --------------
            expect(await l0b.shardPeerInfo.getPeers()).toHaveLength(1)
            // add some delay because replication might take some time and is not synchronous
            await disconnectPeers([peer, peer2]);


        })

    })

    describe('peer', () => {
        test('isServer=false no subscriptions on idle on shardStoreShard', async () => {
            let peerNonServer = await getPeer(undefined, false);
            let l0 = await shardStoreShard();
            await l0.init(peerNonServer);
            const subscriptions = await peerNonServer.node.pubsub.ls();
            expect(subscriptions.length).toEqual(0);  // non server should not have any subscriptions idle
            await disconnectPeers([peerNonServer]);
        })

        test('isServer=false no subscriptions on idle on documentStoreShard', async () => {

            class Document { }
            let peerNonServer = await getPeer(undefined, false);
            peerNonServer.options.behaviours.typeMap[Document.name] = Document;
            let l0 = await documentStoreShard(Document, 'id');
            await l0.init(peerNonServer);
            const subscriptions = await peerNonServer.node.pubsub.ls();
            expect(subscriptions.length).toEqual(0); // non server should not have any subscriptions idle
            await disconnectPeers([peerNonServer]);
        })


        test('isServer=false can write', async () => {

            let peerServer = await getPeer(undefined, true);
            let peerNonServer = await getPeer(undefined, false);
            await connectPeers(peerServer, peerNonServer)

            peerServer.options.behaviours.typeMap[Document.name] = Document;
            let l0 = await documentStoreShard(Document, 'id');
            await l0.init(peerServer);
            await l0.replicate();


            peerNonServer.options.behaviours.typeMap[Document.name] = Document;
            let l0Write = await Shard.loadFromCID<DocumentStoreInterface<Document>>(l0.cid, peerNonServer.node);
            await l0Write.init(peerNonServer);

            //await peerNonServer.orbitDB["_pubsub"].subscribe(l0Write.interface.db.address.toString(), peerNonServer.orbitDB["_onMessage"].bind(peerNonServer.orbitDB), peerNonServer.orbitDB["_onPeerConnected"].bind(peerNonServer.orbitDB))
            let subscriptions = await peerNonServer.node.pubsub.ls();
            expect(subscriptions.length).toEqual(0); // non server should not have any subscriptions idle
            await delay(3000)
            await l0Write.interface.db.load();
            await l0Write.interface.db.write((x) => l0Write.interface.db.db.put(x), new Document({
                id: 'hello'
            }))

            await l0.interface.db.load();
            await waitFor(() => l0.interface.db.db.size > 0);
            subscriptions = await peerNonServer.node.pubsub.ls();
            expect(subscriptions.length).toEqual(0);  // non server should not have any subscriptions after write
            await disconnectPeers([peerServer, peerNonServer]);
        })

        test('query subscription are combined', async () => {

            let [peer] = await getConnectedPeers(2)

            // Create Root shard

            let l0 = await shardStoreShard();
            await l0.init(peer)
            await l0.replicate();
            // Create 2 feed stores
            let feedStore1 = await (await documentStoreShard(Document)).init(peer, l0.cid); // <-- This should trigger a swarm connection from peer to peer2
            await feedStore1.replicate();
            let feedStore2 = await (await documentStoreShard(Document)).init(peer, l0.cid); // <-- This should trigger a swarm connection from peer to peer2
            await feedStore2.replicate();
            const subscriptions = await peer.node.pubsub.ls();
            expect(subscriptions.filter(x => x.endsWith("/query"))).toHaveLength(1);
            disconnectPeers([peer]);
        })
    })

    // TODO: Autosharding on new data
    // TODO: Sharding if overflow

});
