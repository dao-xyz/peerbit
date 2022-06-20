
import { Shard } from '../shard';
import { AnyPeer } from '../node';
import { BinaryFeedStoreInterface, disconnectPeers, DocumentStoreInterface, documentStoreShard, getPeer, shardStoreShard } from './utils';
import { P2PTrust } from '../trust';
import { PublicKey } from '../key';
import { delay, waitFor } from '../utils';
import { Document } from './utils';
import { Peer } from '../peer';
import BN from 'bn.js';

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

            let peer = await getPeer();
            let l0 = await documentStoreShard(Document);
            await l0.init(peer);
            expect(l0.cid).toBeDefined();
            expect(l0.trust).toBeInstanceOf(P2PTrust);
            expect((l0.trust as P2PTrust).rootTrust).toBeDefined();
            expect((l0.trust as P2PTrust).rootTrust).toEqual(PublicKey.from(peer.orbitDB.identity))


            let peer2 = await getPeer();
            let newTrustee = PublicKey.from(peer2.orbitDB.identity);
            await l0.trust.addTrust(newTrustee);

            await l0.init(peer2);
            await l0.trust.load(1);
            expect(l0.trust.db.db.size).toEqual(1)

        })

        describe('isTrusted', () => {

            test('trusted by chain', async () => {

                let peer = await getPeer();

                let l0 = await documentStoreShard(Document);
                await l0.init(peer);

                let peer2 = await getPeer();
                let peer2Key = PublicKey.from(peer2.orbitDB.identity);
                await l0.trust.load();
                await l0.trust.addTrust(peer2Key);

                await l0.init(peer2);
                let peer3 = await getPeer();
                let peer3Key = PublicKey.from(peer3.orbitDB.identity);
                await l0.trust.load();
                await l0.trust.addTrust(peer3Key);

                // now check if peer3 is trusted from peer perspective
                await l0.init(peer);
                await l0.trust.load(2);
                expect(l0.trust.isTrusted(peer3Key));
            })

            test('untrusteed by chain', async () => {

                let peer = await getPeer();

                let l0 = await documentStoreShard(Document);
                await l0.init(peer);

                let peer2 = await getPeer();
                await l0.init(peer2);
                let peer3 = await getPeer();
                let peer3Key = PublicKey.from(peer3.orbitDB.identity);
                await l0.trust.addTrust(peer3Key);

                // now check if peer3 is trusted from peer perspective
                //  await l0.init(peer);
                await l0.trust.load(1);
                expect(l0.trust.rootTrust.address).toEqual(peer.orbitDB.identity.publicKey);
                expect(l0.trust.isTrusted(peer3Key)).toBeFalsy();
            })
        })



    })
    describe('manifest', () => {
        test('save load', async () => {

            let peer = await getPeer();
            let l0 = await documentStoreShard(Document);
            await l0.init(peer);
            expect(l0.cid).toBeDefined();

            let peer2 = await getPeer();
            let loadedShard = await Shard.loadFromCID<BinaryFeedStoreInterface>(l0.cid, peer2.node);
            expect(loadedShard.interface.db.address).toEqual(l0.interface.db.address);
        })
    })

    describe('recursive shard', () => {
        test('peer backward connect', async () => {

            let peer = await getPeer();

            // Create Root shard
            let l0 = await shardStoreShard();
            await l0.init(peer)
            await l0.replicate();
            const xyz = (l0.peers.address);
            // Create Feed store
            let peer2 = await getPeer();

            expect(await isInSwarm(peer, peer2)).toBeFalsy();

            let feedStore = await (await documentStoreShard(Document)).init(peer2, l0.cid); // <-- This should trigger a swarm connection from peer to peer2
            await feedStore.replicate();
            expect(feedStore.interface.db.address.endsWith(l0.cid + '-documents'));
            expect(await isInSwarm(peer, peer2)).toBeTruthy();
        })
    })

    describe('resiliance', () => {
        test('connect to remote', async () => {

            let peer = await getPeer();

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
            let peer2 = await getPeer();
            await l0.init(peer2)
            await l0.interface.db.load(1);
            expect(Object.keys(l0.interface.db.db.index._index).length).toEqual(1);
            let feedStoreLoaded = await l0.interface.loadShard(documentStore.cid);
            await feedStoreLoaded.interface.db.load(1);
            expect(Object.keys(feedStoreLoaded.interface.db.db.index._index).length).toEqual(1);
            await disconnectPeers([peer, peer2]);

        })

        test('first peer drop, data still alive because 2nd peer is up', async () => {
            let peer = await getPeer();

            // Create Root shard
            let l0 = await shardStoreShard();
            await l0.init(peer)
            await l0.replicate();

            // Create Feed store
            let l1 = await (await documentStoreShard(Document)).init(peer, l0.cid);
            await l0.interface.db.load();
            await l0.interface.db.db.put(l1);

            // --- Load assert, from another peer
            let peer2 = await getPeer();
            await l0.init(peer2)
            await l0.interface.db.load(1);
            await l0.replicate();
            expect(Object.keys(l0.interface.db.db.index._index).length).toEqual(1);


            // Drop 1 peer and make sure a third peer can access data
            await disconnectPeers([peer]);

            let peer3 = await getPeer();
            await l0.init(peer3)
            await l0.interface.db.load(1);
            expect(Object.keys(l0.interface.db.db.index._index).length).toEqual(1);
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
        test('peer remote counter', async () => {
            let peer = await getPeer();
            let peer2 = await getPeer();

            let l0a = await shardStoreShard();
            await l0a.init(peer);
            let thisPeer = new Peer({
                key: PublicKey.from(peer.orbitDB.identity),
                addresses: (await peer.node.id()).addresses.map(x => x.toString()),
                timestamp: new BN(+new Date)
            });
            await l0a.peers.load();
            await l0a.peers.db.put(thisPeer);
            await delay(10000);
            let l0b = await Shard.loadFromCID(l0a.cid, peer2.node);
            await l0b.init(peer2);
            expect(await l0b.getRemotePeersSize()).toEqual(1);
        })

        test('subscribe, request', async () => {
            let peer = await getPeer();
            let l0a = await shardStoreShard();
            await l0a.init(peer);

            let peer2 = await getPeer(undefined, false);

            await l0a.trust.addTrust(PublicKey.from(peer2.orbitDB.identity));
            await waitFor(() => l0a.trust.db.db.size == 1)// add some delay because trust db is not synchronous

            let l0b = await Shard.loadFromCID(l0a.cid, peer2.node);
            await l0b.init(peer2)

            expect(await l0a.getRemotePeersSize()).toEqual(0)
            expect(await l0b.getRemotePeersSize()).toEqual(0)

            expect(l0a.trust.rootTrust.equals(l0b.trust.rootTrust));            // Replication step
            await peer.subscribeForReplication(l0a.trust);
            await delay(1000); // Pubsub is flaky, wait some time before requesting shard
            await l0b.requestReplicate();
            //  --------------
            expect(await l0b.getRemotePeersSize()).toEqual(1)
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
            peerServer.options.behaviours.typeMap[Document.name] = Document;
            let l0 = await documentStoreShard(Document, 'id');
            await l0.init(peerServer);
            await l0.replicate();

            let peerNonServer = await getPeer(undefined, false);
            peerNonServer.options.behaviours.typeMap[Document.name] = Document;
            let l0Write = await Shard.loadFromCID<DocumentStoreInterface<Document>>(l0.cid, peerNonServer.node);
            await l0Write.init(peerNonServer);

            //await peerNonServer.orbitDB["_pubsub"].subscribe(l0Write.interface.db.address.toString(), peerNonServer.orbitDB["_onMessage"].bind(peerNonServer.orbitDB), peerNonServer.orbitDB["_onPeerConnected"].bind(peerNonServer.orbitDB))
            let subscriptions = await peerNonServer.node.pubsub.ls();
            expect(subscriptions.length).toEqual(0); // non server should not have any subscriptions idle
            await delay(10000)
            await l0Write.interface.db.load();
            await l0Write.interface.db.write((x) => l0Write.interface.db.db.put(x), new Document({
                id: 'hello'
            }))

            await l0.interface.db.load();
            await waitFor(() => l0.interface.db.db.size > 0);
            subscriptions = await peerNonServer.node.pubsub.ls();
            expect(subscriptions.length).toEqual(0);  // non server should not have any subscriptions after write
            await disconnectPeers([peerNonServer]);
        })
    })

    // TODO: Autosharding on new data
    // TODO: Sharding if overflow

});
