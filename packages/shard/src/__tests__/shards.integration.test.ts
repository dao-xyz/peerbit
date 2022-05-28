
import { RecursiveShard, Shard, AnyPeer } from '../index';
import Identities from 'orbit-db-identity-provider';
import { Keypair } from '@solana/web3.js';
import { SolanaIdentityProvider } from '../identity-providers/solana-identity-provider';
import FeedStore from 'orbit-db-feedstore';
import { FeedStoreOptions } from '../stores';
import { BN } from 'bn.js';
import { clean, createIPFSNode, dummyShard, getPeer } from './utils';
import { generateUUID } from '../id';
import { createOrbitDBInstance, ServerOptions } from '../node';
import { P2PTrust } from '../trust';
import { PublicKey } from '../signer';
import { delay } from '../utils';






const getPeersSameIdentity = async (amount: number = 1, peerCapacity: number): Promise<AnyPeer[]> => {
    let ids = Array(amount).fill(0).map((_) => generateUUID());
    for (const id in ids) {
        await clean(id);
    }

    Identities.addIdentityProvider(SolanaIdentityProvider)
    let keypair = Keypair.generate();
    const rootIdentity = await Identities.createIdentity({ type: 'solana', wallet: keypair.publicKey, keypair: keypair })
    const peers = await Promise.all(ids.map(async (id) => {

        let node = await createIPFSNode(false, id);
        let orbitDB = await createOrbitDBInstance(node, id, rootIdentity);
        const peer = new AnyPeer(id);
        let options = new ServerOptions({
            behaviours: {
                typeMap: {}
            },
            id,
            replicationCapacity: peerCapacity
        });

        await peer.create({ rootAddress: 'root', options, orbitDB });
        return peer;
    }));

    return peers;
}



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


const disconnectPeers = async (peers: AnyPeer[]): Promise<void> => {
    await Promise.all(peers.map(peer => peer.disconnect()));
}


describe('cluster', () => {
    describe('trust', () => {
        test('add trustee', async () => {

            let peer = await getPeer();
            let l0 = await dummyShard();
            await l0.init(peer);
            expect(l0.cid).toBeDefined();
            expect(l0.trust).toBeInstanceOf(P2PTrust);
            expect((l0.trust as P2PTrust).rootTrust).toBeDefined();
            expect((l0.trust as P2PTrust).rootTrust).toEqual(PublicKey.from(peer.orbitDB.identity))


            let peer2 = await getPeer();
            let newTrustee = PublicKey.from(peer2.orbitDB.identity);
            await l0.trust.addTrust(newTrustee);

            await l0.init(peer2);
            let trust = await l0.trust.loadTrust(1);
            expect(Object.values(trust.all)).toHaveLength(1);

        })

        describe('isTrusted', () => {

            test('trusted by chain', async () => {

                let peer = await getPeer();

                let l0 = await dummyShard();
                await l0.init(peer);

                let peer2 = await getPeer();
                let peer2Key = PublicKey.from(peer2.orbitDB.identity);
                await l0.trust.addTrust(peer2Key);

                await l0.init(peer2);
                let peer3 = await getPeer();
                let peer3Key = PublicKey.from(peer3.orbitDB.identity);
                await l0.trust.addTrust(peer3Key);

                // now check if peer3 is trusted from peer perspective
                await l0.init(peer);
                await l0.trust.loadTrust(2);
                expect(l0.trust.isTrusted(peer3Key));
            })

            test('untrusteed by chain ', async () => {

                let peer = await getPeer();

                let l0 = await dummyShard();
                await l0.init(peer);

                let peer2 = await getPeer();
                await l0.init(peer2);
                let peer3 = await getPeer();
                let peer3Key = PublicKey.from(peer3.orbitDB.identity);
                await l0.trust.addTrust(peer3Key);

                // now check if peer3 is trusted from peer perspective
                //  await l0.init(peer);
                await l0.trust.loadTrust(1);
                expect(l0.trust.rootTrust.address).toEqual(peer.orbitDB.identity.publicKey);
                expect(l0.trust.isTrusted(peer3Key)).toBeFalsy();
            })
        })



    })
    describe('manifest', () => {
        test('save load', async () => {

            let peer = await getPeer();
            let l0 = new RecursiveShard<FeedStore<string>>({
                cluster: 'x',
                shardSize: new BN(500)
            })
            await l0.init(peer);
            expect(l0.cid).toBeDefined();

            let peer2 = await getPeer();
            let loadedShard = await Shard.loadFromCID(l0.cid, peer2.node);
            expect(loadedShard.address).toEqual(l0.address);
        })
    })

    describe('recursive shard', () => {
        test('peer backward connect', async () => {

            let peer = await getPeer();

            // Create Root shard
            let l0 = new RecursiveShard<FeedStore<string>>({
                cluster: 'x',
                shardSize: new BN(500)
            })
            await l0.init(peer)
            await l0.replicate();

            // Create Feed store
            let peer2 = await getPeer();

            expect(await isInSwarm(peer, peer2)).toBeFalsy();

            let feedStore = await new Shard<FeedStore<string>>({
                cluster: 'xx',
                shardSize: new BN(500),
                storeOptions: new FeedStoreOptions()
            }).init(peer2, l0); // <-- This should trigger a swarm connection from peer to peer2
            await feedStore.replicate();

            expect(await isInSwarm(peer, peer2)).toBeTruthy();
        })
    })



    describe('resiliance', () => {

        test('connect to remote', async () => {

            let peer = await getPeer();

            // Create Root shard
            let l0 = new RecursiveShard<FeedStore<string>>({
                cluster: 'x',
                shardSize: new BN(500)
            })
            await l0.init(peer)
            await l0.replicate();

            // Create Feed store
            let feedStore = await new Shard<FeedStore<string>>({
                cluster: 'xx',
                shardSize: new BN(500),
                storeOptions: new FeedStoreOptions()
            }).init(peer, l0);
            await feedStore.replicate();
            await feedStore.blocks.add("hello");


            // --- Load assert, from another peer
            let peer2 = await getPeer();
            await l0.init(peer2)
            await l0.loadBlocks(1);
            expect(Object.keys(l0.blocks._index._index).length).toEqual(1);
            let feedStoreLoaded = await l0.loadShard(0, { expectedBlockReplicationEvents: 1 })
            expect(Object.keys(feedStoreLoaded.blocks._index._index).length).toEqual(1);
            await disconnectPeers([peer, peer2]);

        })

        test('first peer drop, data still alive because 2nd peer is up', async () => {
            let peer = await getPeer();

            // Create Root shard
            let l0 = new RecursiveShard<FeedStore<string>>({
                cluster: 'x',
                shardSize: new BN(500 * 1000)
            })
            await l0.init(peer)
            await l0.replicate();

            // Create Feed store
            await new Shard<FeedStore<string>>({
                cluster: 'xx',
                shardSize: new BN(500 * 1000),
                storeOptions: new FeedStoreOptions()
            }).init(peer, l0);

            // --- Load assert, from another peer
            let peer2 = await getPeer();
            await l0.init(peer2)
            await l0.loadBlocks(1);
            await l0.replicate();
            expect(Object.keys(l0.blocks._index._index).length).toEqual(1);


            // Drop 1 peer and make sure a third peer can access data
            await disconnectPeers([peer]);

            let peer3 = await getPeer();
            await l0.init(peer3)
            await l0.loadBlocks(1);
            expect(Object.keys(l0.blocks._index._index).length).toEqual(1);
            await disconnectPeers([peer2, peer3]);
        })
    })


    describe('presharding', () => {

        test('nested block store', async () => {


            let peers = await getPeersSameIdentity(2, 100000000);
            let peer = peers[0];
            let peer2 = peers[1];

            // Create Root shard
            let l0 = new RecursiveShard<FeedStore<string>>({
                cluster: 'x',
                shardSize: new BN(500 * 1000)
            })
            await l0.init(peer)
            await l0.replicate();

            // Create Feed store
            let feedStore = await new Shard<FeedStore<string>>({
                cluster: 'xx',
                shardSize: new BN(500 * 1000),
                storeOptions: new FeedStoreOptions()
            }).init(peer, l0);
            await feedStore.replicate();
            await (await l0.loadShard(0)).blocks.add("xxx");



            // --- Load assert
            let l0b = new RecursiveShard<FeedStore<string>>({
                id: l0.id,
                cluster: 'x',
                shardSize: new BN(500 * 1000)
            })
            await l0b.init(peer2)
            await l0b.loadBlocks(1);
            expect(Object.keys(l0.blocks._index._index).length).toEqual(1);
            let feedStoreLoaded = await l0b.loadShard(0, { expectedBlockReplicationEvents: 1 })
            expect(Object.keys(feedStoreLoaded.blocks._index._index).length).toEqual(1);
            await disconnectPeers(peers);
        });
    })

    describe('ondemand-sharding', () => {
        test('subscribe, request', async () => {
            let peer = await getPeer();
            await peer.subscribeForReplication();

            let peer2 = await getPeer();
            let l0 = new RecursiveShard<FeedStore<string>>({
                cluster: 'x',
                shardSize: new BN(500 * 1000)
            })

            await l0.init(peer2)
            await l0.requestReplicate();
        })
    })
    // TODO: Autosharding on new data
    // TODO: Sharding if overflow

});
