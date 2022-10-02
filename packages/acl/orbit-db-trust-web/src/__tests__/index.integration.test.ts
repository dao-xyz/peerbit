import { getConnectedPeers, disconnectPeers } from '@dao-xyz/peer-test-utils'
import { AllowAllAccessController, AnyRelation, createIdentityGraphStore, getFromByTo, getFromByToGenerator, RegionAccessController } from '..';
import { waitFor } from '@dao-xyz/time';
import { AccessError } from "@dao-xyz/peerbit-crypto";
import { DocumentQueryRequest, QueryRequestV0, QueryResponseV0, ResultWithSource } from '@dao-xyz/query-protocol';
import { query } from '@dao-xyz/orbit-db-query-store';
import { Ed25519PublicKey, Secp256k1PublicKeyData } from '@dao-xyz/identity';
import { SodiumPlus } from 'sodium-plus';
import { Wallet } from '@ethersproject/wallet'




describe('identity-graph', () => {

    it('getFromByTo', async () => {

        let [peer] = await getConnectedPeers(1);
        const crypto = await SodiumPlus.auto();
        const a = new Ed25519PublicKey({
            publicKey: await crypto.crypto_sign_publickey((await crypto.crypto_sign_keypair()))
        })

        const b = new Secp256k1PublicKeyData({
            address: await Wallet.createRandom().getAddress()
        })

        const c = new Ed25519PublicKey({
            publicKey: await crypto.crypto_sign_publickey((await crypto.crypto_sign_keypair()))
        })

        const store = createIdentityGraphStore({ name: peer.id, accessController: new AllowAllAccessController() })
        await peer.orbitDB.open(store);

        const ab = new AnyRelation({
            to: b,
            from: a
        });
        const bc = new AnyRelation({
            to: c,
            from: b
        })
        await store.put(ab);
        await store.put(bc);

        // Get relations one by one

        const trustingC = await getFromByTo(c, store);
        expect(trustingC).toHaveLength(1);
        expect(((trustingC[0] as ResultWithSource).source as AnyRelation).id).toEqual(bc.id);


        const trustingB = await getFromByTo(b, store);
        expect(trustingB).toHaveLength(1);
        expect(((trustingB[0] as ResultWithSource).source as AnyRelation).id).toEqual(ab.id);

        // Compare with generator
        const relationsFromGenerator = [];
        for await (const relation of getFromByToGenerator(c, store)) {
            relationsFromGenerator.push(relation);
        }
        expect(relationsFromGenerator).toHaveLength(2);
        expect(relationsFromGenerator[0].id).toEqual(bc.id);
        expect(relationsFromGenerator[1].id).toEqual(ab.id);


        await disconnectPeers([peer]);

    })

    // TODO add revoke test



})


describe('RegionAccessController', () => {

    it('trusted by chain', async () => {

        let [peer, peer2, peer3, peer4] = await getConnectedPeers(4);
        const l0a = new RegionAccessController({
            rootTrust: peer.orbitDB.identity,
        });

        await peer.orbitDB.open(l0a);

        let peer2Key = peer2.orbitDB.identity;
        await l0a.addTrust(peer2Key);

        let l0b: RegionAccessController = await RegionAccessController.load(peer2.node, l0a.address) as any
        await peer2.orbitDB.open(l0b);

        await waitFor(() => Object.keys(l0b.trustGraph._index._index).length == 1)

        let peer3Key = peer3.orbitDB.identity;
        await l0b.addTrust(peer3Key); // Will only work if peer2 is trusted

        await waitFor(() => Object.keys(l0b.trustGraph._index._index).length == 2)
        await waitFor(() => Object.keys(l0a.trustGraph._index._index).length == 2)

        // Try query with trusted
        let responses: QueryResponseV0[] = [];
        await query(peer3.node.pubsub, l0b.trustGraph.queryTopic, new QueryRequestV0({
            type: new DocumentQueryRequest({
                queries: []
            })
        }), (response) => {
            responses.push(response);
        },
            {
                signer: async (bytes) => {
                    return {
                        publicKey: peer3.orbitDB.publicKey,
                        signature: await peer3.orbitDB.sign(bytes)
                    }
                },
                maxAggregationTime: 3000,
                waitForAmount: 2 // response from peer and peer2
            })
        expect(responses).toHaveLength(2);

        // Try query with untrusted
        let untrustedResponse = undefined;
        await query(peer4.node.pubsub, l0b.trustGraph.queryTopic, new QueryRequestV0({
            type: new DocumentQueryRequest({
                queries: []
            })
        }), (response) => {
            untrustedResponse = response
        },
            {
                signer: async (bytes) => {
                    return {
                        publicKey: peer4.orbitDB.publicKey,
                        signature: await peer4.orbitDB.sign(bytes)
                    }
                },
                maxAggregationTime: 3000
            })

        expect(untrustedResponse).toBeUndefined();

        // now check if peer3 is trusted from peer perspective
        expect(await l0a.isTrusted(peer3Key));
        await disconnectPeers([peer, peer2, peer3, peer4]);

    })

    it('can not append with wrong truster', async () => {

        let [peer] = await getConnectedPeers(3);

        let l0a = new RegionAccessController({
            rootTrust: peer.orbitDB.identity
        });
        await peer.orbitDB.open(l0a);

        expect(l0a.trustGraph.put(new AnyRelation({
            to: new Secp256k1PublicKeyData({
                address: await Wallet.createRandom().getAddress()
            })
            ,
            from: new Secp256k1PublicKeyData({
                address: await Wallet.createRandom().getAddress()
            })
        }))).rejects.toBeInstanceOf(AccessError);
        await disconnectPeers([peer]);

    })


    it('untrusteed by chain', async () => {

        let [peer, peer2, peer3] = await getConnectedPeers(3);

        let l0a = new RegionAccessController({
            rootTrust: peer.orbitDB.identity
        });

        await peer.orbitDB.open(l0a);

        let l0b: RegionAccessController = await RegionAccessController.load(peer2.node, l0a.address) as any
        await peer2.orbitDB.open(l0b);

        let peer3Key = peer3.orbitDB.identity

        // Can not append peer3Key since its not trusted by the root
        await expect(l0b.addTrust(peer3Key)).rejects.toBeInstanceOf(AccessError);
        await disconnectPeers([peer, peer2, peer3]);

    })
}) 