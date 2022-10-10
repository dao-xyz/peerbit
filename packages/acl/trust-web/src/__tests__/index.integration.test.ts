import { Session, Peer } from '@dao-xyz/orbit-db-test-utils'
import { AllowAllAccessController, AnyRelation, createIdentityGraphStore, getFromByTo, getFromByToGenerator, RegionAccessController } from '..';
import { waitFor } from '@dao-xyz/time';
import { AccessError, Ed25519Keypair } from "@dao-xyz/peerbit-crypto";
import { DocumentQueryRequest, QueryRequestV0, QueryResponseV0, ResultWithSource } from '@dao-xyz/query-protocol';
import { query } from '@dao-xyz/orbit-db-query-store';
import { Secp256k1PublicKey } from '@dao-xyz/peerbit-crypto';
import { Wallet } from '@ethersproject/wallet'
import { Identity } from '@dao-xyz/ipfs-log';
import { createStore } from '@dao-xyz/orbit-db-test-utils';
import { Level } from 'level';
import { fileURLToPath } from 'url';
import path from 'path';
import { jest } from '@jest/globals';
import { CachedValue, DefaultOptions, Store, StoreLike } from '@dao-xyz/orbit-db-store';
import Cache from '@dao-xyz/orbit-db-cache';
const __filename = fileURLToPath(import.meta.url);
const __filenameBase = path.parse(__filename).base;

const createIdentity = async () => {
    const ed = await Ed25519Keypair.create();
    return {
        publicKey: ed.publicKey,
        sign: (data) => ed.sign(data)
    } as Identity
}
describe('index', () => {
    let session: Session, identites: Identity[], cacheStore: Level[]

    const identity = (i: number) => identites[i];
    const init = (store: StoreLike<any>, i: number) => store.init && store.init(session.peers[i].ipfs, identites[i], { ...DefaultOptions, resolveCache: async () => new Cache<CachedValue>(cacheStore[i]) })
    beforeAll(async () => {
        session = await Session.connected(4);
        identites = [];
        cacheStore = [];
        for (let i = 0; i < session.peers.length; i++) {
            identites.push(await createIdentity());
            cacheStore.push(await createStore(__filenameBase + '/cache/' + i))
        }

    })

    afterAll(async () => {
        await session.stop();
        await Promise.all(cacheStore?.map((c) => c.close()));
    })
    describe('identity-graph', () => {


        it('getFromByTo', async () => {

            const a = (await Ed25519Keypair.create()).publicKey;
            const b = new Secp256k1PublicKey({
                address: await Wallet.createRandom().getAddress()
            })
            const c = (await Ed25519Keypair.create()).publicKey;

            const store = createIdentityGraphStore({ name: session.peers[0].id.toString(), accessController: new AllowAllAccessController() })
            await init(store, 0);

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

        })

        // TODO add revoke test



    })


    describe('RegionAccessController', () => {

        it('trusted by chain', async () => {

            const l0a = new RegionAccessController({
                rootTrust: identity(0).publicKey
            });

            await init(l0a, 0);

            await l0a.addTrust(identity(1).publicKey);

            let l0b: RegionAccessController = await RegionAccessController.load(session.peers[1].ipfs, l0a.address) as any
            await init(l0b, 1);

            await l0b.sync(l0a.oplog.heads);

            await waitFor(() => Object.keys(l0b.trustGraph._index._index).length == 1)

            await l0b.addTrust(identity(2).publicKey); // Will only work if peer2 is trusted

            await l0a.sync(l0b.oplog.heads);

            await waitFor(() => Object.keys(l0b.trustGraph._index._index).length == 2)
            await waitFor(() => Object.keys(l0a.trustGraph._index._index).length == 2)

            // Try query with trusted
            let responses: QueryResponseV0[] = [];
            await query(session.peers[2].ipfs, l0b.trustGraph.queryTopic, new QueryRequestV0({
                type: new DocumentQueryRequest({
                    queries: []
                })
            }), (response) => {
                responses.push(response);
            },
                {
                    signer: identity(2),
                    maxAggregationTime: 20000,
                    waitForAmount: 2 // response from peer and peer2
                })

            expect(responses).toHaveLength(2);

            // Try query with untrusted
            let untrustedResponse = undefined;
            await query(session.peers[3].ipfs, l0b.trustGraph.queryTopic, new QueryRequestV0({
                type: new DocumentQueryRequest({
                    queries: []
                })
            }), (response) => {
                untrustedResponse = response
            },
                {
                    signer: identity(3),
                    maxAggregationTime: 3000
                })

            expect(untrustedResponse).toBeUndefined();

            // now check if peer3 is trusted from peer perspective
            expect(await l0a.isTrusted(identity(2).publicKey));

        })

        it('can not append with wrong truster', async () => {


            let l0a = new RegionAccessController({
                rootTrust: identity(0).publicKey
            });
            await init(l0a, 0);

            expect(l0a.trustGraph.put(new AnyRelation({
                to: new Secp256k1PublicKey({
                    address: await Wallet.createRandom().getAddress()
                })
                ,
                from: new Secp256k1PublicKey({
                    address: await Wallet.createRandom().getAddress()
                })
            }))).rejects.toBeInstanceOf(AccessError);

        })


        it('untrusteed by chain', async () => {


            let l0a = new RegionAccessController({
                rootTrust: identity(0).publicKey
            });

            await init(l0a, 0);

            let l0b: RegionAccessController = await RegionAccessController.load(session.peers[1].ipfs, l0a.address) as any
            await init(l0b, 1);


            // Can not append peer3Key since its not trusted by the root
            await expect(l0b.addTrust(identity(2).publicKey)).rejects.toBeInstanceOf(AccessError);

        })
    })
})
