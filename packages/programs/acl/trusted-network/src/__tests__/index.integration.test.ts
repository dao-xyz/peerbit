import { Session, Peer, waitForPeers } from '@dao-xyz/peerbit-test-utils'
import { AnyRelation, createIdentityGraphStore, getFromByTo, getPathGenerator, getToByFrom, TrustedNetwork, KEY_OFFSET, PUBLIC_KEY_WIDTH, Relation } from '..';
import { waitFor } from '@dao-xyz/peerbit-time';
import { AccessError, Ed25519Keypair } from "@dao-xyz/peerbit-crypto";
import { DocumentQueryRequest, Results, ResultWithSource } from '@dao-xyz/peerbit-dsearch';
import { Secp256k1PublicKey } from '@dao-xyz/peerbit-crypto';
import { Wallet } from '@ethersproject/wallet'
import { Identity } from '@dao-xyz/ipfs-log';
import { createStore } from '@dao-xyz/peerbit-test-utils';
import { Level } from 'level';
import { fileURLToPath } from 'url';
import path, { dirname } from 'path';
import { CachedValue, DefaultOptions, IStoreOptions } from '@dao-xyz/peerbit-store';
import Cache from '@dao-xyz/peerbit-cache';
import { field, serialize, variant } from '@dao-xyz/borsh';
import { Program, RootProgram } from '@dao-xyz/peerbit-program';
import { DDocs } from '@dao-xyz/peerbit-ddoc';
const __filename = fileURLToPath(import.meta.url);

const createIdentity = async () => {
    const ed = await Ed25519Keypair.create();
    return {
        publicKey: ed.publicKey,
        sign: (data) => ed.sign(data)
    } as Identity
}


@variant([0, 242])
class IdentityGraph extends Program implements RootProgram {

    @field({ type: DDocs })
    store: DDocs<Relation>

    constructor(properties?: { store: DDocs<Relation> }) {
        super();
        if (properties) {
            this.store = properties.store;
        }
    }
    async setup(): Promise<void> {
        await this.store.setup({ type: Relation })
    }

}
describe('index', () => {
    let session: Session, identites: Identity[], cacheStore: Level[]

    const identity = (i: number) => identites[i];
    const init = (store: Program, i: number, options: { store?: IStoreOptions<any> } = {}) => store.init && store.init(session.peers[i].ipfs, identites[i], { ...options, store: { ...DefaultOptions, replicate: true, resolveCache: async () => new Cache<CachedValue>(cacheStore[i]), ...options.store } })
    beforeAll(async () => {
        session = await Session.connected(4);
        identites = [];
        cacheStore = [];
        for (let i = 0; i < session.peers.length; i++) {
            identites.push(await createIdentity());
            cacheStore.push(await createStore(path.join(__filename, '/cache/', i.toString())))
        }

    })

    afterAll(async () => {
        await session.stop();
        await Promise.all(cacheStore?.map((c) => c.close()));
    })
    describe('identity-graph', () => {

        it('serializes relation with right padding ed25519', async () => {
            const from = (await Ed25519Keypair.create()).publicKey;
            const to = (await Ed25519Keypair.create()).publicKey;
            const relation = new AnyRelation({ from, to })
            const serRelation = serialize(relation);
            const serFrom = serialize(from);
            const serTo = serialize(to);

            expect(serRelation.slice(KEY_OFFSET, KEY_OFFSET + serFrom.length)).toEqual(serFrom) // From key has a fixed offset from 0
            expect(serRelation.slice(KEY_OFFSET + PUBLIC_KEY_WIDTH)).toEqual(serTo) // To key has a fixed offset from 0
        })

        it('serializes relation with right padding sepc256k1', async () => {
            const from = new Secp256k1PublicKey({
                address: await Wallet.createRandom().getAddress()
            })
            const to = (await Ed25519Keypair.create()).publicKey;
            const relation = new AnyRelation({ from, to })
            const serRelation = serialize(relation);
            const serFrom = serialize(from);
            const serTo = serialize(to);

            expect(serRelation.slice(KEY_OFFSET, KEY_OFFSET + serFrom.length)).toEqual(serFrom) // From key has a fixed offset from 0
            expect(serRelation.slice(KEY_OFFSET + PUBLIC_KEY_WIDTH)).toEqual(serTo) // To key has a fixed offset from 0
        })



        it('path', async () => {

            const a = (await Ed25519Keypair.create()).publicKey;
            const b = new Secp256k1PublicKey({
                address: await Wallet.createRandom().getAddress()
            })
            const c = (await Ed25519Keypair.create()).publicKey;

            const store = new IdentityGraph({
                store: createIdentityGraphStore({ name: session.peers[0].id.toString() })
            })
            await init(store, 0);

            const ab = new AnyRelation({
                to: b,
                from: a
            });
            const bc = new AnyRelation({
                to: c,
                from: b
            })
            await store.store.put(ab);
            await store.store.put(bc);

            // Get relations one by one
            const trustingC = await getFromByTo.resolve(c, store.store);
            expect(trustingC).toHaveLength(1);
            expect(((trustingC[0] as ResultWithSource).source as AnyRelation).id).toEqual(bc.id);

            const bIsTrusting = await getToByFrom.resolve(b, store.store);
            expect(bIsTrusting).toHaveLength(1);
            expect(((bIsTrusting[0] as ResultWithSource).source as AnyRelation).id).toEqual(bc.id);


            const trustingB = await getFromByTo.resolve(b, store.store);
            expect(trustingB).toHaveLength(1);
            expect(((trustingB[0] as ResultWithSource).source as AnyRelation).id).toEqual(ab.id);

            const aIsTrusting = await getToByFrom.resolve(a, store.store);
            expect(aIsTrusting).toHaveLength(1);
            expect(((aIsTrusting[0] as ResultWithSource).source as AnyRelation).id).toEqual(ab.id);

            // Test generator
            const relationsFromGeneratorFromByTo = [];
            for await (const relation of getPathGenerator(c, store.store, getFromByTo)) {
                relationsFromGeneratorFromByTo.push(relation);
            }
            expect(relationsFromGeneratorFromByTo).toHaveLength(2);
            expect(relationsFromGeneratorFromByTo[0].id).toEqual(bc.id);
            expect(relationsFromGeneratorFromByTo[1].id).toEqual(ab.id);


            const relationsFromGeneratorToByFrom = [];
            for await (const relation of getPathGenerator(a, store.store, getToByFrom)) {
                relationsFromGeneratorToByFrom.push(relation);
            }
            expect(relationsFromGeneratorToByFrom).toHaveLength(2);
            expect(relationsFromGeneratorToByFrom[0].id).toEqual(ab.id);
            expect(relationsFromGeneratorToByFrom[1].id).toEqual(bc.id);

        })

        it('can revoke', async () => {

            const a = (await Ed25519Keypair.create()).publicKey;
            const b = new Secp256k1PublicKey({
                address: await Wallet.createRandom().getAddress()
            })

            const store = new IdentityGraph({
                store: createIdentityGraphStore({ name: session.peers[0].id.toString() })
            })
            await init(store, 0);

            const ab = new AnyRelation({
                to: b,
                from: a
            });

            await store.store.put(ab);

            let trustingB = await getFromByTo.resolve(b, store.store);
            expect(trustingB).toHaveLength(1);
            expect(((trustingB[0] as ResultWithSource).source as AnyRelation).id).toEqual(ab.id);

            await store.store.del(ab.id);
            trustingB = await getFromByTo.resolve(b, store.store);
            expect(trustingB).toHaveLength(0);
        })
    })


    describe('TrustedNetwork', () => {

        it('trusted by chain', async () => {

            const l0a = new TrustedNetwork({
                rootTrust: identity(0).publicKey
            });

            await init(l0a, 0);

            await l0a.add(identity(1).publicKey);

            let l0b: TrustedNetwork = await TrustedNetwork.load(session.peers[1].ipfs, l0a.address) as any
            await init(l0b, 1);

            await l0b.trustGraph.store.sync(l0a.trustGraph.store.oplog.heads);

            await waitFor(() => Object.keys(l0b.trustGraph._index._index).length == 1)

            await l0b.add(identity(2).publicKey); // Will only work if peer2 is trusted

            await l0a.trustGraph.store.sync(l0b.trustGraph.store.oplog.heads);

            await waitFor(() => Object.keys(l0b.trustGraph._index._index).length == 2)
            await waitFor(() => Object.keys(l0a.trustGraph._index._index).length == 2)

            await waitForPeers(session.peers[2].ipfs, [session.peers[0].id, session.peers[1].id], l0b.trustGraph.search._query.queryTopic)
            // Try query with trusted
            let responses: Results[] = [];
            await l0b.trustGraph.search.query(new DocumentQueryRequest({
                queries: []
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
            await l0b.trustGraph.search.query(new DocumentQueryRequest({
                queries: []
            }), (response) => {
                untrustedResponse = response
            },
                {
                    signer: identity(3),
                    maxAggregationTime: 3000
                })

            expect(untrustedResponse).toBeUndefined();

            // now check if peer3 is trusted from peer perspective
            expect(await l0a.isTrusted(identity(2).publicKey)).toBeTrue();

            // check if peer3 is trusted from a peer that is not rpelicating
            let l0observer: TrustedNetwork = await TrustedNetwork.load(session.peers[1].ipfs, l0a.address) as any
            await init(l0observer, 1, { store: { replicate: false } });
            expect(await l0observer.isTrusted(identity(2).publicKey)).toBeTrue();
            expect(await l0observer.isTrusted(identity(3).publicKey)).toBeFalse();


            const trusted = await l0a.getTrusted();
            expect(trusted.map(k => k.bytes)).toContainAllValues([identity(0).publicKey.bytes, identity(1).publicKey.bytes, identity(2).publicKey.bytes])


        })

        it('has relation', async () => {

            const l0a = new TrustedNetwork({
                rootTrust: identity(0).publicKey
            });

            await init(l0a, 0);

            await l0a.add(identity(1).publicKey);
            expect(l0a.hasRelation(identity(0).publicKey, identity(1).publicKey)).toBeFalse()
            expect(l0a.hasRelation(identity(1).publicKey, identity(0).publicKey)).toBeTrue()

        })

        it('can not append with wrong truster', async () => {


            let l0a = new TrustedNetwork({
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


            let l0a = new TrustedNetwork({
                rootTrust: identity(0).publicKey
            });

            await init(l0a, 0);

            let l0b: TrustedNetwork = await TrustedNetwork.load(session.peers[1].ipfs, l0a.address) as any
            await init(l0b, 1);


            // Can not append peer3Key since its not trusted by the root
            await expect(l0b.add(identity(2).publicKey)).rejects.toBeInstanceOf(AccessError);

        })
    })
})
