import { field, variant } from "@dao-xyz/borsh";
import { createStore, Session } from '@dao-xyz/peerbit-test-utils';
import { Access, AccessType } from "../access";
import { AnyAccessCondition, PublicKeyAccessCondition } from "../condition";
import { waitFor } from '@dao-xyz/peerbit-time';
import { PageQueryRequest, DSearch, FieldStringMatchQuery, Results } from "@dao-xyz/peerbit-anysearch";
import { AccessError, Ed25519Keypair, MaybeEncrypted, SignatureWithKey } from "@dao-xyz/peerbit-crypto";
import { CustomBinaryPayload } from "@dao-xyz/peerbit-bpayload";
import { DDocuments, DocumentIndex } from "@dao-xyz/peerbit-document";
import type { CanAppend, Identity, Payload } from "@dao-xyz/ipfs-log";
import { Level } from 'level';
import { CachedValue, DefaultOptions } from '@dao-xyz/peerbit-store';
import { fileURLToPath } from 'url';
import path from 'path';
import Cache from '@dao-xyz/peerbit-cache';
import { CanRead, DQuery } from "@dao-xyz/peerbit-query";
import { Program } from "@dao-xyz/peerbit-program";
import { DynamicAccessController } from "../acl-db";
const __filename = fileURLToPath(import.meta.url);
const __filenameBase = path.parse(__filename).base;

@variant("document")
class Document extends CustomBinaryPayload {

    @field({ type: 'string' })
    id: string;

    constructor(props?: { id: string }) {
        super();
        if (props) {
            this.id = props.id;
        }
    }
}

const createIdentity = async () => {
    const ed = await Ed25519Keypair.create();
    return {
        publicKey: ed.publicKey,
        sign: (data) => ed.sign(data)
    } as Identity
}


@variant([0, 251])
class TestStore extends Program {

    @field({ type: DDocuments })
    store: DDocuments<Document>

    @field({ type: DynamicAccessController })
    accessController: DynamicAccessController

    constructor(properties: { name?: string, identity: Identity, accessControllerName?: string }) {
        super(properties)
        if (properties) {
            this.store = new DDocuments({
                name: 'test',
                index: new DocumentIndex({
                    indexBy: 'id',
                    search: new DSearch({
                        query: new DQuery({})
                    })
                })
            });
            this.accessController = new DynamicAccessController({
                name: properties.accessControllerName || 'test-acl',
                rootTrust: properties.identity?.publicKey
            })
        }
    }

    async setup() {
        await this.accessController.setup();
        await this.store.setup({ type: Document, canRead: this.accessController.canRead.bind(this.accessController), canAppend: (entry) => this.accessController.canAppend(entry) });
    }
}
describe('index', () => {

    let session: Session, identites: Identity[], cacheStore: Level[]

    const identity = (i: number) => identites[i];
    const init = <T extends Program>(store: T, i: number, options: { store: { replicate: boolean }, canRead?: CanRead, canAppend?: CanAppend<T> } = { store: { replicate: true } }) => (store.init && store.init(session.peers[i].ipfs, identites[i], { ...options, store: { ...DefaultOptions, ...options.store, resolveCache: async () => new Cache<CachedValue>(cacheStore[i]) } })) as Promise<T>

    beforeAll(async () => {
        session = await Session.connected(3);
        identites = [];
        cacheStore = [];
        for (let i = 0; i < session.peers.length; i++) {
            identites.push(await createIdentity());
            cacheStore.push(await createStore(path.join(__filename, 'cache', i.toString())))
        }

    })

    afterAll(async () => {
        await session.stop();
        await Promise.all(cacheStore?.map((c) => c.close()));
    })


    it('can write from trust web', async () => {

        const s = new TestStore({ identity: identity(0) });
        const l0a = await init(s, 0);

        await l0a.store.put(new Document({
            id: '1'
        }));

        const l0b = await init(await TestStore.load(session.peers[1].ipfs, l0a.address), 1) as TestStore;

        await expect(l0b.store.put(new Document({
            id: 'id'
        }))).rejects.toBeInstanceOf(AccessError); // Not trusted
        await l0a.accessController.trustedNetwork.add(identity(1).publicKey);

        await (l0b.accessController).trustedNetwork.trustGraph.store.sync((l0a.accessController).trustedNetwork.trustGraph.store.oplog.heads);

        await waitFor(() => l0b.accessController.trustedNetwork.trustGraph.store.oplog.length === 1);
        await waitFor(() => (l0b.accessController).trustedNetwork.trustGraph._index.size === 1);

        await l0b.store.put(new Document({
            id: '2'
        })) // Now trusted 

        await l0a.store.store.sync(l0b.store.store.oplog.heads);
        await l0b.store.store.sync(l0a.store.store.oplog.heads);

        await waitFor(() => l0a.store.index.size === 2);
        await waitFor(() => l0b.store.index.size === 2);

    })


    describe('conditions', () => {
        it('publickey', async () => {

            const l0a = await init(new TestStore({ identity: identity(0) }), 0);

            await l0a.store.put(new Document({
                id: '1'
            }));

            const l0b = await init(await TestStore.load(session.peers[1].ipfs, l0a.address), 1) as TestStore;

            await l0b.store.store.sync(l0a.store.store.oplog.heads);
            await waitFor(() => l0b.store.index.size === 1)
            await expect(l0b.store.put(new Document({
                id: 'id'
            }))).rejects.toBeInstanceOf(AccessError); // Not trusted


            await (l0a.accessController).access.put(new Access({
                accessCondition: new PublicKeyAccessCondition({
                    key: identity(1).publicKey
                }),
                accessTypes: [AccessType.Any]
            }));

            await (l0b.accessController).access.store.sync((l0a.accessController).access.store.oplog.heads);
            await waitFor(() => (l0b.accessController).access.index.size === 1);
            await l0b.store.put(new Document({
                id: '2'
            })) // Now trusted 


        })


        it('through trust chain', async () => {

            const l0a = await init(new TestStore({ identity: identity(0) }), 0);

            await l0a.store.put(new Document({
                id: '1'
            }));

            const l0b = await init(await TestStore.load(session.peers[1].ipfs, l0a.address), 1) as TestStore;
            const l0c = await init(await TestStore.load(session.peers[2].ipfs, l0a.address), 2) as TestStore;

            await expect(l0c.store.put(new Document({
                id: 'id'
            }))).rejects.toBeInstanceOf(AccessError); // Not trusted


            await (l0a.accessController).access.put(new Access({
                accessCondition: new PublicKeyAccessCondition({
                    key: identity(1).publicKey
                }),
                accessTypes: [AccessType.Any]
            }));

            await (l0b.accessController).access.store.sync((l0a.accessController).access.store.oplog.heads);
            await (l0c.accessController).access.store.sync((l0a.accessController).access.store.oplog.heads);

            await expect(l0c.store.put(new Document({
                id: 'id'
            }))).rejects.toBeInstanceOf(AccessError); // Not trusted


            await waitFor(() => (l0b.accessController).access.index.size == 1)
            await (((l0b.accessController).identityGraphController.addRelation(identity(2).publicKey)));
            await (l0c.accessController).identityGraphController.relationGraph.store.sync((l0b.accessController).identityGraphController.relationGraph.store.oplog.heads);

            await waitFor(() => (l0c.accessController).identityGraphController.relationGraph.index.size === 1);
            await l0c.store.put(new Document({
                id: '2'
            })) // Now trusted 


        })



        it('any access', async () => {

            const l0a = await init(new TestStore({ identity: identity(0) }), 0);
            await l0a.store.put(new Document({
                id: '1'
            }));


            const l0b = await init(await TestStore.load(session.peers[1].ipfs, l0a.address), 1) as TestStore;
            await expect(l0b.store.put(new Document({
                id: 'id'
            }))).rejects.toBeInstanceOf(AccessError); // Not trusted


            const access = new Access({
                accessCondition: new AnyAccessCondition(),
                accessTypes: [AccessType.Any]
            });
            expect(access.id).toBeDefined();
            await (l0a.accessController).access.put(access);
            await (l0b.accessController).access.store.sync((l0a.accessController).access.store.oplog.heads);

            await waitFor(() => (l0b.accessController).access.index.size === 1);
            await l0b.store.put(new Document({
                id: '2'
            })) // Now trusted 


        })


        it('read access', async () => {


            const l0a = await init(new TestStore({ identity: identity(0) }), 0);

            await l0a.store.put(new Document({
                id: '1'
            }));


            const q = async (): Promise<Results> => {
                let results: Results = undefined as any;
                l0a.store.index.search.query(new PageQueryRequest({
                    queries: [new FieldStringMatchQuery({
                        key: 'id',
                        value: '1'
                    })]
                })
                    , (response) => {
                        results = response;
                    }, {
                    signer: identity(1),
                    maxAggregationTime: 3000
                })
                try {
                    await waitFor(() => !!results);
                } catch (error) {
                }
                return results;
            }

            expect(await q()).toBeUndefined(); // Because no read access

            await (l0a.accessController).access.put(new Access({
                accessCondition: new AnyAccessCondition(),
                accessTypes: [AccessType.Read]
            }).initialize());

            expect(await q()).toBeDefined(); // Because read access



        })
    })

    it('manifests are unique', async () => {

        const l0a = await init(new TestStore({ identity: identity(0) }), 0);
        const l0b = await init(new TestStore({ identity: identity(0) }), 0);
        expect(l0a.address).not.toEqual(l0b.address)
        expect((l0a.accessController).address.toString()).not.toEqual((l0b.accessController).address.toString())

    })

    it('can query', async () => {


        const l0a = await init(new TestStore({ identity: identity(0) }), 0, { store: { replicate: true }, canRead: () => Promise.resolve(true) });
        await (l0a.accessController).access.put(new Access({
            accessCondition: new AnyAccessCondition(),
            accessTypes: [AccessType.Any]
        }).initialize());

        const dbb = await TestStore.load(session.peers[1].ipfs, l0a.address) as TestStore;

        const l0b = await init(dbb, 1, { store: { replicate: false }, canRead: () => Promise.resolve(true) });

        // Allow all for easy query
        (l0b.accessController).access.store.sync((l0a.accessController).access.store.oplog.heads)
        await waitFor(() => (l0a.accessController).access.index.size === 1);
        await waitFor(() => (l0b.accessController).access.index.size === 1);

        let results: Results = undefined as any;
        l0a.accessController.access.index.search.query(new PageQueryRequest({
            queries: []
        })
            , (response) => {
                results = response;
            }, {
            signer: identity(1),
            waitForAmount: 1
        })

        await waitFor(() => !!results);

        // Now trusted because append all is 'true'c


    })



}) 