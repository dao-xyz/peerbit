import { Constructor, field, variant } from "@dao-xyz/borsh";
import { createStore, Session } from '@dao-xyz/orbit-db-test-utils';
import { DynamicAccessController } from "..";
import { Access, AccessType } from "../access";
import { AnyAccessCondition, PublicKeyAccessCondition } from "../condition";
import { waitFor } from '@dao-xyz/time';
import { DocumentQueryRequest, DSearch, FieldStringMatchQuery, Results } from "@dao-xyz/peerbit-dsearch";
import { AccessError, Ed25519Keypair, MaybeEncrypted, SignatureWithKey } from "@dao-xyz/peerbit-crypto";
import { BinaryPayload, CustomBinaryPayload } from "@dao-xyz/bpayload";
import { DDocs } from "@dao-xyz/peerbit-ddoc";
import type { Identity, Payload } from "@dao-xyz/ipfs-log";
import { Level } from 'level';
import { CachedValue, DefaultOptions, IInitializationOptions, Store, StoreLike } from '@dao-xyz/peerbit-dstore';
import { fileURLToPath } from 'url';
import path from 'path';
import Cache from '@dao-xyz/orbit-db-cache';
import { DQuery } from "@dao-xyz/peerbit-dquery";
import { Program } from "@dao-xyz/peerbit-program";
import { IPFS } from "ipfs-core-types";
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
const typeMap: { [key: string]: Constructor<any> } = { [Document.name]: Document, };

const createIdentity = async () => {
    const ed = await Ed25519Keypair.create();
    return {
        publicKey: ed.publicKey,
        sign: (data) => ed.sign(data)
    } as Identity
}


@variant([0, 251])
class TestStore<T extends BinaryPayload> extends Program {

    @field({ type: DDocs })
    store: DDocs<T>

    @field({ type: DynamicAccessController })
    accessController: DynamicAccessController<T>

    constructor(properties: { name?: string, identity: Identity, accessControllerName?: string }) {
        super(properties)
        if (properties) {
            this.store = new DDocs({
                name: 'test',
                indexBy: 'id',
                objectType: Document.name,
                search: new DSearch({
                    query: new DQuery({})
                })
            });
            this.accessController = new DynamicAccessController({
                name: properties.accessControllerName || 'test-acl',
                rootTrust: properties.identity?.publicKey
            })
        }
    }
    async init(ipfs: IPFS<{}>, identity: Identity, options: IInitializationOptions<any>): Promise<this> {
        options = { ...options, replicate: true };
        await super.init(ipfs, identity, options);
        await this.accessController.init(ipfs, identity, { ...options });
        await this.store.init(ipfs, identity, { ...options, canRead: this.accessController.canRead.bind(this.accessController), canAppend: this.accessController.canAppend.bind(this.accessController) });

        return this;
    }
}
describe('index', () => {

    let session: Session, identites: Identity[], cacheStore: Level[]

    const identity = (i: number) => identites[i];
    const init = <T extends Program>(store: T, i: number, options: { canRead?: (key: SignatureWithKey) => Promise<boolean>, canAppend?: (payload: MaybeEncrypted<Payload<any>>, key: MaybeEncrypted<SignatureWithKey>) => Promise<boolean> } = {}) => (store.init && store.init(session.peers[i].ipfs, identites[i], { ...DefaultOptions, typeMap, resolveCache: async () => new Cache<CachedValue>(cacheStore[i]), ...options })) as Promise<T>

    beforeAll(async () => {
        session = await Session.connected(3);
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


    it('can write from trust web', async () => {

        const s = new TestStore({ identity: identity(0) });
        const l0a = await init(s, 0);

        await l0a.store.put(new Document({
            id: '1'
        }));

        const l0b = await init(await TestStore.load(session.peers[1].ipfs, l0a.address), 1) as TestStore<Document>;

        await expect(l0b.store.put(new Document({
            id: 'id'
        }))).rejects.toBeInstanceOf(AccessError); // Not trusted
        await l0a.accessController.acl.trustedNetwork.add(identity(1).publicKey);

        await (l0b.accessController).acl.trustedNetwork.trustGraph.store.sync((l0a.accessController).acl.trustedNetwork.trustGraph.store.oplog.heads);
        await waitFor(() => Object.keys((l0b.accessController).acl.trustedNetwork.trustGraph._index._index).length === 1);

        await l0b.store.put(new Document({
            id: '2'
        })) // Now trusted 

        await l0a.store.store.sync(l0b.store.store.oplog.heads);
        await l0b.store.store.sync(l0a.store.store.oplog.heads);

        await waitFor(() => Object.keys(l0a.store._index._index).length === 2);
        await waitFor(() => Object.keys(l0b.store._index._index).length === 2);

    })


    describe('conditions', () => {
        it('publickey', async () => {

            const l0a = await init(new TestStore({ identity: identity(0) }), 0);

            await l0a.store.put(new Document({
                id: '1'
            }));

            const l0b = await init(await TestStore.load(session.peers[1].ipfs, l0a.address), 1) as TestStore<Document>;

            await l0b.store.store.sync(l0a.store.store.oplog.heads);
            await waitFor(() => Object.keys(l0b.store._index._index).length === 1)
            await expect(l0b.store.put(new Document({
                id: 'id'
            }))).rejects.toBeInstanceOf(AccessError); // Not trusted


            await (l0a.accessController).acl.access.put(new Access({
                accessCondition: new PublicKeyAccessCondition({
                    key: identity(1).publicKey
                }),
                accessTypes: [AccessType.Any]
            }));

            await (l0b.accessController).acl.access.store.sync((l0a.accessController).acl.access.store.oplog.heads);
            await waitFor(() => Object.keys((l0b.accessController).acl.access._index._index).length === 1);
            await l0b.store.put(new Document({
                id: '2'
            })) // Now trusted 


        })


        it('through trust chain', async () => {

            const l0a = await init(new TestStore({ identity: identity(0) }), 0);

            await l0a.store.put(new Document({
                id: '1'
            }));

            const l0b = await init(await TestStore.load(session.peers[1].ipfs, l0a.address), 1) as TestStore<Document>;
            const l0c = await init(await TestStore.load(session.peers[2].ipfs, l0a.address), 2) as TestStore<Document>;

            await expect(l0c.store.put(new Document({
                id: 'id'
            }))).rejects.toBeInstanceOf(AccessError); // Not trusted


            await (l0a.accessController).acl.access.put(new Access({
                accessCondition: new PublicKeyAccessCondition({
                    key: identity(1).publicKey
                }),
                accessTypes: [AccessType.Any]
            }));

            await (l0b.accessController).acl.access.store.sync((l0a.accessController).acl.access.store.oplog.heads);
            await (l0c.accessController).acl.access.store.sync((l0a.accessController).acl.access.store.oplog.heads);

            await expect(l0c.store.put(new Document({
                id: 'id'
            }))).rejects.toBeInstanceOf(AccessError); // Not trusted


            await waitFor(() => Object.keys((l0b.accessController).acl.access._index._index).length == 1)
            await (((l0b.accessController).acl.identityGraphController.addRelation(identity(2).publicKey)));
            await (l0c.accessController).acl.identityGraphController.relationGraph.store.sync((l0b.accessController).acl.identityGraphController.relationGraph.store.oplog.heads);

            await waitFor(() => Object.keys((l0c.accessController).acl.identityGraphController.relationGraph._index._index).length === 1);
            await l0c.store.put(new Document({
                id: '2'
            })) // Now trusted 


        })



        it('any access', async () => {

            const l0a = await init(new TestStore({ identity: identity(0) }), 0);
            await l0a.store.put(new Document({
                id: '1'
            }));


            const l0b = await init(await TestStore.load(session.peers[1].ipfs, l0a.address), 1) as TestStore<Document>;
            await expect(l0b.store.put(new Document({
                id: 'id'
            }))).rejects.toBeInstanceOf(AccessError); // Not trusted


            const access = new Access({
                accessCondition: new AnyAccessCondition(),
                accessTypes: [AccessType.Any]
            });
            expect(access.id).toBeDefined();
            await (l0a.accessController).acl.access.put(access);
            await (l0b.accessController).acl.access.store.sync((l0a.accessController).acl.access.store.oplog.heads);

            await waitFor(() => Object.keys((l0b.accessController).acl.access._index._index).length === 1);
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
                l0a.store.search.query(new DocumentQueryRequest({
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

            await (l0a.accessController).acl.access.put(new Access({
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
        expect((l0a.accessController).acl.address.toString()).not.toEqual((l0b.accessController).acl.address.toString())

    })

    it('can query', async () => {


        const l0a = await init(new TestStore({ identity: identity(0) }), 0, { canRead: () => Promise.resolve(true) });
        await (l0a.accessController).acl.access.put(new Access({
            accessCondition: new AnyAccessCondition(),
            accessTypes: [AccessType.Any]
        }).initialize());

        const dbb = await TestStore.load(session.peers[1].ipfs, l0a.address) as TestStore<Document>;

        const l0b = await init(dbb, 1, { canRead: () => Promise.resolve(true) });

        // Allow all for easy query
        (l0b.accessController).acl.access.store.sync((l0a.accessController).acl.access.store.oplog.heads)
        await waitFor(() => Object.keys((l0a.accessController).acl.access._index._index).length === 1);
        await waitFor(() => Object.keys((l0b.accessController).acl.access._index._index).length === 1);

        let results: Results = undefined as any;
        l0a.accessController.acl.access.search.query(new DocumentQueryRequest({
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