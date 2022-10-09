import { Constructor, field, variant } from "@dao-xyz/borsh";
import { disconnectPeers, getConnectedPeers, getPeer, Peer } from '@dao-xyz/peer-test-utils';
import { DynamicAccessController, DYNAMIC_ACCESS_CONTROLER } from "..";
import { Access, AccessType } from "../access";
import { AnyAccessCondition, PublicKeyAccessCondition } from "../condition";
import { waitFor } from '@dao-xyz/time';
import { DocumentQueryRequest, FieldStringMatchQuery, QueryRequestV0, QueryResponseV0 } from "@dao-xyz/query-protocol";
import { AccessError } from "@dao-xyz/peerbit-crypto";
import { CustomBinaryPayload } from "@dao-xyz/bpayload";
import { BinaryDocumentStore } from "@dao-xyz/orbit-db-bdocstore";
import { query } from "@dao-xyz/orbit-db-query-store";
import { RegionAccessController } from "@dao-xyz/orbit-db-trust-web";

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

/* const defaultOptions = (trust: P2PTrust, heapSizeLimt = 10e15, onMemoryExceeded?: () => void) => {
    return {
        clazz: Document,
        nameResolver: (n) => n,
        subscribeToQueries: true,
        accessController: {
            type: DYNAMIC_ACCESS_CONTROLER,
            trustResolver: () => trust,
            heapSizeLimit: () => heapSizeLimt,
            onMemoryExceeded,
            storeOptions: {
                subscribeToQueries: true,
                cache: undefined,
                
                replicate: true
            }
        },
        cache: undefined,
        
        replicate: true,
        typeMap: {
            [Document.name]: Document
        }
    }
}; */

/* 
const getTrust = async (peer: Peer) => {
    const acl = new DynamicAccessController({
        name: peer.id,
        rootTrust: peer.orbitDB.identity
    });
    await peer.orbitDB.open(acl);
    return acl
}

const loadTrust = async (peer: Peer, cid: string) => {
    const trust = await DynamicAccessController.load(cid, peer.node)
    await trust.init(peer.orbitDB, defaultOptions(trust));
    await trust.load();
    return trust
} */
describe('index', () => {

    it('it can share region access controller across stores', async () => {
        const [peer] = await Session.connected(1)
        const l0a = await peer.orbitDB.open(new BinaryDocumentStore({
            name: 'a',
            indexBy: 'id',
            objectType: Document.name,
            accessController: new DynamicAccessController({
                name: 'test-acl-a',
                regionAccessController: new RegionAccessController({
                    rootTrust: peer.orbitDB.identity,
                    name: 'region',
                })
            })
        }), { typeMap });

        const l0b = await peer.orbitDB.open(new BinaryDocumentStore({
            name: 'b',
            indexBy: 'id',
            objectType: Document.name,
            accessController: new DynamicAccessController({
                name: 'test-acl-b',
                regionAccessController: new RegionAccessController({
                    rootTrust: peer.orbitDB.identity,
                    name: 'region',
                })
            })
        }), { typeMap });

        expect(l0a.accessController !== l0b.accessController)
        expect((l0a.accessController as DynamicAccessController<any>).trust === (l0b.accessController as DynamicAccessController<any>).trust)

        await disconnectPeers([peer])
    })

    it('can write from trust web', async () => {
        const [peer, peer2] = await Session.connected(2)
        const l0a = await peer.orbitDB.open(new BinaryDocumentStore({
            name: 'test',
            indexBy: 'id',
            objectType: Document.name,
            accessController: new DynamicAccessController({
                name: 'test-acl',
                rootTrust: peer.orbitDB.identity
            })
        }), { typeMap });

        await l0a.put(new Document({
            id: '1'
        }));

        const l0b = await peer2.orbitDB.open(await BinaryDocumentStore.load(peer2.orbitDB._ipfs, l0a.address), { typeMap });

        await expect(l0b.put(new Document({
            id: 'id'
        }))).rejects.toBeInstanceOf(AccessError); // Not trusted
        await (l0a.accessController as DynamicAccessController<Document>).trust.addTrust(peer2.orbitDB.identity);

        await waitFor(() => Object.keys((l0b.accessController as DynamicAccessController<Document>).trust.trustGraph._index._index).length === 1);

        await l0b.put(new Document({
            id: '2'
        })) // Now trusted 

        await waitFor(() => Object.keys(l0a._index._index).length === 2);
        await waitFor(() => Object.keys(l0b._index._index).length === 2);

        await disconnectPeers([peer, peer2])
    })


    describe('conditions', () => {
        it('publickey', async () => {
            const [peer, peer2] = await Session.connected(2)
            const l0a = await peer.orbitDB.open(new BinaryDocumentStore({
                name: 'test',
                indexBy: 'id',
                objectType: Document.name,
                accessController: new DynamicAccessController({
                    name: 'test-acl',
                    rootTrust: peer.orbitDB.identity
                })
            }), { typeMap });
            await l0a.put(new Document({
                id: '1'
            }));


            const l0b = await peer2.orbitDB.open(await BinaryDocumentStore.load(peer2.orbitDB._ipfs, l0a.address), { typeMap });
            await expect(l0b.put(new Document({
                id: 'id'
            }))).rejects.toBeInstanceOf(AccessError); // Not trusted


            await (l0a.accessController as DynamicAccessController<Document>).acl.access.put(new Access({
                accessCondition: new PublicKeyAccessCondition({
                    key: peer2.orbitDB.identity
                }),
                accessTypes: [AccessType.Any]
            }));

            await waitFor(() => Object.keys((l0b.accessController as DynamicAccessController<Document>).acl.access._index._index).length === 1);
            await l0b.put(new Document({
                id: '2'
            })) // Now trusted 

            await disconnectPeers([peer, peer2])
        })


        it('through trust chain', async () => {
            const [peer, peer2, peer3] = await Session.connected(3)
            const l0a = await peer.orbitDB.open(new BinaryDocumentStore({
                name: 'test',
                indexBy: 'id',
                objectType: Document.name,
                accessController: new DynamicAccessController({
                    name: 'test-acl',
                    rootTrust: peer.orbitDB.identity
                })
            }), { typeMap });
            await l0a.put(new Document({
                id: '1'
            }));


            const l0b = await peer2.orbitDB.open(await BinaryDocumentStore.load(peer2.orbitDB._ipfs, l0a.address), { typeMap });
            const l0c = await peer3.orbitDB.open(await BinaryDocumentStore.load(peer3.orbitDB._ipfs, l0a.address), { typeMap });

            await expect(l0c.put(new Document({
                id: 'id'
            }))).rejects.toBeInstanceOf(AccessError); // Not trusted


            await (l0a.accessController as DynamicAccessController<Document>).acl.access.put(new Access({
                accessCondition: new PublicKeyAccessCondition({
                    key: peer2.orbitDB.identity
                }),
                accessTypes: [AccessType.Any]
            }));


            await expect(l0c.put(new Document({
                id: 'id'
            }))).rejects.toBeInstanceOf(AccessError); // Not trusted


            await waitFor(() => Object.keys((l0b.accessController as DynamicAccessController<Document>).acl.access._index._index).length == 1)
            await (((l0b.accessController as DynamicAccessController<Document>).acl.identityGraphController.addRelation(peer3.orbitDB.identity)));
            await waitFor(() => Object.keys((l0c.accessController as DynamicAccessController<Document>).acl.identityGraphController.relationGraph._index._index).length === 1);
            await l0c.put(new Document({
                id: '2'
            })) // Now trusted 

            await disconnectPeers([peer, peer2])
        })



        it('any access', async () => {
            const [peer, peer2] = await Session.connected(2)
            const l0a = await peer.orbitDB.open(new BinaryDocumentStore({
                name: 'test',
                indexBy: 'id',
                objectType: Document.name,
                accessController: new DynamicAccessController({
                    name: 'test-acl',
                    rootTrust: peer.orbitDB.identity
                })
            }), { typeMap });
            await l0a.put(new Document({
                id: '1'
            }));


            const l0b = await peer2.orbitDB.open(await BinaryDocumentStore.load(peer2.orbitDB._ipfs, l0a.address), { typeMap });
            await expect(l0b.put(new Document({
                id: 'id'
            }))).rejects.toBeInstanceOf(AccessError); // Not trusted


            const access = new Access({
                accessCondition: new AnyAccessCondition(),
                accessTypes: [AccessType.Any]
            });
            expect(access.id).toBeDefined();
            await (l0a.accessController as DynamicAccessController<Document>).acl.access.put(access);

            await waitFor(() => Object.keys((l0b.accessController as DynamicAccessController<Document>).acl.access._index._index).length === 1);
            await l0b.put(new Document({
                id: '2'
            })) // Now trusted 

            await disconnectPeers([peer, peer2])
        })


        it('read access', async () => {
            const [peer, peer2] = await Session.connected(2)

            const l0a = await peer.orbitDB.open(new BinaryDocumentStore({
                name: 'test',
                indexBy: 'id',
                objectType: Document.name,
                accessController: new DynamicAccessController({
                    name: 'test-acl',
                    rootTrust: peer.orbitDB.identity
                })
            }), { typeMap });

            await l0a.put(new Document({
                id: '1'
            }));


            let results: QueryResponseV0 = undefined;
            const q = () => query(peer2.node.pubsub, l0a.queryTopic, new QueryRequestV0({
                type: new DocumentQueryRequest({
                    queries: [new FieldStringMatchQuery({
                        key: 'id',
                        value: '1'
                    })]
                })
            }), (response) => {
                results = response;
            }, {
                signer: async (bytes) => {
                    return {
                        publicKey: peer2.orbitDB.publicKey,
                        signature: await peer2.orbitDB.sign(bytes)
                    }
                },
                maxAggregationTime: 3000
            })

            await q();

            expect(results).toBeUndefined(); // Because no read access

            await (l0a.accessController as DynamicAccessController<Document>).acl.access.put(new Access({
                accessCondition: new AnyAccessCondition(),
                accessTypes: [AccessType.Read]
            }).initialize());

            await q();

            expect(results).toBeDefined(); // Because no read access


            await disconnectPeers([peer, peer2])
        })
    })

    it('append all', async () => {
        const [peer, peer2] = await Session.connected(2)
        const l0a = await peer.orbitDB.open(new BinaryDocumentStore({
            name: 'test',
            indexBy: 'id',
            objectType: Document.name,
            accessController: new DynamicAccessController({
                name: 'test-acl',
                rootTrust: peer.orbitDB.identity
            })
        }), { typeMap });
        await l0a.put(new Document({
            id: '1'
        }));
        const dbb = await BinaryDocumentStore.load(peer2.orbitDB._ipfs, l0a.address);
        (dbb.accessController as DynamicAccessController<Document>).allowAll = true;
        const l0b = await peer2.orbitDB.open(dbb, { typeMap });
        await l0b.put(new Document({
            id: '2'
        })) // Now trusted because append all is 'true'

        // but entry will not be replicated on l0a since it still respects ACL
        await waitFor(() => Object.keys(l0a._index._index).length === 1);
        await disconnectPeers([peer, peer2])
    })

    it('on memory exceeded', async () => {

        const peer = await getPeer()
        let memoryExceeded = false;
        const acl = new DynamicAccessController({
            name: 'test-acl',
            rootTrust: peer.orbitDB.identity
        });

        acl.memoryOptions = {
            heapSizeLimit: () => 0,
            onMemoryExceeded: () => memoryExceeded = true
        }

        const l0a = await peer.orbitDB.open(new BinaryDocumentStore({
            name: 'test',
            indexBy: 'id',
            objectType: Document.name,
            accessController: acl
        }), { typeMap })

        await expect(l0a.put(new Document({
            id: '1'
        }))).rejects.toBeInstanceOf(AccessError);
        expect(memoryExceeded);
        await disconnectPeers([peer])
    })


    it('manifests are unique', async () => {

        const [peer] = await Session.connected(1)
        const l0a = await peer.orbitDB.open(new BinaryDocumentStore({
            name: 'test',
            indexBy: 'id',
            objectType: Document.name,
            accessController: new DynamicAccessController({
                name: 'test-acl',
                rootTrust: peer.orbitDB.identity
            })
        }), { typeMap });
        const l0b = await peer.orbitDB.open(new BinaryDocumentStore({
            name: 'test',
            indexBy: 'id',
            objectType: Document.name,
            accessController: new DynamicAccessController({
                name: 'test-acl-2',
                rootTrust: peer.orbitDB.identity
            })
        }), { typeMap })
        expect(l0a.address).not.toEqual(l0b.address)
        expect((l0a.accessController as DynamicAccessController<Document>).acl.address.toString()).not.toEqual((l0b.accessController as DynamicAccessController<Document>).acl.address.toString())
        await disconnectPeers([peer])

    })

    it('can query', async () => {

        const [peer, peer2] = await Session.connected(2)
        const l0a = await peer.orbitDB.open(new BinaryDocumentStore({
            name: 'test',
            indexBy: 'id',
            objectType: Document.name,
            accessController: new DynamicAccessController({
                name: 'test-acl',
                rootTrust: peer.orbitDB.identity
            })
        }), { typeMap });;
        await (l0a.accessController as DynamicAccessController<Document>).acl.access.put(new Access({
            accessCondition: new AnyAccessCondition(),
            accessTypes: [AccessType.Any]
        }).initialize());

        const dbb = await BinaryDocumentStore.load(peer2.orbitDB._ipfs, l0a.address);

        const l0b = await peer2.orbitDB.open(dbb, { typeMap });

        // Allow all for easy query
        (l0a.accessController as DynamicAccessController<Document>).allowAll = true;
        (l0b.accessController as DynamicAccessController<Document>).allowAll = true;

        await waitFor(() => Object.keys((l0a.accessController as DynamicAccessController<Document>).acl.access._index._index).length === 1);
        await waitFor(() => Object.keys((l0b.accessController as DynamicAccessController<Document>).acl.access._index._index).length === 1);

        let resp: QueryResponseV0 = undefined;
        await (l0b.accessController as DynamicAccessController<Document>).acl.access.query(new QueryRequestV0({
            type: new DocumentQueryRequest({
                queries: []
            })
        }), (r) => { resp = r }, {
            signer: async (bytes) => {
                return {
                    publicKey: peer2.orbitDB.publicKey,
                    signature: await peer2.orbitDB.sign(bytes)
                }
            }, waitForAmount: 1
        });
        await waitFor(() => !!resp);

        // Now trusted because append all is 'true'c
        await disconnectPeers([peer, peer2])

    })



}) 