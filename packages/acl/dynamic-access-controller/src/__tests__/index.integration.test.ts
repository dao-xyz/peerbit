import { Constructor, field, variant } from "@dao-xyz/borsh";
import { createStore, Session } from '@dao-xyz/orbit-db-test-utils';
import { DynamicAccessController, DYNAMIC_ACCESS_CONTROLER } from "..";
import { Access, AccessType } from "../access";
import { AnyAccessCondition, PublicKeyAccessCondition } from "../condition";
import { waitFor } from '@dao-xyz/time';
import { DocumentQueryRequest, FieldStringMatchQuery, QueryRequestV0, QueryResponseV0 } from "@dao-xyz/query-protocol";
import { AccessError, Ed25519Keypair } from "@dao-xyz/peerbit-crypto";
import { CustomBinaryPayload } from "@dao-xyz/bpayload";
import { BinaryDocumentStore } from "@dao-xyz/orbit-db-bdocstore";
import { query } from "@dao-xyz/orbit-db-query-store";
import { TrustedNetwork } from "@dao-xyz/peerbit-trusted-network";
import { Identity } from "@dao-xyz/ipfs-log";
import { Level } from 'level';
import { CachedValue, DefaultOptions, Store, StoreLike } from '@dao-xyz/orbit-db-store';
import { fileURLToPath } from 'url';
import path from 'path';
import Cache from '@dao-xyz/orbit-db-cache';
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
        rootTrust: identity(0).publicKey
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

    let session: Session, identites: Identity[], cacheStore: Level[]

    const identity = (i: number) => identites[i];
    const init = <T extends StoreLike<any>>(store: T, i: number) => (store.init && store.init(session.peers[i].ipfs, identites[i], { ...DefaultOptions, typeMap, resolveCache: async () => new Cache<CachedValue>(cacheStore[i]) })) as Promise<T>
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

    /*  This test belongs to the client
     
    it('it can share region access controller across stores', async () => {
         const l0a = await peer.orbitDB.open(new BinaryDocumentStore({
             name: 'a',
             indexBy: 'id',
             objectType: Document.name,
             accessController: new DynamicAccessController({
                 name: 'test-acl-a',
                 TrustedNetwork: new TrustedNetwork({
                     rootTrust: identity(0).publicKey,
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
                 TrustedNetwork: new TrustedNetwork({
                     rootTrust: identity(0).publicKey,
                     name: 'region',
                 })
             })
         }), { typeMap });
     
         expect(l0a.accessController !== l0b.accessController)
         expect((l0a.accessController as DynamicAccessController<any>).trust === (l0b.accessController as DynamicAccessController<any>).trust)
     
     }) */

    it('can write from trust web', async () => {

        const l0a = await init(new BinaryDocumentStore({
            name: 'test',
            indexBy: 'id',
            objectType: Document.name,
            accessController: new DynamicAccessController({
                name: 'test-acl',
                rootTrust: identity(0).publicKey
            })
        }), 0);

        await l0a.put(new Document({
            id: '1'
        }));

        const l0b = await init(await BinaryDocumentStore.load(session.peers[1].ipfs, l0a.address), 1);

        await expect(l0b.put(new Document({
            id: 'id'
        }))).rejects.toBeInstanceOf(AccessError); // Not trusted
        await (l0a.accessController as DynamicAccessController<Document>).trust.addTrust(identity(1).publicKey);

        await (l0b.accessController as DynamicAccessController<Document>).trust.sync((l0a.accessController as DynamicAccessController<Document>).trust.oplog.heads);
        await waitFor(() => Object.keys((l0b.accessController as DynamicAccessController<Document>).trust.trustGraph._index._index).length === 1);

        await l0b.put(new Document({
            id: '2'
        })) // Now trusted 

        await l0a.sync(l0b.oplog.heads);
        await l0b.sync(l0a.oplog.heads);

        await waitFor(() => Object.keys(l0a._index._index).length === 2);
        await waitFor(() => Object.keys(l0b._index._index).length === 2);

    })


    describe('conditions', () => {
        it('publickey', async () => {

            const l0a = await init(new BinaryDocumentStore({
                name: 'test',
                indexBy: 'id',
                objectType: Document.name,
                accessController: new DynamicAccessController({
                    name: 'test-acl',
                    rootTrust: identity(0).publicKey
                })
            }), 0);
            await l0a.put(new Document({
                id: '1'
            }));

            const l0b = await init(await BinaryDocumentStore.load(session.peers[1].ipfs, l0a.address), 1);

            await l0b.sync(l0a.oplog.heads);
            await expect(l0b.put(new Document({
                id: 'id'
            }))).rejects.toBeInstanceOf(AccessError); // Not trusted


            await (l0a.accessController as DynamicAccessController<Document>).acl.access.put(new Access({
                accessCondition: new PublicKeyAccessCondition({
                    key: identity(1).publicKey
                }),
                accessTypes: [AccessType.Any]
            }));

            await (l0b.accessController as DynamicAccessController<Document>).acl.access.sync((l0a.accessController as DynamicAccessController<Document>).acl.access.oplog.heads);
            await waitFor(() => Object.keys((l0b.accessController as DynamicAccessController<Document>).acl.access._index._index).length === 1);
            await l0b.put(new Document({
                id: '2'
            })) // Now trusted 


        })


        it('through trust chain', async () => {

            const l0a = await init(new BinaryDocumentStore({
                name: 'test',
                indexBy: 'id',
                objectType: Document.name,
                accessController: new DynamicAccessController({
                    name: 'test-acl',
                    rootTrust: identity(0).publicKey
                })
            }), 0);

            await l0a.put(new Document({
                id: '1'
            }));


            const l0b = await init(await BinaryDocumentStore.load(session.peers[1].ipfs, l0a.address), 1);
            const l0c = await init(await BinaryDocumentStore.load(session.peers[2].ipfs, l0a.address), 2);

            await expect(l0c.put(new Document({
                id: 'id'
            }))).rejects.toBeInstanceOf(AccessError); // Not trusted


            await (l0a.accessController as DynamicAccessController<Document>).acl.access.put(new Access({
                accessCondition: new PublicKeyAccessCondition({
                    key: identity(1).publicKey
                }),
                accessTypes: [AccessType.Any]
            }));

            await (l0b.accessController as DynamicAccessController<Document>).acl.access.sync((l0a.accessController as DynamicAccessController<Document>).acl.access.oplog.heads);
            await (l0c.accessController as DynamicAccessController<Document>).acl.access.sync((l0a.accessController as DynamicAccessController<Document>).acl.access.oplog.heads);

            await expect(l0c.put(new Document({
                id: 'id'
            }))).rejects.toBeInstanceOf(AccessError); // Not trusted


            await waitFor(() => Object.keys((l0b.accessController as DynamicAccessController<Document>).acl.access._index._index).length == 1)
            await (((l0b.accessController as DynamicAccessController<Document>).acl.identityGraphController.addRelation(identity(2).publicKey)));
            await (l0c.accessController as DynamicAccessController<Document>).acl.identityGraphController.sync((l0b.accessController as DynamicAccessController<Document>).acl.identityGraphController.oplog.heads);

            await waitFor(() => Object.keys((l0c.accessController as DynamicAccessController<Document>).acl.identityGraphController.relationGraph._index._index).length === 1);
            await l0c.put(new Document({
                id: '2'
            })) // Now trusted 


        })



        it('any access', async () => {

            const l0a = await init(new BinaryDocumentStore({
                name: 'test',
                indexBy: 'id',
                objectType: Document.name,
                accessController: new DynamicAccessController({
                    name: 'test-acl',
                    rootTrust: identity(0).publicKey
                })
            }), 0);
            await l0a.put(new Document({
                id: '1'
            }));


            const l0b = await init(await BinaryDocumentStore.load(session.peers[1].ipfs, l0a.address), 1);
            await expect(l0b.put(new Document({
                id: 'id'
            }))).rejects.toBeInstanceOf(AccessError); // Not trusted


            const access = new Access({
                accessCondition: new AnyAccessCondition(),
                accessTypes: [AccessType.Any]
            });
            expect(access.id).toBeDefined();
            await (l0a.accessController as DynamicAccessController<Document>).acl.access.put(access);
            await (l0b.accessController as DynamicAccessController<Document>).acl.access.sync((l0a.accessController as DynamicAccessController<Document>).acl.access.oplog.heads);

            await waitFor(() => Object.keys((l0b.accessController as DynamicAccessController<Document>).acl.access._index._index).length === 1);
            await l0b.put(new Document({
                id: '2'
            })) // Now trusted 


        })


        it('read access', async () => {


            const l0a = await init(new BinaryDocumentStore({
                name: 'test',
                indexBy: 'id',
                objectType: Document.name,
                accessController: new DynamicAccessController({
                    name: 'test-acl',
                    rootTrust: identity(0).publicKey
                })
            }), 0);

            await l0a.put(new Document({
                id: '1'
            }));


            let results: QueryResponseV0 = undefined as any;
            const q = () => query(session.peers[1].ipfs, l0a.queryTopic, new QueryRequestV0({
                type: new DocumentQueryRequest({
                    queries: [new FieldStringMatchQuery({
                        key: 'id',
                        value: '1'
                    })]
                })
            }), (response) => {
                results = response;
            }, {
                signer: identity(1),
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



        })
    })

    it('append all', async () => {

        const l0a = await init(new BinaryDocumentStore({
            name: 'test',
            indexBy: 'id',
            objectType: Document.name,
            accessController: new DynamicAccessController({
                name: 'test-acl',
                rootTrust: identity(0).publicKey
            })
        }), 0);
        await l0a.put(new Document({
            id: '1'
        }));
        const dbb = await BinaryDocumentStore.load(session.peers[1].ipfs, l0a.address);
        (dbb.accessController as DynamicAccessController<Document>).allowAll = true;
        const l0b = await init(dbb, 1);
        await l0b.put(new Document({
            id: '2'
        })) // Now trusted because append all is 'true'

        // but entry will not be replicated on l0a since it still respects ACL
        await waitFor(() => Object.keys(l0a._index._index).length === 1);

    })

    /* This is not cmpatible with our sharding method  
     it('on memory exceeded', async () => {
  
          let memoryExceeded = false;
          const acl = new DynamicAccessController({
              name: 'test-acl',
              rootTrust: identity(0).publicKey
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
   */

    it('manifests are unique', async () => {

        const l0a = await init(new BinaryDocumentStore({
            name: 'test',
            indexBy: 'id',
            objectType: Document.name,
            accessController: new DynamicAccessController({
                name: 'test-acl',
                rootTrust: identity(0).publicKey
            })
        }), 0);
        const l0b = await init(new BinaryDocumentStore({
            name: 'test',
            indexBy: 'id',
            objectType: Document.name,
            accessController: new DynamicAccessController({
                name: 'test-acl-2',
                rootTrust: identity(0).publicKey
            })
        }), 0)
        expect(l0a.address).not.toEqual(l0b.address)
        expect((l0a.accessController as DynamicAccessController<Document>).acl.address.toString()).not.toEqual((l0b.accessController as DynamicAccessController<Document>).acl.address.toString())

    })

    it('can query', async () => {


        const l0a = await init(new BinaryDocumentStore({
            name: 'test',
            indexBy: 'id',
            objectType: Document.name,
            accessController: new DynamicAccessController({
                name: 'test-acl',
                rootTrust: identity(0).publicKey
            })
        }), 0);;
        await (l0a.accessController as DynamicAccessController<Document>).acl.access.put(new Access({
            accessCondition: new AnyAccessCondition(),
            accessTypes: [AccessType.Any]
        }).initialize());

        const dbb = await BinaryDocumentStore.load(session.peers[1].ipfs, l0a.address);

        const l0b = await init(dbb, 1);

        // Allow all for easy query
        (l0a.accessController as DynamicAccessController<Document>).allowAll = true;
        (l0b.accessController as DynamicAccessController<Document>).allowAll = true;
        (l0b.accessController as DynamicAccessController<Document>).acl.access.sync((l0a.accessController as DynamicAccessController<Document>).acl.access.oplog.heads)
        await waitFor(() => Object.keys((l0a.accessController as DynamicAccessController<Document>).acl.access._index._index).length === 1);
        await waitFor(() => Object.keys((l0b.accessController as DynamicAccessController<Document>).acl.access._index._index).length === 1);

        let resp: QueryResponseV0 = undefined as any;
        await (l0b.accessController as DynamicAccessController<Document>).acl.access.query(new QueryRequestV0({
            type: new DocumentQueryRequest({
                queries: []
            })
        }), (r) => { resp = r }, {
            signer: identity(1), waitForAmount: 1
        });
        await waitFor(() => !!resp);

        // Now trusted because append all is 'true'c


    })



}) 