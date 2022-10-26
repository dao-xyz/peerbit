
import { field, option, serialize, variant } from '@dao-xyz/borsh';
import { DDocuments } from '../document-store';
import { Compare, FieldBigIntCompareQuery, SortDirection, FieldStringMatchQuery, ResultWithSource, FieldSort, MemoryCompareQuery, MemoryCompare, Results, DSearch, PageQueryRequest } from '@dao-xyz/peerbit-anysearch';
import { CustomBinaryPayload } from '@dao-xyz/peerbit-bpayload';
import { DQuery } from '@dao-xyz/peerbit-query';
import { Session, createStore } from '@dao-xyz/peerbit-test-utils';
import { DefaultOptions } from '@dao-xyz/peerbit-store';
import { Identity } from '@dao-xyz/ipfs-log';
import { Ed25519Keypair, EncryptedThing, X25519Keypair, X25519PublicKey } from '@dao-xyz/peerbit-crypto';
import { IPFS } from 'ipfs-core-types';
import Cache from '@dao-xyz/peerbit-cache';
import { Level } from 'level';
import { fileURLToPath } from 'url';
import path from 'path';
import { v4 as uuid } from 'uuid';
import { Program } from '@dao-xyz/peerbit-program';
import { waitFor } from '@dao-xyz/peerbit-time';
import { DocumentIndex } from '../document-index';
import { HeadsMessage, LogEntryEncryptionQuery, LogQueryRequest } from '@dao-xyz/peerbit-logindex';

const __filename = fileURLToPath(import.meta.url);



@variant("document")//@variant([1, 0])
class Document extends CustomBinaryPayload {

  @field({ type: 'string' })
  id: string;

  @field({ type: option('string') })
  name?: string;

  @field({ type: option('u64') })
  number?: bigint;


  constructor(opts?: Document) {
    super();
    if (opts) {
      Object.assign(this, opts);
    }
  }
}

@variant([0, 244])
class DocumentDDoc extends Program {

  @field({ type: DDocuments })
  docs: DDocuments<Document>

  constructor(properties?: { docs: DDocuments<Document> }) {
    super();
    if (properties) {
      this.docs = properties.docs;
    }
  }
  async setup(): Promise<void> {
    await this.docs.setup({ type: Document })
  }

}

const bigIntSort = <T extends (number | bigint)>(a: T, b: T): number => (a > b ? 1 : 0 || -(a < b))

describe('index', () => {
  let session: Session, peersCount = 3,
    /*     writerStoreKeypair: X25519Keypair, observer2Keypair: X25519Keypair,
     */
    stores: DocumentDDoc[] = [],
    writeStore: DocumentDDoc,
    cacheStores: Level[] = []

  const createIdentity = async () => {
    const ed = await Ed25519Keypair.create();
    return {
      publicKey: ed.publicKey,
      sign: (data) => ed.sign(data)
    } as Identity
  }

  beforeAll(async () => {
    session = await Session.connected(peersCount)
    for (let i = 0; i < peersCount; i++) {
      cacheStores.push(await createStore(path.join(__filename, 'cache- ' + i)));
    }



    const queryRegion = uuid();
    // Create store
    for (let i = 0; i < peersCount; i++) {
      const store = i > 0 ? await DocumentDDoc.load<DocumentDDoc>(session.peers[i].ipfs, stores[0].address) : new DocumentDDoc({
        docs: new DDocuments<Document>({
          index: new DocumentIndex({
            search: new DSearch({
              query: new DQuery({
                queryRegion: queryRegion
              })
            }),
            indexBy: 'id'
          })
        })
      });
      const keypair = await X25519Keypair.create();
      (await store.init(session.peers[i].ipfs, await createIdentity(), {
        store: {
          ...DefaultOptions, replicate: i === 0, encryption: {
            getEncryptionKeypair: () => Promise.resolve(keypair as Ed25519Keypair | X25519Keypair),
            getAnyKeypair: async (publicKeys: X25519PublicKey[]) => {
              for (let i = 0; i < publicKeys.length; i++) {
                if (publicKeys[i].equals((keypair as X25519Keypair).publicKey)) {
                  return {
                    index: i,
                    keypair: keypair as Ed25519Keypair | X25519Keypair
                  }
                }
              }
            }
          }, resolveCache: () => new Cache(cacheStores[i])
        }
      }));
      stores.push(store);
    }
    writeStore = stores[0];

    let doc = new Document({
      id: '1',
      name: 'hello world',
      number: 1n
    });
    let doc2 = new Document({
      id: '2',
      name: 'hello world',
      number: 2n
    });

    let doc3 = new Document({
      id: '3',
      name: 'foo',
      number: 3n
    });

    await writeStore.docs.put(doc);
    await writeStore.docs.put(doc2);
    await writeStore.docs.put(doc3);


  })

  afterEach(async () => {
  })

  afterAll(async () => {
    await Promise.all(stores.map(x => x.drop()));

    await Promise.all(cacheStores.map(x => x.close()));
    await session.stop();
  })

  describe('operations', () => {

    it('can add and delete', async () => {

      const store = new DocumentDDoc({
        docs: new DDocuments<Document>({
          index: new DocumentIndex({
            search: new DSearch({
              query: new DQuery({
                queryRegion: '_'
              })
            }),
            indexBy: 'id'
          })
        })
      });
      const keypair = await X25519Keypair.create();
      (await store.init(session.peers[0].ipfs, await createIdentity(), {
        store: {
          ...DefaultOptions, replicate: true, encryption: {
            getEncryptionKeypair: () => Promise.resolve(keypair as Ed25519Keypair | X25519Keypair),
            getAnyKeypair: async (publicKeys: X25519PublicKey[]) => {
              for (let i = 0; i < publicKeys.length; i++) {
                if (publicKeys[i].equals((keypair as X25519Keypair).publicKey)) {
                  return {
                    index: i,
                    keypair: keypair as Ed25519Keypair | X25519Keypair
                  }
                }
              }
            }
          }, resolveCache: () => new Cache(cacheStores[0])
        }
      }));

      let doc = new Document({
        id: uuid(),
        name: 'Hello world'
      });
      let doc2 = new Document({
        id: uuid(),
        name: 'Hello world'
      });

      const putOperation = await store.docs.put(doc);
      expect(store.docs._index.size).toEqual(1);
      const putOperation2 = await store.docs.put(doc2);
      expect(store.docs._index.size).toEqual(2);
      expect(putOperation2.next).toContainAllValues([]); // because doc 2 is independent of doc 1

      // delete 1
      const deleteOperation = await store.docs.del(doc.id);
      expect(deleteOperation.next).toContainAllValues([putOperation.hash]); // because delete is dependent on put
      expect(store.docs._index.size).toEqual(1);
    })
  })


  describe('query', () => {

    it('match all', async () => {



      let response: Results = undefined as any;

      //await otherPeer.node.swarm.connect((await creatorPeer.node.id()).addresses[0].toString());
      await stores[1].docs.index.search.query(new PageQueryRequest({
        queries: []
      }), (r: Results) => {
        response = r;
      }, { waitForAmount: 1 })
      expect(response.results).toHaveLength(3);


    });

    it('string', async () => {


      let response: Results = undefined as any;

      //await otherPeer.node.swarm.connect((await creatorPeer.node.id()).addresses[0].toString());
      await stores[1].docs.index.search.query(new PageQueryRequest({
        queries: [new FieldStringMatchQuery({
          key: 'name',
          value: 'ello'
        })]

      }), (r: Results) => {
        response = r;
      }, { waitForAmount: 1 })
      expect(response.results).toHaveLength(2);
      expect(response.results.map(x => ((x as ResultWithSource).source as Document).id)).toEqual(['1', '2']);
    });

    it('offset size', async () => {



      let response: Results = undefined as any;

      //await otherPeer.node.swarm.connect((await creatorPeer.node.id()).addresses[0].toString());
      await stores[1].docs.index.search.query(new PageQueryRequest({
        queries: [new FieldStringMatchQuery({
          key: 'name',
          value: 'hello'
        })],
        size: 1n,
        offset: 1n
      })
        ,
        (r: Results) => {
          response = r;
        }, { waitForAmount: 1 })
      expect(response.results).toHaveLength(1);
      expect((((response.results[0]) as ResultWithSource).source as Document).id).toEqual('2');
    });

    /*  describe('sort', () => {
       it('sort offset ascending', async () => {
         let doc = new Document({
           id: '1',
           name: 'hey',
           number: 1n
         });
 
         let doc2 = new Document({
           id: '2',
           name: 'hey',
           number: 2n
 
         });
 
         let doc3 = new Document({
           id: '3',
           name: 'hey',
           number: 3n
         });
 
         await writeStore.docs.put(doc);
         await writeStore.docs.put(doc2);
         await writeStore.docs.put(doc3);
 
         let response: Results = undefined as any;
 
         //await otherPeer.node.swarm.connect((await creatorPeer.node.id()).addresses[0].toString());
         await stores[1].docs.index.search.query(new PageQueryRequest({
           queries: [new FieldStringMatchQuery({
             key: 'name',
             value: 'hey'
           })],
           offset: 1n,
           sort: new FieldSort({
             key: ['number'],
             direction: SortDirection.Ascending
           })
         }), (r: Results) => {
           response = r;
         }, { waitForAmount: 1 })
         expect(response.results).toHaveLength(2);
         expect(((response.results[0] as ResultWithSource).source as Document).id).toEqual(doc2.id);
         expect(((response.results[1] as ResultWithSource).source as Document).id).toEqual(doc3.id);
 
 
       });
 
 
       it('sort offset descending', async () => {
 
         let doc = new Document({
           id: '1',
           name: 'hey',
           number: 1n
         });
         let doc2 = new Document({
           id: '2',
           name: 'hey',
           number: 2n
 
         });
 
         let doc3 = new Document({
           id: '3',
           name: 'hey',
           number: 3n
 
         });
 
         await writeStore.docs.put(doc);
         await writeStore.docs.put(doc2);
         await writeStore.docs.put(doc3);
 
         let response: Results = undefined as any;
 
         //await otherPeer.node.swarm.connect((await creatorPeer.node.id()).addresses[0].toString());
         await stores[1].docs.index.search.query(new PageQueryRequest({
           queries: [new FieldStringMatchQuery({
             key: 'name',
             value: 'hey'
           })],
           offset: 1n,
           sort: new FieldSort({
             key: ['number'],
             direction: SortDirection.Descending
           })
         }), (r: Results) => {
           response = r;
         }, { waitForAmount: 1 })
         expect(response.results).toHaveLength(2);
         expect(((response.results[0] as ResultWithSource).source as Document).id).toEqual(doc2.id);
         expect(((response.results[1] as ResultWithSource).source as Document).id).toEqual(doc.id);
 
 
       });
     }) */
    describe('number', () => {
      it('equal', async () => {

        let response: Results = undefined as any;
        await stores[1].docs.index.search.query(new PageQueryRequest({
          queries: [new FieldBigIntCompareQuery({
            key: 'number',
            compare: Compare.Equal,
            value: 2n
          })]
        }), (r: Results) => {
          response = r;
        }, { waitForAmount: 1 })
        expect(response.results).toHaveLength(1);
        expect(((response.results[0] as ResultWithSource).source as Document).number).toEqual(2n);

      });


      it('gt', async () => {


        let response: Results = undefined as any;
        await stores[1].docs.index.search.query(new PageQueryRequest({
          queries: [new FieldBigIntCompareQuery({
            key: 'number',
            compare: Compare.Greater,
            value: 2n
          })]
        }), (r: Results) => {
          response = r;
        }, { waitForAmount: 1 })
        expect(response.results).toHaveLength(1);
        expect(((response.results[0] as ResultWithSource).source as Document).number).toEqual(3n);

      });

      it('gte', async () => {

        let response: Results = undefined as any;
        await stores[1].docs.index.search.query(new PageQueryRequest({
          queries: [new FieldBigIntCompareQuery({
            key: 'number',
            compare: Compare.GreaterOrEqual,
            value: 2n
          })]
        }), (r: Results) => {
          response = r;
        }, { waitForAmount: 1 })
        response.results.sort((a, b) => bigIntSort(((a as ResultWithSource).source as Document).number as bigint, ((b as ResultWithSource).source as Document).number as bigint));
        expect(response.results).toHaveLength(2);
        expect(((response.results[0] as ResultWithSource).source as Document).number).toEqual(2n);
        expect(((response.results[1] as ResultWithSource).source as Document).number).toEqual(3n);

      });

      it('lt', async () => {

        let response: Results = undefined as any;
        await stores[1].docs.index.search.query(new PageQueryRequest({
          queries: [new FieldBigIntCompareQuery({
            key: 'number',
            compare: Compare.Less,
            value: 2n
          })]
        }), (r: Results) => {
          response = r;
        }, { waitForAmount: 1 })
        expect(response.results).toHaveLength(1);
        expect(((response.results[0] as ResultWithSource).source as Document).number).toEqual(1n);

      });

      it('lte', async () => {


        let response: Results = undefined as any;
        await stores[1].docs.index.search.query(new PageQueryRequest({
          queries: [new FieldBigIntCompareQuery({
            key: 'number',
            compare: Compare.LessOrEqual,
            value: 2n
          })]
        }), (r: Results) => {
          response = r;
        }, { waitForAmount: 1 })
        response.results.sort((a, b) => bigIntSort(((a as ResultWithSource).source as Document).number as bigint, ((b as ResultWithSource).source as Document).number as bigint));
        expect(response.results).toHaveLength(2);
        expect(((response.results[0] as ResultWithSource).source as Document).number).toEqual(1n);
        expect(((response.results[1] as ResultWithSource).source as Document).number).toEqual(2n);

      });
    })

    describe('Memory compare query', () => {
      it('Can query by memory', async () => {
        const numberToMatch = 123;

        let doc2 = new Document({
          id: '8',
          name: 'x',
          number: BigInt(numberToMatch)

        });

        let doc3 = new Document({
          id: '9',
          name: 'y',
          number: BigInt(numberToMatch)
        });

        const bytes = serialize(doc3);
        const numberOffset = 26;
        expect(bytes[numberOffset]).toEqual(numberToMatch);
        await writeStore.docs.put(doc2);
        await writeStore.docs.put(doc3);

        let response: Results = undefined as any;

        //await otherPeer.node.swarm.connect((await creatorPeer.node.id()).addresses[0].toString());
        await stores[1].docs.index.search.query(new PageQueryRequest({
          queries: [new MemoryCompareQuery({
            compares: [new MemoryCompare({
              bytes: new Uint8Array([123, 0, 0]), // add some 0  trailing so we now we can match more than the exact value
              offset: BigInt(numberOffset)
            })]
          })]
        }), (r: Results) => {
          response = r;
        }, { waitForAmount: 1 })
        expect(response.results).toHaveLength(2);
        expect(((response.results[0] as ResultWithSource).source as Document).id).toEqual(doc2.id);
        expect(((response.results[1] as ResultWithSource).source as Document).id).toEqual(doc3.id);


      });


    })
    describe('Encryption query', () => {

      it('can query by payload key', async () => {

        const someKey = await X25519PublicKey.create();

        let doc = new Document({
          id: 'encrypted',
          name: 'encrypted'
        });

        // write from 1 
        const entry = await stores[1].docs.put(doc, { reciever: { payload: [someKey], clock: undefined, signature: undefined } });
        expect((stores[1].docs.store.oplog.heads[0]._payload as EncryptedThing<any>)._decrypted).toBeDefined()
        delete (stores[1].docs.store.oplog.heads[0]._payload as EncryptedThing<any>)._decrypted
        expect((stores[1].docs.store.oplog.heads[0]._payload as EncryptedThing<any>)._decrypted).toBeUndefined()
        const preLength = writeStore.docs.store.oplog.values.length;
        await writeStore.docs.store.sync(stores[1].docs.store.oplog.heads);
        await waitFor(() => writeStore.docs.store.oplog.values.length === preLength + 1);
        await waitFor(() => writeStore.docs.logIndex.store.oplog.values.length === preLength + 1);
        expect((writeStore.docs.store.oplog.heads[0]._payload as EncryptedThing<any>)._decrypted).toBeUndefined()

        let response: HeadsMessage = undefined as any;

        // read from observer 2
        await stores[2].docs.logIndex.query.query(new LogQueryRequest({
          queries: [new LogEntryEncryptionQuery({
            payload: [someKey],
            clock: [],
            signature: []
          })]
        }), (r: HeadsMessage) => {
          response = r;
        }, { waitForAmount: 1 })
        expect(response.heads).toHaveLength(1);
        expect((response.heads[0].hash)).toEqual(entry.hash);


      });


    })
  })
})
