
import { deserialize, field, option, serialize, variant } from '@dao-xyz/borsh';
import { DDocuments } from '../document-store';
import { DocumentQueryRequest, Compare, FieldBigIntCompareQuery, SortDirection, FieldStringMatchQuery, ResultWithSource, FieldSort, MemoryCompareQuery, MemoryCompare, Results, DSearch } from '@dao-xyz/peerbit-dsearch';
import { CustomBinaryPayload } from '@dao-xyz/peerbit-bpayload';
import { QueryRequestV0, query, QueryOptions, DQuery } from '@dao-xyz/peerbit-dquery';
import { Session, createStore } from '@dao-xyz/peerbit-test-utils';
import { DefaultOptions } from '@dao-xyz/peerbit-store';
import { Identity } from '@dao-xyz/ipfs-log';
import { Ed25519Keypair } from '@dao-xyz/peerbit-crypto';
import { IPFS } from 'ipfs-core-types';
import Cache from '@dao-xyz/peerbit-cache';
import { Level } from 'level';
import { fileURLToPath } from 'url';
import path from 'path';
import { Program } from '@dao-xyz/peerbit-program';

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

const mquery = (ipfs: IPFS, topic: string, request: DocumentQueryRequest, responseHandler: (results: Results) => void, options: QueryOptions | undefined) => (
  query(ipfs, topic, new QueryRequestV0({
    query: serialize(request)
  }), (response) => {
    const results = deserialize(response.response, Results);
    responseHandler(results);
  }, options)
)


describe('index', () => {
  let session: Session, observer: IPFS, writer: IPFS, writeStore: DocumentDDoc, observerStore: DocumentDDoc, cacheStore1: Level, cacheStore2: Level

  beforeAll(async () => {
    session = await Session.connected(2)
    observer = session.peers[0].ipfs;
    writer = session.peers[1].ipfs;
    cacheStore1 = await createStore(path.join(__filename, 'cache1'));
    cacheStore2 = await createStore(path.join(__filename, 'cache2'));

  })

  beforeEach(async () => {

    const createIdentity = async () => {
      const ed = await Ed25519Keypair.create();
      return {
        publicKey: ed.publicKey,
        sign: (data) => ed.sign(data)
      } as Identity
    }

    // Create store
    writeStore = new DocumentDDoc({
      docs: new DDocuments<Document>({
        search: new DSearch({
          query: new DQuery({
            queryRegion: 'world'
          })
        }),
        indexBy: 'id'
      })
    });
    (await writeStore.init(writer, await createIdentity(), { store: { ...DefaultOptions, resolveCache: () => new Cache(cacheStore1) } }));
    observerStore = await DocumentDDoc.load(session.peers[1].ipfs, writeStore.address) as DocumentDDoc
    observerStore.docs.search._query.subscribeToQueries = false;
    (await observerStore.init(observer, await createIdentity(), { store: { ...DefaultOptions, resolveCache: () => new Cache(cacheStore2) } }))
  })

  afterAll(async () => {
    await cacheStore1.close();
    await cacheStore2.close();
    await session.stop();
  })

  describe('operations', () => {

    it('can add and delete', async () => {
      let doc = new Document({
        id: '1',
        name: 'Hello world'
      });
      let doc2 = new Document({
        id: '2',
        name: 'Hello world'
      });

      const putOperation = await writeStore.docs.put(doc);
      expect(Object.keys(writeStore.docs._index._index)).toHaveLength(1);
      const putOperation2 = await writeStore.docs.put(doc2);
      expect(Object.keys(writeStore.docs._index._index)).toHaveLength(2);
      expect(putOperation2.next).toContainAllValues([]); // because doc 2 is independent of doc 1

      // delete 1
      const deleteOperation = await writeStore.docs.del(doc.id);
      expect(deleteOperation.next).toContainAllValues([putOperation.hash]); // because delete is dependent on put
      expect(Object.keys(writeStore.docs._index._index)).toHaveLength(1);
    })
  })


  describe('query', () => {

    it('match all', async () => {

      let doc = new Document({
        id: '1',
        name: 'Hello world'
      });
      let doc2 = new Document({
        id: '2',
        name: 'Foo bar'
      });
      await writeStore.docs.put(doc);
      await writeStore.docs.put(doc2);

      let response: Results = undefined as any;

      //await otherPeer.node.swarm.connect((await creatorPeer.node.id()).addresses[0].toString());
      await observerStore.docs.search.query(new DocumentQueryRequest({
        queries: []
      }), (r: Results) => {
        response = r;
      }, { waitForAmount: 1 })
      expect(response.results).toHaveLength(2);
      expect(((response.results[0]) as ResultWithSource).source).toMatchObject(doc);
      expect(((response.results[1]) as ResultWithSource).source).toMatchObject(doc2);


    });

    it('string', async () => {

      let doc = new Document({
        id: '1',
        name: 'Hello world'
      });
      let doc2 = new Document({
        id: '2',
        name: 'Foo bar'
      });
      await writeStore.docs.put(doc);
      await writeStore.docs.put(doc2);

      let response: Results = undefined as any;

      //await otherPeer.node.swarm.connect((await creatorPeer.node.id()).addresses[0].toString());
      await observerStore.docs.search.query(new DocumentQueryRequest({
        queries: [new FieldStringMatchQuery({
          key: 'name',
          value: 'ello'
        })]

      }), (r: Results) => {
        response = r;
      }, { waitForAmount: 1 })
      expect(response.results).toHaveLength(1);
      expect(((response.results[0]) as ResultWithSource).source).toMatchObject(doc);
    });

    it('offset size', async () => {

      let doc = new Document({
        id: '1',
        name: 'hey'
      });
      let doc2 = new Document({
        id: '2',
        name: 'hey'
      });

      let doc3 = new Document({
        id: '3',
        name: 'hey'
      });

      await writeStore.docs.put(doc);
      await writeStore.docs.put(doc2);
      await writeStore.docs.put(doc3);

      let response: Results = undefined as any;

      //await otherPeer.node.swarm.connect((await creatorPeer.node.id()).addresses[0].toString());
      await observerStore.docs.search.query(new DocumentQueryRequest({
        queries: [new FieldStringMatchQuery({
          key: 'name',
          value: 'hey'
        })],
        size: 1n,
        offset: 1n
      })
        ,
        (r: Results) => {
          response = r;
        }, { waitForAmount: 1 })
      expect(response.results).toHaveLength(1);
      expect(((response.results[0]) as ResultWithSource).source).toMatchObject(doc2);
    });

    describe('sort', () => {
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
        await observerStore.docs.search.query(new DocumentQueryRequest({
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
        await observerStore.docs.search.query(new DocumentQueryRequest({
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
    })
    describe('number', () => {
      it('equal', async () => {

        let doc = new Document({
          id: '1',
          number: 1n
        });

        let doc2 = new Document({
          id: '2',
          number: 2n
        });


        let doc3 = new Document({
          id: '3',
          number: 3n
        });

        await writeStore.docs.put(doc);
        await writeStore.docs.put(doc2);
        await writeStore.docs.put(doc3);

        let response: Results = undefined as any;
        await observerStore.docs.search.query(new DocumentQueryRequest({
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

        let doc = new Document({
          id: '1',
          number: 1n
        });

        let doc2 = new Document({
          id: '2',
          number: 2n
        });


        let doc3 = new Document({
          id: '3',
          number: 3n
        });

        await writeStore.docs.put(doc);
        await writeStore.docs.put(doc2);
        await writeStore.docs.put(doc3);

        let response: Results = undefined as any;
        await observerStore.docs.search.query(new DocumentQueryRequest({
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

        let doc = new Document({
          id: '1',
          number: 1n
        });

        let doc2 = new Document({
          id: '2',
          number: 2n
        });


        let doc3 = new Document({
          id: '3',
          number: 3n
        });

        await writeStore.docs.put(doc);
        await writeStore.docs.put(doc2);
        await writeStore.docs.put(doc3);

        let response: Results = undefined as any;
        await observerStore.docs.search.query(new DocumentQueryRequest({
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

        let doc = new Document({
          id: '1',
          number: 1n
        });

        let doc2 = new Document({
          id: '2',
          number: 2n
        });


        let doc3 = new Document({
          id: '3',
          number: 3n
        });

        await writeStore.docs.put(doc);
        await writeStore.docs.put(doc2);
        await writeStore.docs.put(doc3);

        let response: Results = undefined as any;
        await observerStore.docs.search.query(new DocumentQueryRequest({
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

        let doc = new Document({
          id: '1',
          number: 1n
        });

        let doc2 = new Document({
          id: '2',
          number: 2n
        });

        let doc3 = new Document({
          id: '3',
          number: 3n
        });

        await writeStore.docs.put(doc);
        await writeStore.docs.put(doc2);
        await writeStore.docs.put(doc3);

        let response: Results = undefined as any;
        await observerStore.docs.search.query(new DocumentQueryRequest({
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
        let doc = new Document({
          id: '1',
          name: 'a',
          number: 1n
        });

        let doc2 = new Document({
          id: '2',
          name: 'b',
          number: BigInt(numberToMatch)

        });

        let doc3 = new Document({
          id: '3',
          name: 'c',
          number: BigInt(numberToMatch)
        });

        const bytes = serialize(doc3);
        const numberOffset = 26;
        expect(bytes[numberOffset]).toEqual(numberToMatch);
        await writeStore.docs.put(doc);
        await writeStore.docs.put(doc2);
        await writeStore.docs.put(doc3);

        let response: Results = undefined as any;

        //await otherPeer.node.swarm.connect((await creatorPeer.node.id()).addresses[0].toString());
        await observerStore.docs.search.query(new DocumentQueryRequest({
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
  })
})
