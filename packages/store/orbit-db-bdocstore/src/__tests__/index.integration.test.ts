
import { Constructor, field, option, serialize, variant } from '@dao-xyz/borsh';
import { BinaryDocumentStore } from '../document-store';
import { DocumentQueryRequest, Compare, FieldBigIntCompareQuery, QueryRequestV0, QueryResponseV0, SortDirection, FieldStringMatchQuery, ResultWithSource, FieldSort, MemoryCompareQuery, MemoryCompare } from '@dao-xyz/query-protocol';
import { CustomBinaryPayload } from '@dao-xyz/bpayload';
import { query, ReadWriteAccessController } from '@dao-xyz/orbit-db-query-store';
import { Session, createStore } from '@dao-xyz/orbit-db-test-utils';
import { AccessController, DefaultOptions } from '@dao-xyz/orbit-db-store';
import { Identity } from '@dao-xyz/ipfs-log';
import { Ed25519Keypair } from '@dao-xyz/peerbit-crypto';
import { IPFS } from 'ipfs-core-types';
import Cache from '@dao-xyz/orbit-db-cache';
import { Level } from 'level';
import { fileURLToPath } from 'url';
import path from 'path';
import { delay } from '@dao-xyz/time'

const __filename = fileURLToPath(import.meta.url);
const __filenameBase = path.parse(__filename).base;

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


const bigIntSort = <T extends (number | bigint)>(a: T, b: T): number => (a > b ? 1 : 0 || -(a < b))


@variant([0, 253])
export class SimpleRWAccessController<T> extends ReadWriteAccessController<T>
{
  async canAppend(a: any, b: any) {
    return true;
  }
  async canRead(a: any) {
    return true;
  }
}


describe('query', () => {

  let session: Session, observer: IPFS, writer: IPFS, writeStore: BinaryDocumentStore<Document>, observerStore: BinaryDocumentStore<Document>, cacheStore1: Level, cacheStore2: Level

  beforeAll(async () => {
    session = await Session.connected(2)
    observer = session.peers[0].ipfs;
    writer = session.peers[1].ipfs;
    cacheStore1 = await createStore(__filenameBase + '/cache1')
    cacheStore2 = await createStore(__filenameBase + '/cache2')

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
    const controller = new SimpleRWAccessController<any>();
    controller.allowAll = true;
    writeStore = new BinaryDocumentStore<Document>({
      accessController: controller,
      queryRegion: 'world',
      indexBy: 'id',
      objectType: Document.name
    });
    await writeStore.init(writer, await createIdentity(), { ...DefaultOptions, resolveCache: () => new Cache(cacheStore1), typeMap: { [Document.name]: Document } });

    const observerStore = await BinaryDocumentStore.load(session.peers[1].ipfs, writeStore.address);
    observerStore.subscribeToQueries = false;
    (observerStore.accessController as AccessController<any>).allowAll = true;
    await observerStore.init(observer, await createIdentity(), { ...DefaultOptions, resolveCache: () => new Cache(cacheStore2), typeMap: { [Document.name]: Document } })

  })

  afterAll(async () => {
    await cacheStore1.close();
    await cacheStore2.close();
    await session.stop();
  })

  it('match all', async () => {

    let doc = new Document({
      id: '1',
      name: 'Hello world'
    });
    let doc2 = new Document({
      id: '2',
      name: 'Foo bar'
    });
    await writeStore.put(doc);
    await writeStore.put(doc2);

    let response: QueryResponseV0 = undefined as any;

    //await otherPeer.node.swarm.connect((await creatorPeer.node.id()).addresses[0].toString());
    await query(observer, writeStore.queryTopic, new QueryRequestV0({
      type: new DocumentQueryRequest({
        queries: []
      })
    }), (r: QueryResponseV0) => {
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
    await writeStore.put(doc);
    await writeStore.put(doc2);

    let response: QueryResponseV0 = undefined as any;

    //await otherPeer.node.swarm.connect((await creatorPeer.node.id()).addresses[0].toString());
    await query(observer, writeStore.queryTopic, new QueryRequestV0({
      type: new DocumentQueryRequest({
        queries: [new FieldStringMatchQuery({
          key: 'name',
          value: 'ello'
        })]
      })
    }), (r: QueryResponseV0) => {
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

    await writeStore.put(doc);
    await writeStore.put(doc2);
    await writeStore.put(doc3);

    let response: QueryResponseV0 = undefined as any;

    //await otherPeer.node.swarm.connect((await creatorPeer.node.id()).addresses[0].toString());
    await query(observer, writeStore.queryTopic, new QueryRequestV0({
      type: new DocumentQueryRequest({
        queries: [new FieldStringMatchQuery({
          key: 'name',
          value: 'hey'
        })],
        size: 1n,
        offset: 1n
      })
    }),
      (r: QueryResponseV0) => {
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

      await writeStore.put(doc);
      await writeStore.put(doc2);
      await writeStore.put(doc3);

      let response: QueryResponseV0 = undefined as any;

      //await otherPeer.node.swarm.connect((await creatorPeer.node.id()).addresses[0].toString());
      await query(observer, writeStore.queryTopic, new QueryRequestV0({
        type: new DocumentQueryRequest({
          queries: [new FieldStringMatchQuery({
            key: 'name',
            value: 'hey'
          })],
          offset: 1n,
          sort: new FieldSort({
            key: ['number'],
            direction: SortDirection.Ascending
          })
        })
      }), (r: QueryResponseV0) => {
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

      await writeStore.put(doc);
      await writeStore.put(doc2);
      await writeStore.put(doc3);

      let response: QueryResponseV0 = undefined as any;

      //await otherPeer.node.swarm.connect((await creatorPeer.node.id()).addresses[0].toString());
      await query(observer, writeStore.queryTopic, new QueryRequestV0({
        type: new DocumentQueryRequest({
          queries: [new FieldStringMatchQuery({
            key: 'name',
            value: 'hey'
          })],
          offset: 1n,
          sort: new FieldSort({
            key: ['number'],
            direction: SortDirection.Descending
          })
        })
      }), (r: QueryResponseV0) => {
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

      await writeStore.put(doc);
      await writeStore.put(doc2);
      await writeStore.put(doc3);

      let response: QueryResponseV0 = undefined as any;
      await query(observer, writeStore.queryTopic, new QueryRequestV0({
        type: new DocumentQueryRequest({
          queries: [new FieldBigIntCompareQuery({
            key: 'number',
            compare: Compare.Equal,
            value: 2n
          })]
        })
      }), (r: QueryResponseV0) => {
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

      await writeStore.put(doc);
      await writeStore.put(doc2);
      await writeStore.put(doc3);

      let response: QueryResponseV0 = undefined as any;
      await query(observer, writeStore.queryTopic, new QueryRequestV0({
        type: new DocumentQueryRequest({
          queries: [new FieldBigIntCompareQuery({
            key: 'number',
            compare: Compare.Greater,
            value: 2n
          })]
        })
      }), (r: QueryResponseV0) => {
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

      await writeStore.put(doc);
      await writeStore.put(doc2);
      await writeStore.put(doc3);

      let response: QueryResponseV0 = undefined as any;
      await query(observer, writeStore.queryTopic, new QueryRequestV0({
        type: new DocumentQueryRequest({
          queries: [new FieldBigIntCompareQuery({
            key: 'number',
            compare: Compare.GreaterOrEqual,
            value: 2n
          })]
        })
      }), (r: QueryResponseV0) => {
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

      await writeStore.put(doc);
      await writeStore.put(doc2);
      await writeStore.put(doc3);

      let response: QueryResponseV0 = undefined as any;
      await query(observer, writeStore.queryTopic, new QueryRequestV0({
        type: new DocumentQueryRequest({
          queries: [new FieldBigIntCompareQuery({
            key: 'number',
            compare: Compare.Less,
            value: 2n
          })]
        })
      }), (r: QueryResponseV0) => {
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

      await writeStore.put(doc);
      await writeStore.put(doc2);
      await writeStore.put(doc3);

      let response: QueryResponseV0 = undefined as any;
      await query(observer, writeStore.queryTopic, new QueryRequestV0({
        type: new DocumentQueryRequest({
          queries: [new FieldBigIntCompareQuery({
            key: 'number',
            compare: Compare.LessOrEqual,
            value: 2n
          })]
        })
      }), (r: QueryResponseV0) => {
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
      await writeStore.put(doc);
      await writeStore.put(doc2);
      await writeStore.put(doc3);

      let response: QueryResponseV0 = undefined as any;

      //await otherPeer.node.swarm.connect((await creatorPeer.node.id()).addresses[0].toString());
      await query(observer, writeStore.queryTopic, new QueryRequestV0({
        type: new DocumentQueryRequest({
          queries: [new MemoryCompareQuery({
            compares: [new MemoryCompare({
              bytes: new Uint8Array([123, 0, 0]), // add some 0  trailing so we now we can match more than the exact value
              offset: BigInt(numberOffset)
            })]
          })]
        })
      }), (r: QueryResponseV0) => {
        response = r;
      }, { waitForAmount: 1 })
      expect(response.results).toHaveLength(2);
      expect(((response.results[0] as ResultWithSource).source as Document).id).toEqual(doc2.id);
      expect(((response.results[1] as ResultWithSource).source as Document).id).toEqual(doc3.id);


    });


  })
}) 