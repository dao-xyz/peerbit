
import { field, option, variant } from '@dao-xyz/borsh';
import BN from 'bn.js';
import { BinaryDocumentStore, BINARY_DOCUMENT_STORE_TYPE, DocumentStoreOptions } from '../document-store';
import { Compare, CompareQuery, QueryRequestV0, QueryResponse, Sort, SortDirection, StringMatchQuery } from '../query';
import { Peer, waitFor } from './utils';
import { disconnectPeers, getPeer } from './utils';

@variant(0)
class Document {

  @field({ type: 'String' })
  id: string;

  @field({ type: option('String') })
  name?: string;

  @field({ type: option('u64') })
  number?: BN;


  constructor(opts?: Document) {

    if (opts) {
      Object.assign(this, opts);
    }
  }
}


const documentDbTestSetup = async (): Promise<{
  creator: Peer,
  observer: Peer,
  documentStoreCreator: BinaryDocumentStore<Document>
  documentStoreObserver: BinaryDocumentStore<Document>
}> => {


  let peer = await getPeer();
  let observer = await getPeer();

  // Create store
  let documentStoreCreator = await peer.orbitDB.open<BinaryDocumentStore<Document>>('store', { ...{ clazz: Document, create: true, type: BINARY_DOCUMENT_STORE_TYPE, indexBy: 'id', subscribeToQueries: true } as DocumentStoreOptions<Document> })
  await documentStoreCreator.load();
  let documentStoreObserver = await observer.orbitDB.open<BinaryDocumentStore<Document>>(documentStoreCreator.address.toString(), { ...{ clazz: Document, create: true, type: BINARY_DOCUMENT_STORE_TYPE, indexBy: 'id', subscribeToQueries: false, replicate: false } as DocumentStoreOptions<Document> })

  expect(await peer.node.pubsub.ls()).toHaveLength(2); // replication and query topic
  expect(await observer.node.pubsub.ls()).toHaveLength(0);

  return {
    creator: peer,
    observer,
    documentStoreCreator,
    documentStoreObserver
  }
}



describe('query', () => {

  test('string', async () => {

    let {
      creator,
      observer,
      documentStoreCreator,
      documentStoreObserver
    } = await documentDbTestSetup();

    let blocks = documentStoreCreator;

    let doc = new Document({
      id: '1',
      name: 'Hello world'
    });
    let doc2 = new Document({
      id: '2',
      name: 'Foo bar'
    });
    await blocks.put(doc);
    await blocks.put(doc2);

    let response: QueryResponse<Document> = undefined;

    //await otherPeer.node.swarm.connect((await creatorPeer.node.id()).addresses[0].toString());
    await documentStoreObserver.queryAny(new QueryRequestV0({
      queries: [new StringMatchQuery({
        key: 'name',
        value: 'ello'
      })]
    }), Document, (r: QueryResponse<Document>) => {
      response = r;
    })
    await waitFor(() => !!response);
    expect(response.results).toHaveLength(1);
    expect(response.results[0]).toMatchObject(doc);
    await disconnectPeers([creator, observer]);

  });

  test('offset size', async () => {

    let {
      creator,
      observer,
      documentStoreCreator,
      documentStoreObserver
    } = await documentDbTestSetup();

    let blocks = documentStoreCreator;

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

    await blocks.put(doc);
    await blocks.put(doc2);
    await blocks.put(doc3);

    let response: QueryResponse<Document> = undefined;

    //await otherPeer.node.swarm.connect((await creatorPeer.node.id()).addresses[0].toString());
    await documentStoreObserver.queryAny(new QueryRequestV0({
      queries: [new StringMatchQuery({
        key: 'name',
        value: 'hey'
      })],
      size: new BN(1),
      offset: new BN(1)
    }), Document, (r: QueryResponse<Document>) => {
      response = r;
    })
    await waitFor(() => !!response);
    expect(response.results).toHaveLength(1);
    expect(response.results[0]).toMatchObject(doc2);
    await disconnectPeers([creator, observer]);

  });

  describe('sort', () => {
    test('sort offset ascending', async () => {

      let {
        creator,
        observer,
        documentStoreCreator,
        documentStoreObserver
      } = await documentDbTestSetup();

      let blocks = documentStoreCreator;

      let doc = new Document({
        id: '1',
        name: 'hey',
        number: new BN(1)
      });

      let doc2 = new Document({
        id: '2',
        name: 'hey',
        number: new BN(2)

      });

      let doc3 = new Document({
        id: '3',
        name: 'hey',
        number: new BN(3)
      });

      await blocks.put(doc);
      await blocks.put(doc2);
      await blocks.put(doc3);

      let response: QueryResponse<Document> = undefined;

      //await otherPeer.node.swarm.connect((await creatorPeer.node.id()).addresses[0].toString());
      await documentStoreObserver.queryAny(new QueryRequestV0({
        queries: [new StringMatchQuery({
          key: 'name',
          value: 'hey'
        })],
        offset: new BN(1),
        sort: new Sort({
          fieldPath: ['number'],
          direction: SortDirection.Ascending
        })
      }), Document, (r: QueryResponse<Document>) => {
        response = r;
      })
      await waitFor(() => !!response);
      expect(response.results).toHaveLength(2);
      expect(response.results[0].id).toEqual(doc2.id);
      expect(response.results[1].id).toEqual(doc3.id);
      await disconnectPeers([creator, observer]);

    });


    test('sort offset descending', async () => {

      let {
        creator,
        observer,
        documentStoreCreator,
        documentStoreObserver
      } = await documentDbTestSetup();

      let blocks = documentStoreCreator;

      let doc = new Document({
        id: '1',
        name: 'hey',
        number: new BN(1)
      });
      let doc2 = new Document({
        id: '2',
        name: 'hey',
        number: new BN(2)

      });

      let doc3 = new Document({
        id: '3',
        name: 'hey',
        number: new BN(3)

      });

      await blocks.put(doc);
      await blocks.put(doc2);
      await blocks.put(doc3);

      let response: QueryResponse<Document> = undefined;

      //await otherPeer.node.swarm.connect((await creatorPeer.node.id()).addresses[0].toString());
      await documentStoreObserver.queryAny(new QueryRequestV0({
        queries: [new StringMatchQuery({
          key: 'name',
          value: 'hey'
        })],
        offset: new BN(1),
        sort: new Sort({
          fieldPath: ['number'],
          direction: SortDirection.Descending
        })
      }), Document, (r: QueryResponse<Document>) => {
        response = r;
      })
      await waitFor(() => !!response);
      expect(response.results).toHaveLength(2);
      expect(response.results[0].id).toEqual(doc2.id);
      expect(response.results[1].id).toEqual(doc.id);
      await disconnectPeers([creator, observer]);

    });
  })



  describe('number', () => {
    test('equal', async () => {

      let {
        creator,
        observer,
        documentStoreCreator,
        documentStoreObserver
      } = await documentDbTestSetup();

      let blocks = documentStoreCreator;

      let doc = new Document({
        id: '1',
        number: new BN(1)
      });

      let doc2 = new Document({
        id: '2',
        number: new BN(2)
      });


      let doc3 = new Document({
        id: '3',
        number: new BN(3)
      });

      await blocks.put(doc);
      await blocks.put(doc2);
      await blocks.put(doc3);

      let response: QueryResponse<Document> = undefined;
      await documentStoreObserver.queryAny(new QueryRequestV0({
        queries: [new CompareQuery({
          key: 'number',
          compare: Compare.Equal,
          value: new BN(2)
        })]
      }), Document, (r: QueryResponse<Document>) => {
        response = r;
      })
      await waitFor(() => !!response);
      expect(response.results).toHaveLength(1);
      expect(response.results[0].number.toNumber()).toEqual(2);
      await disconnectPeers([creator, observer]);
    });


    test('gt', async () => {

      let {
        creator,
        observer,
        documentStoreCreator,
        documentStoreObserver
      } = await documentDbTestSetup();

      let blocks = documentStoreCreator;

      let doc = new Document({
        id: '1',
        number: new BN(1)
      });

      let doc2 = new Document({
        id: '2',
        number: new BN(2)
      });


      let doc3 = new Document({
        id: '3',
        number: new BN(3)
      });

      await blocks.put(doc);
      await blocks.put(doc2);
      await blocks.put(doc3);

      let response: QueryResponse<Document> = undefined;
      await documentStoreObserver.queryAny(new QueryRequestV0({
        queries: [new CompareQuery({
          key: 'number',
          compare: Compare.Greater,
          value: new BN(2)
        })]
      }), Document, (r: QueryResponse<Document>) => {
        response = r;
      })
      await waitFor(() => !!response);
      expect(response.results).toHaveLength(1);
      expect(response.results[0].number.toNumber()).toEqual(3);
      await disconnectPeers([creator, observer]);
    });

    test('gte', async () => {

      let {
        creator,
        observer,
        documentStoreCreator,
        documentStoreObserver
      } = await documentDbTestSetup();

      let blocks = documentStoreCreator;

      let doc = new Document({
        id: '1',
        number: new BN(1)
      });

      let doc2 = new Document({
        id: '2',
        number: new BN(2)
      });


      let doc3 = new Document({
        id: '3',
        number: new BN(3)
      });

      await blocks.put(doc);
      await blocks.put(doc2);
      await blocks.put(doc3);

      let response: QueryResponse<Document> = undefined;
      await documentStoreObserver.queryAny(new QueryRequestV0({
        queries: [new CompareQuery({
          key: 'number',
          compare: Compare.GreaterOrEqual,
          value: new BN(2)
        })]
      }), Document, (r: QueryResponse<Document>) => {
        response = r;
      })
      await waitFor(() => !!response);
      response.results.sort((a, b) => a.number.cmp(b.number));
      expect(response.results).toHaveLength(2);
      expect(response.results[0].number.toNumber()).toEqual(2);
      expect(response.results[1].number.toNumber()).toEqual(3);
      await disconnectPeers([creator, observer]);
    });

    test('lt', async () => {

      let {
        creator,
        observer,
        documentStoreCreator,
        documentStoreObserver
      } = await documentDbTestSetup();

      let blocks = documentStoreCreator;

      let doc = new Document({
        id: '1',
        number: new BN(1)
      });

      let doc2 = new Document({
        id: '2',
        number: new BN(2)
      });


      let doc3 = new Document({
        id: '3',
        number: new BN(3)
      });

      await blocks.put(doc);
      await blocks.put(doc2);
      await blocks.put(doc3);

      let response: QueryResponse<Document> = undefined;
      await documentStoreObserver.queryAny(new QueryRequestV0({
        queries: [new CompareQuery({
          key: 'number',
          compare: Compare.Less,
          value: new BN(2)
        })]
      }), Document, (r: QueryResponse<Document>) => {
        response = r;
      })
      await waitFor(() => !!response);
      expect(response.results).toHaveLength(1);
      expect(response.results[0].number.toNumber()).toEqual(1);
      await disconnectPeers([creator, observer]);
    });

    test('lte', async () => {

      let {
        creator,
        observer,
        documentStoreCreator,
        documentStoreObserver
      } = await documentDbTestSetup();

      let blocks = documentStoreCreator;

      let doc = new Document({
        id: '1',
        number: new BN(1)
      });

      let doc2 = new Document({
        id: '2',
        number: new BN(2)
      });


      let doc3 = new Document({
        id: '3',
        number: new BN(3)
      });

      await blocks.put(doc);
      await blocks.put(doc2);
      await blocks.put(doc3);

      let response: QueryResponse<Document> = undefined;
      await documentStoreObserver.queryAny(new QueryRequestV0({
        queries: [new CompareQuery({
          key: 'number',
          compare: Compare.LessOrEqual,
          value: new BN(2)
        })]
      }), Document, (r: QueryResponse<Document>) => {
        response = r;
      })
      await waitFor(() => !!response);
      response.results.sort((a, b) => a.number.cmp(b.number));
      expect(response.results).toHaveLength(2);
      expect(response.results[0].number.toNumber()).toEqual(1);
      expect(response.results[1].number.toNumber()).toEqual(2);
      await disconnectPeers([creator, observer]);
    });
  })
}) 