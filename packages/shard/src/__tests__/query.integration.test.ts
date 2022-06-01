
import { Shard, AnyPeer } from '../index';
import { TypedBehaviours } from '../shard';
import { Constructor, field, option, variant } from '@dao-xyz/borsh';
import BN from 'bn.js';
import { Compare, CompareQuery, QueryRequestV0, QueryResponse, StringMatchQuery } from '../query';
import { waitFor } from '../utils';
import { disconnectPeers, DocumentStoreInterface, documentStoreShard, getPeer, shardStoreShard } from './utils';





/*
const getPeers = async (amount: number = 1, peerCapacity: number, behaviours: TypedBehaviours = testBehaviours): Promise<ShardedDB[]> => {


    let idx = Array(amount).fill(0).map((_, idx) => idx);
    Identities.addIdentityProvider(SolanaIdentityProvider)
    let keypair = Keypair.generate();
    const rootIdentity = await Identities.createIdentity({ type: 'solana', wallet: keypair.publicKey, keypair: keypair })
    fs.rmSync('./ipfs', { recursive: true, force: true });
    fs.rmSync('./orbitdb', { recursive: true, force: true });
    fs.rmSync('./orbit-db-stores', { recursive: true, force: true });

    let nodeRoots = idx.map(x => './ipfs/' + x);

    const peers = await Promise.all(nodeRoots.map(async (root) => {
        const peer = new ShardedDB();
        await peer.create({ rootAddress: 'root', local: false, repo: root, identity: rootIdentity, behaviours, replicationCapacity: peerCapacity });
        return peer;
    }));

    return peers;
} */

/* const getPeers = async (amount: number = 1, peerCapacity: number, behaviours: TypedBehaviours = testBehaviours): Promise<ShardedDB[]> => {
    let ids = Array(amount).fill(0).map((_) => generateUUID());
    Identities.addIdentityProvider(SolanaIdentityProvider)
    let keypair = Keypair.generate();
    const rootIdentity = await Identities.createIdentity({ type: 'solana', wallet: keypair.publicKey, keypair: keypair })
    for (const id in ids) {
        await clean(id);
    }

    let nodeRoots = ids.map(x => './ipfs/' + x);
    const peers = await Promise.all(nodeRoots.map(async (root) => {
        const peer = new ShardedDB();
        await peer.create({ rootAddress: 'root', local: false, repo: root, identity: rootIdentity, behaviours, replicationCapacity: peerCapacity });
        return peer;
    }));

    return peers;
}
 */


const documentDbTestSetup = async<T>(clazz: Constructor<T>, indexBy: string, shardSize = new BN(100000)): Promise<{
    creatorPeer: AnyPeer,
    otherPeer: AnyPeer,
    documentStore: Shard<DocumentStoreInterface<T>>

}> => {

    let rootAddress = 'root';
    let behaviours: TypedBehaviours = {
        typeMap: {
            [Document.name]: Document
        }
    }
    let peer = await getPeer(rootAddress, behaviours);
    // Create Root shard
    let l0 = await shardStoreShard();
    await l0.init(peer);
    await l0.replicate();

    // Create Feed store
    let documentStore = await (await documentStoreShard(clazz, indexBy)).init(peer, l0.cid);
    await documentStore.replicate();


    return {
        creatorPeer: peer,
        otherPeer: await getPeer(rootAddress, behaviours),
        documentStore
    }
}


/* 
const feedStoreTestSetup = async<T>(shardSize = new BN(100000)): Promise<{
    creatorPeer: AnyPeer,
    otherPeer: AnyPeer

}> => {
   

    let peer = await getPeer();

    // Create Root shard
    let l0 = new RecursiveShard<FeedStore<string>>({
        cluster: 'x',
        shardSize: new BN(500)
    })
    await l0.init(peer)
    await l0.replicate();

    // Create Feed store
    let feedStore = await new Shard<FeedStore<string>>({
        cluster: 'xx',
        shardSize: new BN(500),
        storeOptions: new FeedStoreOptions()
    }).init(peer, l0);
    await feedStore.replicate();
    await (await l0.loadShard(0)).blocks.add("xxx");



    // --- Load assert, from another peer
    let peer2 = await getPeer();
    await l0.init(peer2)

    return {
        creatorPeer: peer,
        otherPeer: peer2
    }
}
 */



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

describe('query', () => {

    test('string', async () => {

        let {
            creatorPeer,
            otherPeer,
            documentStore
        } = await documentDbTestSetup(Document, 'id');

        let blocks = documentStore.interface.db.db;

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
        await otherPeer.query(documentStore.queryTopic, new QueryRequestV0({
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
        await disconnectPeers([creatorPeer, otherPeer]);

    });


    describe('number', () => {
        test('equal', async () => {

            let {
                creatorPeer,
                otherPeer,
                documentStore
            } = await documentDbTestSetup(Document, 'id');

            let blocks = documentStore.interface.db.db;

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
            await otherPeer.query(documentStore.queryTopic, new QueryRequestV0({
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
            await disconnectPeers([creatorPeer, otherPeer]);
        });


        test('gt', async () => {

            let {
                creatorPeer,
                otherPeer,
                documentStore
            } = await documentDbTestSetup(Document, 'id');

            let blocks = documentStore.interface.db.db;

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
            await otherPeer.query(documentStore.queryTopic, new QueryRequestV0({
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
            await disconnectPeers([creatorPeer, otherPeer]);
        });

        test('gte', async () => {

            let {
                creatorPeer,
                otherPeer,
                documentStore
            } = await documentDbTestSetup(Document, 'id');

            let blocks = documentStore.interface.db.db;

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
            await otherPeer.query(documentStore.queryTopic, new QueryRequestV0({
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
            await disconnectPeers([creatorPeer, otherPeer]);
        });

        test('lt', async () => {

            let {
                creatorPeer,
                otherPeer,
                documentStore
            } = await documentDbTestSetup(Document, 'id');

            let blocks = documentStore.interface.db.db;

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
            await otherPeer.query(documentStore.queryTopic, new QueryRequestV0({
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
            await disconnectPeers([creatorPeer, otherPeer]);
        });

        test('lte', async () => {

            let {
                creatorPeer,
                otherPeer,
                documentStore
            } = await documentDbTestSetup(Document, 'id');

            let blocks = documentStore.interface.db.db;

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
            await otherPeer.query(documentStore.queryTopic, new QueryRequestV0({
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
            await disconnectPeers([creatorPeer, otherPeer]);
        });
    })
}) 