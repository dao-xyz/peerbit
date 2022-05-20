
import { ShardedDB } from '../index';
import fs from 'fs';
import Identities from 'orbit-db-identity-provider';
import { Keypair } from '@solana/web3.js';
import { SolanaIdentityProvider } from '../identity-providers/solana-identity-provider';
import { ShardChain, TypedBehaviours } from '../shard';
import FeedStore from 'orbit-db-feedstore';
import { BinaryDocumentStore } from '@dao-xyz/orbit-db-bdocstore';
import { BinaryDocumentStoreOptions, FeedStoreOptions } from '../stores';
import { Constructor, field, option, variant } from '@dao-xyz/borsh';
import BN from 'bn.js';
import { Compare, CompareQuery, QueryRequestV0, QueryResponse, StringMatchQuery } from '../query';
import { delay, waitFor } from '../utils';



const testBehaviours: TypedBehaviours = {
    typeMap: {}
}


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
}

const documentDbTestSetup = async<T>(clazz: Constructor<T>, indexBy: string, shardSize = new BN(100000)): Promise<{
    creatorPeer: ShardedDB,
    otherPeer: ShardedDB,
    shardChain: ShardChain<BinaryDocumentStore<T>>

}> => {
    let peers = await getPeers(2, 1, {
        ...testBehaviours,
        typeMap: {
            [clazz.name]: clazz
        }
    });
    let creatorPeer = peers[0];
    await creatorPeer.subscribeForReplication();

    //  --- Create
    let rootChains = creatorPeer.shardChainChain;

    // Create Root shard
    await rootChains.addPeerToShards();
    expect(rootChains.shardCounter.value).toEqual(1);

    // Create Feed store
    let options = new BinaryDocumentStoreOptions<T>({
        indexBy,
        objectType: clazz.name
    });

    let chain = await creatorPeer.createShardChain("test", options, shardSize);
    await chain.addPeerToShards();
    return {
        creatorPeer,
        otherPeer: peers[1],
        shardChain: chain
    }
}

const feedStoreTestSetup = async<T>(shardSize = new BN(100000)): Promise<{
    creatorPeer: ShardedDB,
    otherPeer: ShardedDB,
    shardChain: ShardChain<FeedStore<T>>

}> => {
    let peers = await getPeers(2, 1, {
        ...testBehaviours
    });
    let creatorPeer = peers[0];
    await creatorPeer.subscribeForReplication();

    //  --- Create
    let rootChains = creatorPeer.shardChainChain;

    // Create Root shard
    await rootChains.addPeerToShards();
    expect(rootChains.shardCounter.value).toEqual(1);

    // Create Feed store
    let options = new FeedStoreOptions<T>();
    let chain = await creatorPeer.createShardChain("test", options, shardSize);
    await chain.addPeerToShards();
    return {
        creatorPeer,
        otherPeer: peers[1],
        shardChain: chain
    }
}



const disconnectPeers = async (peers: ShardedDB[]): Promise<void> => {
    await Promise.all(peers.map(peer => peer.disconnect()));
}

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
            shardChain
        } = await documentDbTestSetup(Document, 'id');
        let shard = await shardChain.getWritableShard();
        let blocks = await shard.loadBlocks();

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
        await otherPeer.query(shardChain.queryTopic, new QueryRequestV0({
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
                shardChain
            } = await documentDbTestSetup(Document, 'id');
            let shard = await shardChain.getWritableShard();
            let blocks = await shard.loadBlocks();

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
            await otherPeer.query(shardChain.queryTopic, new QueryRequestV0({
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
                shardChain
            } = await documentDbTestSetup(Document, 'id');
            let shard = await shardChain.getWritableShard();
            let blocks = await shard.loadBlocks();

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
            await otherPeer.query(shardChain.queryTopic, new QueryRequestV0({
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
                shardChain
            } = await documentDbTestSetup(Document, 'id');
            let shard = await shardChain.getWritableShard();
            let blocks = await shard.loadBlocks();

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
            await otherPeer.query(shardChain.queryTopic, new QueryRequestV0({
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
                shardChain
            } = await documentDbTestSetup(Document, 'id');
            let shard = await shardChain.getWritableShard();
            let blocks = await shard.loadBlocks();

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
            await otherPeer.query(shardChain.queryTopic, new QueryRequestV0({
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
                shardChain
            } = await documentDbTestSetup(Document, 'id');
            let shard = await shardChain.getWritableShard();
            let blocks = await shard.loadBlocks();

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
            await otherPeer.query(shardChain.queryTopic, new QueryRequestV0({
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