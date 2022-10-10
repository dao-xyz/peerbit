
import { StringResultSource, StringStore, STRING_STORE_TYPE } from '../string-store.js';
import { QueryRequestV0, QueryResponseV0, ResultWithSource, StringQueryRequest, StringMatchQuery, RangeCoordinate, RangeCoordinates, StoreAddressMatchQuery } from '@dao-xyz/query-protocol';
import { query, ReadWriteAccessController } from '@dao-xyz/orbit-db-query-store';
import { Range } from '../range.js';
import { createStore, Session } from '@dao-xyz/orbit-db-test-utils';
import { IPFS } from 'ipfs-core-types';
import { Level } from 'level';;
import Cache from '@dao-xyz/orbit-db-cache';
import { fileURLToPath } from 'url';
import path from 'path';
import { delay } from '@dao-xyz/time'
import { Identity } from '@dao-xyz/ipfs-log';
import { Ed25519Keypair } from '@dao-xyz/peerbit-crypto';
import { AccessController, DefaultOptions } from '@dao-xyz/orbit-db-store';
import { variant } from '@dao-xyz/borsh';

const __filename = fileURLToPath(import.meta.url);
const __filenameBase = path.parse(__filename).base;
/* const storeTestSetup = async (): Promise<{
    creator: Peer,
    observer: Peer,
    storeCreator: StringStore
}> => {

    const session = await Session.connected(2)

    let [peer, observer] = session.peers;

    // Create store
    const accessController = new IPFSAccessController({
        write: ['*']
    });
    accessController.allowAll = true;
    const store = new StringStore({
        name: 'store',
        accessController: accessController
    });

    store.queryRegion = 'world';

    let storeCreator = await peer.orbitDB.open<StringStore>(store)
    await storeCreator.load();
    await storeCreator._initializationPromise;

    expect(await peer.node.pubsub.ls()).toHaveLength(2); // replication and query topic
    const observerSubscriptions = await observer.ls();
    expect(observerSubscriptions).toHaveLength(0);

    return {
        creator: peer,
        observer,
        storeCreator
    }
}
 */

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


    let session: Session, observer: IPFS, writer: IPFS, writeStore: StringStore, observerStore: StringStore, cacheStore1: Level, cacheStore2: Level

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
        writeStore = new StringStore({
            accessController: controller,
            queryRegion: 'world'
        });
        await writeStore.init(writer, await createIdentity(), { ...DefaultOptions, resolveCache: () => new Cache(cacheStore1) });

        const observerStore = await StringStore.load(session.peers[1].ipfs, writeStore.address);
        observerStore.subscribeToQueries = false;
        (observerStore.accessController as AccessController<any>).allowAll = true;
        await observerStore.init(observer, await createIdentity(), { ...DefaultOptions, resolveCache: () => new Cache(cacheStore2) })

    })

    afterAll(async () => {
        await cacheStore1.close();
        await cacheStore2.close();
        await session.stop();
    })

    it('only context', async () => {



        await writeStore.add('hello', new Range({ offset: 0n, length: 'hello'.length }));
        await writeStore.add('world', new Range({ offset: BigInt('hello '.length), length: 'world'.length }));

        let response: QueryResponseV0 = undefined as any;

        await query(observer, writeStore.queryTopic, new QueryRequestV0({
            type: new StringQueryRequest({
                queries: [
                    new StoreAddressMatchQuery({
                        address: writeStore.address.toString()
                    })
                ]
            })
        }), (r: QueryResponseV0) => {
            response = r;
        }, { waitForAmount: 1 })

        expect(response.results).toHaveLength(1);
        expect(((response.results[0]) as ResultWithSource)).toMatchObject(new ResultWithSource({
            source: new StringResultSource({
                string: 'hello world'
            }),
            coordinates: undefined //  because we are matching without any specific query
        }));


    });

    it('match all', async () => {



        await writeStore.add('hello', new Range({ offset: 0n, length: 'hello'.length }));
        await writeStore.add('world', new Range({ offset: BigInt('hello '.length), length: 'world'.length }));

        let response: QueryResponseV0 = undefined as any;

        await query(observer, writeStore.queryTopic, new QueryRequestV0({
            type: new StringQueryRequest({
                queries: []
            })
        }), (r: QueryResponseV0) => {
            response = r;
        }, { waitForAmount: 1 })
        expect(response.results).toHaveLength(1);
        expect(((response.results[0]) as ResultWithSource)).toMatchObject(new ResultWithSource({
            source: new StringResultSource({
                string: 'hello world'
            }),
            coordinates: undefined //  because we are matching without any specific query
        }));


    });

    it('match part', async () => {



        await writeStore.add('hello', new Range({ offset: 0n, length: 'hello'.length }));
        await writeStore.add('world', new Range({ offset: BigInt('hello '.length), length: 'world'.length }));

        let response: QueryResponseV0 = undefined as any;

        await query(observer, writeStore.queryTopic, new QueryRequestV0({
            type: new StringQueryRequest({
                queries: [new StringMatchQuery({
                    exactMatch: true,
                    value: 'o w'
                }),
                new StringMatchQuery({
                    exactMatch: true,
                    value: 'orld'
                }),
                ]
            })
        }), (r: QueryResponseV0) => {
            response = r;
        }, { waitForAmount: 1 })
        expect(response.results).toHaveLength(1);
        let result = ((response.results[0]) as ResultWithSource);
        expect(result.source).toMatchObject(new StringResultSource({
            string: 'hello world'
        }));
        expect((result.coordinates as RangeCoordinates).coordinates).toHaveLength(2);
        expect((result.coordinates as RangeCoordinates).coordinates[0].offset).toEqual(BigInt('hell'.length));
        expect((result.coordinates as RangeCoordinates).coordinates[0].length).toEqual(BigInt('w o'.length));
        expect((result.coordinates as RangeCoordinates).coordinates[1].offset).toEqual(BigInt('hello w'.length));
        expect((result.coordinates as RangeCoordinates).coordinates[1].length).toEqual(BigInt('orld'.length));


    });



}) 