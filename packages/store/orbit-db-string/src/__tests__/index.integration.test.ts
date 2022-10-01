
import { StringResultSource, StringStore, STRING_STORE_TYPE } from '../string-store.js';
import { QueryRequestV0, QueryResponseV0, ResultWithSource, StringQueryRequest, StringMatchQuery, RangeCoordinate, RangeCoordinates, StoreAddressMatchQuery } from '@dao-xyz/query-protocol';
import { query } from '@dao-xyz/orbit-db-query-store';
import { Range } from '../range.js';
import { Session } from '@dao-xyz/orbit-db-test-utils';
import { IPFSAccessController } from '@dao-xyz/orbit-db-ipfs-access-controller';

const storeTestSetup = async (): Promise<{
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
    const observerSubscriptions = await observer.node.pubsub.ls();
    expect(observerSubscriptions).toHaveLength(0);

    return {
        creator: peer,
        observer,
        storeCreator
    }
}



describe('query', () => {

    it('only context', async () => {
        let {
            creator,
            observer,
            storeCreator
        } = await storeTestSetup();

        let blocks = storeCreator;
        await blocks.add('hello', new Range({ offset: 0n }));
        await blocks.add('world', new Range({ offset: BigInt('hello '.length) }));

        let response: QueryResponseV0 = undefined;

        await query(observer.node.pubsub, blocks.queryTopic, new QueryRequestV0({
            type: new StringQueryRequest({
                queries: [
                    new StoreAddressMatchQuery({
                        address: blocks.address.toString()
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
        await disconnectPeers([creator, observer]);

    });

    it('match all', async () => {
        let {
            creator,
            observer,
            storeCreator
        } = await storeTestSetup();

        let blocks = storeCreator;
        await blocks.add('hello', new Range({ offset: 0n }));
        await blocks.add('world', new Range({ offset: BigInt('hello '.length) }));

        let response: QueryResponseV0 = undefined;

        await query(observer.node.pubsub, blocks.queryTopic, new QueryRequestV0({
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
        await disconnectPeers([creator, observer]);

    });

    it('match part', async () => {
        let {
            creator,
            observer,
            storeCreator
        } = await storeTestSetup();

        let blocks = storeCreator;
        await blocks.add('hello', new Range({ offset: 0n }));
        await blocks.add('world', new Range({ offset: BigInt('hello '.length) }));

        let response: QueryResponseV0 = undefined;

        await query(observer.node.pubsub, blocks.queryTopic, new QueryRequestV0({
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
        await disconnectPeers([creator, observer]);

    });



}) 