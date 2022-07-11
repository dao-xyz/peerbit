import { field } from "@dao-xyz/borsh";
import { BinaryDocumentStore, BinaryDocumentStoreOptions } from "@dao-xyz/orbit-db-bdocstore";
import { disconnectPeers, getConnectedPeers, getPeer, Peer } from '@dao-xyz/peer-test-utils';
import { DynamicAccessController, DYNAMIC_ACCESS_CONTROLER } from "..";
import { Access, AccessType } from "../access";
import { PublicKeyAccessCondition } from "../condition";
import { delay } from '@dao-xyz/time';
import { AccessError } from "@dao-xyz/ipfs-log";
import { P2PTrust } from "@dao-xyz/orbit-db-trust-web";

class Document {

    @field({ type: 'String' })
    _id: string;

    constructor(props?: { id: string }) {
        if (props) {
            this._id = props.id;
        }
    }
}
const defaultOptions = (trust: P2PTrust, heapSizeLimt = 10000000, onMemoryExceeded?: () => void) => {
    return {
        clazz: Document,
        nameResolver: (n) => n,
        queryRegion: 'x',
        subscribeToQueries: true,
        accessController: {
            type: DYNAMIC_ACCESS_CONTROLER,
            trustResolver: () => trust,
            heapSizeLimit: () => heapSizeLimt,
            onMemoryExceeded
        },
        cache: undefined,
        create: true,
        replicate: true,
        typeMap: {
            [Document.name]: Document
        }
    }
};

const getTrust = async (peer: Peer) => {
    const trust = new P2PTrust({
        rootTrust: peer.orbitDB.identity
    });
    await trust.init(peer.orbitDB, defaultOptions(trust));
    await trust.load();
    return trust
}

const loadTrust = async (peer: Peer, cid: string) => {
    const trust = await P2PTrust.loadFromCID(cid, peer.node)
    await trust.init(peer.orbitDB, defaultOptions(trust));
    await trust.load();
    return trust
}
describe('index', () => {

    test('can write from trust web', async () => {
        const [peer, peer2] = await getConnectedPeers(2)
        const l0aTrust = await getTrust(peer);
        let options = new BinaryDocumentStoreOptions({ indexBy: '_id', objectType: Document.name });
        const l0a = await options.newStore('test', peer.orbitDB, defaultOptions(l0aTrust))
        await l0a.put(new Document({
            id: '1'
        }));


        const l0b = await options.newStore(l0a.address.toString(), peer2.orbitDB, defaultOptions(await loadTrust(peer2, l0aTrust.cid)));
        await expect(l0b.put(new Document({
            id: 'id'
        }))).rejects.toBeInstanceOf(AccessError); // Not trusted

        await (l0a.access as DynamicAccessController<Document, any>).trust.addTrust(peer2.orbitDB.identity);
        await (l0b.access as DynamicAccessController<Document, any>).trust.load(1);
        await l0b.put(new Document({
            id: '2'
        })) // Now trusted 

        await disconnectPeers([peer, peer2])
    })


    test('can write from acl', async () => {
        const [peer, peer2] = await getConnectedPeers(2)
        const l0aTrust = await getTrust(peer);
        let options = new BinaryDocumentStoreOptions({ indexBy: '_id', objectType: Document.name });
        const l0a = await options.newStore('test', peer.orbitDB, defaultOptions(l0aTrust))
        await l0a.put(new Document({
            id: '1'
        }));


        const l0b = await options.newStore(l0a.address.toString(), peer2.orbitDB, defaultOptions(await loadTrust(peer2, l0aTrust.cid)));
        await expect(l0b.put(new Document({
            id: 'id'
        }))).rejects.toBeInstanceOf(AccessError); // Not trusted


        await (l0a.access as DynamicAccessController<Document, any>).aclDB.db.put(new Access({
            accessCondition: new PublicKeyAccessCondition({
                key: peer2.orbitDB.identity.id,
                type: peer2.orbitDB.identity.type
            }),
            accessTypes: [AccessType.Admin]
        }).initialize());

        await (l0b.access as DynamicAccessController<Document, any>).aclDB.load(1);
        await l0b.put(new Document({
            id: '2'
        })) // Now trusted 

        await disconnectPeers([peer, peer2])
    })

    test('append all', async () => {
        const [peer, peer2] = await getConnectedPeers(2)
        const l0aTrust = await getTrust(peer);
        let options = new BinaryDocumentStoreOptions({ indexBy: '_id', objectType: Document.name });
        const l0a = await options.newStore('test', peer.orbitDB, defaultOptions(l0aTrust))
        await l0a.put(new Document({
            id: '1'
        }));

        const l0b = await options.newStore(l0a.address.toString(), peer2.orbitDB, {
            ...defaultOptions(await loadTrust(peer2, l0aTrust.cid)), accessController: {
                type: DYNAMIC_ACCESS_CONTROLER,
                appendAll: true
            }
        });

        await l0b.put(new Document({
            id: '2'
        })) // Now trusted because append all is 'true'

        // but entry will not be replicated on l0a since it still respects ACL
        await delay(5000); // Arbritary delay
        expect(Object.keys(l0a.index._index)).toHaveLength(1);
        await disconnectPeers([peer, peer2])
    })

    test('on memory exceeded', async () => {

        const peer = await getPeer()
        const l0aTrust = await getTrust(peer);
        let options = new BinaryDocumentStoreOptions({ indexBy: '_id', objectType: Document.name });
        let memoryExceeded = false;
        const l0a = await options.newStore('test', peer.orbitDB, defaultOptions(l0aTrust, 0, () => memoryExceeded = true))
        await expect(l0a.put(new Document({
            id: '1'
        }))).rejects.toBeInstanceOf(AccessError);
        expect(memoryExceeded);
        await disconnectPeers([peer])
    })


    test('manifests are unique', async () => {

        const [peer] = await getConnectedPeers(1)
        const l0aTrust = await getTrust(peer);
        let options = new BinaryDocumentStoreOptions({ indexBy: '_id', objectType: Document.name });
        const l0a = await options.newStore('test', peer.orbitDB, defaultOptions(l0aTrust))
        const l0b = await options.newStore('test-2', peer.orbitDB, defaultOptions(l0aTrust))
        expect(l0a.address).not.toEqual(l0b.address)
        expect((l0a.access as DynamicAccessController<Document, any>).aclDB.address).not.toEqual((l0b.access as DynamicAccessController<Document, any>).aclDB.address)
        await disconnectPeers([peer])

    })


})