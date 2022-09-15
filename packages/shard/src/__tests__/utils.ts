import { AnyPeer, PeerOptions } from '../peer';
import { getPeer as getPeerTest, getConnectedPeers as getConnectedPeersTest, Peer } from '@dao-xyz/peer-test-utils';
import { PublicKey } from '@dao-xyz/identity';
export const getPeer = async (identity?: PublicKey, isServer?: boolean, peerCapacity?: number) => getPeerTest(identity).then((peer) => createAnyPeer(peer, isServer, peerCapacity));
export const getConnectedPeers = async (amountOf: number, isServer?: boolean, peerCapacity?: number) => getConnectedPeersTest(amountOf).then(peers => Promise.all(peers.map(peer => createAnyPeer(peer, isServer, peerCapacity))));
const createAnyPeer = async (peer: Peer, isServer: boolean = true, peerCapacity: number = 1000 * 1000 * 1000): Promise<AnyPeer> => {
    const anyPeer = new AnyPeer(peer.id);
    let options = new PeerOptions({
        directoryId: peer.id,
        heapSizeLimit: peerCapacity,
        isServer
    });

    await anyPeer.create({ options, orbitDB: peer.orbitDB });
    return anyPeer;

}


/* const testBehaviours: TypedBehaviours = {

    typeMap: {
        [Document.name]: Document
    }
} */

/* @variant([1, 0])
export class BinaryFeedStoreInterface extends SingleDBInterface<Document, BinaryFeedStore<Document>> {
 */
/* @field({ type: SingleDBInterface })
db: ;

constructor(opts?: { db: SingleDBInterface<Document, BinaryFeedStore<Document>> }) {
    super();
    if (opts) {
        Object.assign(this, opts);
    }
}

get initialized(): boolean {
    return !!this.db?.db && !!this.db._peer;
}

get loaded(): boolean {
    return this.db.loaded
}

close() {
    this.db.db = undefined;
}

async init(peer: AnyPeer, options: IStoreOptions<Document, any>): Promise<void> {
    await this.db.init(peer, options);
}

async load(): Promise<void> {
    await this.db.load();
} */
//}



/* 
@variant([1, 1])
export class DocumentStoreInterface extends SingleDBInterface<Document, BinaryDocumentStore<Document>> {
    init(orbitDB: OrbitDB, options: IStoreOptions<Document, any, any>): Promise<void> {
        return super.init(orbitDB, {
            ...options, typeMap: {
                [Document.name]: Document
            }
        })
    }
}
*/

/* 



 */