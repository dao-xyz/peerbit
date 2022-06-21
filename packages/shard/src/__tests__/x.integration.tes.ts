import { P2PTrust } from '../trust';
import { Shard } from '../shard';
import { AnyPeer } from '../node';
import { BinaryFeedStoreInterface, disconnectPeers, DocumentStoreInterface, documentStoreShard, getPeer, shardStoreShard } from './utils';
import { PublicKey } from '../index';
import { delay, waitFor } from '../utils';
import { Document } from './utils';
import { Peer } from '../peer';
import BN from 'bn.js';

describe('a', () => {
    test('add trustee', async () => {
        let peer = await getPeer();
        let l0 = await documentStoreShard(Document);
        await l0.init(peer);
        expect(l0.cid).toBeDefined();

        let peer2 = await getPeer();
        let loadedShard = await Shard.loadFromCID<BinaryFeedStoreInterface>(l0.cid, peer2.node);
        expect(loadedShard.interface.db.address).toEqual(l0.interface.db.address);
        await disconnectPeers([peer]);

    });
})