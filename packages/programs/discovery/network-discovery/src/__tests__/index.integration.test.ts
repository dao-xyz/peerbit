import { Session, Peer, waitForPeers } from '@dao-xyz/peerbit-test-utils'
import { waitFor } from '@dao-xyz/time';
import { AccessError, Ed25519Keypair } from "@dao-xyz/peerbit-crypto";
import { DocumentQueryRequest, Results, ResultWithSource } from '@dao-xyz/peerbit-dsearch';
import { QueryRequestV0, QueryResponseV0, query } from '@dao-xyz/peerbit-dquery';
import { Secp256k1PublicKey } from '@dao-xyz/peerbit-crypto';
import { Wallet } from '@ethersproject/wallet'
import { Identity } from '@dao-xyz/ipfs-log';
import { createStore } from '@dao-xyz/peerbit-test-utils';
import { Level } from 'level';
import { fileURLToPath } from 'url';
import path, { dirname } from 'path';
import { CachedValue, DefaultOptions, IInitializationOptions, IStoreOptions, Store } from '@dao-xyz/peerbit-dstore';
import Cache from '@dao-xyz/peerbit-cache';
import { serialize } from '@dao-xyz/borsh';
import { Program } from '@dao-xyz/peerbit-program';
import { TrustedNetwork } from '@dao-xyz/peerbit-trusted-network';
import { NetworkDiscovery } from '../controller';
import { jest } from '@jest/globals';

const __filename = fileURLToPath(import.meta.url);

const createIdentity = async () => {
    const ed = await Ed25519Keypair.create();
    return {
        publicKey: ed.publicKey,
        sign: (data) => ed.sign(data)
    } as Identity
}
describe('index', () => {
    jest.setTimeout(360000);
    let session: Session, identites: Identity[], cacheStore: Level[]

    const identity = (i: number) => identites[i];
    const init = (store: Program | Store<any>, i: number, options: IStoreOptions<any> = {}) => store.init && store.init(session.peers[i].ipfs, identites[i], { ...DefaultOptions, replicate: true, resolveCache: async () => new Cache<CachedValue>(cacheStore[i]), ...options })
    beforeAll(async () => {
        session = await Session.connected(4);
        identites = [];
        cacheStore = [];
        for (let i = 0; i < session.peers.length; i++) {
            identites.push(await createIdentity());
            cacheStore.push(await createStore(path.join(__filename, 'cache', i.toString())))
        }

    })

    afterAll(async () => {
        await session.stop();
        await Promise.all(cacheStore?.map((c) => c.close()));
    })


    describe('NetworkDiscovery', () => {

        it('can append if trusted', async () => {

            // created network with identity 0, 1, 2
            const l0a = new TrustedNetwork({
                rootTrust: identity(0).publicKey
            });

            await init(l0a, 0);
            await l0a.add(identity(1).publicKey);
            let l0b: TrustedNetwork = await TrustedNetwork.load(session.peers[1].ipfs, l0a.address) as any
            await init(l0b, 1);

            await l0b.trustGraph.store.sync(l0a.trustGraph.store.oplog.heads);

            await waitFor(() => Object.keys(l0b.trustGraph._index._index).length == 1)

            await l0b.add(identity(2).publicKey); // Will only work if peer2 is trusted
            await l0a.trustGraph.store.sync(l0b.trustGraph.store.oplog.heads);

            await waitFor(() => Object.keys(l0b.trustGraph._index._index).length == 2)
            await waitFor(() => Object.keys(l0a.trustGraph._index._index).length == 2)

            const discovery = new NetworkDiscovery()
            await init(discovery, 3, { replicate: true });

            // now identity 2 should be able to append to discovery because it is trusted by the network
            const discovery2 = (await NetworkDiscovery.load(session.peers[2].ipfs, discovery.address)) as NetworkDiscovery;
            await init(discovery2, 2, { replicate: true });

            // should not be ok because peer 4 is not trusted
            try {
                await discovery.addInfo(l0a)
                fail();
            } catch (error) {
                if (error instanceof AccessError === false) {
                    throw error;
                }
                expect(error).toBeInstanceOf(AccessError);
            }

            // should be ok since peer 2 is trusted
            await discovery2.addInfo(l0a);
            await discovery.info.store.sync(discovery2.info.store.oplog.heads)
            await waitFor(() => discovery.info.store.oplog.length === 1)

        })
    })
})
