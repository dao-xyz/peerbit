
import rmrf from 'rimraf';
import path from 'path';
import assert from 'assert'
import {
    connectPeers,
    startIpfs,
    stopIpfs,
    getIpfsPeerId,
} from '@dao-xyz/orbit-db-test-utils'
import { DirectChannel as Channel } from '../direct-channel.js';
import { v1 as PROTOCOL } from '../protocol.js';
import { delay, waitFor } from '@dao-xyz/time';
import { waitForPeers } from '../wait-for-peers.js';

const API = 'js-ipfs'

describe(`DirectChannel js-ipfs`, function () {

    let ipfsd1, ipfsd2, ipfsd3, ipfs1, ipfs2, ipfs3
    let id1, id2, id3
    let expectedPeerIDs = []

    beforeEach(async () => {
        ipfsd1 = await startIpfs(API)
        ipfsd2 = await startIpfs(API)
        ipfsd3 = await startIpfs(API)
        ipfs1 = ipfsd1.api
        ipfs2 = ipfsd2.api
        ipfs3 = ipfsd3.api
        id1 = await getIpfsPeerId(ipfs1)
        id2 = await getIpfsPeerId(ipfs2)
        id3 = await getIpfsPeerId(ipfs3)
        await connectPeers(ipfs1, ipfs2)
        await connectPeers(ipfs1, ipfs3)
        await connectPeers(ipfs2, ipfs3)

        // Note, we only create channels between peer1 and peer2 in these test,
        // peer3 is used for "external actor" tests
        expectedPeerIDs = Array.from([id1, id2]).sort()
    })

    afterEach(async () => {
        await stopIpfs(ipfsd1)
        await stopIpfs(ipfsd2)
        await stopIpfs(ipfsd3)
        rmrf.sync('./tmp/') // remove test data directory
    })

    describe('create a channel', function () {
        it('has two participants', async () => {
            const c = await Channel.open(ipfs1, id2, () => { })
            expect(c.peers).toContainAllValues(expectedPeerIDs)
            c.close()
        })

        it('has correct ID', async () => {
            const expectedID = path.join('/', PROTOCOL, expectedPeerIDs.join('/'))
            const c = await Channel.open(ipfs1, id2, () => { })
            assert.deepEqual(c.id, expectedID)
            c.close()
        })

        it('has two peers', async () => {
            const c1 = await Channel.open(ipfs1, id2, () => { })
            const c2 = await Channel.open(ipfs2, id1, () => { })
            expect(c1.peers).toContainAllValues(expectedPeerIDs)
            expect(c2.peers).toContainAllValues(expectedPeerIDs)
            expect(c1.id).toEqual(path.join('/', PROTOCOL, expectedPeerIDs.join('/')))
            expect(c2.id).toEqual(path.join('/', PROTOCOL, expectedPeerIDs.join('/')))
            c1.close()
            c2.close()
        })

        it('can be created with one line', async () => {
            const c = await Channel.open(ipfs1, id2, () => { })
            const topics = await ipfs1.pubsub.ls()
            const channelID = topics.find(e => e === c.id)
            expect(channelID).toEqual(c.id)
            c.close()
        })
    })

    describe('properties', function () {
        let c

        beforeEach(async () => {
            c = await Channel.open(ipfs1, id2, () => { })
        })

        afterEach(() => {
            if (c) {
                c.close()
            }
        })

        it('has an id', async () => {
            expect(c.id).toEqual(path.join('/', PROTOCOL, expectedPeerIDs.join('/')))
        })

        it('has two peers', async () => {
            expect(c.peers.length).toEqual(2)
            expect(c.peers).toContainAllValues(expectedPeerIDs)
        })


    })

    describe('messaging', function () {
        it('sends and receives messages', async () => {

            let c1Response = undefined;
            const c1 = await Channel.open(ipfs1, id2, (topic, content, from) => {
                expect(from.equals(id2))
                expect(content).toEqual(new Uint8Array([1]))
                expect(topic).toEqual(c1.id)
                expect(topic).toEqual(c2.id)
                c1.close()
                c2.close()
                c1Response = true;
            })
            const c2 = await Channel.open(ipfs2, id1, (topic, content, from) => {
                expect(from.equals(id1))
                expect(content).toEqual(new Uint8Array([0]))
                expect(topic).toEqual(c1.id)
                expect(topic).toEqual(c2.id)
                c2.send(new Uint8Array([1]))
            })

            await c1.connect()
            await c2.connect()

            await c1.send(new Uint8Array([0]))
            await waitFor(() => !!c1Response);
        })

        it('can share a channel for multiple handlers', async () => {

            let c1Response = undefined;
            const c1 = await Channel.open(ipfs1, id2, (topic, content, from) => {
                c1Response = true;
            })
            const c2 = await Channel.open(ipfs2, id1, (topic, content, from) => {
                c2.send(new Uint8Array([1]))
            })
            const c1x = await Channel.open(ipfs1, id2, (topic, content, from) => {
            })
            const c2x = await Channel.open(ipfs2, id1, (topic, content, from) => {

            })

            await c1.connect()
            await c2.connect()

            await c1x.connect()
            await c2x.connect();
            await c1x.close();
            await c2x.close();

            await c1.send(new Uint8Array([0]))
            await waitFor(() => !!c1Response);
        })
    })

    describe('connect', function () {
        it('connects the peers', async () => {
            let c1: Channel, c2: Channel

            let callbackJoinedCounter: number = 0;
            let callbackLeavingCounter: number = 0;

            c1 = await Channel.open(ipfs1, id2, () => { }, { onNewPeerCallback: (channel) => { callbackJoinedCounter += 1 }, onPeerLeaveCallback: (channel) => { callbackLeavingCounter += 1 } })
            c2 = await Channel.open(ipfs2, id1, () => { })

            let peers = await ipfs1.pubsub.peers(c1.id)
            // assert.deepEqual(peers, [])

            await c1.connect()

            peers = await ipfs1.pubsub.peers(c1.id)
            expect(peers.map(x => x.toString())).toContainAllValues([id2.toString()])
            await delay(2000); // wait for all callbacks
            expect(callbackJoinedCounter).toEqual(1);
            await c2.close()
            await delay(2000); // wait for all callbacks
            expect(callbackLeavingCounter).toEqual(1);
            await c1.close()

        })
    })

    describe('disconnecting', function () {
        it('closes a channel', async () => {
            const c1 = await Channel.open(ipfs1, id2, () => { })
            const c2 = await Channel.open(ipfs2, id1, () => { })

            await c1.connect()
            await c2.connect()

            return new Promise(async (resolve, reject) => {
                expect(c1._closed).toEqual(false)
                expect(c1._isClosed()).toEqual(false)
                c1.close()
                const topics1 = await ipfs1.pubsub.ls()
                assert.deepEqual(topics1, [])
                expect(c1._closed).toEqual(true)
                expect(c1._isClosed()).toEqual(true)

                expect(c2._closed).toEqual(false)
                expect(c2._isClosed()).toEqual(false)
                c2.close()
                const topics2 = await ipfs2.pubsub.ls()
                assert.deepEqual(topics1, [])
                expect(c2._closed).toEqual(true)
                expect(c2._isClosed()).toEqual(true)

                setTimeout(async () => {
                    const peers1 = await ipfs1.pubsub.peers(c1.id)
                    const peers2 = await ipfs2.pubsub.peers(c1.id)
                    assert.deepEqual(peers1, [])
                    assert.deepEqual(peers2, [])
                    resolve(true)
                }, 200)
            })
        })
    })

    describe('non-participant peers can\'t send messages', function () {
        it('doesn\'t receive unwanted messages', async () => {
            const c1 = await Channel.open(ipfs1, id2, (topic, content, from) => {
                expect(from.equals(id2))
                expect(content).toEqual(new Uint8Array([0]))
                expect(topic).toEqual(c1.id)
                expect(topic).toEqual(c2.id)
            })
            const c2 = await Channel.open(ipfs2, id1, () => { })

            await c1.connect()
            await c2.connect()


            await ipfs3.pubsub.subscribe(c1.id, () => { })
            await waitForPeers(ipfs1, [id3], c1.id, c1._isClosed.bind(c1))
            await ipfs3.pubsub.publish(c1.id, Buffer.from('OMG!'))

            return new Promise((resolve, reject) => {
                setTimeout(() => {
                    c2.send(new Uint8Array([0]))
                    setTimeout(() => {
                        c1.close()
                        c2.close()
                        resolve(true)
                    }, 1000)
                }, 1000)
            })
        })
    })
})
