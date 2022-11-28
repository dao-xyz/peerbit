/* eslint-env mocha */
import { strict as assert } from "assert";
import io from "../index.js";

// Test utils
import {
    nodeConfig as config,
    testAPIs,
    startIpfs,
    stopIpfs,
    Session,
    waitForPeers,
} from "@dao-xyz/peerbit-test-utils";
import { IPFS } from "ipfs-core-types";
import { Controller } from "ipfsd-ctl";
import { jest } from "@jest/globals";

Object.keys(testAPIs).forEach((IPFS) => {
    describe(`encoding (${IPFS})`, function () {
        jest.setTimeout(10000);

        let ipfs: IPFS, ipfsd: Controller;

        beforeAll(async () => {
            ipfsd = await startIpfs(IPFS, config);
            ipfs = ipfsd.api;
        });

        afterAll(async () => {
            await stopIpfs(ipfsd);
        });

        describe("dag-cbor", () => {
            let cid1, cid2;
            const data: any = { test: "object" };

            it("writes", async () => {
                cid1 = await io.write(ipfs, "dag-cbor", data, { pin: true });
                expect(cid1).toEqual(
                    "zdpuAwHevBbd7V9QXeP8zC1pdb3HmugJ7zgzKnyiWxJG3p2Y4"
                );

                let obj = await io.read(ipfs, cid1, {});
                assert.deepStrictEqual(obj, data);

                data[cid1] = cid1;
                cid2 = await io.write(ipfs, "dag-cbor", data, {
                    links: [cid1],
                });
                expect(cid2).toEqual(
                    "zdpuAqeyAtvp1ACxnWZLPW9qMEN5rJCD9N3vjUbMs4AAodTdz"
                );

                obj = await io.read(ipfs, cid2, { links: [cid1] });
                data[cid1] = cid1;
                assert.deepStrictEqual(obj, data);
            });
        });

        describe("dag-pb", () => {
            let cid;
            const data = { test: "object" };

            it("writes", async () => {
                cid = await io.write(ipfs, "dag-pb", data, { pin: true });
                expect(cid).toEqual(
                    "QmaPXy3wcj4ds9baLreBGWf94zzwAUM41AiNG1eN51C9uM"
                );

                const obj = await io.read(ipfs, cid, {});
                assert.deepStrictEqual(obj, data);
            });
        });

        describe("raw", () => {
            let cid;
            const data = new Uint8Array([1, 2, 3]);

            it("writes", async () => {
                cid = await io.write(ipfs, "raw", data, { pin: true });
                expect(cid).toEqual(
                    "zb2rhWtC5SY6zV1y2SVN119ofpxsbEtpwiqSoK77bWVzHqeWU"
                );
                const obj = await io.read(ipfs, cid, {});
                expect(obj).toEqual(data);
            });
        });

        describe("rm", () => {
            it("pinned", async () => {
                const data: any = { test: "pinned" };
                const cid1 = await io.write(ipfs, "dag-cbor", data, {
                    pin: true,
                });
                let obj = await io.read(ipfs, cid1, {});
                assert.deepStrictEqual(obj, data);

                await io.rm(ipfs, cid1);
                try {
                    await io.read(ipfs, cid1, { timeout: 3000 });
                    fail();
                } catch (error) { }
            });

            it("unpinned", async () => {
                const data: any = { test: "unpinned" };
                const cid1 = await io.write(ipfs, "dag-cbor", data, {
                    pin: false,
                });
                let obj = await io.read(ipfs, cid1, {});
                assert.deepStrictEqual(obj, data);

                await io.rm(ipfs, cid1);
                try {
                    await io.read(ipfs, cid1, { timeout: 3000 });
                    fail();
                } catch (error) { }
            });
        });
    });
    describe(`pubsub (${IPFS})`, function () {
        let session: Session;
        beforeAll(async () => {
            session = await Session.connected(2);
        });

        afterAll(async () => {
            await session.stop();
        });

        it("rw", async () => {
            let cid;
            const data = new Uint8Array([1, 2, 3]);
            cid = await io.write(session.peers[0].ipfs, "raw", data, {
                pin: true,
            });
            expect(cid).toEqual(
                "zb2rhWtC5SY6zV1y2SVN119ofpxsbEtpwiqSoK77bWVzHqeWU"
            );
            await waitForPeers(
                session.peers[1].ipfs,
                [session.peers[0].id],
                io.BLOCK_TRANSPORT_TOPIC
            );

            const obj = await io.readFromPubSub(session.peers[1].ipfs, cid, {
                timeout: 10000,
            });
            expect(obj).toEqual(data);
        });
    });
});
