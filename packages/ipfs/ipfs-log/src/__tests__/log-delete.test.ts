import rmrf from "rimraf";
import fs from "fs-extra";
import { Log } from "../log.js";
import { createStore, Keystore, KeyWithMeta } from "@dao-xyz/peerbit-keystore";
import { jest } from "@jest/globals";
import io from "@dao-xyz/peerbit-io-utils";
// Test utils
import {
    nodeConfig as config,
    testAPIs,
    startIpfs,
    stopIpfs,
} from "@dao-xyz/peerbit-test-utils";
import { Controller } from "ipfsd-ctl";
import { IPFS } from "ipfs-core-types";
import { Ed25519Keypair } from "@dao-xyz/peerbit-crypto";
import { dirname } from "path";
import { fileURLToPath } from "url";
import path from "path";

const __filename = fileURLToPath(import.meta.url);
const __filenameBase = path.parse(__filename).base;
const __dirname = dirname(__filename);

let ipfsd: Controller, ipfs: IPFS, signKey: KeyWithMeta<Ed25519Keypair>;

Object.keys(testAPIs).forEach((IPFS) => {
    describe("Log - Delete", function () {
        jest.setTimeout(config.timeout);

        const { signingKeyFixtures, signingKeysPath } = config;

        let keystore: Keystore;

        beforeAll(async () => {
            rmrf.sync(signingKeysPath(__filenameBase));

            await fs.copy(
                signingKeyFixtures(__dirname),
                signingKeysPath(__filenameBase)
            );

            keystore = new Keystore(
                await createStore(signingKeysPath(__filenameBase))
            );

            //@ts-ignore
            signKey = await keystore.getKey(new Uint8Array([0]));
            ipfsd = await startIpfs(IPFS, config.defaultIpfsConfig);
            ipfs = ipfsd.api;
        });

        afterAll(async () => {
            await stopIpfs(ipfsd);
            rmrf.sync(signingKeysPath(__filenameBase));
            await keystore?.close();
        });

        it("deletes recursively", async () => {
            const blockExists = async (hash: string): Promise<boolean> => {
                try {
                    return !!(await io.read(ipfs, hash, { timeout: 3000 }));
                } catch (error) {
                    return false;
                }
            };
            const log = new Log<string>(
                ipfs,
                {
                    ...signKey.keypair,
                    sign: async (data: Uint8Array) =>
                        await signKey.keypair.sign(data),
                },
                { logId: "A" }
            );
            const e1 = await log.append("hello1");
            const e2 = await log.append("hello2");
            const e3 = await log.append("hello3");
            await log.deleteRecursively(e2);
            expect(log.get(e1.hash)).toBeUndefined();
            expect(await blockExists(e1.hash)).toBeFalse();
            expect(log.get(e2.hash)).toBeUndefined();
            expect(await blockExists(e2.hash)).toBeFalse();
            expect(log.get(e3.hash)).toBeDefined();
            expect(await blockExists(e3.hash)).toBeTrue();
        });
    });
});
