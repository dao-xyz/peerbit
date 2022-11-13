import rmrf from "rimraf";
import { Peerbit } from "../peer";
import { EventStore } from "./utils/stores/event-store";

import { jest } from "@jest/globals";
import { Controller } from "ipfsd-ctl";
import { IPFS } from "ipfs-core-types";

// Include test utilities
import {
    nodeConfig as config,
    startIpfs,
    stopIpfs,
    connectPeers,
} from "@dao-xyz/peerbit-test-utils";

import { SimpleStoreContract } from "./utils/access";

const orbitdbPath1 = "./orbitdb/tests/reuse-store/1";
const orbitdbPath2 = "./orbitdb/tests/reuse-store/2";
const dbPath1 = "./orbitdb/tests/reuse-store/1/db1";
const dbPath2 = "./orbitdb/tests/reuse-store/2/db2";

describe(`orbit-db - shared`, function () {
    jest.setTimeout(config.timeout * 2);

    let ipfsd1: Controller, ipfsd2: Controller, ipfs1: IPFS, ipfs2: IPFS;
    let orbitdb1: Peerbit,
        orbitdb2: Peerbit,
        db1: SimpleStoreContract,
        db2: SimpleStoreContract;

    beforeAll(async () => {
        ipfsd1 = await startIpfs("js-ipfs", config.daemon1);
        ipfsd2 = await startIpfs("js-ipfs", config.daemon2);
        ipfs1 = ipfsd1.api;
        ipfs2 = ipfsd2.api;
        // Connect the peers manually to speed up test times
        const isLocalhostAddress = (addr: string) =>
            addr.toString().includes("127.0.0.1");
        await connectPeers(ipfs1, ipfs2, { filter: isLocalhostAddress });
    });

    afterAll(async () => {
        if (ipfsd1) await stopIpfs(ipfsd1);

        if (ipfsd2) await stopIpfs(ipfsd2);
    });

    beforeEach(async () => {
        rmrf.sync(orbitdbPath1);
        rmrf.sync(orbitdbPath2);
        rmrf.sync(dbPath1);
        rmrf.sync(dbPath2);

        orbitdb1 = await Peerbit.create(ipfs1, { directory: orbitdbPath1 });
        orbitdb2 = await Peerbit.create(ipfs2, { directory: orbitdbPath2 });
    });

    afterEach(async () => {
        if (db1) await db1.store.drop();

        if (db2) await db2.store.drop();

        if (orbitdb1) await orbitdb1.stop();

        if (orbitdb2) await orbitdb2.stop();
    });

    it("open same store twice will share instance", async () => {
        const replicationTopic = "topic";
        db1 = await orbitdb1.open(
            new SimpleStoreContract({
                store: new EventStore({ id: "some db" }),
            }),
            { replicationTopic }
        );
        const sameDb = await orbitdb1.open(
            new SimpleStoreContract({
                store: new EventStore({ id: "some db" }),
            }),
            { replicationTopic }
        );
        expect(db1 === sameDb);
    });

    it("can share nested stores", async () => {
        const replicationTopic = "topic";
        db1 = await orbitdb1.open(
            new SimpleStoreContract({
                store: new EventStore<string>({
                    id: "event store",
                }),
            }),
            { replicationTopic }
        );
        db2 = await orbitdb1.open(
            new SimpleStoreContract({
                store: new EventStore<string>({
                    id: "event store",
                }),
            }),
            { replicationTopic }
        );
        expect(db1 !== db2);
        expect(db1.store === db2.store);
    });

    // TODO add tests and define behaviour for cross topic programs
    // TODO add tests for shared subprogams
    // TODO add tests for subprograms that is also open as root program
});
