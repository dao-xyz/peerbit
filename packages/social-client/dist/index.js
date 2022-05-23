"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.createClient = void 0;
const ipfs_http_client_1 = require("ipfs-http-client");
const createClient = async ( /* url: string | URL | IpfsClient.multiaddr, host = '5001' */) => {
    const ipfs = (0, ipfs_http_client_1.create)({
        host: 'localhost',
        port: 5002
        // HEADERS for auth
    });
    /* let orbitDB = await OrbitDB.createInstance(ipfs);
    let peer = new AnyPeer();
    await peer.create({
        node: ipfs,
        orbitDB,
        rootAddress: 'root'

    }) */
    /* ipfs.pubsub
    shardedDB.create({
        orbitDB: orbitDB,
        behaviours: {
            typeMap: {}
        },
        local: false,
        replicationCapacity: undefined,
        repo: undefined,
        rootAddress: undefined;
    }) */
    await ipfs.pubsub.publish("hello", Buffer.from("world"));
    return undefined;
};
exports.createClient = createClient;
const clientPromise = (0, exports.createClient)();
process.on("SIGTERM", async () => {
    console.log("SIGTERM");
    const client = await clientPromise;
    await client.disconnect();
    process.exitCode = 0;
    process.exit();
});
process.on("SIGINT", async () => {
    console.log("SIGINT");
    const client = await clientPromise;
    await client.disconnect();
    process.exitCode = 0;
    process.exit();
});
process.stdin.resume();
//# sourceMappingURL=index.js.map