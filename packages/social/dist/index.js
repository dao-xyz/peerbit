"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.createNode = void 0;
const social_interface_1 = require("@dao-xyz/social-interface");
const process_1 = __importDefault(require("process"));
const node_1 = require("@dao-xyz/node");
const bn_js_1 = __importDefault(require("bn.js"));
const node_2 = require("@dao-xyz/node");
const ipfs_http_client_1 = require("ipfs-http-client");
const createNode = async (genesis = new node_1.RecursiveShard({
    cluster: 'genesis',
    shardSize: new bn_js_1.default(500 * 1000)
})) => {
    const db = new social_interface_1.DaoDB(genesis);
    console.log("Starting node ...");
    let ipfsNode = await (0, ipfs_http_client_1.create)({
        host: 'localhost',
        port: 5001
        // HEADERS for auth
    });
    const id = (0, node_1.generateUUID)();
    let orbitDB = await (0, node_2.createOrbitDBInstance)(ipfsNode, id);
    await db.create({
        rootAddress: 'root',
        orbitDB,
        id
    });
    console.log("Created node with");
    console.log("OrbitDB: " + db.peer.orbitDB.id);
    console.log("Root shards address: " + genesis.address);
    const l0 = await genesis.loadShard(0);
    console.log((await l0.loadPeers()).all);
    console.log("IPFS: " + (await db.peer.node.id()).id);
    console.log("Swarm: ");
    let swarmAddresses = (await db.peer.node.swarm.addrs()).map(x => x.addrs).map(y => y.map(z => z.toString()).join('\n')).join('\n');
    console.log(swarmAddresses);
    db.peer.node.pubsub.subscribe("hello", (msg) => {
        console.log("GOT MESSAGE", msg);
    });
    console.log('CONFIG', await db.peer.node.config.getAll());
    return db;
};
exports.createNode = createNode;
let nodePromise = (0, exports.createNode)();
const terminate = async () => {
    await (await nodePromise).peer.disconnect();
};
process_1.default.on("SIGTERM", async () => {
    console.log("SIGTERM");
    await terminate();
    process_1.default.exitCode = 0;
    process_1.default.exit();
});
process_1.default.on("SIGINT", async () => {
    console.log("SIGINT");
    await terminate();
    process_1.default.exitCode = 0;
    process_1.default.exit();
});
process_1.default.stdin.resume();
//# sourceMappingURL=index.js.map