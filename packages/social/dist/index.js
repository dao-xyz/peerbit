"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.createNode = void 0;
const dao_1 = require("./dao");
const process_1 = __importDefault(require("process"));
const createNode = async () => {
    const db = new dao_1.DaoDB();
    console.log("Starting node ...");
    await db.create({
        rootId: 'root',
        local: false,
    });
    console.log("Created node with");
    console.log("OrbitDB: " + db.orbitDB.id);
    console.log("Root DB address: " + db.shardChainChain.id);
    let root = db.shardChainChain;
    // Create Root shard
    await root.addPeerToShards();
    const l0 = await root.getShard(0);
    console.log((await l0.loadPeers()).all);
    console.log("IPFS: " + (await db.node.id()).id);
    console.log("Swarm: ");
    let swarmAddresses = (await db.node.swarm.addrs()).map(x => x.addrs).map(y => y.map(z => z.toString()).join('\n')).join('\n');
    console.log(swarmAddresses);
    return db;
};
exports.createNode = createNode;
let nodePromise = (0, exports.createNode)();
const terminate = async () => {
    await (await nodePromise).disconnect();
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