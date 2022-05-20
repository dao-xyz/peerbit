import { DaoDB } from "./dao"
import process from 'process';

export const createNode = async (): Promise<DaoDB> => {
    const db = new DaoDB();
    console.log("Starting node ...")
    await db.create({
        rootId: 'root',
        local: false,
    })

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
}

let nodePromise = createNode();

const terminate = async () => {
    await (await nodePromise).disconnect();
}

process.on("SIGTERM", async () => { // Why it will not be executed?
    console.log("SIGTERM");
    await terminate();
    process.exitCode = 0;
    process.exit();
})
process.on("SIGINT", async () => { // Why it will not be executed?
    console.log("SIGINT");
    await terminate();
    process.exitCode = 0;
    process.exit();

})
process.stdin.resume();
