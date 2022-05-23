import { DaoDB } from "@dao-xyz/social-interface";
import process from 'process';
import { generateUUID, RecursiveShard } from "@dao-xyz/node";
import BN from 'bn.js';
import { createOrbitDBInstance } from "@dao-xyz/node";
import { create } from 'ipfs-http-client';
export const createNode = async (genesis: RecursiveShard<any> = new RecursiveShard<any>({
    cluster: 'genesis',
    shardSize: new BN(500 * 1000)
})): Promise<DaoDB> => {

    const db = new DaoDB(genesis);

    console.log("Starting node ...")

    let ipfsNode = await create({
        host: 'localhost',
        port: 5001
        // HEADERS for auth
    });

    const id = generateUUID();

    let orbitDB = await createOrbitDBInstance(ipfsNode as any, id);

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
    db.peer.node.pubsub.subscribe("hello", (msg: any) => {
        console.log("GOT MESSAGE", msg)
    })
    console.log('CONFIG', await db.peer.node.config.getAll())
    return db;
}

let nodePromise = createNode();

const terminate = async () => {
    await (await nodePromise).peer.disconnect();
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
