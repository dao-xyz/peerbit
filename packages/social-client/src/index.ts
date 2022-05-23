import { create } from 'ipfs-http-client'
import OrbitDB from 'orbit-db';
import { AnyPeer } from '@dao-xyz/node';
export const createClient = async (/* url: string | URL | IpfsClient.multiaddr, host = '5001' */): Promise<OrbitDB> => {
    const ipfs = create({
        host: 'localhost',
        port: 5001
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
}
const clientPromise = createClient();

process.on("SIGTERM", async () => { // Why it will not be executed?
    console.log("SIGTERM");
    const client = await clientPromise;
    await client.disconnect();
    process.exitCode = 0;
    process.exit();
})
process.on("SIGINT", async () => { // Why it will not be executed?
    console.log("SIGINT");
    const client = await clientPromise;
    await client.disconnect();
    process.exitCode = 0;
    process.exit();

})
process.stdin.resume();


