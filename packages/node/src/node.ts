
import { Peerbit } from '@dao-xyz/peerbit';
import { networkFromTopic } from './utils.js';
import { multiaddr } from '@multiformats/multiaddr'
import { delay } from '@dao-xyz/peerbit-time';
import { AccessError } from '@dao-xyz/peerbit-crypto';
import { TrustedNetwork } from '@dao-xyz/peerbit-trusted-network';

export const DEFAULT_TOPIC = 'world';





export type ReplicatorOptions = { bootstrap?: string[], peer: Peerbit, onReady?: (config: { replicationTopic: string, network?: TrustedNetwork }) => void, topic?: string, root?: boolean, timeout: number };
/**
 * Replicating node
 * @param options 
 */
export const replicator = async (options: ReplicatorOptions) => {
    const node = options.peer;
    options.bootstrap && await Promise.all(options.bootstrap.map(b => node.ipfs.swarm.connect(multiaddr(b))))

    if (options.root && options.topic && options.topic !== DEFAULT_TOPIC) {
        throw new Error("Expecting either root or topic to be provided")
    }

    // Start participating in the network (assumes this node is trusted)
    let network: TrustedNetwork | undefined;
    if (options.root) {
        network = new TrustedNetwork({ rootTrust: node.identity.publicKey })
    }
    else if (options.topic) {
        network = await networkFromTopic(node.ipfs, options.topic);
    }
    let replicationTopic: string;
    if (network) {

        // Assumes that this node is trusted by the network, else this is meaningless 
        // TODO add check to see if trusted
        network = await node.openNetwork(network, { replicate: true })

        let t0 = +new Date;
        console.log('Connnecting to network: ' + network.address.toString())
        let done = false;
        while (+new Date < t0 + 2 * 60 * 1000) {
            try {
                await node.joinNetwork(network)
                done = true;
                break;
            } catch (error) {
                if (error instanceof AccessError === false) {
                    throw error;
                }
            }
            console.log('Peer not trusted yet by the network, waiting ...')
            console.log('\tPeer info:')
            console.log('\t\tIPFS: ' + (await node.ipfs.id()).id.toString())
            console.log('\t\tIdentity: ' + node.identity.publicKey.toString())
            await delay(5000);
        }
        if (!done) {
            throw new Error("Failed to join network: " + network.address.toString());
        }

        replicationTopic = network.address.toString();
    }
    else {

        if (!options.topic) {
            throw new Error("Missing 'topic' or 'root' argument")
        }
        // "Anarchy"
        await node.subscribeToReplicationTopic(options.topic)
        replicationTopic = options.topic;
    }

    const terminate = async () => {
        console.log('save snapshot')
        /*   await rootShard.interface.comments.db.saveSnapshot() */
        console.log('save snapshot done');
        await node.disconnect();
    }

    process.on("SIGTERM", async () => {
        console.log("SIGTERM");
        await terminate();
        process.exitCode = 0;
        process.exit();
    })
    process.on("SIGINT", async () => {
        console.log("SIGINT");
        await terminate();
        process.exitCode = 0;
        process.exit();
    })

    options?.onReady && options?.onReady({ replicationTopic, network });
    while (true) {

        const x = () => {
            console.log("Replication topics: " + Object.keys(node.programs).length);
            for (const topic of Object.keys(node.programs)) {
                console.log('\tTopic: ' + topic, ", programs: " + Object.keys(node.programs[topic]).length);
            }
        }
        x();
        await new Promise(r => setTimeout(r, 4000));

    }
}
