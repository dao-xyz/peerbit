
import { hideBin } from 'yargs/helpers'
import { DEFAULT_TOPIC, replicator } from './node.js';
import { Peerbit } from '@dao-xyz/peerbit';
import { startIpfs } from './ipfs.js';
import { setupDomain } from './domain.js';
import { TrustedNetwork } from '@dao-xyz/peerbit-trusted-network';

export type ReplicatorCLIArgs = { _: string[], host: string, /* iip?: string,  */topic: string, ipfs: 'go' | 'js', bootstrap: string[], root: boolean, timeout: number };

export const REPLICATOR_CLI_CONFIG: { [key: string]: any } = {
    ipfs: {
        description: 'IPFS type',
        type: 'string',
        choices: ['go', 'js'],
        default: 'go',
    },
    host: {
        description: 'Host',
        alias: 'h',
        type: 'string',
        default: 'localhost'
    },
    verbose: {
        description: 'Verbose',
        alias: 'v',
        type: 'boolean',
        default: false
    },

    bootstrap: {
        description: 'Bootstrap addresses (initial swarm connections)',
        alias: 'bs',
        type: 'array',
        default: []
    },
    topic: {
        description: "Network address or rootTrust key in form [CHAIN TYPE]/[PUBLICKEY]. e.g. if ethereum: \"ethereum/0x4e54fD83...\" or any topic",
        alias: 't',
        type: 'string',
        default: DEFAULT_TOPIC
    },
    timeout: {
        description: "Time out parameter for initialization tasks",
        type: 'number',
        default: 10 * 1000
    },
    root: {
        description: 'Open a network I am the root identity',
        boolean: true
    }
};

export type DomainCLIArgs = { _: string[], ipfs: 'go' | 'js', email: string, outdir?: string, wait: boolean };


export const DOMAIN_CLI_CONFIG: { [key: string]: any } = {
    ipfs: {
        description: 'IPFS type',
        type: 'string',
        choices: ['go', 'js'],
        default: 'go',
    },
    email: {
        description: 'Email for Lets encrypt autorenewal messages',
        type: 'string',
        demandOption: true
    },
    outdir: {
        description: 'Output path for Nginx config',
        type: 'string',
        alias: 'o'
    },

    wait: {
        description: 'Wait for setup to succeed (or fail)',
        type: 'boolean',
        default: false
    }
};

export const addReplicatorCommands = <T extends { command: (...any) => T }>(yargs: T): T => yargs.command('start', 'Start node', REPLICATOR_CLI_CONFIG).command('domain', 'Setup domain', DOMAIN_CLI_CONFIG)
export const getReplicatorArgs = async () => {
    const yargs = await import('yargs');
    addReplicatorCommands(yargs.default(hideBin(process.argv))).help().argv;
}

export const cli = async (options?: { onStart: (properties: { replicationTopic: string, network?: TrustedNetwork, peer: Peerbit }) => void }) => {

    const args = getReplicatorArgs() as any as (ReplicatorCLIArgs | DomainCLIArgs);
    const controller = await startIpfs(args.ipfs)
    const cmd = args._[0];
    if (cmd === 'start') {
        const parsed = args as ReplicatorCLIArgs;
        const peer = await Peerbit.create(controller.api);
        replicator({
            ...parsed, peer,
            onReady: ({ replicationTopic, network }) => {
                options?.onStart && options.onStart({
                    replicationTopic,
                    network,
                    peer: peer

                })
            }
        });
    }
    else if (cmd === 'domain') {
        const parsed = args as DomainCLIArgs;
        await setupDomain(controller.api, parsed.email, parsed.outdir, parsed.wait)
    }
}


/* 
 let work: Work = {
            shards: [],
            trustSubscriptions: trustSubscriptions.map(x => x.cid as string)
        }
        
        if (fs.existsSync(WORK_PATH)) {
            console.log('Loading previous work');
            const loadedWork: Work = JSON.parse(fs.readFileSync(WORK_PATH).toString());
        
            work.shards = loadedWork.shards;
            console.log(`Re-replicating ${work.shards.length} shards`)
            await Promise.all(work.shards.map(cid => Shard.loadFromCID(cid, node.node).then((shard => shard.replicate(node)))))
            console.log('Re-replication done')
        
            for (const cid of loadedWork.trustSubscriptions) {
                try {
                    let trust = await P2PTrust.loadFromCID(cid, node.node);
                    trustSubscriptions.push(trust)
                    work.trustSubscriptions.push(trust.cid as string);
                } catch (error) {
                    throw new Error("Failed to load trust with cid: " + cid + " when resuming work")
                }
            }
        }
        
        const saveWork = () => {
            fs.writeFileSync(WORK_PATH, JSON.stringify(
                work
            ));
        }
        
        
        for (const trust of trustSubscriptions) {
            await Shard.subscribeForReplication(node, trust, (shard) => {
                work.shards.push(shard.cid);
                saveWork();
            })
            console.log('Subscribing to root trust with cid', trust.cid)
        }
        saveWork();
        
        if (callback) {
            callback(node);
        }

        */