
import { DEFAULT_TOPIC, replicator } from './node.js';
import { Peerbit } from '@dao-xyz/peerbit';
import { startIpfs } from './ipfs.js';
import { setupDomain } from './domain.js';
import { TrustedNetwork } from '@dao-xyz/peerbit-trusted-network';
export type AnyCLIArgs = { _: string[], ipfs: 'go' | 'js', disposable: boolean }

export type ReplicatorCLIArgs = AnyCLIArgs & {  /*host: string, iip?: string,  */topic: string,  /* bootstrap: string[],  */root: boolean, timeout: number };
const ANY_ARGS_CONFIG = {
    ipfs: {
        description: 'IPFS type',
        type: 'string',
        choices: ['go', 'js'],
        default: 'go',
    },
    disposable: {
        description: 'Run IPFS node as disposable (will be destroyed on termination)',
        boolean: true
    }
}
export const REPLICATOR_CLI_CONFIG: { [key: string]: any } = {
    ...ANY_ARGS_CONFIG,
    /*     host: {
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
        }, */
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


export const RELAY_CLI_CONFIG: { [key: string]: any } = ANY_ARGS_CONFIG;

export type RelayCLIArgs = AnyCLIArgs;

export type DomainCLIArgs = AnyCLIArgs & { email: string, outdir?: string, wait: boolean };


export const DOMAIN_CLI_CONFIG: { [key: string]: any } = {
    ...ANY_ARGS_CONFIG,
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

export const addReplicatorCommands = <T extends { command: (...any) => T }>(yargs: T): T => yargs.command('start', 'Start node', REPLICATOR_CLI_CONFIG).command('relay', 'Start IPFS as a simple node', RELAY_CLI_CONFIG).command('domain', 'Setup domain', DOMAIN_CLI_CONFIG)
export const getReplicatorArgs = async () => {
    const yargs = await import('yargs');
    const { hideBin } = await import('yargs/helpers');

    return addReplicatorCommands(yargs.default(hideBin(process.argv))).help().argv;
}

export const cli = async (options?: { cliName: string, onStart: (properties: { replicationTopic: string, network?: TrustedNetwork, peer: Peerbit }) => void }) => {

    const cliName = options?.cliName || 'peerbit'
    const args = (await getReplicatorArgs()) as any as (ReplicatorCLIArgs | DomainCLIArgs | RelayCLIArgs);
    const controller = await startIpfs(args.ipfs, { module: { disposable: args.disposable } })
    const cmd = args._[0];
    const printNodeInfo = async () => {
        console.log('Starting node with address(es): ');
        const id = await controller.api.id()
        id.addresses.forEach((addr) => {
            console.log(addr.toString());
        })
    }
    if (cmd === 'replicator') {
        await printNodeInfo();
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
    else if (cmd === 'relay') {
        await printNodeInfo();


        // do nothing, just dont shut down (IPFS is running)

        // TODO add listener for ctrl c (exit)
    }
    // add stop command below
    /// ...
    /// ...
    else if (cmd === 'domain') {
        if (args.disposable && process.env.JEST_WORKER_ID !== undefined) {
            throw new Error("Disposable nodes behind a domain is currently not supported") // because the frontend will not show the right address

        }

        const parsed = args as DomainCLIArgs;
        await setupDomain(controller.api, parsed.email, parsed.outdir, parsed.wait)
        await controller.api.stop();
        console.log('Before you can connect to your node you need to run:')
        console.log('Either:')
        console.log(cliName + ' relay')
        console.log('or: ')
        console.log(cliName + ' replicator')
        console.log('add "--help" for documentation:')
        const { exit } = await import('process');
        exit();
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