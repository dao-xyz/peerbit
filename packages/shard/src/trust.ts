import { deserialize, field, serialize, variant } from "@dao-xyz/borsh";
import { BinaryDocumentStore } from "@dao-xyz/orbit-db-bdocstore";
import { AnyPeer, IPFSInstanceExtended } from "./node";
import { Shard } from "./shard";
import { PublicKey } from "./key";
import { BinaryDocumentStoreOptions, waitForReplicationEvents } from "./stores";
export const TRUSTEE_KEY = 'trustee';
@variant(0)
export class P2PTrustRelation {

    /*  @field({ type: PublicKey })
     truster: PublicKey  *///  Dont need this becaause its going to be signed with truster anyway (bc orbitdb)

    @field({ type: PublicKey })
    [TRUSTEE_KEY]: PublicKey  // the key to trust

    /* @field({ type: 'String' }) 
    signature: string */ // Dont need this because its going to be signed anyway (bc orbitdb)

    constructor(props?: P2PTrustRelation) {
        if (props) {
            Object.assign(this, props)
        }
    }

}



@variant(0) // We prepend with 0 if we in the future would have an other trust setup
export class P2PTrust {

    @field({ type: PublicKey })
    rootTrust: PublicKey

    @field({ type: 'String' })
    trustAddress: string;

    trustDB?: BinaryDocumentStore<P2PTrustRelation>
    shard?: Shard<any>
    cid?: string;

    constructor(props?: {
        rootTrust: PublicKey
        trustCircleAddress: string;
    } | {
        rootTrust: PublicKey
    }) {
        if (props) {
            Object.assign(this, props)
        }
    }
    async create(peer: AnyPeer, shard: Shard<any>) {

        // TODO: this is ugly but ok for now
        peer.options.behaviours.typeMap[P2PTrustRelation.name] = P2PTrustRelation;
        this.shard = shard;
        await this.loadTrust();
        this.trustAddress = this.trustDB.address.toString()
    }


    async loadTrust(waitForReplicationEventsCount: number = 0) {
        this.trustDB = await new BinaryDocumentStoreOptions<P2PTrustRelation>({
            indexBy: TRUSTEE_KEY,
            objectType: P2PTrustRelation.name
        }).newStore(this.trustAddress ? this.trustAddress : this.shard.getDBName("trust"), this.shard.peer.orbitDB, this.shard.peer.options.defaultOptions, this.shard.peer.options.behaviours)
        this.trustDB.load();
        await waitForReplicationEvents(this.trustDB, waitForReplicationEventsCount);
        return this.trustDB;
    }

    async addTrust(trustee: PublicKey) {
        await this.trustDB.put(new P2PTrustRelation({
            trustee
        }));
    }

    async save(node: IPFSInstanceExtended): Promise<string> {
        if (!this.trustAddress || !this.rootTrust) {
            throw new Error("Not initialized");
        }

        let arr = serialize(this);
        let addResult = await node.add(arr)
        let pinResult = await node.pin.add(addResult.cid)
        this.cid = pinResult.toString();
        return this.cid;
    }


    static async loadFromCID(cid: string, node: IPFSInstanceExtended): Promise<P2PTrust> {
        let arr = await node.cat(cid);
        for await (const obj of arr) {
            let der = deserialize(Buffer.from(obj), P2PTrust);
            der.cid = cid;
            return der;
        }
    }

    get replicationTopic() {
        if (!this.cid) {
            throw new Error("Not initialized, replication topic requires known cid");
        }
        return this.cid + '_' + 'replication'
    }


    /**
     * Follow trust path back to trust root.
     * Trust root is always trusted.
     * Hence if
     * Root trust A trust B trust C
     * C is trusted by Root
     * @param trustee 
     * @returns true, if trusted
     */
    isTrusted(trustee: PublicKey): boolean {

        /**
         * TODO: Currently very inefficient
         */
        if (!this.trustDB) {
            throw new Error("Not initalized")
        }
        if (trustee.equals(this.rootTrust)) {
            return true;
        }
        let currentTrustee = trustee;
        let visited = new Set<string>();
        while (true) {
            let trust = this.trustDB.index.get(currentTrustee.toString(), true) as LogEntry<P2PTrustRelation>;
            if (!trust) {
                return false;
            }

            // TODO: could be multiple but we just follow one path for now
            if (currentTrustee == trust.payload.value.trustee) {
                return false;
            }

            // Assumed message is signed
            let truster = PublicKey.from(trust.identity);

            if (truster.equals(this.rootTrust)) {
                return true;
            }
            let key = truster.toString();
            if (visited.has(key)) {
                return false; // we are in a loop, abort
            }
            visited.add(key);
            currentTrustee = truster; // move upwards in trust tree
        }
    }
}

