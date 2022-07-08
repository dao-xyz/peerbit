import { deserialize, field, serialize, variant } from "@dao-xyz/borsh";
import { BinaryDocumentStore, BinaryDocumentStoreOptions, LogEntry } from "@dao-xyz/orbit-db-bdocstore";
import { IPFS as IPFSInstance } from 'ipfs-core-types';
import { Shard } from "./shard";
import { DBInterface, SingleDBInterface } from "@dao-xyz/orbit-db-store-interface";

import { IStoreOptions } from "@dao-xyz/orbit-db-store";
import { IdentitySerializable } from "@dao-xyz/orbit-db-identity-provider";
import { OrbitDB } from "@dao-xyz/orbit-db";
export const TRUSTEE_KEY = 'trustee';

@variant(0)
export class P2PTrustRelation {

    /*  @field({ type: PublicKey })
     truster: PublicKey  *///  Dont need this becaause its going to be signed with truster anyway (bc orbitdb)

    @field({ type: IdentitySerializable })
    [TRUSTEE_KEY]: IdentitySerializable  // the key to trust


    truster: IdentitySerializable // will be set manually, upon deserialization from the oplog

    /* @field({ type: 'String' }) 
    signature: string */ // Dont need this because its going to be signed anyway (bc orbitdb)

    constructor(props?: {
        [TRUSTEE_KEY]: IdentitySerializable
    }) {
        if (props) {
            Object.assign(this, props)
        }
    }

}


@variant([2, 0])
export class P2PTrustRelationInterface extends SingleDBInterface<P2PTrustRelation, BinaryDocumentStore<P2PTrustRelation>>
{

}

@variant([0, 2])
export class P2PTrust extends DBInterface {

    @field({ type: IdentitySerializable })
    rootTrust: IdentitySerializable

    @field({ type: P2PTrustRelationInterface })
    db: P2PTrustRelationInterface

    cid?: string;

    constructor(props?: {
        rootTrust: IdentitySerializable
        db: P2PTrustRelationInterface;
    } | {
        rootTrust: IdentitySerializable
    }) {
        super();
        if (props) {
            Object.assign(this, props)
        }
        if (!this.db) {
            this.db = new P2PTrustRelationInterface({
                name: '_trust',
                storeOptions: new BinaryDocumentStoreOptions({
                    indexBy: TRUSTEE_KEY,
                    objectType: P2PTrustRelation.name
                })
            })
        }
    }

    get initialized(): boolean {
        return this.db.initialized
    }

    close() {
        this.db.close();
    }

    async init(orbitDB: OrbitDB, options: IStoreOptions<any, any>): Promise<void> {
        options.typeMap[P2PTrustRelation.name] = P2PTrustRelation;
        await this.db.init(orbitDB, options);
        if (!this.cid) {
            await this.save(orbitDB._ipfs);
        }

    }


    async load(waitForReplicationEventsCount = 0): Promise<void> {
        await this.db.load(waitForReplicationEventsCount);
    }


    async addTrust(trustee: IdentitySerializable) {
        if (!this.db.db) {
            await this.db.load();
        }
        await this.db.db.put(new P2PTrustRelation({
            trustee
        }));
    }

    async save(node: IPFSInstance): Promise<string> {
        if (!this.db.initialized || !this.rootTrust) {
            throw new Error("Not initialized");
        }

        let arr = serialize(this);
        let addResult = await node.add(arr)
        let pinResult = await node.pin.add(addResult.cid)
        this.cid = pinResult.toString();
        return this.cid;
    }


    static async loadFromCID(cid: string, node: IPFSInstance): Promise<P2PTrust> {
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
        return this.cid + '-' + 'replication'
    }


    /**
     * Follow trust path back to trust root.
     * Trust root is always trusted.
     * Hence if
     * Root trust A trust B trust C
     * C is trusted by Root
     * @param trustee 
     * @param truster, the truster "root", if undefined defaults to the root trust
     * @returns true, if trusted
     */
    isTrusted(trustee: IdentitySerializable, truster: IdentitySerializable = this.rootTrust): boolean {

        /**
         * TODO: Currently very inefficient
         */
        return !!getTrustPath(trustee, truster, this.db);
    }



}

export const getTrustPath = (start: IdentitySerializable, end: IdentitySerializable, db: SingleDBInterface<P2PTrustRelation, BinaryDocumentStore<P2PTrustRelation>>): P2PTrustRelation[] => {
    return getTargetPath(start, (key) => end.publicKey === key.publicKey, db)
}


/**
 * Get path, to target.
 * @param start 
 * @param target 
 * @param db 
 * @returns 
 */
export const getTargetPath = (start: IdentitySerializable, target: (key: IdentitySerializable) => boolean, db: SingleDBInterface<P2PTrustRelation, BinaryDocumentStore<P2PTrustRelation>>, fullOp: boolean = false): P2PTrustRelation[] => {

    /**
     * TODO: Currently very inefficient
     */
    if (!db) {
        throw new Error("Not initalized")
    }

    let path = [];
    let current = start;
    const visited = new Set();
    while (true) {
        if (target(current)) {
            return path;
        }
        let trust = db.db.index.get(current.toString(), true) as LogEntry<P2PTrustRelation>;
        if (!trust) {
            return undefined; // no path
        }

        // TODO: could be multiple but we just follow one path for now
        if (current == trust.payload.value.trustee) {
            return undefined; // no path
        }

        // Assumed message is signed
        let truster = trust.identity;
        let trustRelation = trust.payload.value;
        trustRelation.truster = truster;
        let key = truster.toString();
        if (visited.has(key)) {
            return undefined; // we are in a loop, abort
        }

        visited.add(key);
        current = truster; // move upwards in trust tree
        path.push(trustRelation);

    }
}

