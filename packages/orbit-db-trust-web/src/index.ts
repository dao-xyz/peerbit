import { deserialize, field, option, serialize, variant } from "@dao-xyz/borsh";
import { BinaryDocumentStore, BinaryDocumentStoreOptions, LogEntry } from "@dao-xyz/orbit-db-bdocstore";
import { IPFS as IPFSInstance } from 'ipfs-core-types';
import { SingleDBInterface } from "@dao-xyz/orbit-db-store-interface";
import { Identities, Identity, IdentitySerializable } from "@dao-xyz/orbit-db-identity-provider";
import { OrbitDB } from "@dao-xyz/orbit-db";
import { BStoreOptions } from "@dao-xyz/orbit-db-bstores";
import AccessController from "orbit-db-access-controllers/src/access-controller-interface";
import AccessControllers from "orbit-db-access-controllers";
import { Entry } from "@dao-xyz/ipfs-log";
import { createHash } from "crypto";
import { IQueryStoreOptions } from "@dao-xyz/orbit-db-query-store";
import { PublicKey, TrustData } from "@dao-xyz/identity";

import isNode from 'is-node';
import { IStoreOptions } from "@dao-xyz/orbit-db-store";
let v8 = undefined;
if (isNode) {
    v8 = require('v8');
}
/**
 * Get path, to target.
 * @param start 
 * @param target 
 * @param db 
 * @returns 
 */
export const getTargetPath = (start: PublicKey, target: (key: PublicKey) => boolean, db: SingleDBInterface<P2PTrustRelation, BinaryDocumentStore<P2PTrustRelation>>, fullOp: boolean = false): P2PTrustRelation[] => {

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
        let trust = db.db.index.get(PublicKey.from(current).hashCode(), true) as LogEntry<P2PTrustRelation>;
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



@variant(1)
export class P2PTrustRelation extends TrustData {

    /*  @field({ type: PublicKey })
     truster: PublicKey  *///  Dont need this becaause its going to be signed with truster anyway (bc orbitdb)

    @field({ type: PublicKey })
    trustee: PublicKey  // the key to trust


    truster: PublicKey // will be set manually, upon deserialization from the oplog

    /* @field({ type: 'String' }) 
    signature: string */ // Dont need this because its going to be signed anyway (bc orbitdb)

    constructor(props?: {
        trustee: PublicKey
    }) {
        super();
        if (props) {
            this.trustee = props.trustee;
        }
    }

}

@variant([2, 0])
export class P2PTrust extends SingleDBInterface<P2PTrustRelation, BinaryDocumentStore<P2PTrustRelation>>
{
    @field({ type: PublicKey })
    rootTrust: PublicKey

    cid?: string;

    constructor(props?: {
        name?: string,
        rootTrust: PublicKey | Identity | IdentitySerializable;
        address?: string;
        storeOptions?: BStoreOptions<BinaryDocumentStore<P2PTrustRelation>>;
    }) {
        super({
            name: props?.name ? props?.name : '' + '_trust', address: props?.address, storeOptions: new BinaryDocumentStoreOptions({
                indexBy: 'trustee',
                objectType: P2PTrustRelation.name
            })
        });
        if (props) {
            this.rootTrust = PublicKey.from(props.rootTrust);
        }

    }

    getStoreOptions(replicate: boolean, directory?: string): IQueryStoreOptions<P2PTrustRelation, any> {
        return {
            subscribeToQueries: replicate,
            replicate,
            directory,
            queryRegion: undefined,
            typeMap: {
                [P2PTrustRelation.name]: P2PTrustRelation
            },
            create: replicate,
            cache: undefined,
            nameResolver: (name: string) => name,
            accessController: {
                type: TRUST_WEB_ACCESS_CONTROLLER,
                trustResolver: () => this,
                skipManifest: true
            } as TrustWebAccessControllerOptions
        }
    }

    async init(orbitDB: OrbitDB, options: IStoreOptions<any, any>): Promise<void> {
        const storeOptions = this.getStoreOptions(options.replicate, options.directory);
        await super.init(orbitDB, storeOptions);
        if (!this.cid) {
            await this.save(orbitDB._ipfs);
        }
    }

    async addTrust(trustee: PublicKey | Identity | IdentitySerializable) {

        trustee = PublicKey.from(trustee);
        if (!this.db) {
            await this.load();
        }

        await this.db.put(new P2PTrustRelation({
            trustee
        }));
    }

    async save(node: IPFSInstance): Promise<string> {
        if (!this.initialized || !this.rootTrust) {
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
    isTrusted(trustee: PublicKey | Identity | IdentitySerializable, truster: PublicKey = this.rootTrust): boolean {

        trustee = PublicKey.from(trustee);
        /**
         * TODO: Currently very inefficient
         */
        return !!getTrustPath(trustee, truster, this);
    }

    hashCode(): string {
        return createHash('sha1').update(serialize(this)).digest('hex')
    }



}

export const getTrustPath = (start: PublicKey, end: PublicKey, db: SingleDBInterface<P2PTrustRelation, BinaryDocumentStore<P2PTrustRelation>>): P2PTrustRelation[] => {
    return getTargetPath(start, (key) => end.id === key.id && end.type === key.type, db)
}


export const TRUST_WEB_ACCESS_CONTROLLER = 'trust-web-access-controller';

export type TrustWebAccessControllerOptions = {
    trustResolver: () => P2PTrust;
    skipManifest: true,
    appendAll?: boolean
};

export class TrustWebAccessController extends AccessController {

    // MAKE DISJOIN
    _trustResolver: () => P2PTrust
    _orbitDB: OrbitDB;
    _appendAll: boolean;
    constructor(props?: TrustWebAccessControllerOptions & { orbitDB: OrbitDB }) {
        super();
        if (props) {
            this._appendAll = props.appendAll;
            this._orbitDB = props.orbitDB;
            this._trustResolver = props.trustResolver;

        }

    }

    async canAppend(entry: Entry<any>, identityProvider: Identities): Promise<boolean> {

        if (!identityProvider.verifyIdentity(entry.identity)) {
            return false;
        }
        if (this._appendAll) {
            return true;
        }

        return this._trustResolver().isTrusted(entry.identity)
    }


    async load(cid: string): Promise<void> {
        // Nothing to load!
    }

    async save(): Promise<{ address: string, skipManifest: boolean }> {

        /*    let arr = Uint8Array.from([0]);
           let addResult = await this._orbitDB._ipfs.add(arr)
           let pinResult = await this._orbitDB._ipfs.pin.add(addResult.cid)
           let cid = pinResult.toString(); */

        return {
            address: '',
            skipManifest: true
        };
    }

    async close() {

    }

    static get type() { return TRUST_WEB_ACCESS_CONTROLLER } // Return the type for this controller

    static async create(orbitDB: OrbitDB, options: TrustWebAccessControllerOptions): Promise<TrustWebAccessController> {
        const controller = new TrustWebAccessController({ orbitDB, ...options })
        return controller;
    }
}

AccessControllers.addAccessController({ AccessController: TrustWebAccessController })
