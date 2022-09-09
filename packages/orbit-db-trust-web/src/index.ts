import { deserialize, field, option, serialize, variant } from "@dao-xyz/borsh";
import { BinaryDocumentStore } from "@dao-xyz/orbit-db-bdocstore";
import { Identities, Identity, IdentitySerializable } from "@dao-xyz/orbit-db-identity-provider";
import { OrbitDB } from "@dao-xyz/orbit-db";
import { AccessController, Address, IInitializationOptions, IStoreOptions, StoreAccessController } from "@dao-xyz/orbit-db-store";
import { Payload } from "@dao-xyz/ipfs-log-entry";

import { createHash } from "crypto";
import { PublicKey, TrustData } from "@dao-xyz/identity";
import { arraysEqual } from "@dao-xyz/io-utils";

import isNode from 'is-node';
import { MaybeEncrypted } from "@dao-xyz/encryption-utils";
import { IPFS } from "ipfs-core-types/src";

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
export const getTargetPath = (start: PublicKey, target: (key: PublicKey) => boolean, db: BinaryDocumentStore<P2PTrustRelation>, fullOp: boolean = false): P2PTrustRelation[] => {

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
        let trust = db._index.get(PublicKey.from(current).hashCode());
        if (!trust) {
            return undefined; // no path
        }


        // TODO: could be multiple but we just follow one path for now
        /*    if (current.equals(trust.value.trustee)) {
               return undefined; // no path
           } */

        // Assumed message is signed
        let truster = trust.entry.identity;
        let trustRelation = trust.value;
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

    /* @field({ type: 'string' }) 
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
/* 
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

    getStoreOptions(options: { queryRegion?: string, replicate?: boolean, directory?: string }): IQueryStoreOptions<P2PTrustRelation, any, any> {
        return {
            subscribeToQueries: options.replicate,
            replicate: options.replicate,
            directory: options.directory,
            typeMap: {
                [P2PTrustRelation.name]: P2PTrustRelation
            },
            queryRegion: options.queryRegion,
            create: options.replicate,
            cache: undefined,
            nameResolver: (name: string) => name,
            accessController: {
                type: TRUST_WEB_ACCESS_CONTROLLER,
                trustResolver: () => this,
                skipManifest: true
            } // TODO fix types
        }
    }

    async init(orbitDB: OrbitDB, options: IQueryStoreOptions<any, any, any>): Promise<void> {
        const storeOptions = this.getStoreOptions(options);
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


   
    isTrusted(trustee: PublicKey | Identity | IdentitySerializable, truster: PublicKey = this.rootTrust): boolean {

        trustee = PublicKey.from(trustee);
     
        const trustPath = !!getTrustPath(trustee, truster, this);
        return trustPath;
    }

    hashCode(): string {
        return createHash('sha1').update(serialize(this)).digest('hex')
    }



} */

export const getTrustPath = (start: PublicKey, end: PublicKey, db: BinaryDocumentStore<P2PTrustRelation>): P2PTrustRelation[] => {
    return getTargetPath(start, (key) => end.type === key.type && arraysEqual(end.id, key.id), db)
}


@variant([0, 0])
export class TrustWebAccessController extends StoreAccessController<P2PTrustRelation, BinaryDocumentStore<P2PTrustRelation>, P2PTrustRelation> {

    @field({ type: PublicKey })
    rootTrust: PublicKey

    _appendAll: boolean;
    _orbitDB: OrbitDB;

    constructor(props?: {
        name?: string,
        queryRegion?: string,
        rootTrust: PublicKey | Identity | IdentitySerializable
    }) {
        super(props ? {
            store: new BinaryDocumentStore({
                indexBy: 'trustee',
                name: props?.name ? props?.name : '' + '_trust',
                accessController: undefined,
                objectType: P2PTrustRelation.name,
                queryRegion: props.queryRegion
            })
        } : undefined);
        if (props) {
            this.rootTrust = PublicKey.from(props.rootTrust);
            this.store._clazz = P2PTrustRelation;
        }
    }

    async init(ipfs: IPFS, identity: Identity, options: IInitializationOptions<any>): Promise<void> {
        const typeMap = options.typeMap ? { ...options.typeMap } : {}
        typeMap[P2PTrustRelation.name] = P2PTrustRelation;
        return super.init(ipfs, identity, { ...options, typeMap, fallbackAccessController: this }) // self referencing access controller
    }

    async canAppend(payload: MaybeEncrypted<Payload<any>>, identity: MaybeEncrypted<IdentitySerializable>, identityProvider: Identities): Promise<boolean> {

        await identity.decrypt();
        const identityDecrypted = identity.decrypted.getValue(IdentitySerializable);
        if (!identityProvider.verifyIdentity(identityDecrypted)) {
            return false;
        }
        if (this._appendAll) {
            return true;
        }

        const isTrusted = this.isTrusted(identityDecrypted)
        return isTrusted;
    }

    getStoreOptions(options: { queryRegion?: string, replicate?: boolean, directory?: string }): IStoreOptions<P2PTrustRelation> {
        return {
            subscribeToQueries: options.replicate,
            replicate: options.replicate,
            directory: options.directory,
            typeMap: {
                [P2PTrustRelation.name]: P2PTrustRelation
            },
            queryRegion: options.queryRegion,
            create: options.replicate,
            cache: undefined,
            nameResolver: (name: string) => name,
            accessController: {
                skipManifest: true
            } // TODO fix types
        } as any
    }



    async addTrust(trustee: PublicKey | Identity | IdentitySerializable) {
        trustee = PublicKey.from(trustee);
        await this.store.put(new P2PTrustRelation({
            trustee
        }));
    }
    /* 
        async save(): Promise<{ address: string }> {
            if (!this.store) {
                throw new Error("Not initialized");
            }
    
            let arr = serialize(this);
            let addResult = await this._orbitDB._ipfs.add(arr)
            let pinResult = await this._orbitDB._ipfs.pin.add(addResult.cid)
            this.cid = pinResult.toString();
            return {
                address: this.cid
            };
        }
    
     */


    /*  static async loadFromCID(cid: string, node: IPFSInstance): Promise<P2PTrust> {
         let arr = await node.cat(cid);
         for await (const obj of arr) {
             let der = deserialize(Buffer.from(obj), P2PTrust);
             der.cid = cid;
             return der;
         }
     }
  */
    /* get replicationTopic() {
        if (!this.cid) {
            throw new Error("Not initialized, replication topic requires known cid");
        }
        return this.cid + '-' + 'replication'
    } */


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
        const trustPath = !!getTrustPath(trustee, truster, this.store);
        return trustPath;
    }

    hashCode(): string {
        return createHash('sha1').update(serialize(this)).digest('hex')
    }

    async close() {

    }
}

