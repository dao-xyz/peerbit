import { deserialize, field, option, serialize, variant } from "@dao-xyz/borsh";
import { BinaryDocumentStore, BinaryDocumentStoreOptions, LogEntry } from "@dao-xyz/orbit-db-bdocstore";
import { IPFS as IPFSInstance } from 'ipfs-core-types';
import { SingleDBInterface } from "@dao-xyz/orbit-db-store-interface";
import { IStoreOptions, Store } from "@dao-xyz/orbit-db-store";
import { Identities, Identity, IdentitySerializable } from "@dao-xyz/orbit-db-identity-provider";
import { OrbitDB } from "@dao-xyz/orbit-db";
import { BStoreOptions } from "@dao-xyz/orbit-db-bstores";
import AccessController from "orbit-db-access-controllers/src/access-controller-interface";
import AccessControllers from "orbit-db-access-controllers";
import { Entry } from "@dao-xyz/ipfs-log";
import bs58 from 'bs58';
import { createHash } from "crypto";
const TRUSTEE_PROPERTY_KEY = 'trustee';
@variant(0)
export class P2PTrustRelation {

    /*  @field({ type: PublicKey })
     truster: PublicKey  *///  Dont need this becaause its going to be signed with truster anyway (bc orbitdb)

    @field({ type: IdentitySerializable })
    [TRUSTEE_PROPERTY_KEY]: IdentitySerializable  // the key to trust


    truster: IdentitySerializable // will be set manually, upon deserialization from the oplog

    /* @field({ type: 'String' }) 
    signature: string */ // Dont need this because its going to be signed anyway (bc orbitdb)

    constructor(props?: {
        trustee: IdentitySerializable
    }) {
        if (props) {
            Object.assign(this, props)
        }
    }

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
        let trust = db.db.index.get(IdentitySerializable.from(current).hashCode(), true) as LogEntry<P2PTrustRelation>;
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


@variant([2, 0])
export class P2PTrust extends SingleDBInterface<P2PTrustRelation, BinaryDocumentStore<P2PTrustRelation>>
{
    @field({ type: IdentitySerializable })
    rootTrust: IdentitySerializable

    cid?: string;

    constructor(props?: {
        name?: string,
        rootTrust: Identity | IdentitySerializable;
        address?: string;
        storeOptions?: BStoreOptions<BinaryDocumentStore<P2PTrustRelation>>;
    }) {
        super({
            name: props?.name ? props?.name : '' + '_trust', address: props?.address, storeOptions: new BinaryDocumentStoreOptions({
                indexBy: TRUSTEE_PROPERTY_KEY,
                objectType: P2PTrustRelation.name
            })
        });
        if (props) {
            this.rootTrust = props.rootTrust instanceof IdentitySerializable ? props.rootTrust : props.rootTrust.toSerializable()
        }

    }



    async init(orbitDB: OrbitDB, options: IStoreOptions<any, any>): Promise<void> {
        options = { ...options };
        options.typeMap[P2PTrustRelation.name] = P2PTrustRelation;
        options.accessController = {
            type: TRUST_WEB_ACCESS_CONTROLLER,
            trustResolver: () => this,
            skipManifest: true
        } as TrustWebAccessControllerOptions

        await super.init(orbitDB, options);
        if (!this.cid) {
            await this.save(orbitDB._ipfs);
        }

    }

    async addTrust(trustee: IdentitySerializable | Identity) {
        if (trustee instanceof Identity)
            trustee = trustee.toSerializable();

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
    isTrusted(trustee: IdentitySerializable | Identity, truster: IdentitySerializable = this.rootTrust): boolean {

        /**
         * TODO: Currently very inefficient
         */
        return !!getTrustPath(trustee instanceof Identity ? trustee.toSerializable() : trustee, truster instanceof Identity ? truster.toSerializable() : truster, this);
    }

    hashCode(): string {
        return createHash('sha1').update(serialize(this)).digest('hex')
    }



}

export const getTrustPath = (start: IdentitySerializable, end: IdentitySerializable, db: SingleDBInterface<P2PTrustRelation, BinaryDocumentStore<P2PTrustRelation>>): P2PTrustRelation[] => {
    return getTargetPath(start, (key) => end.publicKey === key.publicKey, db)
}


export const TRUST_WEB_ACCESS_CONTROLLER = 'trust-web-access-controller';

export type TrustWebAccessControllerOptions = {
    trustResolver: () => P2PTrust;
    skipManifest: true
};

export class TrustWebAccessController extends AccessController {

    // MAKE DISJOIN
    _trustResolver: () => P2PTrust
    _orbitDB: OrbitDB;

    constructor(props?: TrustWebAccessControllerOptions & { orbitDB: OrbitDB }) {
        super();
        if (props) {
            this._orbitDB = props.orbitDB;
            this._trustResolver = props.trustResolver;
        }

    }

    async canAppend(entry: Entry<any>, identityProvider: Identities): Promise<boolean> {

        if (!identityProvider.verifyIdentity(entry.identity)) {
            return false;
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
