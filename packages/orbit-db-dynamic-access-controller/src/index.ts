
// Can modify owned entries?
// Can remove owned entries?
// Can modify any entries?
// Can remove any entries?

// Relation with enc/dec?
import { deserialize, field, serialize, variant } from "@dao-xyz/borsh";
import { Identities, Identity, IdentitySerializable } from '@dao-xyz/orbit-db-identity-provider';
import { Entry, Payload } from '@dao-xyz/ipfs-log-entry';
import { AccessController, Address, IInitializationOptions, IStoreOptions, Store, StoreAccessController, StoreLike } from '@dao-xyz/orbit-db-store';
import { OrbitDB } from '@dao-xyz/orbit-db';
import { ACLInterface } from './acl-db';
import { Access, AccessData } from './access';
export * from './access';
import isNode from 'is-node';
import { MaybeEncrypted } from "@dao-xyz/encryption-utils";
import { PublicKey } from "@dao-xyz/identity";
import { TrustWebAccessController } from "@dao-xyz/orbit-db-trust-web";
import { Log } from "@dao-xyz/ipfs-log";
import Cache from '@dao-xyz/orbit-db-cache';
import { Operation } from "@dao-xyz/orbit-db-bdocstore";
import { Ed25519PublicKey } from 'sodium-plus';
let v8 = undefined;
if (isNode) {
    v8 = require('v8');
}
@variant(0)
export class AccessRequest {

    @field({ type: String })
    shard: string;

    @field({ type: Access })
    access: Access;

    constructor(opts?: {
        shard?: string,
        access?: Access
    }) {
        if (opts) {
            Object.assign(this, opts);
        }
    }

    public get accessTopic() {
        return this.shard + '/access';
    }
}

export type AccessVerifier = (identity: IdentitySerializable) => Promise<boolean>




export const DYNAMIC_ACCESS_CONTROLER = 'dynamic-access-controller';

export type OnMemoryExceededCallback<T> = (payload: MaybeEncrypted<Payload<T>>, identity: IdentitySerializable) => void;

@variant([0, 2])
export class DynamicAccessController<T> extends AccessController<T> implements StoreLike<Operation> {

    /*  _storeAccessCondition: (entry: Entry<T>, store: B) => Promise<boolean>; */

    @field({ type: ACLInterface })
    _db: ACLInterface

    appendAll: boolean;


    _initializationPromise: Promise<void>;
    _orbitDB: OrbitDB
    _heapSizeLimit?: () => number;
    _onMemoryExceeded?: OnMemoryExceededCallback<T>;


    constructor(properties?: { name: string, rootTrust: PublicKey | Identity | IdentitySerializable }) {
        super();
        if (properties) {
            this._db = new ACLInterface({
                name: properties.name + "_acl",
                rootTrust: properties.rootTrust
            })
            /*  this._acldb = ); */
            //subscribeToQueriies to not exist on store options
            /*           this._storeOptions = Object.assign({}, { typeMap: {}, appendAll: options.appendAll, trustResolver: options?.trustResolver }, options.storeOptions);
                      
                      this._orbitDB = options?.orbitDB;
                      this._appendAll = options.appendAll; */


        }
    }
    set memoryOptions(options: { heapSizeLimit: () => number; onMemoryExceeded: OnMemoryExceededCallback<T> }) {
        this._heapSizeLimit = options.heapSizeLimit;
        this._onMemoryExceeded = options.onMemoryExceeded;
    }

    //{ heapSizeLimit: () => number, onMemoryExceeded: OnMemoryExceededCallback<T>, storeAccessCondition: (entry: Entry<T>, store: B) => Promise<boolean>/* , trust: TrustWebAccessController */ }
    /* this._heapSizeLimit = options.heapSizeLimit;
    this._onMemoryExceeded = options.onMemoryExceeded;
    this._storeAccessCondition = options.storeAccessCondition; */

    get trust(): TrustWebAccessController {
        return (this._db.store.access as TrustWebAccessController);
    }

    get acl(): ACLInterface {
        return this._db;
    }

    /* async load(cid: string): Promise<void> {
        this._initializationPromise = new Promise(async (resolve, _reject) => {
            let arr = await this._orbitDB._ipfs.cat(cid);
            for await (const obj of arr) {
                let der = deserialize(Buffer.from(obj), ACLInterface);
                this._aclDB = der;
                await this._acldb.init(this._orbitDB, this._storeOptions);
                await this._acldb.load();
            }
            resolve();
        })
        await this._initializationPromise;


    } */

    /* async save(): Promise<{ address: string }> {
        if (!this._aclDB) {
            throw new Error("Not initialized");
        }
        if (!this._acldb.initialized) {
            await this._acldb.init(this._orbitDB, this._storeOptions);
        }

        let arr = serialize(this._aclDB);
        let addResult = await this._orbitDB._ipfs.add(arr)
        let pinResult = await this._orbitDB._ipfs.pin.add(addResult.cid)
        return {
            address: pinResult.toString()
        };
    } */


    /*   setStore(store: B) {
          this._store = store;
      } */

    async canRead(payload: MaybeEncrypted<Payload<T>>, key: Ed25519PublicKey): Promise<boolean> {

        // Check whether it is trusted by trust web
        if (await this._db.rootTrust.isTrusted(key)) {
            return true;
        }

        if (await this._db.canRead(payload, key)) {
            return true; // Creator of entry does not own NFT or token, or publickey etc
        }
        return false;
    }

    async canAppend(payload: MaybeEncrypted<Payload<T>>, identityEncrypted: MaybeEncrypted<IdentitySerializable>, identityProvider: Identities) {
        const identity = (await identityEncrypted.decrypt()).getValue(IdentitySerializable);
        if (!identityProvider.verifyIdentity(identity)) {
            return false;
        }

        if (this.appendAll) {
            return true;
        }

        await this._initializationPromise;

        if (this._heapSizeLimit) {
            const usedHeapSize = v8?.getHeapStatistics().used_heap_size;
            if (usedHeapSize > this._heapSizeLimit()) {
                if (this._onMemoryExceeded)
                    this._onMemoryExceeded(payload, identity);
                return false;
            }
        }



        // Check whether it is trusted by trust web
        if (await this._db.rootTrust.isTrusted(identity)) {
            return true;
        }

        if (await this._db.canWrite(payload, identityEncrypted)) {
            return true; // Creator of entry does not own NFT or token, or publickey etc
        }
        return false;
    }



    /* async close() {
        await this._initializationPromise;
        await this._acldb.close();
    } */

    async init(ipfs, identity, options: IInitializationOptions<Access>) {
        /*  this._trust = options.trust; */
        return this._db.init(ipfs, identity, options)
    }

    close(): Promise<void> {
        return this._db.close();
    }
    drop(): Promise<void> {
        return this._db.drop();
    }
    load(): Promise<void> {
        return this._db.load();
    }
    save(ipfs: any, options?: { format?: string; pin?: boolean; timeout?: number; }) {
        return this._db.save(ipfs, options);
    }
    sync(heads: Entry<Access>[]): Promise<void> {
        return this._db.sync(heads);
    }
    get replicationTopic(): string {
        return this._db.replicationTopic;
    }
    get events(): import("events") {
        return this._db.events;
    }
    get address(): Address {
        return this._db.address;
    }
    get oplog(): Log<Access> {
        return this._db.oplog;
    }
    get cache(): Cache {
        return this._db.cache;
    }
    get id(): string {
        return this._db.id;
    }
    get replicate(): boolean {
        return this._db.replicate;
    }
    getHeads(): Promise<Entry<Operation>[]> {
        return this._db.getHeads();
    }
    get name(): string {
        return this._db.name;
    }
}




/* 
@variant(0) // version
export class SignedAccessRequest {

    @field({ type: AccessRequest })
    request: AccessRequest

    // Include time so we can invalidate "old" requests
    @field({ serialize: (arg: number, writer) => writer.writeU64(arg), deserialize: (reader) => reader.readU64() })
    time: number;

    @field({ type: IdentitySerializable })
    identity: IdentitySerializable

    @field({ type: option('string') })
    signature: string;


    constructor(options?: { request: AccessRequest }) {

        if (options) {
            Object.assign(this, options);
        }
    }

    serializePresigned(): Uint8Array {
        return serialize(new SignedAccessRequest({ ...this, signature: undefined }))
    }

    async sign(identity: Identity) {
        this.signature = await identity.provider.sign(this.serializePresigned(),identity)
    }

    async verifySignature(identities: Identities): Promise<boolean> {
        return identities.verify(this.signature, this.identity.publicKey, this.serializePresigned(), 'v1')
    }
}
 */