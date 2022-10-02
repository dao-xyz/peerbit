
// Can modify owned entries?
// Can remove owned entries?
// Can modify any entries?
// Can remove any entries?

// Relation with enc/dec?
import { field, variant } from "@dao-xyz/borsh";
import { Entry, Payload } from '@dao-xyz/ipfs-log-entry';
import { Address, IInitializationOptions, StoreLike } from '@dao-xyz/orbit-db-store';
import { OrbitDB } from '@dao-xyz/orbit-db';
import { AccessStore } from './acl-db';
import { Access } from './access';
export * from './access';
import isNode from 'is-node';
import { MaybeEncrypted } from "@dao-xyz/peerbit-crypto";
import { PublicKey } from "@dao-xyz/peerbit-crypto";
import { RegionAccessController } from "@dao-xyz/orbit-db-trust-web";
import { Log } from "@dao-xyz/ipfs-log";
import Cache from '@dao-xyz/orbit-db-cache';
import { Operation } from "@dao-xyz/orbit-db-bdocstore";
import { ReadWriteAccessController } from "@dao-xyz/orbit-db-query-store";
import { v4 as uuid } from 'uuid';

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

export const DYNAMIC_ACCESS_CONTROLER = 'dynamic-access-controller';
export type AccessVerifier = (identity: PublicKey) => Promise<boolean>


@variant([0, 3])
export class DynamicAccessController<T> extends ReadWriteAccessController<T> implements StoreLike<Operation<T>> {

    /*  _storeAccessCondition: (entry: Entry<T>, store: B) => Promise<boolean>; */

    @field({ type: AccessStore })
    _db: AccessStore


    _initializationPromise: Promise<void>;
    _orbitDB: OrbitDB
    /*     _heapSizeLimit?: () => number;
        _onMemoryExceeded?: OnMemoryExceededCallback<T>; */


    constructor(properties?: {
        name?: string,
        rootTrust?: PublicKey,
        regionAccessController?: RegionAccessController
    }) {
        super();
        if (properties) {
            this._db = new AccessStore({
                name: (uuid() || properties.name) + "_acl",
                rootTrust: properties.rootTrust,
                regionAccessController: properties.regionAccessController

            })
            /*  this._acldb = ); */
            //subscribeToQueriies to not exist on store options
            /*           this._storeOptions = Object.assign({}, { typeMap: {}, allowAll: options.allowAll, trustResolver: options?.trustResolver }, options.storeOptions);
                      
                      this._orbitDB = options?.orbitDB;
                      this._allowAll = options.allowAll; */


        }
    }
    set memoryOptions(options: { heapSizeLimit: () => number; onMemoryExceeded: OnMemoryExceededCallback<T> }) {
        this._heapSizeLimit = options.heapSizeLimit;
        this._onMemoryExceeded = options.onMemoryExceeded;
    }

    //{ heapSizeLimit: () => number, onMemoryExceeded: OnMemoryExceededCallback<T>, storeAccessCondition: (entry: Entry<T>, store: B) => Promise<boolean>/* , trust: RegionAccessController */ }
    /* this._heapSizeLimit = options.heapSizeLimit;
    this._onMemoryExceeded = options.onMemoryExceeded;
    this._storeAccessCondition = options.storeAccessCondition; */

    get trust(): RegionAccessController {
        return (this._db.access.accessController as RegionAccessController);
    }


    get allowAll(): boolean {
        return this._allowAll;
    }

    set allowAll(value: boolean) {
        this._allowAll = value;
        this._db.trust._allowAll = value;
    }

    get acl(): AccessStore {
        return this._db;
    }

    /* async load(cid: string): Promise<void> {
        this._initializationPromise = new Promise(async (resolve, _reject) => {
            let arr = await this._orbitDB._ipfs.cat(cid);
            for await (const obj of arr) {
                let der = deserialize(obj, ACL);
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

    async canRead(key: PublicKey): Promise<boolean> {

        if (this.allowAll) {
            return true;
        }

        // Check whether it is trusted by trust web
        if (await this._db.trust.isTrusted(key)) {
            return true;
        }

        if (await this._db.canRead(key)) {
            return true; // Creator of entry does not own NFT or token, or publickey etc
        }
        return false;
    }

    async canAppend(payload: MaybeEncrypted<Payload<T>>, identityEncrypted: MaybeEncrypted<PublicKey>) {
        const identity = (await identityEncrypted.decrypt()).getValue(PublicKey);

        if (this.allowAll) {
            return true;
        }

        await this._initializationPromise;

        // Check whether it is trusted by trust web
        if (await this._db.trust.isTrusted(identity)) {
            return true;
        }


        if (await this._db.canWrite(identity)) {
            return true; // Creator of entry does not own NFT or token, or publickey etc
        }



        return false;
    }



    /* async close() {
        await this._initializationPromise;
        await this._acldb.close();
    } */

    async init(ipfs, publicKey: PublicKey, sign: (data: Uint8Array) => Promise<Uint8Array>, options: IInitializationOptions<Access>): Promise<DynamicAccessController<T>> {
        /*  this._trust = options.trust; */
        await this._db.init(ipfs, publicKey, sign, options)
        return this;
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
    getHeads(): Promise<Entry<Operation<any>>[]> {
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