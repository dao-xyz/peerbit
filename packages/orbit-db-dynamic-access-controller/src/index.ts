
// Can modify owned entries?
// Can remove owned entries?
// Can modify any entries?
// Can remove any entries?

// Relation with enc/dec?
import { deserialize, field, serialize, variant } from "@dao-xyz/borsh";
import { Identities, Identity, IdentitySerializable } from '@dao-xyz/orbit-db-identity-provider';
import { Entry, Payload } from '@dao-xyz/ipfs-log-entry';
import { AccessController, AccessControllers } from "@dao-xyz/orbit-db-access-controllers";
import { Store } from '@dao-xyz/orbit-db-store';
import { OrbitDB } from '@dao-xyz/orbit-db';
import { ACLInterface, ACLInterfaceOptions } from './acl-db';
import { Access, AccessData } from './access';
import { P2PTrust } from "@dao-xyz/orbit-db-trust-web";
export * from './access';
import isNode from 'is-node';
import { MaybeEncrypted } from "@dao-xyz/encryption-utils";
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
export class DynamicAccessController<T, B extends Store<T, any, any, any>> extends AccessController<T> {

    aclDB: ACLInterface;
    _store: B;
    _initializationPromise: Promise<void>;
    _storeAccessCondition: (entry: Entry<T>, store: B) => Promise<boolean>;
    _storeOptions: ACLInterfaceOptions;
    _orbitDB: OrbitDB
    _appendAll: boolean;
    _heapSizeLimit: () => number;
    _onMemoryExceeded: OnMemoryExceededCallback<T>;

    @field({ type: 'string' })
    name: string;

    constructor(options?: { orbitDB: OrbitDB, name: string, heapSizeLimit: () => number, onMemoryExceeded: OnMemoryExceededCallback<T>, appendAll?: boolean, trustResolver: () => P2PTrust, storeAccessCondition: (entry: Entry<T>, store: B) => Promise<boolean>, storeOptions: ACLInterfaceOptions }) {
        super();
        if (options) {
            this._storeAccessCondition = options?.storeAccessCondition;
            this.name = options?.name
            if (!this.aclDB) {
                this.aclDB = new ACLInterface({
                    name: options.name + "_acl"

                })
            }
            //subscribeToQueriies to not exist on store options
            this._storeOptions = Object.assign({}, { typeMap: {}, appendAll: options.appendAll, trustResolver: options?.trustResolver }, options.storeOptions);
            this._storeOptions.typeMap[AccessData.name] = AccessData;
            this._orbitDB = options?.orbitDB;
            this._appendAll = options.appendAll;
            this._heapSizeLimit = options.heapSizeLimit;
            this._onMemoryExceeded = options.onMemoryExceeded;
        }
    }

    async load(cid: string): Promise<void> {
        this._initializationPromise = new Promise(async (resolve, _reject) => {
            let arr = await this._orbitDB._ipfs.cat(cid);
            for await (const obj of arr) {
                let der = deserialize(Buffer.from(obj), ACLInterface);
                this.aclDB = der;
                await this.aclDB.init(this._orbitDB, this._storeOptions);
                await this.aclDB.load();
            }
            resolve();
        })
        await this._initializationPromise;


    }

    async save(): Promise<{ address: string }> {
        if (!this.aclDB) {
            throw new Error("Not initialized");
        }
        if (!this.aclDB.initialized) {
            await this.aclDB.init(this._orbitDB, this._storeOptions);
        }

        let arr = serialize(this.aclDB);
        let addResult = await this._orbitDB._ipfs.add(arr)
        let pinResult = await this._orbitDB._ipfs.pin.add(addResult.cid)
        return {
            address: pinResult.toString()
        };
    }


    setStore(store: B) {
        this._store = store;
    }


    async canAppend(payload: MaybeEncrypted<Payload<T>>, identityEncrypted: MaybeEncrypted<IdentitySerializable>, identityProvider: Identities) {
        const identity = (await identityEncrypted.decrypt()).getValue(IdentitySerializable);
        if (!identityProvider.verifyIdentity(identity)) {
            return false;
        }

        if (this._appendAll) {
            return true;
        }

        await this._initializationPromise;
        const usedHeapSize = v8.getHeapStatistics().used_heap_size;
        if (usedHeapSize > this._heapSizeLimit()) {
            if (this._onMemoryExceeded)
                this._onMemoryExceeded(payload, identity);
            return false;
        }


        // Check whether it is trusted by trust web
        if (await this._storeOptions.trustResolver().isTrusted(identity)) {
            return true;
        }

        if (await this.aclDB.allowed(payload, identityEncrypted)) {
            return true; // Creator of entry does not own NFT or token, or publickey etc
        }
        return false;
    }

    get trust() {
        return this._storeOptions.trustResolver();
    }

    async close() {
        await this._initializationPromise;
        await this.aclDB.close();
    }

    static get type() { return DYNAMIC_ACCESS_CONTROLER } // Return the type for this controller


    static async create<T, B extends Store<T, any, any, any>>(orbitDB: OrbitDB, options: { name: string, appendAll?: boolean, heapSizeLimit: () => number, onMemoryExceeded: OnMemoryExceededCallback<T>, trustResolver: () => P2PTrust, storeAccessCondition: (entry: Entry<T>, store: B) => Promise<boolean>, storeOptions: ACLInterfaceOptions }): Promise<DynamicAccessController<T, B>> {
        const controller = new DynamicAccessController({ orbitDB, ...options })
        return controller;
    }
}

AccessControllers.addAccessController({ AccessController: DynamicAccessController })






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