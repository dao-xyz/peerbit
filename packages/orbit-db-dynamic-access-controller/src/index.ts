import * as ipfs from 'ipfs';
import { IPFS as IPFSInstance } from 'ipfs-core-types'
// Can modify owned entries?
// Can remove owned entries?
// Can modify any entries?
// Can remove any entries?

// Relation with enc/dec?
import { field, option, serialize, variant, vec } from "@dao-xyz/borsh";
import { Identities, Identity, IdentitySerializable } from '@dao-xyz/orbit-db-identity-provider';
import { Entry } from '@dao-xyz/ipfs-log';
import AccessController from "orbit-db-access-controllers/src/access-controller-interface";
import AccessControllers from 'orbit-db-access-controllers';
import { IStoreOptions, Store } from '@dao-xyz/orbit-db-store';
import { OrbitDB } from '@dao-xyz/orbit-db';
import { ACLInterface } from './acl-db';
import { Access, AccessData } from './access';
import { BinaryDocumentStoreOptions } from '@dao-xyz/orbit-db-bdocstore';





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

@variant(0) // version
export class SignedAccessRequest {

    @field({ type: AccessRequest })
    request: AccessRequest

    // Include time so we can invalidate "old" requests
    @field({ serialize: (arg: number, writer) => writer.writeU64(arg), deserialize: (reader) => reader.readU64() })
    time: number;

    @field({ type: IdentitySerializable })
    identity: IdentitySerializable

    @field({ type: option('String') })
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
        this.signature = await identity.provider.sign(identity, this.serializePresigned())
    }

    async verifySignature(identities: Identities): Promise<boolean> {
        return identities.verify(this.signature, this.identity.publicKey, this.serializePresigned(), 'v1')
    }
}

export type AccessVerifier = (identity: IdentitySerializable) => Promise<boolean>




export const DYNAMIC_ACCESS_CONTROLER = 'dynamic-access-controller';

export class DynamicAccessController<T, B extends Store<T, any, any>> extends AccessController {

    @field({ type: ACLInterface })
    aclDB: ACLInterface;

    _store: B;
    _initializationPromise: Promise<void>;
    _storeAccessCondition: (entry: Entry<T>, store: B) => Promise<boolean>;
    _storeOptions: IStoreOptions<Access, any>;
    _orbitDB: OrbitDB
    constructor(options?: { orbitDB: OrbitDB, storeAccessCondition: (entry: Entry<T>, store: B) => Promise<boolean>, storeOptions: IStoreOptions<Access, any> }) {
        super();
        this._storeAccessCondition = options?.storeAccessCondition;
        if (!this.aclDB) {
            this.aclDB = new ACLInterface({
                name: 'acl',
                storeOptions: new BinaryDocumentStoreOptions<Access>({
                    indexBy: 'id',
                    objectType: AccessData.name
                })
            })
        }
        this._storeOptions = options?.storeOptions;
        this._storeOptions.typeMap[AccessData.name] = AccessData;
        this._orbitDB = options?.orbitDB;
    }

    async load(_address: string): Promise<void> {


        // ACL DB
        this._initializationPromise = new Promise(async (resolve, reject) => {
            await this.aclDB.init(this._orbitDB, this._storeOptions);
            await this.aclDB.load();
            resolve()
        })
        await this._initializationPromise;


    }

    setStore(store: B) {
        this._store = store;
    }

    static get type() { return DYNAMIC_ACCESS_CONTROLER } // Return the type for this controller

    async canAppend(entry: Entry<any>, identityProvider: Identities) {
        await this._initializationPromise;
        // logic to determine if entry can be added, for example:
        /*  if (entry.payload === "hello world" && entry.identity.id === identity.id && identityProvider.verifyIdentity(entry.identity))
           return true */

        // Check identity
        if (!identityProvider.verifyIdentity(entry.identity)) {
            return false;
        }

        if (await !this.aclDB.allowed(entry)) {
            return false; // Creator of entry does not own NFT or token, or publickey etc
        }



        // Verify message is trusted
        /*         let key = PublicKey.from(entry.identity);
         */
        /*  if (!this.trustRegionResolver().isTrusted(key)) {
                    return false
                } */
        return true;
    }


    /*  async load(address: string) {
         await super.load(address); */
    /*      if (address) {
             try {
                 if (address.indexOf('/ipfs') === 0) { address = address.split('/')[2] }
                 const access = await io.read(this._ipfs, address)
                 this.contractAddress = access.contractAddress
                 this.abi = JSON.parse(access.abi)
             } catch (e) {
                 console.log('ContractAccessController.load ERROR:', e)
             }
         }
         this.contract = new this.web3.eth.Contract(this.abi, this.contractAddress) */
    /*  } */

    async close() {
        await this._initializationPromise;
        await this.aclDB.close();
    }

    static async create<T, B extends Store<T, any, any>>(orbitDB: OrbitDB, options: { storeAccessCondition: (entry: Entry<T>, store: B) => Promise<boolean>, storeOptions: IStoreOptions<Access, any> }): Promise<DynamicAccessController<T, B>> {
        return new DynamicAccessController({ orbitDB, ...options })
    }
}

AccessControllers.addAccessController({ AccessController: DynamicAccessController })




