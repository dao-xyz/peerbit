
// Can modify owned entries?
// Can remove owned entries?
// Can modify any entries?
// Can remove any entries?

// Relation with enc/dec?
import { field, variant } from "@dao-xyz/borsh";
import { Entry, Identity, Payload } from '@dao-xyz/ipfs-log';
import { Address, IInitializationOptions, StoreLike } from '@dao-xyz/peerbit-dstore';
import { OrbitDB } from '@dao-xyz/orbit-db';
import { AccessStore } from './acl-db';
import { Access } from './access';
export * from './access';
// @ts-ignore

import { MaybeEncrypted, SignatureWithKey } from "@dao-xyz/peerbit-crypto";
import { PublicSignKey } from "@dao-xyz/peerbit-crypto";
import { TrustedNetwork } from "@dao-xyz/peerbit-trusted-network";
import { Log } from "@dao-xyz/ipfs-log";
import { Operation } from "@dao-xyz/peerbit-ddoc";
// @ts-ignore
import { v4 as uuid } from 'uuid';
import { IPFS } from "ipfs-core-types";
import { QueryStoreInitializationOptions } from "@dao-xyz/orbit-db-query-store";

/* let v8 = undefined;
if (isNode) {
    v8 = require('v8');
} */


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
export type AccessVerifier = (identity: PublicSignKey) => Promise<boolean>


@variant([0, 3])
export class DynamicAccessController<T> implements StoreLike<Operation<T>> {

    /*  _storeAccessCondition: (entry: Entry<T>, store: B) => Promise<boolean>; */

    @field({ type: AccessStore })
    _db: AccessStore


    _initializationPromise: Promise<void>;
    _orbitDB: OrbitDB
    /*     _heapSizeLimit?: () => number;
        _onMemoryExceeded?: OnMemoryExceededCallback<T>; */


    constructor(properties?: {
        name?: string,
        rootTrust?: PublicSignKey,
        trustedNetwork?: TrustedNetwork
    }) {
        if (properties) {
            this._db = new AccessStore({
                name: (uuid() || properties.name) + "_acl",
                rootTrust: properties.rootTrust,
                trustedNetwork: properties.trustedNetwork

            })
        }
    }

    get acl(): AccessStore {
        return this._db;
    }
    async canRead(s: SignatureWithKey): Promise<boolean> {

        // Check whether it is trusted by trust web
        if (await this._db.trustedNetwork.isTrusted(s.publicKey)) {
            return true;
        }

        if (await this._db.canRead(s)) {
            return true; // Creator of entry does not own NFT or token, or PublicSignKey etc
        }
        return false;
    }

    async canAppend(payload: MaybeEncrypted<Payload<Operation<T>>>, identityEncrypted: MaybeEncrypted<SignatureWithKey>) {
        const identity = (await identityEncrypted.decrypt(this._db.oplog._encryption?.getAnyKeypair || (() => Promise.resolve(undefined)))).getValue(SignatureWithKey).publicKey;


        await this._initializationPromise;

        // Check whether it is trusted by trust web
        if (await this._db.trustedNetwork.isTrusted(identity)) {
            return true;
        }


        if (await this._db.canAppend(payload, identityEncrypted)) {
            return true; // Creator of entry does not own NFT or token, or PublicSignKey etc
        }



        return false;
    }


    async init(ipfs: IPFS, identity: Identity, options: QueryStoreInitializationOptions<Operation<Access>>): Promise<DynamicAccessController<T>> {
        /*  this._trust = options.trust; */
        await this._db.init(ipfs, identity, options)
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
    sync(heads: Entry<Operation<Access>>[]): Promise<void> {
        return this._db.sync(heads);
    }

    get address(): Address {
        return this._db.address;
    }
    get oplog(): Log<Operation<Access>> {
        return this._db.oplog;
    }
    get id(): string {
        return this._db.id;
    }
    get replicate(): boolean {
        return this._db.replicate;
    }

    get name(): string {
        return this._db.name;
    }
}