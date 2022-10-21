
// Can modify owned entries?
// Can remove owned entries?
// Can modify any entries?
// Can remove any entries?

// Relation with enc/dec?
import { field, variant } from "@dao-xyz/borsh";
import { Entry, Identity, Payload } from '@dao-xyz/ipfs-log';
import { Address, IInitializationOptions } from '@dao-xyz/peerbit-store';
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
import { DSearchInitializationOptions } from "@dao-xyz/peerbit-dsearch";
import { Program, ProgramInitializationOptions, RootProgram } from "@dao-xyz/peerbit-program";


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
export class DynamicAccessController<T> extends Program implements RootProgram {

    /*  _storeAccessCondition: (entry: Entry<T>, store: B) => Promise<boolean>; */

    @field({ type: AccessStore })
    _db: AccessStore



    _canRead?: (key: SignatureWithKey) => Promise<boolean>
    /*     _heapSizeLimit?: () => number;
        _onMemoryExceeded?: OnMemoryExceededCallback<T>; */


    constructor(properties?: {
        name?: string,
        rootTrust?: PublicSignKey,
        trustedNetwork?: TrustedNetwork
    }) {
        super(properties)
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

    async init(ipfs: IPFS, identity: Identity, options: ProgramInitializationOptions): Promise<this> {
        /*  this._trust = options.trust; */
        await this._db.init(ipfs, identity, { ...options })
        return this;
    }

    async canRead(s: SignatureWithKey): Promise<boolean> {
        return this._db.canRead(s)
    }

    async canAppend(payload: MaybeEncrypted<Payload<Operation<T>>>, identityEncrypted: MaybeEncrypted<SignatureWithKey>) {
        return this._db.canAppend(payload, identityEncrypted)
    }

    async setup() {
        this._db.setup();
    }
}