import * as ipfs from 'ipfs';
import { IPFS as IPFSInstance } from 'ipfs-core-types'
// Can modify owned entries?
// Can remove owned entries?
// Can modify any entries?
// Can remove any entries?

// Relation with enc/dec?
import { PublicKey, BIdentity } from "@dao-xyz/identity";
import { field, serialize, variant, vec } from "@dao-xyz/borsh";
import Identities, { Identity } from 'orbit-db-identity-provider';
/*  */ // TODO MOVE OUT PUBLICKEY

export enum AccessType {
    Admin = 0,
    Add = 1,
    Remove = 2,
    ModifySelf = 3,
}

export class AccessCondition { }

@variant(0)
export class NoAccessCondition extends AccessCondition {
    constructor() {
        super();
    }
}

@variant(1)
export class PublicKeyAccessCondition extends AccessCondition {

    @field({ type: PublicKey })
    key: PublicKey

    constructor() {
        super();
    }
}


export class Access {

    @field({ type: vec('u8') })
    accessTypes: AccessType[]

    @field({ type: AccessCondition })
    accessCondition: AccessCondition
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

@variant(0)
export class SignedAccessRequest {

    @field({ type: AccessRequest })
    request: AccessRequest

    @field({ type: BIdentity })
    identity: BIdentity

    constructor(options?: { request: AccessRequest }) {

        if (options) {
            Object.assign(this, options);
        }
    }

    async sign(identity: Identity) {
        const identityJSON = identity.toJSON();
        this.signature = await identity.provider.sign(identityJSON, serialize(this.request))
        this.identity = BIdentity.from(identityJSON)
    }

    async verifySignature(identities: Identities): Promise<boolean> {
        if (! await identities.verifyIdentity(this.identity.toIdentityJSON())) {
            return false
        }
        return identities.verify(this.signature, this.publicKey, serialize(this.request))
    }
}

export class DynamicACL {
    constructor(options: { canModifyAcaccess: (key: PublicKey) => Promise<boolean>, grantAccess: (access: Access) => Promise<void>, revokeAccess: (access: Access) => Promise<void> }/* options: { isOwned: (entryId: string, key: PublicKey) => boolean } */) { }

    async process(request: RequestAccess) {

        // verify 
    }

    static async requestAccess(request: RequestAccess, identity: Identity, ipfs: IPFSInstance) {
        let signature = identity.provider.sign(identity.toJSON(),)
        await ipfs.pubsub.publish(request.accessTopic, serialize(request));
    }
}


