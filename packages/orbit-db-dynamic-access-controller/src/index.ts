import * as ipfs from 'ipfs';
import { IPFS as IPFSInstance } from 'ipfs-core-types'
// Can modify owned entries?
// Can remove owned entries?
// Can modify any entries?
// Can remove any entries?

// Relation with enc/dec?
import { PublicKey, BIdentity } from "@dao-xyz/identity";
import { field, option, serialize, variant, vec } from "@dao-xyz/borsh";
import { Identities, Identity, IdentityAsJson } from '@dao-xyz/orbit-db-identity-provider';
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

    // Include time so we can invalidate "old" requests
    @field({ serialize: (arg: number, writer) => writer.writeU64(arg), deserialize: (reader) => reader.readU64() })
    time: number;

    @field({ type: BIdentity })
    identity: BIdentity

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
        return identities.verify(this.signature, this.publicKey, this.serializePresigned(), 'v1')
    }
}

export class DynamicACL {
    constructor(options: { canModifyAcaccess: (key: PublicKey) => Promise<boolean>, grantAccess: (access: Access) => Promise<void>, revokeAccess: (access: Access) => Promise<void> }/* options: { isOwned: (entryId: string, key: PublicKey) => boolean } */) { }

    async process(request: SignedAccessRequest, identities: Identities) {

        // verify 
        if (!await request.verifySignature(identities)) {
            // No ok!
            return;
        }


    }

    static async requestAccess(request: SignedAccessRequest, identity: Identity, ipfs: IPFSInstance) {
        await request.sign(identity);
        await ipfs.pubsub.publish(request.request.accessTopic, serialize(request));
    }
}


