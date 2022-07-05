// Can modify owned entries?
// Can remove owned entries?
// Can modify any entries?
// Can remove any entries?

// Relation with enc/dec?

import { field, variant, vec } from "@dao-xyz/borsh";
/* import { PublicKey } from "@dao-xyz/shard"; */ // TODO MOVE OUT PUBLICKEY

export enum AccessType {
    Admin = 0,
    Add = 1,
    Remove = 2,
    ModifySelf = 3,
}

export class AccessCondition { }
/* 
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

export class DynamicACL {
    constructor(options: { isOwned: (entryId: string, key: PublicKey) => boolean }) { }

    async grantAcces() { }
    static async requestAccess() {

    }
}


 */