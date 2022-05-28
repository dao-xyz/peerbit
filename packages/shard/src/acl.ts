import { field, variant, vec } from "@dao-xyz/borsh";
import { BinaryFeedStore } from "@dao-xyz/orbit-db-bfeedstore";
import { AnyPeer } from "./node";
import { Shard } from "./shard";
import { PublicKey } from "./signer";
import { BinaryFeedStoreOptions } from "./stores";

export class ACL {
    async create(peer: AnyPeer, shard: Shard<any>) {
        throw new Error("Not implemented");
    }
}

export enum AccessType {
    Admin = 0,
    Vibe = 1,
    DeleteVibe = 2,
}

export class AccessCondition {


}

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
export class ACLV1 extends ACL {

    @field({ type: 'String' })
    accessesAddress: string

    accesses?: BinaryFeedStore<Access>
    constructor(props: {
        accessesAddress: string
    } | any) {
        super();
        if (props) {
            Object.assign(this, props)
        }
    }

    async create(peer: AnyPeer, shard: Shard<any>) {
        peer.options.behaviours.typeMap[Access.name] = Access;
        this.accesses = await new BinaryFeedStoreOptions<Access>({
            objectType: Access.name
        }).newStore(this.accessesAddress ? this.accessesAddress : shard.getDBName("acl"), peer.orbitDB, peer.options.defaultOptions, peer.options.behaviours)
        this.accessesAddress = this.accesses.address.toString()
    }

    async hasAccess(type: AccessType): Promise<boolean> {
        return true; // TODO; fix integration
    }
}