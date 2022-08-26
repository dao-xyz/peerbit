import { field, variant } from "@dao-xyz/borsh";
import { MaybeEncrypted } from "@dao-xyz/encryption-utils";
import { U8IntArraySerializer, arraysEqual } from '@dao-xyz/io-utils';
import { Payload } from "@dao-xyz/ipfs-log-entry";
import { IdentitySerializable } from "@dao-xyz/orbit-db-identity-provider";

@variant(0)
export class Network {

    @field({ type: 'String' })
    type: string;

    @field({ type: 'String' })
    rpc: string;
}

export class AccessCondition<T> {

    async allowed(_entry: MaybeEncrypted<Payload<T>>, _identity: MaybeEncrypted<IdentitySerializable>): Promise<boolean> {
        throw new Error("Not implemented")
    }
}

@variant([0, 0])
export class AnyAccessCondition<T> extends AccessCondition<T> {
    constructor() {
        super();
    }
    async allowed(_entry: MaybeEncrypted<Payload<T>>, _identity: MaybeEncrypted<IdentitySerializable>): Promise<boolean> {
        return true;
    }
}

@variant([0, 1])
export class PublicKeyAccessCondition<T> extends AccessCondition<T> {

    @field({ type: 'String' })
    type: string

    @field(U8IntArraySerializer)
    key: Uint8Array

    constructor(options?: {
        type: string,
        key: Uint8Array
    }) {
        super();
        if (options) {
            this.type = options.type;
            this.key = options.key
        }
    }

    async allowed(_payload: MaybeEncrypted<Payload<T>>, identity: MaybeEncrypted<IdentitySerializable>): Promise<boolean> {
        const i = (await identity.decrypt()).getValue(IdentitySerializable);
        return this.type === i.type && arraysEqual(this.key, i.id)
    }
}

/*  Not yet :)

@variant([0, 2])
export class TokenAccessCondition extends AccessCondition {

    @field({ type: Network })
    network: Network

    @field({ type: 'String' })
    token: string

    @field({ type: 'u64' })
    amount: BN

    constructor() {
        super();
    }
}


@variant(0)
export class NFTPropertyCondition {
    @field({ type: 'String' })
    field: string

    @field({ type: 'String' })
    value: string;
}

 @variant([0, 3])  // distinguish between ERC-721, ERC-1155, Solana Metaplex? 
export class NFTAccessCondition extends AccessCondition {

    @field({ type: Network })
    network: Network

    @field({ type: 'String' })
    name: string

    @field({ type: option(vec(NFTPropertyCondition)) })
    properties: NFTPropertyCondition

    constructor() {
        super();
    }
}
 */
