import { MaybeEncrypted } from "@dao-xyz/encryption-utils"
import { Payload } from "@dao-xyz/ipfs-log-entry"
import { AccessController } from "@dao-xyz/orbit-db-store"
import { Identities, IdentitySerializable } from "@dao-xyz/orbit-db-identity-provider"
import { variant, field } from '@dao-xyz/borsh';

@variant([0, 253])
export class SimpleAccessController<T> extends AccessController<T>
{
    async canAppend(payload: MaybeEncrypted<Payload<T>>, entryIdentity: MaybeEncrypted<IdentitySerializable>, _identityProvider: Identities) {
        return true;
    }
}