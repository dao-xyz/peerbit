import { MaybeEncrypted } from "@dao-xyz/encryption-utils"
import { Payload } from "@dao-xyz/ipfs-log-entry"
import { AccessController } from "@dao-xyz/orbit-db-store"
import { variant, field } from '@dao-xyz/borsh';
import { Ed25519PublicKeyData } from "@dao-xyz/identity";
@variant([0, 253])
export class SimpleAccessController<T> extends AccessController<T>
{
    async canAppend(payload: MaybeEncrypted<Payload<T>>, signKey: MaybeEncrypted<Ed25519PublicKeyData>) {
        return true;
    }
}