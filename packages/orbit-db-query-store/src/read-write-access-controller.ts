import { MaybeEncrypted } from "@dao-xyz/encryption-utils";
import { PublicKey } from "@dao-xyz/identity";
import { AccessController } from "@dao-xyz/orbit-db-store"
import { Ed25519PublicKey } from 'sodium-plus';

export class ReadWriteAccessController<T> extends AccessController<T> {
    canRead?(key: PublicKey): Promise<boolean>;
}
