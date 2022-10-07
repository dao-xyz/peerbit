import { MaybeEncrypted } from "@dao-xyz/peerbit-crypto";
import { PublicKey } from "@dao-xyz/peerbit-crypto";
import { AccessController } from "@dao-xyz/orbit-db-store"
import { Ed25519PublicKey } from '@dao-xyz/peerbit-crypto';

export class ReadWriteAccessController<T> extends AccessController<T> {
    canRead?(key: PublicKey): Promise<boolean>;
}
