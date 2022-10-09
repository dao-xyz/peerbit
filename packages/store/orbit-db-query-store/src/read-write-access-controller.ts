import { AccessController } from "@dao-xyz/orbit-db-store"
import { SignKey } from '@dao-xyz/peerbit-crypto';

export class ReadWriteAccessController<T> extends AccessController<T> {
    canRead?(key: SignKey): Promise<boolean>;
}
