import { MaybeEncrypted } from "@dao-xyz/encryption-utils";
import { Identities, IdentitySerializable } from "@dao-xyz/orbit-db-identity-provider";
import { AccessController } from "@dao-xyz/orbit-db-store"

export class ReadWriteAccessController<T> extends AccessController<T> {
    canRead?(identity: MaybeEncrypted<IdentitySerializable>, identityProvider: Identities): Promise<boolean>;
}
