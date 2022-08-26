import { Identities, IdentitySerializable } from "@dao-xyz/orbit-db-identity-provider";
import { Payload } from '@dao-xyz/ipfs-log-entry';
import { MaybeEncrypted } from "@dao-xyz/encryption-utils";

export interface AccessController<T> {
  canAppend(payload: MaybeEncrypted<Payload<T>>, identity: MaybeEncrypted<IdentitySerializable>, identityProvider: Identities): Promise<boolean> | boolean;
}

export class DefaultAccessController<T> implements AccessController<T> {
  async canAppend(payload: MaybeEncrypted<Payload<T>>, identity: MaybeEncrypted<IdentitySerializable>, _identityProvider: Identities): Promise<boolean> {
    return true
  }
}
