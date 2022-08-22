import { Identities, IdentitySerializable } from "@dao-xyz/orbit-db-identity-provider";
import { Payload } from '@dao-xyz/ipfs-log-entry';

export interface AccessController<T> {
  canAppend(payload: Payload<T>, identityResolver: () => Promise<IdentitySerializable>, identityProvider: Identities): Promise<boolean> | boolean;
}

export class DefaultAccessController<T> implements AccessController<T> {
  async canAppend(_payload: Payload<T>, _identityResolver: () => Promise<IdentitySerializable>, identityProvider: Identities): Promise<boolean> {
    return true
  }
}
