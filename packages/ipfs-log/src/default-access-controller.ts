import { Identities, IdentitySerializable } from "@dao-xyz/orbit-db-identity-provider";
import { Payload } from '@dao-xyz/ipfs-log-entry';

export interface AccessController<T> {
  canAppend(payload: Payload<T>, identity: IdentitySerializable, identityProvider: Identities): Promise<boolean> | boolean;
  encrypt?(data: Uint8Array): Uint8Array
  decrypt?(data: Uint8Array): Uint8Array
}

export class DefaultAccessController<T> implements AccessController<T> {
  async canAppend(payload: Payload<T>, identity: IdentitySerializable, identityProvider: Identities): Promise<boolean> {
    return true
  }

  encrypt(data: Uint8Array): Uint8Array {
    return data;
  }

  decrypt(data: Uint8Array): Uint8Array {
    return data;
  }


}
