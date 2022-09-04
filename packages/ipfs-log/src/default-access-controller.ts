const EventEmitter = require('events').EventEmitter
import { MaybeEncrypted } from '@dao-xyz/encryption-utils';
import { Payload } from '@dao-xyz/ipfs-log-entry';
import { Identities, IdentitySerializable } from '@dao-xyz/orbit-db-identity-provider';

// TODO extend IPFS-LOG access controller interface for canAppend method
/**
 * Interface for OrbitDB Access Controllers
 *
 * Any OrbitDB access controller needs to define and implement
 * the methods defined by the interface here.
 */
export interface CanAppendAccessController<T> {

  canAppend(payload: MaybeEncrypted<Payload<T>>, identity: MaybeEncrypted<IdentitySerializable>, identityProvider: Identities): Promise<boolean>;

}

export class DefaultAccessController<T> implements CanAppendAccessController<T> {
  async canAppend(payload: MaybeEncrypted<Payload<T>>, identity: MaybeEncrypted<IdentitySerializable>, _identityProvider: Identities): Promise<boolean> {
    return true
  }
}
