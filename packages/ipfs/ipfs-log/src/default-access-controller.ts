const EventEmitter = require('events').EventEmitter
import { MaybeEncrypted } from "@dao-xyz/peerbit-crypto";
import { Payload, Signature } from '@dao-xyz/ipfs-log-entry';
import { Ed25519PublicKey, PublicSignKey } from '@dao-xyz/peerbit-crypto'
// TODO extend IPFS-LOG access controller interface for canAppend method
/**
 * Interface for OrbitDB Access Controllers
 *
 * Any OrbitDB access controller needs to define and implement
 * the methods defined by the interface here.
 */
export interface CanAppendAccessController<T> {

  get allowAll(): boolean;
  canAppend(payload: MaybeEncrypted<Payload<T>>, key: MaybeEncrypted<PublicSignKey> | MaybeEncrypted<Signature>): Promise<boolean>;

}

export class DefaultAccessController<T> implements CanAppendAccessController<T> {
  get allowAll(): boolean {
    return true;
  }

  async canAppend(payload: MaybeEncrypted<Payload<T>>, key: MaybeEncrypted<PublicSignKey>): Promise<boolean> {
    return true
  }
}
