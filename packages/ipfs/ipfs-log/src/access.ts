import { MaybeEncrypted, SignatureWithKey, SignKey } from "@dao-xyz/peerbit-crypto";
import { Payload } from './entry';

// TODO extend IPFS-LOG access controller interface for canAppend method
/**
 * Interface for OrbitDB Access Controllers
 *
 * Any OrbitDB access controller needs to define and implement
 * the methods defined by the interface here.
 */
/* export interface CanAppendAccessController<T> {

  canAppend(payload: MaybeEncrypted<Payload<T>>, key: MaybeEncrypted<SignatureWithKey>): Promise<boolean>;

}

export class DefaultAccessController<T> implements CanAppendAccessController<T> {

  async canAppend(payload: MaybeEncrypted<Payload<T>>, key: MaybeEncrypted<SignatureWithKey>): Promise<boolean> {
    return true
  }
}
 */
export type CanAppend<T> = (payload: () => Promise<T>, identity: () => Promise<SignKey>) => Promise<boolean> | boolean