import { deserialize, serialize } from '@dao-xyz/borsh';
import io from '@dao-xyz/io-utils'

// TODO extend IPFS-LOG access controller interface for canAppend method

import { MaybeEncrypted } from "@dao-xyz/peerbit-crypto"
import { CanAppendAccessController } from "@dao-xyz/ipfs-log"
import { Payload } from "@dao-xyz/ipfs-log-entry"
import { IInitializationOptions } from './store.js';
import { IPFS } from 'ipfs-core-types';
import { PublicKey } from '@dao-xyz/peerbit-crypto';
import { Initiable } from './store-like.js';
/**
 * Interface for OrbitDB Access Controllers
 *
 * Any OrbitDB access controller needs to define and implement
 * the methods defined by the interface here.
 */

export class AccessController<T> implements CanAppendAccessController<T>, Initiable<T> {

  _allowAll: boolean = false;
  get allowAll(): boolean {
    return this._allowAll;
  }

  set allowAll(value: boolean) {
    this._allowAll = value;
  }
  async init(ipfs: IPFS, key: PublicKey, sign: (data: Uint8Array) => Promise<Uint8Array>, options: IInitializationOptions<any>): Promise<AccessController<T>> {
    return this;
  }

  async canAppend(payload: MaybeEncrypted<Payload<T>>, key: MaybeEncrypted<PublicKey>): Promise<boolean> {
    throw new Error("Not implemented")
  }
  clone(newName: string): AccessController<T> {
    throw new Error("Not implemented")
  }
  async canAccessKeys?(identity: { type: string, key: Uint8Array }): Promise<boolean>;
  async close?(): Promise<void>;

}





/* 

export class AccessControllers {


  static async create(orbitdb: { _ipfs: any }, accessController: AccessController<any>) {
    await accessController.save();
    const manifest: Manifest = {
      data: serialize(accessController)
    }
    const hash = io.write(orbitdb._ipfs, 'dag-cbor', manifest)
    return hash
  }

  static async resolve(orbitdb: { _ipfs: any }, address: string) {
    const { data } = await io.read(orbitdb._ipfs, address)
    const ac: AccessController<any> = deserialize(data, AccessController)
    return ac;
  }
}
 */