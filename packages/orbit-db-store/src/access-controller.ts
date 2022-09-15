import { deserialize, serialize } from '@dao-xyz/borsh';
import io from '@dao-xyz/orbit-db-io'

// TODO extend IPFS-LOG access controller interface for canAppend method

import { MaybeEncrypted } from "@dao-xyz/encryption-utils"
import { CanAppendAccessController } from "@dao-xyz/ipfs-log"
import { Payload } from "@dao-xyz/ipfs-log-entry"
import { IInitializationOptions } from './store';
import { IPFS } from 'ipfs-core-types/src';
import { PublicKey } from '@dao-xyz/identity';
/**
 * Interface for OrbitDB Access Controllers
 *
 * Any OrbitDB access controller needs to define and implement
 * the methods defined by the interface here.
 */

export class AccessController<T> implements CanAppendAccessController<T> {

  _allowAll: boolean = false;
  get allowAll(): boolean {
    return this._allowAll;
  }

  set allowAll(value: boolean) {
    this._allowAll = value;
  }

  init?(ipfs: IPFS, key: PublicKey, sign: (data: Uint8Array) => Promise<Uint8Array>, options: IInitializationOptions<any>): Promise<void>;
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
    const ac: AccessController<any> = deserialize(Buffer.from(data), AccessController)
    return ac;
  }
}
 */