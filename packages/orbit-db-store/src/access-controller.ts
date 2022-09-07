import { deserialize, serialize } from '@dao-xyz/borsh';
import io from '@dao-xyz/orbit-db-io'

// TODO extend IPFS-LOG access controller interface for canAppend method

import { MaybeEncrypted } from "@dao-xyz/encryption-utils"
import { CanAppendAccessController } from "@dao-xyz/ipfs-log"
import { Payload } from "@dao-xyz/ipfs-log-entry"
import { Identities, Identity, IdentitySerializable } from "@dao-xyz/orbit-db-identity-provider"
import { variant } from '@dao-xyz/borsh';
import { IStoreOptions } from './store';
import { Address, load, Manifest } from './io';

/**
 * Interface for OrbitDB Access Controllers
 *
 * Any OrbitDB access controller needs to define and implement
 * the methods defined by the interface here.
 */

@variant(0)
export class AccessController<T> implements CanAppendAccessController<T> {

  async canAppend(payload: MaybeEncrypted<Payload<T>>, identity: MaybeEncrypted<IdentitySerializable>, identityProvider: Identities): Promise<boolean> {
    throw new Error("Not implemented")
  }
  clone(newName: string): AccessController<T> {
    throw new Error("Not implemented")
  }
  async init?(ipfs: any, identity: Identity, options: IStoreOptions<T>): Promise<void>;
  async canRead?(payload: MaybeEncrypted<Payload<T>>, identity: MaybeEncrypted<IdentitySerializable>, identityProvider: Identities): Promise<boolean>;
  async canAccessKeys?(identity: { type: string, key: Uint8Array }): Promise<boolean>;
  async close?(): Promise<void>;
  async save?(): Promise<{ address: string }>

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