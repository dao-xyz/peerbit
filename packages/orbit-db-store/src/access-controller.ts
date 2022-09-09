import { deserialize, serialize } from '@dao-xyz/borsh';
import io from '@dao-xyz/orbit-db-io'

// TODO extend IPFS-LOG access controller interface for canAppend method

import { MaybeEncrypted } from "@dao-xyz/encryption-utils"
import { CanAppendAccessController } from "@dao-xyz/ipfs-log"
import { Payload } from "@dao-xyz/ipfs-log-entry"
import { Identities, Identity, IdentitySerializable } from "@dao-xyz/orbit-db-identity-provider"
import { IInitializationOptions } from './store';
import { IPFS } from 'ipfs-core-types/src';
import { Message } from 'ipfs-core-types/src/pubsub';

/**
 * Interface for OrbitDB Access Controllers
 *
 * Any OrbitDB access controller needs to define and implement
 * the methods defined by the interface here.
 */

export class AccessController<T> implements CanAppendAccessController<T> {

  init?(ipfs: IPFS, identity: Identity, options: IInitializationOptions<any>): Promise<void>;
  async canAppend(payload: MaybeEncrypted<Payload<T>>, identity: MaybeEncrypted<IdentitySerializable>, identityProvider: Identities): Promise<boolean> {
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