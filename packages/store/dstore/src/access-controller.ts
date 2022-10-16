import { MaybeEncrypted, SignatureWithKey } from "@dao-xyz/peerbit-crypto"
import { Payload } from "@dao-xyz/ipfs-log"
import { IInitializationOptions } from './store.js';
import { IPFS } from 'ipfs-core-types';
import { Initiable } from './store-like.js';


/* export class AccessController<T> implements CanAppendAccessController<T>, Initiable<T> {

  _allowAll: boolean = false;
  get allowAll(): boolean {
    return this._allowAll;
  }

  set allowAll(value: boolean) {
    this._allowAll = value;
  }
  async init(ipfs: IPFS, identity: Identity, options: IInitializationOptions<any>): Promise<AccessController<T>> {
    return this;
  }

  async canAppend(payload: MaybeEncrypted<Payload<T>>, key: MaybeEncrypted<SignatureWithKey>): Promise<boolean> {
    throw new Error("Not implemented")
  }
  clone(newName: string): AccessController<T> {
    throw new Error("Not implemented")
  }
  async close?(): Promise<void>;

} */





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