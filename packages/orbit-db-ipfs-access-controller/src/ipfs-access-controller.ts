import { Payload } from '@dao-xyz/ipfs-log-entry';
import io from '@dao-xyz/orbit-db-io'
import { MaybeEncrypted } from '@dao-xyz/encryption-utils';
import { variant, field, vec } from '@dao-xyz/borsh';
import { ReadWriteAccessController } from '@dao-xyz/orbit-db-query-store';
import { PublicKey } from '@dao-xyz/identity';

@variant([1, 0])
export class IPFSAccessController<T> extends ReadWriteAccessController<T> {

  @field({ type: vec('string') })
  _write: string[];

  _ipfs: any;

  constructor(options?: { write?: (string | Uint8Array)[] }) {
    super()
    if (options) {
      this._write = Array.from(options.write || []).map(x => x instanceof Uint8Array ? Buffer.from(x).toString('base64') : x)
    }
  }

  // Return a Set of keys that have `access` capability
  get write() {
    return this._write
  }

  async canAppend<T>(payload: MaybeEncrypted<Payload<T>>, keyEncrypted: MaybeEncrypted<PublicKey>) {
    // Allow if access list contain the writer's publicKey or is '*'
    let key: PublicKey = undefined;

    try {
      key = (await keyEncrypted.decrypt()).getValue(PublicKey);
    } catch (error) {
      // Can not access identity
      return this.write.includes('*');
    }

    if (this.write.includes('*')) {
      return true; // we can end up with encrypted identities
    }
    return this.write.includes(key.getBuffer().toString('base64'))
  }

  async canRead(key: PublicKey) {

    // Allow if access list contain the writer's publicKey or is '*'
    if (this.write.includes('*')) {
      return true; // we can end up with encrypted identities
    }
    return this.write.includes(key.getBuffer().toString('base64'))
  }

  async load(address) {
    // Transform '/ipfs/QmPFtHi3cmfZerxtH9ySLdzpg1yFhocYDZgEZywdUXHxFU'
    // to 'QmPFtHi3cmfZerxtH9ySLdzpg1yFhocYDZgEZywdUXHxFU'
    if (address.indexOf('/ipfs') === 0) { address = address.split('/')[2] }

    try {
      this._write = (await io.read(this._ipfs, address) as any).write;
      if (typeof this.write === 'string') {
        this._write = JSON.parse(this._write as any as string);
      }
    } catch (e) {
      console.log('IPFSAccessController.load ERROR:', e)
    }
  }

  async save() {
    let cid
    try {
      cid = await io.write(this._ipfs, 'dag-cbor', { write: JSON.stringify(this.write, null, 2) })
    } catch (e) {
      console.log('IPFSAccessController.save ERROR:', e)
    }
    // return the manifest data
    return { address: cid }
  }

}

