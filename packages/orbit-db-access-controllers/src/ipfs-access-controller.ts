import { Payload } from '@dao-xyz/ipfs-log-entry';
import io from '@dao-xyz/orbit-db-io'
import { Identities, IdentitySerializable } from '@dao-xyz/orbit-db-identity-provider';
import { MaybeEncrypted } from '@dao-xyz/encryption-utils';
import { AccessController } from './access-controller-interface';

const type = 'ipfs'

export class IPFSAccessController<T> extends AccessController<T> {
  _write: string[];
  _ipfs: any;
  constructor(ipfs, options?: { write?: (string | Uint8Array)[] }) {
    super()
    this._ipfs = ipfs
    this._write = Array.from(options.write || []).map(x => x instanceof Uint8Array ? Buffer.from(x).toString('base64') : x)
  }

  // Returns the type of the access controller
  static get type() { return type }

  // Return a Set of keys that have `access` capability
  get write() {
    return this._write
  }

  async canAppend<T>(payload: MaybeEncrypted<Payload<T>>, identityEncrypted: MaybeEncrypted<IdentitySerializable>, identityProvider: Identities) {
    // Allow if access list contain the writer's publicKey or is '*'
    let identity = undefined;

    try {
      identity = (await identityEncrypted.decrypt()).getValue(IdentitySerializable);
      if (!identityProvider.verifyIdentity(identity)) {
        return false;
      }
    } catch (error) {
      // Can not access identity
      return this.write.includes('*');
    }

    if (this.write.includes('*')) {
      return true; // we can end up with encrypted identities
    }
    return this.write.includes(Buffer.from(identity.id).toString('base64'))
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

  static async create(orbitdb, options: { write?: (string | Uint8Array)[] } = {}) {
    options = { ...options, ...{ write: options.write || [Buffer.from(orbitdb.identity.id).toString('base64')] } }
    return new IPFSAccessController(orbitdb._ipfs, options)
  }
}

