import { AccessController } from './access-controller-interface'
import { Entry } from '@dao-xyz/ipfs-log-entry';
import io from '@dao-xyz/orbit-db-io'
import { Identities } from '@dao-xyz/orbit-db-identity-provider';

const type = 'ipfs'

export class IPFSAccessController<T> extends AccessController<T> {
  _write: string[];
  constructor(ipfs, options?: { write?: string[] }) {
    super()
    this._ipfs = ipfs
    this._write = Array.from(options.write || [])
  }

  // Returns the type of the access controller
  static get type() { return type }

  // Return a Set of keys that have `access` capability
  get write() {
    return this._write
  }

  async canAppend<T>(entry: Entry<T>, identityProvider: Identities) {
    // Allow if access list contain the writer's publicKey or is '*'
    const key = entry.data.identity.id
    if (this.write.includes(Buffer.from(key).toString()) || this.write.includes('*')) {
      // check identity is valid
      return identityProvider.verifyIdentity(entry.data.identity)
    }
    return false
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

  static async create(orbitdb, options: { write?: string[] } = {}) {
    options = { ...options, ...{ write: options.write || [Buffer.from(orbitdb.identity.id).toString()] } }
    return new IPFSAccessController(orbitdb._ipfs, options)
  }
}
