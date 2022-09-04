import { ensureAddress } from "./utils"
import { Payload } from '@dao-xyz/ipfs-log-entry';
const pMapSeries = require('p-map-series')
import { IdentitySerializable } from "@dao-xyz/orbit-db-identity-provider";
import OrbitDB from "orbit-db";
import { MaybeEncrypted } from "@dao-xyz/encryption-utils";
import { AccessController } from "./access-controller-interface";
const type = 'orbitdb'
export type Options = {
  admin?: Uint8Array
}
export class OrbitDBAccessController<T> extends AccessController<T> {

  _db: any;
  _options: Options;
  _orbitdb: OrbitDB;
  constructor(orbitdb, options?: Options) {
    super()
    this._orbitdb = orbitdb
    this._db = null
    this._options = options || {}
  }

  // Returns the type of the access controller
  static get type() { return type }

  // Returns the address of the OrbitDB used as the AC
  get address() {
    return this._db.address
  }

  // Return true if entry is allowed to be added to the database
  async canAppend<T>(payload: MaybeEncrypted<Payload<T>>, identity: MaybeEncrypted<IdentitySerializable>, identityProvider) {

    await payload.decrypt()
    // Write keys and admins keys are allowed
    const access = new Set([...this.get('write'), ...this.get('admin')])
    // If the ACL contains the writer's public key or it contains '*'

    await identity.decrypt();
    if (access.has(Buffer.from(identity.decrypted.getValue(IdentitySerializable).id).toString('base64')) || access.has('*')) {
      const verifiedIdentity = await identityProvider.verifyIdentity(identity)
      // Allow access if identity verifies
      return verifiedIdentity
    }

    return false
  }

  get capabilities() {
    if (this._db) {
      const capabilities = this._db.index

      const toSet = (e) => {
        const key = e[0]
        capabilities[key] = new Set([...(capabilities[key] || []), ...e[1]])
      }

      // Merge with the access controller of the database
      // and make sure all values are Sets
      Object.entries({
        ...capabilities,
        // Add the root access controller's 'write' access list
        // as admins on this controller
        ...{ admin: new Set([...(capabilities.admin || []), ...this._db.access.write]) }
      }).forEach(toSet)

      return capabilities
    }
    return {}
  }

  get(capability) {
    return this.capabilities[capability] || new Set([])
  }

  async close() {
    await this._db.close()
  }

  async load(address) {
    if (this._db) { await this._db.close() }

    // Force '<address>/_access' naming for the database
    this._db = await this._orbitdb.open(ensureAddress(address), {
      // use ipfs controller as a immutable "root controller"
      accessController: {
        type: 'ipfs',
        write: (this._options.admin ? Buffer.from(this._options.admin).toString('base64') : undefined) || [Buffer.from(this._orbitdb.identity.id).toString('base64')]
      },
      sync: true
    })

    this._db.events.on('ready', this._onUpdate.bind(this))
    this._db.events.on('write', this._onUpdate.bind(this))
    this._db.events.on('replicated', this._onUpdate.bind(this))

    await this._db.load()
  }

  async save() {
    // return the manifest data
    return {
      address: this._db.address.toString()
    }
  }

  async hasCapability(capability, identity) {
    // Write keys and admins keys are allowed
    const access = new Set(this.get(capability))
    return access.has(Buffer.from(identity.id).toString('base64')) || access.has("*")
  }

  async grant(capability: string, key: Uint8Array) {
    // Merge current keys with the new key
    const capabilities = new Set([...(this._db.get(capability) || []), ...[Buffer.from(key).toString('base64')]])
    await this._db.put(capability, Array.from(capabilities.values()))
  }

  async revoke(capability: string, key: Uint8Array) {
    const capabilities = new Set(this._db.get(capability) || [])
    capabilities.delete(Buffer.from(key).toString('base64'))
    if (capabilities.size > 0) {
      await this._db.put(capability, Array.from(capabilities.values()))
    } else {
      await this._db.del(capability)
    }
  }

  /* Private methods */
  _onUpdate() {
    this.emit('updated')
  }

  /* Factory */
  static async create(orbitdb, options: any = {}) {
    const ac = new OrbitDBAccessController(orbitdb, options)
    await ac.load(options.address || options.name || 'default-access-controller')

    // Add write access from options
    if (options.write && !options.address) {
      await pMapSeries(options.write, async (e) => ac.grant('write', e))
    }

    return ac
  }
}