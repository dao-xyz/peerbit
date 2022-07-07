import { Identity, IdentityAsJson } from './identity';
import { IdentityProvider } from './identity-provider-interface'
const Keystore = require('orbit-db-keystore')
const type = 'orbitdb'

export class OrbitDBIdentityProvider extends IdentityProvider {
  _keystore: any;
  constructor(keystore) {
    super()
    if (!keystore) {
      throw new Error('OrbitDBIdentityProvider requires a keystore')
    }
    this._keystore = keystore
  }

  // Returns the type of the identity provider
  static get type() { return type }

  async getId(options: { id?: string } = {}) {
    const id = options.id
    if (!id) {
      throw new Error('id is required')
    }

    const keystore = this._keystore
    const key = await keystore.getKey(id) || await keystore.createKey(id)
    return Buffer.from(key.public.marshal()).toString('hex')
  }

  async sign(data, options: { id?: string } = {}) {
    const id = options.id
    if (!id) {
      throw new Error('id is required')
    }
    const keystore = this._keystore
    const key = await keystore.getKey(id)
    if (!key) {
      throw new Error(`Signing key for '${id}' not found`)
    }

    return keystore.sign(key, data)
  }

  static async verify(signature: string, data: string | Uint8Array, publicKey: string): Promise<boolean> {
    return Keystore.verify(
      signature,
      data,
      publicKey
    )
  }

  static async verifyIdentity(identity: IdentityAsJson) {
    // Verify that identity was signed by the ID
    return OrbitDBIdentityProvider.verify(
      identity.signatures.publicKey,
      identity.id,
      identity.publicKey + identity.signatures.id
    )
  }
}

