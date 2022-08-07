import { Identity, IdentitySerializable } from './identity';
import { IdentityProvider } from './identity-provider-interface'
import { Keystore } from '@dao-xyz/orbit-db-keystore'
import { Ed25519PublicKey } from 'sodium-plus';
import { joinUint8Arrays } from '@dao-xyz/io-utils';

export class OrbitDBIdentityProvider extends IdentityProvider {
  _keystore: Keystore;
  constructor(keystore) {
    super()
    if (!keystore) {
      throw new Error('OrbitDBIdentityProvider requires a keystore')
    }
    this._keystore = keystore
  }

  // Returns the type of the identity provider
  static get type() { return 'orbitdb' }

  async getId(options: { id?: Uint8Array } = {}) {
    const id = options.id
    if (!id) {
      throw new Error('id is required')
    }

    const keystore = this._keystore
    const idString = Buffer.from(id).toString('base64');
    const existingKey = await keystore.getKey(idString);
    const key = (existingKey) || (await keystore.createKey(idString))
    return new Uint8Array((await Keystore.getPublicSign(key)).getBuffer());
  }

  async sign(data: string | Uint8Array | Buffer, options: { id?: Uint8Array } = {}) {
    const id = options.id
    if (!id) {
      throw new Error('id is required')
    }
    const keystore = this._keystore
    const idString = Buffer.from(id).toString('base64');
    const key = await keystore.getKey(idString)
    if (!key) {
      throw new Error(`Signing key for '${idString}' not found`)
    }
    return keystore.sign(key, data);
  }

  static async verify(signature: Uint8Array, data: Uint8Array, publicKey: Uint8Array): Promise<boolean> {
    return Keystore.verify(
      signature,
      new Ed25519PublicKey(Buffer.from(publicKey)),
      data,
    )
  }

  static async verifyIdentity(identity: Identity | IdentitySerializable) {
    // Verify that identity was signed by the ID
    return OrbitDBIdentityProvider.verify(
      identity.signatures.publicKey,
      joinUint8Arrays([identity.publicKey, identity.signatures.id]),
      identity.id
    )
  }
}

