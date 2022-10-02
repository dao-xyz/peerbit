import { Identity } from './identity';
import { IdentityProvider } from './identity-provider-interface'
import { Keystore, SignKeyWithMeta } from '@dao-xyz/orbit-db-keystore'
import { Ed25519PublicKey } from 'sodium-plus';
import { verifySignatureEd25519 } from '@dao-xyz/peerbit-crypto';

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
    const existingKey = await keystore.getKeyByPath(idString, SignKeyWithMeta);
    const key = (existingKey) || (await keystore.createKey(idString, SignKeyWithMeta))
    return new Uint8Array(key.publicKey.getBuffer());
  }

  async sign(data: string | Uint8Array | Buffer, options: { id?: Uint8Array } = {}) {
    const id = options.id
    if (!id) {
      throw new Error('id is required')
    }
    const keystore = this._keystore
    const idString = Buffer.from(id).toString('base64');
    const key = await keystore.getKeyByPath(idString, SignKeyWithMeta)
    if (!key) {
      throw new Error(`Signing key for '${idString}' not found`)
    }
    return Keystore.sign(data, key);
  }

  /* static async verify(signature: Uint8Array, data: Uint8Array, publicKey: Uint8Array): Promise<boolean> {
    return Keystore.verify(
      signature,
      new Ed25519PublicKey(Buffer.from(publicKey)),
      data,
    )
  } */

  static async verifyIdentity(identity: Identity) {
    // Verify that identity was signed by the ID
    if (identity.id instanceof Ed25519PublicKey) {
      return verifySignatureEd25519(identity.signatures.publicKey, identity.id, Buffer.concat([identity.publicKey.getBuffer(), identity.signatures.id]));
      /* return OrbitDBIdentityProvider.verify(
        identity.signatures.publicKey,
        new Uint8Array(Buffer.concat([identity.publicKey.getBuffer(), identity.signatures.id])),
        identity.id
      ) */
    }
    return false;
  }

}

