/* import { DIDIdentityProvider, DIDIdentityProviderOptions } from "./did-identity-provider"
 */import { EthIdentityProvider, EthIdentityProviderOptions } from "./ethereum-identity-provider"
import { Identity, Signatures } from "./identity"
import { IdentityProvider } from "./identity-provider-interface"
import { OrbitDBIdentityProvider } from "./orbit-db-identity-provider"
import { Keystore } from '@dao-xyz/orbit-db-keystore'
import path from 'path'
import { SolanaIdentityProviderOptions } from "./solana-identity-provider"
import Cache from "@dao-xyz/orbit-db-cache"
import { verify } from "@dao-xyz/peerbit-crypto"
const defaultType = 'orbitdb'
const identityKeysPath = path.join('./orbitdb', 'identity', 'identitykeys')

/* const supportedTypes = {
  orbitdb: OrbitDBIdentityProvider,
  [DIDIdentityProvider.type]: DIDIdentityProvider,
  [EthIdentityProvider.type]: EthIdentityProvider
}

const getHandlerFor = (type: string) => {
  if (!Identities.isSupported(type)) {
    throw new Error(`IdentityProvider type '${type}' is not supported`)
  }
  return supportedTypes[type]
} */

export class Identities {
  _keystore: Keystore;
  _signingKeystore: Keystore;/* 
  _knownIdentities: { get(id: string): Identity, set(id: string, identity: Identity): void }; */

  constructor(options: { keystore: Keystore, signingKeystore?: Keystore, cache?: Cache, cacheSize?: number }) {
    this._keystore = options.keystore
    this._signingKeystore = options.signingKeystore || this._keystore/* 
    this._knownIdentities = options.cache || new LRU(options.cacheSize || 100) */
  }

  static get IdentityProvider() { return IdentityProvider }

  get keystore() { return this._keystore }

  get signingKeystore() { return this._signingKeystore }

  async sign(data: Uint8Array, identity: Identity) {
    const signingKey = await this.keystore.getKeyByPath<KeyWithMeta<Ed25519Keypair>>(Buffer.from(identity.id).toString('base64'))
    if (!signingKey) {
      throw new Error('Private signing key not found from Keystore')
    }
    const sig = await Keystore.sign(data, signingKey)
    return sig
  }


  async verify(signature: Uint8Array, publicKey: Ed25519PublicKey, data: Uint8Array) {
    return Keystore.verify(signature, publicKey, data)
  }

  async createIdentity(options: { type?: string, keystore?: Keystore, signingKeystore?: Keystore, id?: Uint8Array, migrate?: (options: { targetStore: any, targetId: Uint8Array }) => Promise<void> } & (DIDIdentityProviderOptions | EthIdentityProviderOptions | SolanaIdentityProviderOptions) = {}) {

    const keystore = options.keystore || this.keystore
    const type = options.type || defaultType
    const identityProvider = type === defaultType ? new OrbitDBIdentityProvider(options.signingKeystore || keystore) : new (getHandlerFor(type))(options as any)
    const id = await identityProvider.getId(options)
    const identity = await this.createUnsignedIdentity(id, options)
    const pubKeyIdSignature = await identityProvider.sign(Buffer.concat([identity.publicKey.getBuffer(), identity.signatures.id]), options)
    identity.signatures.publicKey = pubKeyIdSignature
    return identity;
  }

  /**
   * When we dont have access to the identityProvider, we can still sign it using our key and let it be signed
   * later by the id
   * @param id 
   * @param options 
   * @returns 
   */
  async createUnsignedIdentity(id: Identity, options: { type?: string, keystore?: Keystore, signingKeystore?: Keystore, id?: Uint8Array, migrate?: (options: { targetStore: any, targetId: Uint8Array }) => Promise<void> } & (DIDIdentityProviderOptions | EthIdentityProviderOptions | SolanaIdentityProviderOptions) = {}) {
    const keystore = options.keystore || this.keystore
    const type = options.type || defaultType

    if (options.migrate) {
      await options.migrate({ targetStore: keystore._store, targetId: id })
    }

    // Sign id (and generate signer key of this id)
    const { publicKey, idSignature } = await this.signId(new Uint8Array(id.id.getBuffer()))
    const identity = new Identity({
      id, publicKey: publicKey, signatures: new Signatures({
        id: idSignature, publicKey: undefined
      })
    })
    return identity;
  }


  async signId(id: Uint8Array) {
    const keystore = this.keystore
    const existingKey = await keystore.getKeyByPath(id, KeyWithMeta<Ed25519Keypair>);
    const key = existingKey || await keystore.createKey(id, KeyWithMeta<Ed25519Keypair>)
    const publicKey = key.publicKey
    const idSignature = await Keystore.sign(id, key)
    return { publicKey, idSignature }
  }

  /* async verifyIdentity(identity: Identity) {
 

    const identityHex = Buffer.from(identity.signatures.id).toString('base64');
    const knownID = this._knownIdentities.get(identityHex)
    if (knownID) {
      return identity.equals(knownID)
    }

    const verifyIdSig = await Keystore.verify(
      identity.signatures.id,
      identity.publicKey,
      identity.id
    )

    if (!verifyIdSig) {
      return false
    }

    const IdentityProvider = getHandlerFor(identity.type)
    const verified = await IdentityProvider.verifyIdentity(identity)
    if (verified) {
      this._knownIdentities.set(identityHex, identity)
    }
    return verified
  }
 */
  static async verifyIdentity(identity: Identity) {
    /*   if (!Identity.isIdentity(identity)) {
        return false
      } */

    const verifyIdSig = await verify(
      identity.signatures.id,
      identity.publicKey,
      identity.id
    )

    if (!verifyIdSig) return false

    return await verify(identity.signatures.publicKey, identity.id, Buffer.concat([identity.publicKey.getBuffer(), identity.signatures.id]))
  }

  static async createIdentity(options: { type?: string, identityKeysPath?: string, signingKeysPath?: string, keystore?: Keystore, signingKeystore?: Keystore, id?: Uint8Array, migrate?: (options: { targetStore: any, targetId: Uint8Array }) => Promise<void> } & (DIDIdentityProviderOptions | EthIdentityProviderOptions | SolanaIdentityProviderOptions) = {}) {
    if (!options.keystore) {
      options.keystore = new (Keystore as any)(options.identityKeysPath || identityKeysPath)
    }
    if (!options.signingKeystore) {
      if (options.signingKeysPath) {
        options.signingKeystore = new (Keystore as any)(options.signingKeysPath)
      } else {
        options.signingKeystore = options.keystore
      }
    }
    options = Object.assign({}, { type: defaultType }, options)
    const identities = new Identities(options as any) // TODO fix types
    return identities.createIdentity(options)
  }
  /* 
    static isSupported(type): boolean {
      return Object.keys(supportedTypes).includes(type)
    }
  
    static addIdentityProvider(IdentityProvider) {
      if (!IdentityProvider) {
        throw new Error('IdentityProvider class needs to be given as an option')
      }
  
      if (!IdentityProvider.type ||
        typeof IdentityProvider.type !== 'string') {
        throw new Error('Given IdentityProvider class needs to implement: static get type() { return a string }.')
      }
  
      supportedTypes[IdentityProvider.type] = IdentityProvider
    }
  
    static removeIdentityProvider(type) {
      delete supportedTypes[type]
    } */
}

