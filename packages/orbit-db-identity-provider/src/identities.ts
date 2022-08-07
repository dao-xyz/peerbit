import { DIDIdentityProvider, DIDIdentityProviderOptions } from "./did-identity-provider"
import { EthIdentityProvider, EthIdentityProviderOptions } from "./ethereum-identity-provider"
import { Identity, IdentitySerializable, Signatures } from "./identity"
import { IdentityProvider } from "./identity-provider-interface"
import { OrbitDBIdentityProvider } from "./orbit-db-identity-provider"
import { Keystore } from '@dao-xyz/orbit-db-keystore'
import LRU from 'lru'
import path from 'path'
import { Ed25519PublicKey } from 'sodium-plus';
import { joinUint8Arrays } from "./utils"
import { SolanaIdentityProviderOptions } from "./solana-identity-provider"

const defaultType = 'orbitdb'
const identityKeysPath = path.join('./orbitdb', 'identity', 'identitykeys')

const supportedTypes = {
  orbitdb: OrbitDBIdentityProvider,
  [DIDIdentityProvider.type]: DIDIdentityProvider,
  [EthIdentityProvider.type]: EthIdentityProvider
}

const getHandlerFor = (type: string) => {
  if (!Identities.isSupported(type)) {
    throw new Error(`IdentityProvider type '${type}' is not supported`)
  }
  return supportedTypes[type]
}

export class Identities {
  _keystore: Keystore;
  _signingKeystore: Keystore;
  _knownIdentities: { get(id: string): IdentitySerializable, set(id: string, identity: IdentitySerializable): void };

  constructor(options) {
    this._keystore = options.keystore
    this._signingKeystore = options.signingKeystore || this._keystore
    this._knownIdentities = options.cache || new LRU(options.cacheSize || 100)
  }

  static get IdentityProvider() { return IdentityProvider }

  get keystore() { return this._keystore }

  get signingKeystore() { return this._signingKeystore }

  async sign(identity: Identity | IdentitySerializable, data: Uint8Array) {
    const signingKey = await this.keystore.getKey(Buffer.from(identity.id).toString('base64'))
    if (!signingKey) {
      throw new Error('Private signing key not found from Keystore')
    }
    const sig = await this.keystore.sign(signingKey, data)
    return sig
  }


  async verify(signature: Uint8Array, publicKey: Ed25519PublicKey, data) {
    return this.keystore.verify(signature, publicKey, data)
  }

  async createIdentity(options: { type?: string, keystore?: Keystore, signingKeystore?: Keystore, id?: Uint8Array, migrate?: (options: { targetStore: any, targetId: Uint8Array }) => Promise<void> } & (DIDIdentityProviderOptions | EthIdentityProviderOptions | SolanaIdentityProviderOptions) = {}) {
    const keystore = options.keystore || this.keystore
    const type = options.type || defaultType
    const identityProvider = type === defaultType ? new OrbitDBIdentityProvider(options.signingKeystore || keystore) : new (getHandlerFor(type))(options as any)
    const id = await identityProvider.getId(options)

    if (options.migrate) {
      await options.migrate({ targetStore: keystore._store, targetId: id })
    }

    const { publicKey, idSignature } = await this.signId(id)
    const publicKeyBytes = new Uint8Array(publicKey.getBuffer());
    const pubKeyIdSignature = await identityProvider.sign(joinUint8Arrays([publicKeyBytes, idSignature]), options)


    const identity = new Identity({
      id, publicKey: publicKeyBytes, signatures: new Signatures({
        id: idSignature, publicKey: pubKeyIdSignature
      }), type, provider: this
    })
    return identity;
  }

  async signId(id: Uint8Array) {
    const keystore = this.keystore
    const idString = Buffer.from(id).toString('base64');
    const existingKey = await keystore.getKey(idString);
    const key = existingKey || await keystore.createKey(idString)
    const publicKey = await Keystore.getPublicSign(key)
    const idSignature = await keystore.sign(key, id)
    return { publicKey, idSignature }
  }

  async verifyIdentity(identity: Identity | IdentitySerializable) {
    if (!Identity.isIdentity(identity)) {
      return false
    }

    const identityHex = Buffer.from(identity.signatures.id).toString('base64');
    const knownID = this._knownIdentities.get(identityHex)
    if (knownID) {
      return identity.equals(knownID)
    }

    const verifyIdSig = await this.keystore.verify(
      identity.signatures.id,
      new Ed25519PublicKey(Buffer.from(identity.publicKey)),
      identity.id
    )

    if (!verifyIdSig) {
      return false
    }

    const IdentityProvider = getHandlerFor(identity.type)
    const verified = await IdentityProvider.verifyIdentity(identity)
    if (verified) {
      this._knownIdentities.set(identityHex, identity instanceof Identity ? identity.toSerializable() : identity)
    }
    return verified
  }

  static async verifyIdentity(identity: Identity) {
    if (!Identity.isIdentity(identity)) {
      return false
    }

    const verifyIdSig = await Keystore.verify(
      identity.signatures.id,
      new Ed25519PublicKey(Buffer.from(identity.publicKey)),
      identity.id
    )

    if (!verifyIdSig) return false

    const IdentityProvider = getHandlerFor(identity.type)
    return IdentityProvider.verifyIdentity(identity)
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
    const identities = new Identities(options)
    return identities.createIdentity(options)
  }

  static isSupported(type): boolean {
    return Object.keys(supportedTypes).includes(type)
  }

  static addIdentityProvider(IdentityProvider) {
    if (!IdentityProvider) {
      throw new Error('IdentityProvider class needs to be given as an option')
    }

    if (!IdentityProvider.type ||
      typeof IdentityProvider.type !== 'string') {
      throw new Error('Given IdentityProvider class needs to implement: static get type() { /* return a string */}.')
    }

    supportedTypes[IdentityProvider.type] = IdentityProvider
  }

  static removeIdentityProvider(type) {
    delete supportedTypes[type]
  }
}

