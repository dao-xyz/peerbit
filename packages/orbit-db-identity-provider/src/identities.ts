import { DIDIdentityProvider, DIDIdentityProviderOptions } from "./did-identity-provider"
import { EthIdentityProvider, EthIdentityProviderOptions } from "./ethereum-identity-provider"
import { Identity, IdentitySerializable, Signatures } from "./identity"
import { IdentityProvider } from "./identity-provider-interface"
import { OrbitDBIdentityProvider } from "./orbit-db-identity-provider"
import Keystore from 'orbit-db-keystore'
import LRU from 'lru'
import path from 'path'

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
  _keystore: any;
  _signingKeystore: any;
  _knownIdentities: any;

  constructor(options) {
    this._keystore = options.keystore
    this._signingKeystore = options.signingKeystore || this._keystore
    this._knownIdentities = options.cache || new LRU(options.cacheSize || 100)
  }

  static get IdentityProvider() { return IdentityProvider }

  get keystore() { return this._keystore }

  get signingKeystore() { return this._signingKeystore }

  async sign(identity: IdentitySerializable, data) {
    const signingKey = await this.keystore.getKey(identity.id)
    if (!signingKey) {
      throw new Error('Private signing key not found from Keystore')
    }
    const sig = await this.keystore.sign(signingKey, data)
    return sig
  }


  async verify(signature: string, publicKey: string, data, verifier = 'v1') {
    return this.keystore.verify(signature, publicKey, data, verifier)
  }

  async createIdentity(options: { type?: string, keystore?: typeof Keystore, signingKeystore?: typeof Keystore, id?: string, migrate?: (options: { targetStore: any, targetId: string }) => Promise<void> } & DIDIdentityProviderOptions & EthIdentityProviderOptions = {}) {
    const keystore = options.keystore || this.keystore
    const type = options.type || defaultType
    const identityProvider = type === defaultType ? new OrbitDBIdentityProvider(options.signingKeystore || keystore) : new (getHandlerFor(type))(options as any)
    const id = await identityProvider.getId(options)

    if (options.migrate) {
      await options.migrate({ targetStore: keystore._store, targetId: id })
    }
    const { publicKey, idSignature } = await this.signId(id)
    const pubKeyIdSignature = await identityProvider.sign(publicKey + idSignature, options)
    return new Identity({
      id, publicKey, signatures: new Signatures({
        id: idSignature, publicKey: pubKeyIdSignature
      }), type, provider: this
    })
  }

  async signId(id: string) {
    const keystore = this.keystore
    const key = await keystore.getKey(id) || await keystore.createKey(id)
    const publicKey = keystore.getPublic(key)
    const idSignature = await keystore.sign(key, id)
    return { publicKey, idSignature }
  }

  async verifyIdentity(identity: IdentitySerializable) {
    if (!Identity.isIdentity(identity)) {
      return false
    }

    const knownID = this._knownIdentities.get(identity.signatures.id)
    if (knownID) {
      return identity.id === knownID.id &&
        identity.publicKey === knownID.publicKey &&
        identity.signatures.id === knownID.signatures.id &&
        identity.signatures.publicKey === knownID.signatures.publicKey
    }

    const verifyIdSig = await this.keystore.verify(
      identity.signatures.id,
      identity.publicKey,
      identity.id
    )
    if (!verifyIdSig) return false

    const IdentityProvider = getHandlerFor(identity.type)
    const verified = await IdentityProvider.verifyIdentity(identity)
    if (verified) {
      this._knownIdentities.set(identity.signatures.id, identity)
    }
    return verified
  }

  static async verifyIdentity(identity) {
    if (!Identity.isIdentity(identity)) {
      return false
    }

    const verifyIdSig = await (Keystore as any).verify(
      identity.signatures.id,
      identity.publicKey,
      identity.id
    )

    if (!verifyIdSig) return false

    const IdentityProvider = getHandlerFor(identity.type)
    return IdentityProvider.verifyIdentity(identity)
  }

  static async createIdentity(options: { type?: string, identityKeysPath?: string, signingKeysPath?: string, keystore?: typeof Keystore, signingKeystore?: typeof Keystore, id?: string, migrate?: (options: { targetStore: any, targetId: string }) => Promise<void> } & DIDIdentityProviderOptions & EthIdentityProviderOptions = {}) {
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

