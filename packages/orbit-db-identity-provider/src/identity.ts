
import { Identities } from "./identities";
import { IdentityProvider } from "./identity-provider-interface";
import { isDefined } from "./is-defined";

export type IdentityProviderType = 'orbitdb' | 'ethereum' | 'solana' | string;

export interface IdentityAsJson {
  id: string;
  publicKey: string;
  signatures: {
    id: string,
    publicKey: string
  };
  type: IdentityProviderType;
}

export class Identity {
  _id: string;
  _publicKey: string;
  _signatures: {
    id: string;
    publicKey: string;
  }
  _type: string;
  _provider: Identities;
  constructor(id: string, publicKey: string, idSignature: string, pubKeyIdSignature: string, type: string, provider: Identities) {
    if (!isDefined(id)) {
      throw new Error('Identity id is required')
    }

    if (!isDefined(publicKey)) {
      throw new Error('Invalid public key')
    }

    if (!isDefined(idSignature)) {
      throw new Error('Signature of the id (idSignature) is required')
    }

    if (!isDefined(pubKeyIdSignature)) {
      throw new Error('Signature of (publicKey + idSignature) is required')
    }

    if (!isDefined(type)) {
      throw new Error('Identity type is required')
    }

    if (!isDefined(provider)) {
      throw new Error('Identity provider is required')
    }

    this._id = id
    this._publicKey = publicKey
    this._signatures = Object.assign({}, { id: idSignature }, { publicKey: pubKeyIdSignature })
    this._type = type
    this._provider = provider
  }

  /**
  * This is only used as a fallback to the clock id when necessary
  * @return {string} public key hex encoded
  */
  get id() {
    return this._id
  }

  get publicKey() {
    return this._publicKey
  }

  get signatures() {
    return this._signatures
  }

  get type() {
    return this._type
  }

  get provider() {
    return this._provider
  }

  toJSON(): IdentityAsJson {
    return {
      id: this.id,
      publicKey: this.publicKey,
      signatures: this.signatures,
      type: this.type
    }
  }

  static isIdentity(identity) {
    return identity.id !== undefined &&
      identity.publicKey !== undefined &&
      identity.signatures !== undefined &&
      identity.signatures.id !== undefined &&
      identity.signatures.publicKey !== undefined &&
      identity.type !== undefined
  }

  static toJSON(identity) {
    return {
      id: identity.id,
      publicKey: identity.publicKey,
      signatures: identity.signatures,
      type: identity.type
    }
  }
}

