
import { Identities } from "./identities";
import { isDefined } from "./is-defined";
import { variant, field, serialize } from '@dao-xyz/borsh';
import { createHash } from "crypto";
export type IdentityProviderType = 'orbitdb' | 'ethereum' | 'solana' | string;


@variant(0)
export class Signatures {

  id: string;
  publicKey: string;

  constructor(options?: {
    id: string;
    publicKey: string;
  }) {
    if (options) {
      if (!isDefined(options.id)) {
        throw new Error('Signature of the id (idSignature) is required')
      }

      if (!isDefined(options.publicKey)) {
        throw new Error('Signature of (publicKey + idSignature) is required')
      }
      this.id = options.id;
      this.publicKey = options.publicKey;
    }
  }

}

@variant(0)
export class IdentitySerializable {

  @field({ type: 'String' })
  id: string;

  @field({ type: 'String' })
  publicKey: string;

  @field({ type: Signatures })
  signatures: Signatures;

  @field({ type: 'String' })
  type: IdentityProviderType;
  constructor(
    options?: {
      id: string,
      publicKey: string,
      signatures: Signatures,
      type: string
    }
  ) {
    if (options) {

      this.id = options.id;
      this.publicKey = options.publicKey;
      this.signatures = options.signatures;
      this.type = options.type;
    }

  }

  public hashCode() {
    return createHash('sha1').update(serialize(this)).digest('hex')
  }

  static from(identity: IdentitySerializable | any) {
    if (identity instanceof IdentitySerializable)
      return identity;
    return new IdentitySerializable({
      id: identity.id,
      publicKey: identity.publicKey,
      signatures: new Signatures({
        id: identity.signatures.id,
        publicKey: identity.signatures.publicKey
      }),
      type: identity.type
    })
  }
}



export class Identity {

  _id: string;
  _publicKey: string;
  _signatures: Signatures;
  _type: string;


  _provider: Identities;
  constructor(options?: { id: string, publicKey: string, signatures: Signatures, type: string, provider: Identities }) {
    if (options) {
      if (!isDefined(options.id)) {
        throw new Error('Identity id is required')
      }

      if (!isDefined(options.publicKey)) {
        throw new Error('Invalid public key')
      }

      if (!isDefined(options.signatures)) {
        throw new Error('Signatures are required')
      }

      if (!isDefined(options.type)) {
        throw new Error('Identity type is required')
      }

      if (!isDefined(options.provider)) {
        throw new Error('Identity provider is required')
      }

      this._id = options.id
      this._publicKey = options.publicKey
      this._signatures = options.signatures
      this._type = options.type
      this._provider = options.provider
    }
  }

  /**
  * This is only used as a fallback to the clock id when necessary
  * @return {string} public key hex encoded
  */
  get id(): string {
    return this._id
  }

  get publicKey(): string {
    return this._publicKey
  }

  get signatures(): Signatures {
    return this._signatures
  }

  get type(): string {
    return this._type
  }

  get provider(): Identities {
    return this._provider
  }

  toSerializable(): IdentitySerializable {
    const ser = new IdentitySerializable();
    ser.id = this.id;
    ser.publicKey = this.publicKey;
    ser.signatures = new Signatures({ ...this.signatures });
    ser.type = this.type;
    return ser;
  }

  static isIdentity(identity) {
    return identity.id !== undefined &&
      identity.publicKey !== undefined &&
      identity.signatures !== undefined &&
      identity.signatures.id !== undefined &&
      identity.signatures.publicKey !== undefined &&
      identity.type !== undefined
  }


  /* static from(identity: IdentitySerializable): Identity {
    return new Identity({
      id: identity.id,
      publicKey: identity.publicKey,
      signatures: new Signatures({
        id: identity.signatures.id,
        publicKey: identity.signatures.publicKey
      }),
      type: identity.type
    })
  } */
}



