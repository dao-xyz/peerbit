
import { Identities } from "./identities";
import { isDefined } from "./is-defined";
import { variant, field, serialize, vec } from '@dao-xyz/borsh';
import { createHash } from "crypto";
import { U8IntArraySerializer, arraysEqual } from "@dao-xyz/io-utils";

@variant(0)
export class Signatures {

  @field(U8IntArraySerializer)
  id: Uint8Array;

  @field(U8IntArraySerializer)
  publicKey: Uint8Array;

  constructor(options?: {
    id: Uint8Array;
    publicKey: Uint8Array;
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
  equals(other: Signatures): boolean {
    return arraysEqual(this.id, other.id) && arraysEqual(this.publicKey, other.publicKey)
  }
}

const identityToString = (identity: IdentitySerializable | Identity) => {
  return identity.type + "/" + Buffer.from(identity.id).toString('base64')
}
@variant(0)
export class IdentitySerializable {

  @field(U8IntArraySerializer)
  id: Uint8Array;

  @field(U8IntArraySerializer)
  publicKey: Uint8Array;

  @field({ type: Signatures })
  signatures: Signatures;

  @field({ type: 'String' })
  type: string;

  constructor(
    options?: {
      id: Uint8Array,
      publicKey: Uint8Array,
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

  equals(other: IdentitySerializable): boolean {
    return arraysEqual(this.id, other.id) && arraysEqual(this.publicKey, other.publicKey) && this.type === other.type && this.signatures.equals(other.signatures)
  }

  toString() {
    return identityToString(this);
  }
}



export class Identity {

  _id: Uint8Array;
  _publicKey: Uint8Array;
  _signatures: Signatures;
  _type: string;


  _provider: Identities;
  constructor(options?: { id: Uint8Array, publicKey: Uint8Array, signatures: Signatures, type: string, provider: Identities }) {
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
  get id(): Uint8Array {
    return this._id
  }

  get publicKey(): Uint8Array {
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

  equals(other: IdentitySerializable): boolean {
    return arraysEqual(this.id, other.id) && arraysEqual(this.publicKey, other.publicKey) && this.type === other.type && this.signatures.equals(other.signatures)
  }

  toString() {
    return identityToString(this);
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



