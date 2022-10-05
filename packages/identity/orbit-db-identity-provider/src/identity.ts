
import { Identities } from "./identities";
import { isDefined } from "./is-defined";
import { variant, field, serialize, vec, option } from '@dao-xyz/borsh';
import { createHash } from "crypto";
import { bufferSerializer, U8IntArraySerializer, arraysEqual } from "@dao-xyz/borsh-utils";
import { PublicSignKey } from "@dao-xyz/peerbit-crypto";

@variant(0)
export class Signatures {

  @field(U8IntArraySerializer)
  id: Uint8Array;

  @field({ type: option(U8IntArraySerializer) })
  publicKey?: Uint8Array;

  constructor(options?: {
    id: Uint8Array;
    publicKey?: Uint8Array;
  }) {
    if (options) {
      if (!isDefined(options.id)) {
        throw new Error('Signature of the id (idSignature) is required')
      }

      this.id = options.id;
      this.publicKey = options.publicKey;
    }
  }
  equals(other: Signatures): boolean {
    return arraysEqual(this.id, other.id) && arraysEqual(this.publicKey, other.publicKey)
  }
}

/* const identityToString = (identity: IdentitySerializable | Identity) => {
  return identity.type + "/" + Buffer.from(identity.id).toString('base64')
} */

@variant(0)
export class Identity {

  @field({ type: PublicKey })
  id: PublicKey;

  @field({ type: PublicKey })
  publicKey: PublicKey;

  @field({ type: Signatures })
  signatures: Signatures;

  constructor(
    options?: {
      id: PublicKey,
      publicKey: PublicKey,
      signatures: Signatures
    }
  ) {
    if (options) {

      this.id = options.id;
      this.publicKey = options.publicKey;
      this.signatures = options.signatures;
    }

  }

  public hashCode() {
    return createHash('sha1').update(serialize(this)).digest('hex')
  }

  static from(identity: Identity | any) {
    if (identity instanceof Identity)
      return identity;
    return new Identity({
      id: identity.id,
      publicKey: identity.publicKey,
      signatures: new Signatures({
        id: identity.signatures.id,
        publicKey: identity.signatures.publicKey
      })
    })
  }

  equals(other: Identity): boolean {
    return this.id.equals(other.id) && this.publicKey.equals(other.publicKey) && this.signatures.equals(other.signatures)
  }

  /* toString() {
    return identityToString(this);
  } */
}


/* 
export class Identity {

  _id: Uint8Array;
  _publicKey: Ed25519PublicKey;
  _signatures: Signatures;
  _type: string;


  _provider: Identities;
  constructor(options?: { id: Uint8Array, publicKey: Ed25519PublicKey, signatures: Signatures, type: string, provider: Identities }) {
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


  get id(): Uint8Array {
    return this._id
  }

  get publicKey(): Ed25519PublicKey {
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
    return arraysEqual(this.id, other.id) && Buffer.compare(this.publicKey.getBuffer(), other.publicKey.getBuffer()) === 0 && this.type === other.type && this.signatures.equals(other.signatures)
  }

} */




/*  toString() {
   return identityToString(this);
 } */


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