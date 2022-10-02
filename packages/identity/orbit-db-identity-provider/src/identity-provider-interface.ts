import { Identity } from "./identity"
export abstract class IdentityProvider {
  /* Return id of identity (to be signed by orbit-db public key) */
  async getId(options: { id?: Uint8Array } = {}): Promise<Uint8Array> {
    throw new Error("Not implemented")
  }

  /* Return signature of a OrbitDB public key signature */
  sign(data: string | Uint8Array | Buffer, options: { id?: Uint8Array } = {}): Promise<Uint8Array> | Uint8Array {
    throw new Error("Not implemented")

  }

  /* Verify a signature of a OrbitDB public key signature */
  static async verifyIdentity(identity: Identity): Promise<boolean> {
    throw new Error("Not implemented")
  }

  static async verify(signature: Uint8Array, data: Uint8Array, publicKey: Uint8Array): Promise<boolean> {
    throw new Error("Not implemented")
  }

  /* Return the type for this identity provider */
  static get type(): string {
    throw new Error('\'static get type ()\' needs to be defined in the inheriting class')
  }

  /*
    Return the type for this identity-procider
    NOTE! This is the only property of the interface that
    shouldn't be overridden in the inherited IdentityProvider
  */
  get type() {
    return this.constructor["type"]
  }
}

