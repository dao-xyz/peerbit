import { IdentityProvider } from "./identity-provider-interface"

// TODO make JWS compliant with PublicKey 
/* 
import * as u8a from 'uint8arrays'
import { DID } from 'dids'
import { Identity } from "./identity";

const TYPE = 'DID'
export type DIDIdentityProviderOptions = { didProvider?: any };

export class DIDIdentityProvider extends IdentityProvider {
  static did: any;
  did: DID;

  constructor(options?: DIDIdentityProviderOptions) {
    super()
    this.did = new DID({
      resolver: DIDIdentityProvider.did._resolver,
      provider: options.didProvider
    })
  }

  static get type() {
    return TYPE
  }

  async getId({ }) {
    if (!this.did.authenticated) await this.did.authenticate()
    return new Uint8Array(Buffer.from(this.did.id))
  }

  async sign(data: Uint8Array, { }): Promise<Uint8Array> {
    if (!this.did.authenticated) await this.did.authenticate()
    const payload = u8a.toString(data, 'base64url')
    const jws = await this.did.createJWS(payload)
    // encode as JWS with detached payload
    return new Uint8Array(Buffer.from(`${jws.signatures[0].protected}..${jws.signatures[0].signature}`))
  }

  static setDIDResolver(resolver) {
    if (!this.did) {
      this.did = new DID({ resolver })
    } else {
      this.did.setResolver(resolver)
    }
  }

  static async verifyIdentity(identity: Identity) {
    if (!this.did) {
      throw new Error('The DID resolver must first be set with setDIDResolver()')
    }
    const signatureWithHeader = identity.signatures.publicKey;
    const data = new Uint8Array(Buffer.concat([identity.publicKey.getBuffer(), identity.signatures.id]));

    try {
      const payload = u8a.toString(data, 'base64url')
      const [header, signature] = Buffer.from(signatureWithHeader).toString().split('..')
      const jws = [header, payload, signature].join('.')
      await this.did.verifyJWS(jws)

    } catch (e) {
      return false
    }
    return true
  }
}

 */