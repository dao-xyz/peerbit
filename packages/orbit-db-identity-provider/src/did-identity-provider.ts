import { IdentityProvider } from "./identity-provider-interface"

const u8a = require('uint8arrays')
import { DID } from 'dids'
import { IdentityAsJson } from "./identity";

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
    return this.did.id
  }

  async sign(data, { }) {
    if (!this.did.authenticated) await this.did.authenticate()
    const payload = u8a.toString(u8a.fromString(data, 'base16'), 'base64url')
    const jws = await this.did.createJWS(payload)
    // encode as JWS with detached payload
    return `${jws.signatures[0].protected}..${jws.signatures[0].signature}`
  }

  static setDIDResolver(resolver) {
    if (!this.did) {
      this.did = new DID({ resolver })
    } else {
      this.did.setResolver(resolver)
    }
  }

  static async verifyIdentity(identity: IdentityAsJson) {
    if (!this.did) {
      throw new Error('The DID resolver must first be set with setDIDResolver()')
    }
    const data = identity.publicKey + identity.signatures.id
    try {
      const payload = u8a.toString(u8a.fromString(data, 'base16'), 'base64url')
      const [header, signature] = identity.signatures.publicKey.split('..')
      const jws = [header, payload, signature].join('.')
      await this.did.verifyJWS(jws)

    } catch (e) {
      return false
    }
    return true
  }
}

