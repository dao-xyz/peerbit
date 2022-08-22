import { Identity, IdentitySerializable, Signatures } from "../identity"
import { Ed25519PublicKey } from 'sodium-plus'
const assert = require('assert')

describe('Identity', function () {
  const id = new Uint8Array([0])
  const publicKey = new Ed25519PublicKey(Buffer.from(new Array(32).fill(0)))
  const idSignature = new Uint8Array([0]) // 'signature for <id>'
  const publicKeyAndIdSignature = new Uint8Array([0]) //'signature for <publicKey + idSignature>'
  const type = 'orbitdb'
  const provider = 'IdentityProviderInstance'

  let identity: Identity

  beforeAll(async () => {
    identity = new Identity({
      id, publicKey, signatures: new Signatures({
        id: idSignature, publicKey: publicKeyAndIdSignature
      }), type, provider: provider as any
    })
  })

  it('has the correct id', async () => {
    assert.strictEqual(identity.id, id)
  })

  it('has the correct publicKey', async () => {
    assert.strictEqual(identity.publicKey, publicKey)
  })

  it('has the correct idSignature', async () => {
    assert.strictEqual(identity.signatures.id, idSignature)
  })

  it('has the correct publicKeyAndIdSignature', async () => {
    assert.strictEqual(identity.signatures.publicKey, publicKeyAndIdSignature)
  })

  it('has the correct provider', async () => {
    assert.deepStrictEqual(identity.provider, provider)
  })

  it('converts identity to a JSON object', async () => {
    const expected = new IdentitySerializable({
      id: id,
      publicKey: publicKey,
      signatures: new Signatures({ id: idSignature, publicKey: publicKeyAndIdSignature }),
      type: type
    })
    assert(identity.toSerializable().equals(expected))
  })

  describe('Constructor inputs', () => {
    it('throws and error if id was not given in constructor', async () => {
      let err
      try {
        identity = new Identity({} as any)
      } catch (e) {
        err = e.toString()
      }
      assert.strictEqual(err, 'Error: Identity id is required')
    })

    it('throws and error if publicKey was not given in constructor', async () => {
      let err
      try {
        identity = new Identity({ id: 'abc' } as any)
      } catch (e) {
        err = e.toString()
      }
      assert.strictEqual(err, 'Error: Invalid public key')
    })

    it('throws and error if identity signature was not given in constructor', async () => {
      let err
      try {
        identity = new Identity({
          id: 'abc', publicKey
        } as any)
      } catch (e) {
        err = e.toString()
      }
      assert.strictEqual(err, 'Error: Signatures are required')
    })

    it('throws and error if identity signature was not given in constructor', async () => {
      let err
      try {
        identity = new Identity({
          id: 'abc', publicKey, signatures: new Signatures({
            id: idSignature
          } as any)
        } as any)
      } catch (e) {
        err = e.toString()
      }
      assert.strictEqual(err, 'Error: Signature of (publicKey + idSignature) is required')
    })

    it('throws and error if identity provider was not given in constructor', async () => {
      let err
      try {
        identity = new Identity({
          id: 'abc', publicKey, signatures: new Signatures({
            id: idSignature,
            publicKey: publicKeyAndIdSignature
          }), type
        } as any)
      } catch (e) {
        err = e.toString()
      }
      assert.strictEqual(err, 'Error: Identity provider is required')
    })

    it('throws and error if identity type was not given in constructor', async () => {
      let err
      try {
        identity = new Identity({
          id: 'abc', publicKey, signatures: new Signatures({
            id: idSignature,
            publicKey: publicKeyAndIdSignature
          }), provider: provider as any
        } as any)
      } catch (e) {
        err = e.toString()
      }
      assert.strictEqual(err, 'Error: Identity type is required')
    })
  })
})
