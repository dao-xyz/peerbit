import { Identity } from "../identity"

const assert = require('assert')

describe('Identity', function () {
  const id = '0x01234567890abcdefghijklmnopqrstuvwxyz'
  const publicKey = '<pubkey>'
  const idSignature = 'signature for <id>'
  const publicKeyAndIdSignature = 'signature for <publicKey + idSignature>'
  const type = 'orbitdb'
  const provider = 'IdentityProviderInstance'

  let identity

  beforeAll(async () => {
    identity = new Identity(id, publicKey, idSignature, publicKeyAndIdSignature, type, provider as any)
  })

  test('has the correct id', async () => {
    assert.strictEqual(identity.id, id)
  })

  test('has the correct publicKey', async () => {
    assert.strictEqual(identity.publicKey, publicKey)
  })

  test('has the correct idSignature', async () => {
    assert.strictEqual(identity.signatures.id, idSignature)
  })

  test('has the correct publicKeyAndIdSignature', async () => {
    assert.strictEqual(identity.signatures.publicKey, publicKeyAndIdSignature)
  })

  test('has the correct provider', async () => {
    assert.deepStrictEqual(identity.provider, provider)
  })

  test('converts identity to a JSON object', async () => {
    const expected = {
      id: id,
      publicKey: publicKey,
      signatures: { id: idSignature, publicKey: publicKeyAndIdSignature },
      type: type
    }
    assert.deepStrictEqual(identity.toJSON(), expected)
  })

  describe('Constructor inputs', () => {
    test('throws and error if id was not given in constructor', async () => {
      let err
      try {
        identity = new Identity(undefined, undefined, undefined, undefined, undefined, undefined)
      } catch (e) {
        err = e.toString()
      }
      assert.strictEqual(err, 'Error: Identity id is required')
    })

    test('throws and error if publicKey was not given in constructor', async () => {
      let err
      try {
        identity = new Identity('abc', undefined, undefined, undefined, undefined, undefined)
      } catch (e) {
        err = e.toString()
      }
      assert.strictEqual(err, 'Error: Invalid public key')
    })

    test('throws and error if identity signature was not given in constructor', async () => {
      let err
      try {
        identity = new Identity('abc', publicKey, undefined, undefined, undefined, undefined)
      } catch (e) {
        err = e.toString()
      }
      assert.strictEqual(err, 'Error: Signature of the id (idSignature) is required')
    })

    test('throws and error if identity signature was not given in constructor', async () => {
      let err
      try {
        identity = new Identity('abc', publicKey, idSignature, undefined, undefined, undefined)
      } catch (e) {
        err = e.toString()
      }
      assert.strictEqual(err, 'Error: Signature of (publicKey + idSignature) is required')
    })

    test('throws and error if identity provider was not given in constructor', async () => {
      let err
      try {
        identity = new Identity('abc', publicKey, idSignature, publicKeyAndIdSignature, type, undefined)
      } catch (e) {
        err = e.toString()
      }
      assert.strictEqual(err, 'Error: Identity provider is required')
    })

    test('throws and error if identity type was not given in constructor', async () => {
      let err
      try {
        identity = new Identity('abc', publicKey, idSignature, publicKeyAndIdSignature, null, provider as any)
      } catch (e) {
        err = e.toString()
      }
      assert.strictEqual(err, 'Error: Identity type is required')
    })
  })
})
