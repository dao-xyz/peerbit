import { Identity, Signatures } from "../identity"

const assert = require('assert')

describe('Identity', function () {
  const id = '0x01234567890abcdefghijklmnopqrstuvwxyz'
  const publicKey = '<pubkey>'
  const idSignature = 'signature for <id>'
  const publicKeyAndIdSignature = 'signature for <publicKey + idSignature>'
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
    assert.deepStrictEqual(identity.toSerializable(), expected)
  })

  describe('Constructor inputs', () => {
    test('throws and error if id was not given in constructor', async () => {
      let err
      try {
        identity = new Identity({} as any)
      } catch (e) {
        err = e.toString()
      }
      assert.strictEqual(err, 'Error: Identity id is required')
    })

    test('throws and error if publicKey was not given in constructor', async () => {
      let err
      try {
        identity = new Identity({ id: 'abc' } as any)
      } catch (e) {
        err = e.toString()
      }
      assert.strictEqual(err, 'Error: Invalid public key')
    })

    test('throws and error if identity signature was not given in constructor', async () => {
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

    test('throws and error if identity signature was not given in constructor', async () => {
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

    test('throws and error if identity provider was not given in constructor', async () => {
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

    test('throws and error if identity type was not given in constructor', async () => {
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
