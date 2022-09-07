import { Address } from "../io"

const assert = require('assert')

describe('Parse Address', () => {
  it('throws an error if address is empty', () => {
    let err
    try {
      const result = Address.parse('')
    } catch (e) {
      err = e.toString()
    }
    assert.equal(err, 'Error: Not a valid OrbitDB address: ')
  })

  it('parse address successfully', () => {
    const address = '/orbitdb/zdpuAuK3BHpS7NvMBivynypqciYCuy2UW77XYBPUYRnLjnw13/first-database'
    const result = Address.parse(address)

    const isInstanceOf = result instanceof Address
    assert.equal(isInstanceOf, true)

    assert.equal(result.root, 'zdpuAuK3BHpS7NvMBivynypqciYCuy2UW77XYBPUYRnLjnw13')
    assert.equal(result.path, 'first-database')

    assert.equal(result.toString().indexOf('/orbitdb'), 0)
    assert.equal(result.toString().indexOf('zd'), 9)
  })

  it('parse address with backslashes (win32) successfully', () => {
    const address = '\\orbitdb\\Qmdgwt7w4uBsw8LXduzCd18zfGXeTmBsiR8edQ1hSfzcJC\\first-database'
    const result = Address.parse(address)

    const isInstanceOf = result instanceof Address
    assert.equal(isInstanceOf, true)

    assert.equal(result.root, 'Qmdgwt7w4uBsw8LXduzCd18zfGXeTmBsiR8edQ1hSfzcJC')
    assert.equal(result.path, 'first-database')

    assert.equal(result.toString().indexOf('/orbitdb'), 0)
    assert.equal(result.toString().indexOf('Qm'), 9)
  })
})

describe('isValid Address', () => {
  it('returns false for empty string', () => {
    const result = Address.isValid('')
    assert.equal(result, false)
  })

  it('validate address successfully', () => {
    const address = '/orbitdb/zdpuAuK3BHpS7NvMBivynypqciYCuy2UW77XYBPUYRnLjnw13/first-database'
    const result = Address.isValid(address)

    assert.equal(result, true)
  })

  it('handle missing orbitdb prefix', () => {
    const address = 'zdpuAuK3BHpS7NvMBivynypqciYCuy2UW77XYBPUYRnLjnw13/first-database'
    const result = Address.isValid(address)

    assert.equal(result, true)
  })

  it('handle missing db address name', () => {
    const address = '/orbitdb/zdpuAuK3BHpS7NvMBivynypqciYCuy2UW77XYBPUYRnLjnw13'
    const result = Address.isValid(address)

    assert.equal(result, true)
  })

  it('handle invalid multihash', () => {
    const address = '/orbitdb/Qmdgwt7w4uBsw8LXduzCd18zfGXeTmBsiR8edQ1hSfzc/first-database'
    const result = Address.isValid(address)

    assert.equal(result, false)
  })

  it('validate address with backslashes (win32) successfully', () => {
    const address = '\\orbitdb\\Qmdgwt7w4uBsw8LXduzCd18zfGXeTmBsiR8edQ1hSfzcJC\\first-database'
    const result = Address.isValid(address)

    assert.equal(result, true)
  })
})