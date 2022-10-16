import { Address } from "../io"

import assert from 'assert'

describe('Parse Address', () => {
  it('throws an error if address is empty', () => {
    let err
    try {
      const result = Address.parse('')
    } catch (e: any) {
      err = e.toString()
    }
    expect(err).toEqual('Error: Not a valid OrbitDB address: ')
  })

  it('parse address successfully', () => {
    const address = '/orbitdb/zdpuAuK3BHpS7NvMBivynypqciYCuy2UW77XYBPUYRnLjnw13/first-database'
    const result = Address.parse(address)

    const isInstanceOf = result instanceof Address
    expect(isInstanceOf).toEqual(true)

    expect(result.root).toEqual('zdpuAuK3BHpS7NvMBivynypqciYCuy2UW77XYBPUYRnLjnw13')
    expect(result.path).toEqual('first-database')

    assert.equal(result.toString().indexOf('/orbitdb'), 0)
    assert.equal(result.toString().indexOf('zd'), 9)
  })

  it('parse address with backslashes (win32) successfully', () => {
    const address = '\\orbitdb\\Qmdgwt7w4uBsw8LXduzCd18zfGXeTmBsiR8edQ1hSfzcJC\\first-database'
    const result = Address.parse(address)

    const isInstanceOf = result instanceof Address
    expect(isInstanceOf).toEqual(true)

    expect(result.root).toEqual('Qmdgwt7w4uBsw8LXduzCd18zfGXeTmBsiR8edQ1hSfzcJC')
    expect(result.path).toEqual('first-database')

    assert.equal(result.toString().indexOf('/orbitdb'), 0)
    assert.equal(result.toString().indexOf('Qm'), 9)
  })
})

describe('isValid Address', () => {
  it('returns false for empty string', () => {
    const result = Address.isValid('')
    expect(result).toEqual(false)
  })

  it('validate address successfully', () => {
    const address = '/orbitdb/zdpuAuK3BHpS7NvMBivynypqciYCuy2UW77XYBPUYRnLjnw13/first-database'
    const result = Address.isValid(address)

    expect(result).toEqual(true)
  })

  it('handle missing orbitdb prefix', () => {
    const address = 'zdpuAuK3BHpS7NvMBivynypqciYCuy2UW77XYBPUYRnLjnw13/first-database'
    const result = Address.isValid(address)

    expect(result).toEqual(true)
  })

  it('handle missing db address name', () => {
    const address = '/orbitdb/zdpuAuK3BHpS7NvMBivynypqciYCuy2UW77XYBPUYRnLjnw13'
    const result = Address.isValid(address)

    expect(result).toEqual(true)
  })

  it('handle invalid multihash', () => {
    const address = '/orbitdb/Qmdgwt7w4uBsw8LXduzCd18zfGXeTmBsiR8edQ1hSfzc/first-database'
    const result = Address.isValid(address)

    expect(result).toEqual(false)
  })

  it('validate address with backslashes (win32) successfully', () => {
    const address = '\\orbitdb\\Qmdgwt7w4uBsw8LXduzCd18zfGXeTmBsiR8edQ1hSfzcJC\\first-database'
    const result = Address.isValid(address)

    expect(result).toEqual(true)
  })
})