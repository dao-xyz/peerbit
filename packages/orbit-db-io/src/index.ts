import * as Block from 'multiformats/block'
import { CID } from 'multiformats/cid'
import * as dagPb from '@ipld/dag-pb'
import * as dagCbor from '@ipld/dag-cbor'
import { sha256 as hasher } from 'multiformats/hashes/sha2'
import { base58btc } from 'multiformats/bases/base58'

const mhtype = 'sha2-256'
const defaultBase = base58btc
const unsupportedCodecError = () => new Error('unsupported codec')

const cidifyString = (str) => {
  if (!str) {
    return str
  }

  if (Array.isArray(str)) {
    return str.map(cidifyString)
  }

  return CID.parse(str)
}

const stringifyCid = (cid, options: any = {}): any => {
  if (!cid || typeof cid === 'string') {
    return cid
  }

  if (Array.isArray(cid)) {
    return cid.map(stringifyCid)
  }

  if (cid['/']) {
    return cid['/']
  }

  const base = options.base || defaultBase
  return cid.toString(base)
}

const codecCodes = {
  [dagPb.code]: dagPb,
  [dagCbor.code]: dagCbor
}
const codecMap = {
  // staying backward compatible
  // old writeObj function was never raw codec; defaulted to cbor via ipfs.dag
  raw: dagCbor,
  'dag-pb': dagPb,
  'dag-cbor': dagCbor
}

async function read(ipfs, cid, options: { timeout?: number, links?: string[] } = {}) {
  cid = cidifyString(stringifyCid(cid))

  const codec = codecCodes[cid.code]
  if (!codec) throw unsupportedCodecError()

  const bytes = await ipfs.block.get(cid, { timeout: options.timeout })
  const block = await Block.decode({ bytes, codec, hasher })

  if (block.cid.code === dagPb.code) {
    return JSON.parse(new TextDecoder().decode((block.value as any).Data))
  }
  if (block.cid.code === dagCbor.code) {
    const value = block.value
    const links = options.links || []
    links.forEach((prop) => {
      if (value[prop]) {
        value[prop] = stringifyCid(value[prop], options)
      }
    })
    return value
  }
}

async function write(ipfs, format, value, options: { base?: any, pin?: boolean, timeout?: number, format?: string, links?: string[] } = {}) {
  if (options.format === 'dag-pb') format = options.format
  const codec = codecMap[format]
  if (!codec) throw unsupportedCodecError()

  if (codec.code === dagPb.code) {
    value = typeof value === 'string' ? value : JSON.stringify(value)
    value = { Data: new TextEncoder().encode(value), Links: [] }
  }
  if (codec.code === dagCbor.code) {
    const links = options.links || []
    links.forEach((prop) => {
      if (value[prop]) {
        value[prop] = cidifyString(value[prop])
      }
    })
  }

  const block = await Block.encode({ value, codec, hasher })
  await ipfs.block.put(block.bytes, {
    cid: block.cid.bytes,
    version: block.cid.version,
    format,
    mhtype,
    pin: options.pin,
    timeout: options.timeout
  })

  const cid = codec.code === dagPb.code
    ? block.cid.toV0()
    : block.cid
  return cid.toString(options.base || defaultBase)
}

export default {
  read,
  write
}
