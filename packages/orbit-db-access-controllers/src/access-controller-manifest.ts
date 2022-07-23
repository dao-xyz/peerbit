import io from '@dao-xyz/orbit-db-io'

export class AccessControllerManifest {
  type: string;
  params: any;
  constructor(type, params = {}) {
    this.type = type
    this.params = params
  }

  static async resolve(ipfs, manifestHash, options: { skipManifest?: boolean, type?: string } = {}) {
    if (options.skipManifest) {
      if (!options.type) {
        throw new Error('No manifest, access-controller type required')
      }
      return new AccessControllerManifest(options.type, { address: manifestHash })
    } else {
      // TODO: ensure this is a valid multihash
      if (manifestHash.indexOf('/ipfs') === 0) { manifestHash = manifestHash.split('/')[2] }
      const { type, params } = await io.read(ipfs, manifestHash)
      return new AccessControllerManifest(type, params)
    }
  }

  static async create(ipfs, type, params) {
    if (params.skipManifest) {
      return params.address
    }
    const manifest = {
      type: type,
      params: params
    }
    return io.write(ipfs, 'dag-cbor', manifest)
  }
}