import { AccessControllerManifest } from './access-controller-manifest'
import { IPFSAccessController } from './ipfs-access-controller'
import { OrbitDBAccessController } from './orbitdb-access-controller'
import { AccessController } from './access-controller-interface'
/* 
const supportedTypes = {
  ipfs: IPFSAccessController,
  orbitdb: OrbitDBAccessController
}

const getHandlerFor = (type) => {
  if (!AccessControllers.isSupported(type)) {
    throw new Error(`AccessController type '${type}' is not supported`)
  }
  return supportedTypes[type]
}

export class AccessControllers {
  static get AccessController() { return AccessController }

  static isSupported(type) {
    return Object.keys(supportedTypes).includes(type)
  }

  static addAccessController(options) {
    if (!options.AccessController) {
      throw new Error('AccessController class needs to be given as an option')
    }

    if (!options.AccessController.type ||
      typeof options.AccessController.type !== 'string') {
      throw new Error('Given AccessController class needs to implement: static get type() {}.')
    }

    supportedTypes[options.AccessController.type] = options.AccessController
  }

  static addAccessControllers(options) {
    const accessControllers = options.AccessControllers
    if (!accessControllers) {
      throw new Error('AccessController classes need to be given as an option')
    }

    accessControllers.forEach((accessController) => {
      AccessControllers.addAccessController({ AccessController: accessController })
    })
  }

  static removeAccessController(type) {
    delete supportedTypes[type]
  }

  static async resolve(orbitdb, manifestAddress, options = {}) {
    const { type, params } = await AccessControllerManifest.resolve(orbitdb._ipfs, manifestAddress, options)
    const AccessController = getHandlerFor(type)
    const accessController = await AccessController.create(orbitdb, Object.assign({}, options, params))
    await accessController.load(params.address)
    return accessController
  }

  static async create(orbitdb, type, options = {}) {
    const AccessController = getHandlerFor(type)
    const ac = await AccessController.create(orbitdb, options)
    const params = await ac.save()
    const hash = await AccessControllerManifest.create(orbitdb._ipfs, type, params)
    return hash
  }
}
 */