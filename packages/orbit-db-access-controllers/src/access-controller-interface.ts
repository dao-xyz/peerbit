const EventEmitter = require('events').EventEmitter
import { Payload } from '@dao-xyz/ipfs-log-entry';
import { IdentitySerializable } from '@dao-xyz/orbit-db-identity-provider';

/**
 * Interface for OrbitDB Access Controllers
 *
 * Any OrbitDB access controller needs to define and implement
 * the methods defined by the interface here.
 */
export abstract class AccessController<T> extends EventEmitter {
  /*
    Every AC needs to have a 'Factory' method
    that creates an instance of the AccessController
  */
  static async create<T>(orbitdb, options): Promise<AccessController<T>> {
    throw new Error("Not implemented")

  }

  /* Return the type for this controller */
  static get type(): string {
    throw new Error('\'static get type ()\' needs to be defined in the inheriting class')
  }

  /*
    Return the type for this controller
    NOTE! This is the only property of the interface that
    shouldn't be overridden in the inherited Access Controller
  */
  get type() {
    return this.constructor["type"]
  }

  /* Each Access Controller has some address to anchor to */
  get address(): string {
    throw new Error("Not implemented")
  }

  /*
    Called by the databases (the log) to see if entry should
    be allowed in the database. Return true if the entry is allowed,
    false is not allowed
  */
  async canAppend(payload: Payload<T>, identity: IdentitySerializable, identityProvider): Promise<boolean> {
    throw new Error("Not implemented")
  }

  async canAccessKeys?(identity: { type: string, key: Uint8Array }): Promise<boolean> {
    throw new Error("Not implemented")
  }

  /* Add and remove access */
  async grant(access, identity): Promise<any> { return false }
  async revoke(access, identity): Promise<any> { return false }

  /* AC creation and loading */
  async load(address) { }
  /* Returns AC manifest parameters object */
  async save(options?: any): Promise<{
    address: string,
    skipManifest?: boolean
  }> {
    throw new Error("Not implemented")
  }
  /* Called when the database for this AC gets closed */
  async close() { }
}
