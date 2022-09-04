// TODO extend IPFS-LOG access controller interface for canAppend method

import { MaybeEncrypted } from "@dao-xyz/encryption-utils"
import { CanAppendAccessController } from "@dao-xyz/ipfs-log"
import { Payload } from "@dao-xyz/ipfs-log-entry"
import { Identities, IdentitySerializable } from "@dao-xyz/orbit-db-identity-provider"
import EventEmitter from "events"

/**
 * Interface for OrbitDB Access Controllers
 *
 * Any OrbitDB access controller needs to define and implement
 * the methods defined by the interface here.
 */
export abstract class AccessController<T> extends EventEmitter implements CanAppendAccessController<T> {
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

    async canAppend(payload: MaybeEncrypted<Payload<T>>, identity: MaybeEncrypted<IdentitySerializable>, identityProvider: Identities): Promise<boolean> {
        throw new Error("Not implemented")
    }

    async canRead?(payload: MaybeEncrypted<Payload<T>>, identity: MaybeEncrypted<IdentitySerializable>, identityProvider: Identities): Promise<boolean>;
    async canAccessKeys?(identity: { type: string, key: Uint8Array }): Promise<boolean>;
    async close?(): Promise<void>;
    async save?(): Promise<{ address: string }>
}
