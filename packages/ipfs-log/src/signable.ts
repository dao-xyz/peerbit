
import { LamportClock as Clock } from './lamport-clock'
import { isDefined } from './is-defined'
import * as io from 'orbit-db-io'
import stringify from 'json-stringify-deterministic'
import { IPFS } from 'ipfs-core-types/src/'
import Identities, { Identity, IdentityAsJson } from 'orbit-db-identity-provider'
import { serialize, deserialize, variant, field, vec } from '@dao-xyz/borsh';
const IpfsNotDefinedError = () => new Error('Ipfs instance not defined')

/*
 * @description
 * An ipfs-log entry
 */




@variant(0) // version
export class Entry {

    @field({ type: 'String' })
    id: string // For determining a unique chain

    @field({ type: vec('u8') })
    payload: Uint8Array

    @field({ type: vec('String') })
    next: string[] // Array of hashes

    @field({ type: vec('String') })
    refs?: string[]

    @field({ type: Clock })
    clock: Clock

    sig: string;
    key: string;
    hash: string // "zd...Foo", we'll set the hash after persisting the entry
    identity: IdentityAsJson;

    constructor(options: {
        id: string // For determining a unique chain
        payload: Uint8Array
        next: string[] // Array of hashes
        refs?: string[]
        clock: Clock
    }) {
        if (options) {
            Object.assign(this, options);
        }
    }

    static IPLD_LINKS = ['next', 'refs']


    /**
     * Create an Entry
     * @param {IPFS} ipfs An IPFS instance
     * @param {Identity} identity The identity instance
     * @param {string} logId The unique identifier for this log
     * @param {*} data Data of the entry to be added. Can be any JSON.stringifyable data
     * @param {Array<string|Entry>} [next=[]] Parent hashes or entries
     * @param {LamportClock} [clock] The lamport clock
     * @returns {Promise<Entry>}
     * @example
     * const entry = await Entry.create(ipfs, identity, 'hello')
     * console.log(entry)
     * // { hash: null, payload: "hello", next: [] }
     */
    static async create(ipfs: IPFS, identity: Identity, logId: string, data: Uint8Array | string, next: (Entry | string)[] = [], clock?: Clock, refs: string[] = [], pin?: boolean) {
        if (!isDefined(ipfs)) throw IpfsNotDefinedError()
        if (!isDefined(identity)) throw new Error('Identity is required, cannot create entry')
        if (!isDefined(logId)) throw new Error('Entry requires an id')
        if (!isDefined(data)) throw new Error('Entry requires data')
        if (!isDefined(next) || !Array.isArray(next)) throw new Error("'next' argument is not an array")

        // Clean the next objects and convert to hashes
        const toEntry = (e) => e.hash ? e.hash : e
        const nexts = next.filter(isDefined).map(toEntry)

        if (typeof data == 'string') {
            data = Uint8Array.from(Buffer.from(data));
        }

        const entry: Entry = new Entry({
            id: logId, // For determining a unique chain
            payload: data, // Can be any JSON.stringifyable data
            next: nexts, // Array of hashes
            refs: refs,
            clock: clock || new Clock({ id: identity.publicKey })
        })

        const signature = await identity.provider.sign(identity, Entry.toBuffer(entry))

        entry.key = identity.publicKey;
        entry.identity = identity.toJSON();
        entry.sig = signature;
        entry.hash = await Entry.toMultihash(ipfs, entry, pin);

        return entry
    }

    /**
     * Verifies an entry signature.
     *
     * @param {IdentityProvider} identityProvider The identity provider to use
     * @param {Entry} entry The entry being verified
     * @return {Promise} A promise that resolves to a boolean value indicating if the signature is valid
     */
    static async verify(identityProvider: Identities, signedEntry: Entry) {
        if (!identityProvider) throw new Error('Identity-provider is required, cannot verify entry')
        /*         if (!Entry.isEntry(entry)) throw new Error('Invalid Log entry')
         */
        if (!signedEntry.key) throw new Error("Entry doesn't have a key")
        if (!signedEntry.sig) throw new Error("Entry doesn't have a signature")

        /*      const e = Entry.toEntry(signedEntry.entry, { presigned: true }) */
        return identityProvider.verify(signedEntry.sig, signedEntry.key, Entry.toBuffer(signedEntry), 'v1')
    }

    /**
     * Transforms an entry into a Buffer.
     * @param {Entry} entry The entry
     * @return {Buffer} The buffer
     */
    static toBuffer(entry: Entry) {
        /*  const stringifiedEntry = entry.v === 0 ? JSON.stringify(entry) : stringify(entry)
         return Buffer.from(stringifiedEntry) */
        return Buffer.from(serialize(entry))
    }

    /**
     * Get the multihash of an Entry.
     * @param {IPFS} ipfs An IPFS instance
     * @param {Entry} entry Entry to get a multihash for
     * @returns {Promise<string>}
     * @example
     * const multihash = await Entry.toMultihash(ipfs, entry)
     * console.log(multihash)
     * // "Qm...Foo"
     * @deprecated
     */
    static async toMultihash(ipfs: IPFS, entry: Entry, pin = false) {
        if (!ipfs) throw IpfsNotDefinedError()

        // // Ensure `entry` follows the correct format
        const e = Entry.toEntry(entry)

        return io.write(ipfs, 'dag-cbor', e, { links: Entry.IPLD_LINKS, pin })
    }

    static toEntry(entry: Entry, { presigned = false, includeHash = false } = {}): Entry {
        const e = new Entry({
            id: entry.id,
            payload: entry.payload,
            next: entry.next,
            clock: new Clock({ id: entry.clock.id, time: entry.clock.time })
        });
        if (includeHash)
            e.hash = entry.hash;

        /*  const v = entry["v"]
         if (v > 1) { */
        e.refs = entry.refs // added in v2
        /*  }
         e.v = entry.v */

        if (presigned) {
            return e // don't include key/sig information
        }

        e.key = entry.key
        /*  if (v > 0) { */
        e.identity = entry.identity // added in v1
        /*   } */
        e.sig = entry.sig
        return e
    }

    /**
     * Create an Entry from a hash.
     * @param {IPFS} ipfs An IPFS instance
     * @param {string} hash The hash to create an Entry from
     * @returns {Promise<Entry>}
     * @example
     * const entry = await Entry.fromMultihash(ipfs, "zd...Foo")
     * console.log(entry)
     * // { hash: "Zd...Foo", payload: "hello", next: [] }
     */
    static async fromMultihash(ipfs, hash) {
        if (!ipfs) throw IpfsNotDefinedError()
        if (!hash) throw new Error(`Invalid hash: ${hash}`)
        const e = await io.read(ipfs, hash, { links: Entry.IPLD_LINKS })

        const entry = Entry.toEntry(e)
        entry.hash = hash
        return entry;
    }

    /**
     * Check if an object is an Entry.
     * @param {Entry} obj
     * @returns {boolean}
     */
    /*    static isEntry(obj) {
           return obj && obj.id !== undefined &&
               obj.next !== undefined &&
               obj.payload !== undefined &&
               obj.v !== undefined &&
               obj.hash !== undefined &&
               obj.clock !== undefined &&
               (obj.refs !== undefined || obj.v < 2) // 'refs' added in v2
       } */

    static isEntry(obj: Entry) {
        return true;
    }

    /**
     * Compares two entries.
     * @param {Entry} a
     * @param {Entry} b
     * @returns {number} 1 if a is greater, -1 is b is greater
     */
    static compare(a, b) {
        const distance = Clock.compare(a.clock, b.clock)
        if (distance === 0) return a.clock.id < b.clock.id ? -1 : 1
        return distance
    }

    /**
     * Check if an entry equals another entry.
     * @param {Entry} a
     * @param {Entry} b
     * @returns {boolean}
     */
    static isEqual(a: Entry, b: Entry) {
        return a.hash === b.hash
    }




    /**
    * Check if an entry is a parent to another entry.
    * @param {Entry} entry1 Entry to check
    * @param {Entry} entry2 The parent Entry
    * @returns {boolean}
    */
    static isParent(entry1, entry2) {
        return entry2.next.indexOf(entry1.hash) > -1
    }

    /**
     * Find entry's children from an Array of entries.
     * Returns entry's children as an Array up to the last know child.
     * @param {Entry} entry Entry for which to find the parents
     * @param {Array<Entry>} values Entries to search parents from
     * @returns {Array<Entry>}
     */
    static findChildren(entry: Entry, values: Entry[]) {
        let stack: Entry[] = []
        let parent = values.find((e) => Entry.isParent(entry, e))
        let prev = entry
        while (parent) {
            stack.push(parent)
            prev = parent
            parent = values.find((e) => Entry.isParent(prev, e))
        }
        stack = stack.sort((a, b) => Clock.compare(a.clock, b.clock))
        return stack
    }


}

