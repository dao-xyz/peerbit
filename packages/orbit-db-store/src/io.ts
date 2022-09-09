

import path from 'path'
import io from '@dao-xyz/orbit-db-io';
import { CID } from 'multiformats/cid'
import { serialize, deserialize, Constructor } from '@dao-xyz/borsh';
import { IPFS } from 'ipfs-core-types/src/'

const notEmpty = e => e !== '' && e !== ' '

export interface Manifest {
    data: Uint8Array
}

export interface Addressable { get name(): string }

export class Address {
    root: string;
    path: string;
    constructor(root: string, path: string) {
        this.root = root
        this.path = path
    }

    toString() {
        return Address.join(this.root, this.path)
    }

    static isValid(oaddress: { toString(): string }) {
        const address = oaddress.toString().replace(/\\/g, '/')

        const containsProtocolPrefix = (e, i) => !((i === 0 || i === 1) && address.toString().indexOf('/orbit') === 0 && e === 'orbitdb')

        const parts = address.toString()
            .split('/')
            .filter(containsProtocolPrefix)
            .filter(notEmpty)

        let accessControllerHash

        const validateHash = (hash) => {
            const prefixes = ['zd', 'Qm', 'ba', 'k5']
            for (const p of prefixes) {
                if (hash.indexOf(p) > -1) {
                    return true
                }
            }
            return false
        }

        try {
            accessControllerHash = validateHash(parts[0])
                ? CID.parse(parts[0]).toString()
                : null
        } catch (e) {
            return false
        }

        return accessControllerHash !== null
    }

    static parse(oaddress: { toString(): string }) {
        if (!oaddress) { throw new Error(`Not a valid OrbitDB address: ${oaddress}`) }

        if (!Address.isValid(oaddress)) { throw new Error(`Not a valid OrbitDB address: ${oaddress}`) }

        const address = oaddress.toString().replace(/\\/g, '/')

        const parts = address.toString()
            .split('/')
            .filter((e, i) => !((i === 0 || i === 1) && address.toString().indexOf('/orbit') === 0 && e === 'orbitdb'))
            .filter(e => e !== '' && e !== ' ')

        return new Address(parts[0], parts.slice(1, parts.length).join('/'))
    }

    static join(...paths) {
        return (path.posix || path).join('/orbitdb', ...paths)
    }
}


export const save = async (ipfs: IPFS, thing: Addressable, options: { format?: string, pin?: boolean, timeout?: number } = {}): Promise<Address> => {
    const manifest: Manifest = {
        data: serialize(thing)
    }
    const hash = await io.write(ipfs, options.format || 'dag-cbor', manifest, options)
    return Address.parse(Address.join(hash, thing.name))
}


export const load = async <S extends Addressable & { address: Address }>(ipfs, address: Address, into: Constructor<S>, options: { timeout?: number } = {}): Promise<S> => {
    let hash = address.root
    const manifest: Manifest = await io.read(ipfs, hash, options);
    const der = deserialize(Buffer.from(manifest.data), into)
    der.address = Address.parse(Address.join(hash, der.name))
    return der;
}
