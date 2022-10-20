

import path from 'path'
import io from '@dao-xyz/io-utils';
import { CID } from 'multiformats/cid'
import { serialize, deserialize, Constructor } from '@dao-xyz/borsh';
import { IPFS } from 'ipfs-core-types'
import { variant, field } from '@dao-xyz/borsh';

const notEmpty = (e: string) => e !== '' && e !== ' '

export interface Manifest {
    data: Uint8Array
}

export interface Addressable { get name(): string, get address(): Address | undefined }

@variant(0)
export class Address {

    @field({ type: 'string' })
    cid: string

    constructor(cid: string) {
        this.cid = cid
    }

    toString() {
        return Address.join(this.cid)
    }

    equals(other: Address) {
        return this.cid === other.cid;
    }

    static isValid(oaddress: { toString(): string }) {
        const address = oaddress.toString().replace(/\\/g, '/')

        const containsProtocolPrefix = (e: string, i: number) => !((i === 0 || i === 1) && address.toString().indexOf('/peerbit') === 0 && e === 'peerbit')

        const parts = address.toString()
            .split('/')
            .filter(containsProtocolPrefix)
            .filter(notEmpty)

        let accessControllerHash

        const validateHash = (hash: string) => {
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
        if (!oaddress) { throw new Error(`Not a valid Peerbit address: ${oaddress}`) }

        if (!Address.isValid(oaddress)) { throw new Error(`Not a valid Peerbit address: ${oaddress}`) }

        const address = oaddress.toString().replace(/\\/g, '/')

        const parts = address.toString()
            .split('/')
            .filter((e, i) => !((i === 0 || i === 1) && address.toString().indexOf('/peerbit') === 0 && e === 'peerbit'))
            .filter(e => e !== '' && e !== ' ')

        return new Address(parts[0])
    }

    static join(cid: string) {
        return (path.posix || path).join('/peerbit', cid)
    }
}


export const save = async (ipfs: IPFS, thing: Addressable, options: { format?: string, pin?: boolean, timeout?: number } = {}): Promise<Address> => {
    const manifest: Manifest = {
        data: serialize(thing)
    }
    const hash = await io.write(ipfs, options.format || 'dag-cbor', manifest, options)
    return Address.parse(Address.join(hash))
}


export const load = async <S extends Addressable & { address: Address }>(ipfs: IPFS, address: Address, into: Constructor<S>, options: { timeout?: number } = {}): Promise<S> => {
    const manifest: Manifest = await io.read(ipfs, address.cid, options);
    const der = deserialize(manifest.data, into)
    der.address = Address.parse(Address.join(address.cid))
    return der;
}
