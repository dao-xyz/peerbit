import { field, variant } from "@dao-xyz/borsh";
import { SystemBinaryPayload } from "@dao-xyz/bpayload";
import { Identity } from "@dao-xyz/ipfs-log";
import { IPFS } from "ipfs-core-types";
import { IInitializationOptions, Store, Initiable, Address, Addressable, Saveable, save, load } from '@dao-xyz/peerbit-dstore';

// @ts-ignore
import { v4 as uuid } from 'uuid';

export const checkStoreName = (name: string) => {
    if (name.indexOf("/") !== -1) {
        throw new Error("Name contain '/' which is not allowed since this character used for path separation")
    }
}

export type ProgramInitializationOptions = { saveOrResolve: (ipfs: IPFS, store: Saveable) => Promise<Saveable> };


@variant(1)
export class Program extends SystemBinaryPayload implements Initiable<any>, Addressable, Saveable {

    @field({ type: 'string' })
    name: string;

    address: Address;

    constructor(properties?: { name?: string, parent?: Addressable }) {
        super();
        if (properties) {
            this.name = (properties.parent?.name ? (properties.parent?.name + '/') : '') + (properties.name || uuid());

        }
        else {
            this.name = uuid()
        }
        checkStoreName(this.name);
    }

    async init(ipfs: IPFS, _identity: Identity, options: ProgramInitializationOptions | IInitializationOptions<any>): Promise<this> {
        const saveOrResolved = await options.saveOrResolve(ipfs, this);
        if (saveOrResolved !== this) {
            return saveOrResolved as this;
        }
        return this;
    }

    async save(ipfs: IPFS, options?: {
        format?: string;
        pin?: boolean;
        timeout?: number;
    }): Promise<Address> {
        const address = await save(ipfs, this, options)
        this.address = address;
        return address;
    }

    static load<S extends Program>(ipfs: IPFS, address: Address, options?: {
        timeout?: number;
    }): Promise<S> {
        return load(ipfs, address, Program, options) as Promise<S>
    }
}
