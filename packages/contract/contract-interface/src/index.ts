import { field, variant } from "@dao-xyz/borsh";
import { SystemBinaryPayload } from "@dao-xyz/bpayload";
import { Identity } from "@dao-xyz/ipfs-log";
import { Address, Addressable, IInitializationOptions, Initiable, load, save, StoreLike } from "@dao-xyz/peerbit-dstore";
import { IPFS } from "ipfs-core-types";

// @ts-ignore
import { v4 as uuid } from 'uuid';

@variant(1)
export class Contract extends SystemBinaryPayload implements Initiable<any>, Addressable {

    @field({ type: 'string' })
    name: string;

    address: Address;

    constructor(properties?: { name?: string }) {
        super();
        this.name = properties?.name || uuid();

    }

    async init(ipfs: IPFS, _identity: Identity, options: IInitializationOptions<any>): Promise<this> {
        const saveOrResolved = await options.saveOrResolve(ipfs, this);
        if (saveOrResolved !== this) {
            return saveOrResolved as this;
        }
        return this;
    }

    get stores(): StoreLike<any>[] {
        throw new Error("Not implemented")
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

    static load<S extends Contract>(ipfs: IPFS, address: Address, options?: {
        timeout?: number;
    }): Promise<S> {
        return load(ipfs, address, Contract, options) as Promise<S>
    }
}
