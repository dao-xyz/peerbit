import { Constructor, field, getSchemasBottomUp, variant } from "@dao-xyz/borsh";
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

export type ProgramInitializationOptions = { store: IInitializationOptions<any> };

@variant(1)
export class Program extends SystemBinaryPayload implements Addressable, Saveable {

    @field({ type: 'string' })
    name: string;

    address: Address;
    _ipfs: IPFS;
    _identity: Identity;

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

    async init(ipfs: IPFS, identity: Identity, options: ProgramInitializationOptions): Promise<this> {
        this._ipfs = ipfs;
        this._identity = identity;
        await this.save(ipfs)
        return this;
    }


    _getFieldsWithType<T>(type: Constructor<T>): T[] {
        const schemas = getSchemasBottomUp(this.constructor);
        const fields: string[] = [];
        for (const schema of schemas) {
            for (const field of schema.schema.fields) {
                if (field.type === type) {
                    fields.push(field.key);
                }
            }
        }
        const things = fields.map(field => this[field as keyof Program] as any as T) as T[]
        return things;
    }

    get stores(): Store<any>[] {
        return this._getFieldsWithType(Store)
    }
    get programs(): Program[] {
        return this._getFieldsWithType(Program)
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
