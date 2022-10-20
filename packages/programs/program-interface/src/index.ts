import { Constructor, field, getSchemasBottomUp, variant } from "@dao-xyz/borsh";
import { SystemBinaryPayload } from "@dao-xyz/bpayload";
import { Identity } from "@dao-xyz/ipfs-log";
import { IPFS } from "ipfs-core-types";
import { IInitializationOptions, Store, Initiable, Address, Addressable, Saveable, save, load } from '@dao-xyz/peerbit-dstore';

// @ts-ignore
import { v4 as uuid } from 'uuid';
import { PublicKeyEncryptionResolver } from "@dao-xyz/peerbit-crypto";

export const checkStoreName = (name: string) => {
    if (name.indexOf("/") !== -1) {
        throw new Error("Name contain '/' which is not allowed since this character used for path separation")
    }
}

export type ProgramInitializationOptions = { store: IInitializationOptions<any>, parent?: Program };

const checkClazzesCompatible = (clazzA: Constructor<any>, clazzB: Constructor<any>) => {
    return clazzA == clazzB || clazzA.isPrototypeOf(clazzB) || clazzB.isPrototypeOf(clazzA)
}

export interface RootProgram {
    start(): Promise<void>;
}

@variant(1)
export class Program extends SystemBinaryPayload implements Addressable, Saveable {

    @field({ type: 'string' })
    name: string;

    address: Address;
    _ipfs: IPFS;
    _identity: Identity;
    _encryption?: PublicKeyEncryptionResolver

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
        this._encryption = options.store.encryption;
        await this.save(ipfs)
        await Promise.all(this.stores.map(store => store.init(ipfs, identity, options.store)));
        const nexts = this.programs;
        for (const next of nexts) {
            await next.init(ipfs, identity, { ...options, parent: this });
        }

        if (!options.parent) {
            await (this as any as RootProgram).start(); // call setup on the root program
        }
        return this;
    }

    _getFieldsWithType<T>(type: Constructor<T>): T[] {
        const schemas = getSchemasBottomUp(this.constructor);
        const fields: string[] = [];

        for (const schema of schemas) {
            for (const field of schema.schema.fields) {
                if (checkClazzesCompatible(field.type as Constructor<any>, type)) {
                    fields.push(field.key);
                }
            }
        }
        const things = fields.map(field => this[field as keyof Program] as any as T) as T[]
        return things;
    }

    get ipfs(): IPFS {
        return this._ipfs;
    }

    get identity(): Identity {
        return this._identity;
    }

    get encryption(): PublicKeyEncryptionResolver | undefined {
        return this._encryption;
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
