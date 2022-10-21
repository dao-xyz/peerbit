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

export type ProgramInitializationOptions = { store: IInitializationOptions<any>, parent?: Program, onClose?: () => void, onDrop?: () => void };

const checkClazzesCompatible = (clazzA: Constructor<any>, clazzB: Constructor<any>) => {
    return clazzA == clazzB || clazzA.isPrototypeOf(clazzB) || clazzB.isPrototypeOf(clazzA)
}

export interface RootProgram {
    setup(option?: any): Promise<void>; // Root program should support an empty constructor setup function
}

@variant(1)
export class Program extends SystemBinaryPayload implements Addressable, Saveable {

    @field({ type: 'string' })
    name: string;

    address: Address;
    _ipfs: IPFS;
    _identity: Identity;
    _encryption?: PublicKeyEncryptionResolver
    _onClose?: () => void;
    _onDrop?: () => void;
    _initialized = false;

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

    get initialized() {
        return this._initialized
    }
    async init(ipfs: IPFS, identity: Identity, options: ProgramInitializationOptions): Promise<this> {

        if (this.initialized) {
            throw new Error("Already initialized")
        }

        if (!options.parent) {
            await (this as any as RootProgram).setup(); // call setup on the root program
        }


        this._ipfs = ipfs;
        this._identity = identity;
        this._encryption = options.store.encryption;
        this._onClose = options.onClose;
        this._onDrop = options.onDrop;
        await this.save(ipfs)
        await Promise.all(this.stores.map(store => store.init(ipfs, identity, options.store)));
        const nexts = this.programs;
        for (const next of nexts) {
            await next.init(ipfs, identity, { ...options, parent: this });
        }

        this.allStores; // call this to ensure no store duplicates exist and all addresses are available
        this._initialized = true
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

    async close(): Promise<void> {
        if (!this.initialized) {
            return;
        }
        await Promise.all(this.stores.map(s => s.close()))
        const nexts = this.programs;
        await Promise.all(this.programs.map(p => p.close()))
        this._onClose && this._onClose();
    }

    async drop(): Promise<void> {
        if (!this.initialized) {
            return;
        }
        await Promise.all(this.stores.map(s => s.drop()))
        await Promise.all(this.programs.map(p => p.drop()))
        this._initialized = false;
        this._onDrop && this._onDrop();

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

    _allStores: Map<string, Store<any>>
    get allStores(): Map<string, Store<any>> {
        if (this._allStores) {
            return this._allStores;
        }
        const map = new Map<string, Store<any>>();
        this.stores.map(s => map.set(s.address.toString(), s));
        const nexts = this.programs;
        for (const next of nexts) {
            const submap = next.allStores;
            submap.forEach((store, address) => {
                if (map.has(address)) {
                    throw new Error("Store duplicates detected")
                }
                map.set(address, store);
            })
        }
        this._allStores = map;
        return this._allStores;
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
