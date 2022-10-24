import { AbstractType, Constructor, field, getSchemasBottomUp, option, variant } from "@dao-xyz/borsh";
import { SystemBinaryPayload } from "@dao-xyz/peerbit-bpayload";
import { CanAppend, Entry, Identity, Payload } from "@dao-xyz/ipfs-log";
import { SignKey } from "@dao-xyz/peerbit-crypto";

import { IPFS } from "ipfs-core-types";
import { IInitializationOptions, Store, Address, Addressable, Saveable, save, load } from '@dao-xyz/peerbit-store';

// @ts-ignore
import { v4 as uuid } from 'uuid';
import { PublicKeyEncryptionResolver } from "@dao-xyz/peerbit-crypto";

export type ProgramInitializationOptions = { store: IInitializationOptions<any>, parent?: AbstractProgram, onClose?: () => void, onDrop?: () => void };

const checkClazzesCompatible = (clazzA: Constructor<any> | AbstractType<any>, clazzB: Constructor<any> | AbstractType<any>) => {
    return clazzA == clazzB || clazzA.isPrototypeOf(clazzB) || clazzB.isPrototypeOf(clazzA)
}


@variant(0)
export class ProgramOwner {

    @field({ type: Address })
    address: Address

    @field({ type: option(Address) })
    subProgramAddress?: Address // maybe remove since it is not used actively

    constructor(properties?: {
        address: Address,
        subProgramAddress?: Address
    }) {
        if (properties) {
            this.address = properties.address;
            this.subProgramAddress = properties.subProgramAddress;
        }
    }
}

@variant(1)
export abstract class AbstractProgram extends SystemBinaryPayload implements Addressable, Saveable {



    @field({ type: 'string' })
    name: string;

    @field({ type: option(ProgramOwner) })
    programOwner?: ProgramOwner // Will control whether this program can be opened or not

    address: Address;


    _ipfs: IPFS;
    _identity: Identity;
    _encryption?: PublicKeyEncryptionResolver
    _onClose?: () => void;
    _onDrop?: () => void;
    _initialized = false;
    parentProgram: Program


    constructor(properties?: { name?: string }) {
        super();
        if (properties) {
            this.name = (properties.name || uuid());
        }
        else {
            this.name = uuid()
        }
    }

    get initialized() {
        return this._initialized
    }
    async init(ipfs: IPFS, identity: Identity, options: ProgramInitializationOptions): Promise<this> {


        if (this.initialized) {
            throw new Error("Already initialized")
        }

        this._ipfs = ipfs;
        this._identity = identity;
        this._encryption = options.store.encryption;
        this._onClose = options.onClose;
        this._onDrop = options.onDrop;

        const existingAddress = this.address;
        await this.save(ipfs)
        if (existingAddress && !existingAddress.equals(this.address)) {
            throw new Error("Program properties has been changed after constructor so that the hash has changed. Make sure that the 'setup(...)' function does not modify any properties that are to be serialized")
        }

        await Promise.all(this.stores.map(store => store.init(ipfs, identity, options.store)));
        const nexts = this.programs;
        for (const next of nexts) {
            await next.init(ipfs, identity, { ...options, parent: this });
        }

        this.allStoresMap; // call this to ensure no store duplicates exist and all addresses are available
        this._initialized = true
        return this;
    }

    _getFieldsWithType<T>(type: Constructor<T> | AbstractType<T>): T[] {
        const schemas = getSchemasBottomUp(this.constructor);
        const fields: string[] = [];

        for (const schema of schemas) {
            for (const field of schema.schema.fields) {
                if (checkClazzesCompatible(field.type as Constructor<any>, type)) {
                    fields.push(field.key);
                }
            }
        }
        const things = fields.map(field => this[field as keyof AbstractProgram] as any as T) as T[]
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



    _allStoresMap: Map<string, Store<any>>
    get allStoresMap(): Map<string, Store<any>> {
        if (this._allStoresMap) {
            return this._allStoresMap;
        }
        const map = new Map<string, Store<any>>();
        this.stores.map(s => map.set(s.address.toString(), s));
        const nexts = this.programs;
        for (const next of nexts) {
            const submap = next.allStoresMap;
            submap.forEach((store, address) => {
                if (map.has(address)) {
                    throw new Error("Store duplicates detected")
                }
                map.set(address, store);
            })
        }
        this._allStoresMap = map;
        return this._allStoresMap;
    }

    _allPrograms: AbstractProgram[]
    get allPrograms(): AbstractProgram[] {
        if (this._allPrograms) {
            return this._allPrograms;
        }
        const arr: AbstractProgram[] = this.programs;
        const nexts = this.programs;
        for (const next of nexts) {
            arr.push(...next.allPrograms)
        }
        this._allPrograms = arr;
        return this._allPrograms;
    }

    _allProgramsMap: Map<string, AbstractProgram>
    get allProgramsMap(): Map<string, AbstractProgram> {
        if (this._allProgramsMap) {
            return this._allProgramsMap;
        }
        const map = new Map<string, AbstractProgram>();
        this.programs.map(s => map.set(s.address.toString(), s));
        const nexts = this.programs;
        for (const next of nexts) {
            const submap = next.allProgramsMap;
            submap.forEach((program, address) => {
                if (map.has(address)) {
                    throw new Error("Store duplicates detected")
                }
                map.set(address, program);
            })
        }
        this._allProgramsMap = map;
        return this._allProgramsMap;
    }

    get programs(): AbstractProgram[] {
        return this._getFieldsWithType(AbstractProgram)
    }

    async save(ipfs: IPFS, options?: {
        format?: string;
        pin?: boolean;
        timeout?: number;
    }): Promise<Address> {

        // post setup
        // set parents of subprograms to this 
        for (const [i, program] of this.allPrograms.entries()) {
            if (program instanceof ComposableProgram) {
                program.index = i;
            }
            program.parentProgram = this.parentProgram || this;
        }

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

export interface CanOpenSubPrograms {
    canOpen(programToOpen: Program, fromEntry: Entry<any>): Promise<boolean>
}


@variant(0)
export abstract class Program extends AbstractProgram {
    abstract setup(): Promise<void>
    async init(ipfs: IPFS<{}>, identity: Identity, options: ProgramInitializationOptions): Promise<this> {

        await this.setup();
        return super.init(ipfs, identity, options);
    }
}

/**
 * Building block, but not something you use as a standalone
 */
@variant(1)
export abstract class ComposableProgram extends AbstractProgram {


    @field({ type: 'u32' })
    index: number = 0; // Prevent duplicates for subprograms

}