import { AbstractType, Constructor, field, getSchemasBottomUp, option, variant, WrappedType } from "@dao-xyz/borsh";
import { SystemBinaryPayload } from "@dao-xyz/peerbit-bpayload";
import { Entry, Identity } from "@dao-xyz/ipfs-log";

import { IPFS } from "ipfs-core-types";
import { IInitializationOptions, Store, Address, Addressable, Saveable, save, load } from '@dao-xyz/peerbit-store';

// @ts-ignore
import { v4 as uuid } from 'uuid';
import { PublicKeyEncryptionResolver } from "@dao-xyz/peerbit-crypto";
import { getValuesWithType } from './utils.js';

export type ProgramInitializationOptions = { store: IInitializationOptions<any>, parent?: AbstractProgram, onClose?: () => void, onDrop?: () => void };

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
    id: string;

    @field({ type: option(ProgramOwner) })
    programOwner?: ProgramOwner // Will control whether this program can be opened or not

    address: Address;


    _ipfs: IPFS;
    _identity: Identity;
    _encryption?: PublicKeyEncryptionResolver
    _onClose?: () => void;
    _onDrop?: () => void;
    _initialized = false;
    _initializationPromise: Promise<void>
    parentProgram: Program


    constructor(properties?: { id?: string }) {
        super();
        if (properties) {
            this.id = (properties.id || uuid());
        }
        else {
            this.id = uuid()
        }
    }

    get initialized() {
        return this._initialized
    }
    get initializationPromise(): Promise<void> | undefined {
        return this._initializationPromise
    }


    async init(ipfs: IPFS, identity: Identity, options: ProgramInitializationOptions): Promise<this> {

        if (this.initialized) {
            throw new Error("Already initialized")
        }

        this._initializationPromise = new Promise<void>(async (resolve, reject) => {
            try {
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

                const nexts = this.programs;
                for (const next of nexts) {
                    await next.init(ipfs, identity, { ...options, parent: this });
                }
                await Promise.all(this.stores.map(s => s.init(ipfs, identity, options.store)))

                this.allStoresMap; // call this to ensure no store duplicates exist and all addresses are available
                this._initialized = true
            } catch (error) {
                reject(error)
            }
            resolve()
        })
        await this._initializationPromise;
        return this;


    }



    async close(): Promise<void> {
        if (!this.initialized) {
            return;
        }
        const promises: Promise<void>[] = []
        for (const store of this.stores.values()) {
            promises.push(store.close());
        }
        for (const program of this.programs.values()) {
            promises.push(program.close());
        }
        await Promise.all(promises);
        this._onClose && this._onClose();
    }

    async drop(): Promise<void> {
        if (!this.initialized) {
            return;
        }
        const promises: Promise<void>[] = []
        for (const store of this.stores.values()) {
            promises.push(store.drop());
        }
        for (const program of this.programs.values()) {
            promises.push(program.drop());
        }
        await Promise.all(promises);
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

    _stores: Store<any>[]
    get stores(): Store<any>[] {
        if (this._stores) {
            return this._stores;
        }
        this._stores = getValuesWithType(this, Store, AbstractProgram);
        return this._stores;
    }



    _allStores: Store<any>[]
    get allStores(): Store<any>[] {
        if (this._allStores) {
            return this._allStores;
        }
        this._allStores = getValuesWithType(this, Store);
        return this._allStores;
    }


    _allStoresMap: Map<string, Store<any>>
    get allStoresMap(): Map<string, Store<any>> {
        if (this._allStoresMap) {
            return this._allStoresMap;
        }
        const map = new Map<string, Store<any>>();
        getValuesWithType(this, Store).map(s => map.set(s.address.toString(), s));
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
        return getValuesWithType(this, AbstractProgram)
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
                program._programIndex = i;
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
    _programIndex: number = 0; // Prevent duplicates for subprograms

}