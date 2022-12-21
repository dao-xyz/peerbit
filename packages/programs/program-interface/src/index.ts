import { field, option, variant } from "@dao-xyz/borsh";
import { Entry, Identity } from "@dao-xyz/peerbit-log";
import { IInitializationOptions, Store } from "@dao-xyz/peerbit-store";
import { v4 as uuid } from "uuid";
import { PublicKeyEncryptionResolver } from "@dao-xyz/peerbit-crypto";
import { getValuesWithType } from "./utils.js";
import {
    serialize,
    deserialize,
    Constructor,
    AbstractType,
} from "@dao-xyz/borsh";
import path from "path";
import { CID } from "multiformats/cid";
import { Blocks } from "@dao-xyz/peerbit-block";
import { Libp2p } from "libp2p";
import { waitFor } from "@dao-xyz/peerbit-time";
export * from "./protocol-message.js";

const notEmpty = (e: string) => e !== "" && e !== " ";

export interface Manifest {
    data: Uint8Array;
}

export interface Addressable {
    address?: Address | undefined;
}

export class ProgramPath {
    @field({ type: "u32" })
    index: number;

    constructor(properties: { index: number }) {
        if (properties) {
            this.index = properties.index;
        }
    }

    static from(obj: { index: number } | AbstractProgram) {
        if (obj instanceof AbstractProgram) {
            if (obj.programIndex == undefined) {
                throw new Error(
                    "Path can be created from a program without an index"
                );
            }
            return new ProgramPath({
                index: obj.programIndex,
            });
        } else {
            return new ProgramPath(obj);
        }
    }
}

@variant(0)
export class Address {
    @field({ type: "string" })
    cid: string;

    @field({ type: option(ProgramPath) })
    path?: ProgramPath;

    constructor(properties: { cid: string; path?: ProgramPath }) {
        if (properties) {
            this.cid = properties.cid;
            this.path = properties.path;
        }
    }

    toString() {
        return Address.join(this.cid, this.path);
    }

    equals(other: Address) {
        return this.cid === other.cid;
    }

    withPath(path: ProgramPath | { index: number }): Address {
        return new Address({
            cid: this.cid,
            path: path instanceof ProgramPath ? path : ProgramPath.from(path),
        });
    }

    root(): Address {
        return new Address({ cid: this.cid });
    }

    static isValid(address: { toString(): string }) {
        const parsedAddress = address.toString().replace(/\\/g, "/");

        const containsProtocolPrefix = (e: string, i: number) =>
            !(
                (i === 0 || i === 1) &&
                parsedAddress.toString().indexOf("/peerbit") === 0 &&
                e === "peerbit"
            );

        const parts = parsedAddress
            .toString()
            .split("/")
            .filter(containsProtocolPrefix)
            .filter(notEmpty);

        let accessControllerHash;

        const validateHash = (hash: string) => {
            const prefixes = ["zd", "Qm", "ba", "k5"];
            for (const p of prefixes) {
                if (hash.indexOf(p) > -1) {
                    return true;
                }
            }
            return false;
        };

        try {
            accessControllerHash = validateHash(parts[0])
                ? CID.parse(parts[0]).toString()
                : null;
        } catch (e) {
            return false;
        }

        return accessControllerHash !== null;
    }

    static parse(address: { toString(): string }) {
        if (!address) {
            throw new Error(`Not a valid Peerbit address: ${address}`);
        }

        if (!Address.isValid(address)) {
            throw new Error(`Not a valid Peerbit address: ${address}`);
        }

        const parsedAddress = address.toString().replace(/\\/g, "/");
        const parts = parsedAddress
            .toString()
            .split("/")
            .filter(
                (e, i) =>
                    !(
                        (i === 0 || i === 1) &&
                        parsedAddress.toString().indexOf("/peerbit") === 0 &&
                        e === "peerbit"
                    )
            )
            .filter((e) => e !== "" && e !== " ");

        return new Address({
            cid: parts[0],
            path:
                parts.length == 2
                    ? new ProgramPath({ index: Number(parts[1]) })
                    : undefined,
        });
    }

    static join(cid: string, addressPath?: ProgramPath) {
        const p = path.posix || path;
        if (!addressPath) return p.join("/peerbit", cid);
        else return p.join("/peerbit", cid, addressPath.index.toString());
    }
}

export interface Saveable {
    save(
        ipfs: any,
        options?: {
            format?: string;
            pin?: boolean;
            timeout?: number;
        }
    ): Promise<Address>;
}

export const save = async (
    store: Blocks,
    thing: Addressable,
    options: { format?: string; pin?: boolean; timeout?: number } = {}
): Promise<Address> => {
    const manifest: Manifest = {
        data: serialize(thing),
    };
    const hash = await store.put(
        manifest,
        options.format || "dag-cbor",
        options
    );
    return Address.parse(Address.join(hash));
};

export const load = async <S extends Addressable>(
    store: Blocks,
    address: Address,
    into: Constructor<S> | AbstractType<S>,
    options: { timeout?: number } = {}
): Promise<S | undefined> => {
    const manifest = await store.get<Manifest>(address.cid, options);
    if (!manifest) {
        return undefined;
    }
    const der = deserialize(manifest.data, into);
    der.address = Address.parse(Address.join(address.cid));
    return der;
};

export type ProgramInitializationOptions = {
    store: IInitializationOptions<any>;
    parent?: AbstractProgram;
    topic: string;
    replicate?: boolean;
    onClose?: () => void;
    onDrop?: () => void;
};

@variant(0)
export abstract class AbstractProgram {
    @field({ type: option("u32") })
    _programIndex?: number; // Prevent duplicates for subprograms

    @field({ type: option("string") })
    owner?: string; // Will control whether this program can be opened or not

    _libp2p: Libp2p;
    _identity: Identity;
    _encryption?: PublicKeyEncryptionResolver;
    _onClose?: () => void;
    _onDrop?: () => void;
    _initialized?: boolean;
    _initializationPromise: Promise<void>;
    _replicate?: boolean;
    parentProgram: Program;

    get initialized() {
        return this._initialized;
    }
    get initializationPromise(): Promise<void> | undefined {
        return this._initializationPromise;
    }

    get programIndex(): number | undefined {
        return this._programIndex;
    }

    get replicate() {
        return this._replicate;
    }

    async init(
        libp2p: Libp2p,
        store: Blocks,
        identity: Identity,
        options: ProgramInitializationOptions
    ): Promise<this> {
        if (this.initialized) {
            throw new Error("Already initialized");
        }

        const fn = async () => {
            this._libp2p = libp2p;
            this._identity = identity;
            this._encryption = options.store.encryption;
            this._onClose = options.onClose;
            this._onDrop = options.onDrop;
            this._replicate = options.replicate;

            const nexts = this.programs;
            for (const next of nexts) {
                await next.init(libp2p, store, identity, {
                    ...options,
                    parent: this,
                });
            }

            await Promise.all(
                this.stores.map((s) => s.init(store, identity, options.store))
            );

            this._initialized = true;
        };
        this._initializationPromise = fn();
        await this._initializationPromise;
        return this;
    }

    async close(): Promise<void> {
        if (!this.initialized) {
            return;
        }
        const promises: Promise<void>[] = [];
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
        const promises: Promise<void>[] = [];
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

    get libp2p(): Libp2p {
        return this._libp2p;
    }

    get identity(): Identity {
        return this._identity;
    }

    get encryption(): PublicKeyEncryptionResolver | undefined {
        return this._encryption;
    }

    _stores: Store<any>[];
    get stores(): Store<any>[] {
        if (this._stores) {
            return this._stores;
        }
        this._stores = getValuesWithType(this, Store, AbstractProgram);
        return this._stores;
    }

    _allStores: Store<any>[];
    get allStores(): Store<any>[] {
        if (this._allStores) {
            return this._allStores;
        }
        this._allStores = getValuesWithType(this, Store);
        return this._allStores;
    }

    _allStoresMap: Map<number, Store<any>>;
    get allStoresMap(): Map<number, Store<any>> {
        if (this._allStoresMap) {
            return this._allStoresMap;
        }
        const map = new Map<number, Store<any>>();
        getValuesWithType(this, Store).map((s) => map.set(s._storeIndex, s));
        this._allStoresMap = map;
        return this._allStoresMap;
    }

    _allPrograms: AbstractProgram[];
    get allPrograms(): AbstractProgram[] {
        if (this._allPrograms) {
            return this._allPrograms;
        }
        const arr: AbstractProgram[] = this.programs;
        const nexts = this.programs;
        for (const next of nexts) {
            arr.push(...next.allPrograms);
        }
        this._allPrograms = arr;
        return this._allPrograms;
    }

    _subprogramMap: Map<number, AbstractProgram>;
    get subprogramsMap(): Map<number, AbstractProgram> {
        if (this._subprogramMap) {
            // is static, so we cache naively
            return this._subprogramMap;
        }
        const map = new Map<number, AbstractProgram>();
        this.programs.map((s) => map.set(s._programIndex!, s));
        const nexts = this.programs;
        for (const next of nexts) {
            const submap = next.subprogramsMap;
            submap.forEach((program, address) => {
                if (map.has(address)) {
                    throw new Error("Store duplicates detected");
                }
                map.set(address, program);
            });
        }
        this._subprogramMap = map;
        return this._subprogramMap;
    }

    get programs(): AbstractProgram[] {
        return getValuesWithType(this, AbstractProgram, Store);
    }

    get address() {
        if (this.parentProgram) {
            if (this.programIndex == undefined) {
                throw new Error("Program index not defined");
            }
            return this.parentProgram.address.withPath({
                index: this.programIndex!,
            });
        }
        throw new Error(
            "ComposableProgram does not have an address and `parentProgram` is undefined"
        );
    }
}

export interface CanOpenSubPrograms {
    canOpen(programToOpen: Program, fromEntry: Entry<any>): Promise<boolean>;
}

@variant(0)
export abstract class Program
    extends AbstractProgram
    implements Addressable, Saveable
{
    @field({ type: "string" })
    id: string;

    _address?: Address;

    constructor(properties?: { id?: string }) {
        super();
        if (properties) {
            this.id = properties.id || uuid();
        } else {
            this.id = uuid();
        }
    }
    get address() {
        if (this._address) {
            return this._address;
        }
        return super.address;
    }

    set address(address: Address) {
        this._address = address;
    }

    /**
     * Will be called before program init(...)
     * This function can be used to connect different modules
     */
    abstract setup(): Promise<void>;

    setupIndices(): void {
        for (const [ix, store] of this.allStores.entries()) {
            store._storeIndex = ix;
        }
        // post setup
        // set parents of subprograms to this
        for (const [ix, program] of this.allPrograms.entries()) {
            program._programIndex = ix;
            program.parentProgram = this.parentProgram || this;
        }
    }

    async init(
        libp2p: Libp2p,
        store: Blocks,
        identity: Identity,
        options: ProgramInitializationOptions
    ): Promise<this> {
        // TODO, determine whether setup should be called before or after save
        if (this.parentProgram === undefined) {
            await this.save(store);
        }

        await this.setup();
        await super.init(libp2p, store, identity, options);
        if (this.parentProgram != undefined && this._address) {
            throw new Error(
                "Expecting address to be undefined as this program is part of another program"
            );
        }

        return this;
    }

    async saveSnapshot() {
        await Promise.all(this.allStores.map((store) => store.saveSnapshot()));
    }

    async loadFromSnapshot() {
        await Promise.all(
            this.allStores.map((store) => store.loadFromSnapshot())
        );
    }

    async load() {
        await Promise.all(this.allStores.map((store) => store.load()));
    }

    async save(
        store: Blocks,
        options?: {
            format?: string;
            pin?: boolean;
            timeout?: number;
        }
    ): Promise<Address> {
        await store.open();
        this.setupIndices();
        const existingAddress = this._address;
        const address = await save(store, this, options);
        this._address = address;
        if (!this.address) {
            throw new Error("Unexpected");
        }

        if (existingAddress && !existingAddress.equals(this.address)) {
            throw new Error(
                "Program properties has been changed after constructor so that the hash has changed. Make sure that the 'setup(...)' function does not modify any properties that are to be serialized"
            );
        }

        return address;
    }

    static load<S extends Program>(
        store: Blocks,
        address: Address | string,
        options?: {
            timeout?: number;
        }
    ): Promise<S> {
        return load(
            store,
            address instanceof Address ? address : Address.parse(address),
            Program,
            options
        ) as Promise<S>;
    }
}

/**
 * Building block, but not something you use as a standalone
 */
@variant(1)
export abstract class ComposableProgram extends AbstractProgram {}
