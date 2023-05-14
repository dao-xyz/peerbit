import { field, fixedArray, option, variant } from "@dao-xyz/borsh";
import { Change, Entry, Identity, Log } from "@dao-xyz/peerbit-log";
import {
	PublicKeyEncryptionResolver,
	randomBytes,
	sha256Sync,
} from "@dao-xyz/peerbit-crypto";
import { getValuesWithType } from "./utils.js";
import { serialize, deserialize } from "@dao-xyz/borsh";
import { CID } from "multiformats/cid";
import { BlockStore } from "@dao-xyz/libp2p-direct-block";
import { Libp2pExtended } from "@dao-xyz/peerbit-libp2p";
import { createBlock } from "@dao-xyz/libp2p-direct-block";
import {
	NoType,
	ObserverType,
	ReplicatorType,
	SubscriptionType,
} from "./role.js";
import { LogProperties } from "@dao-xyz/peerbit-log";

export * from "./protocol-message.js";
export * from "./role.js";

const notEmpty = (e: string) => e !== "" && e !== " ";

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
				throw new Error("Path can be created from a program without an index");
			}
			return new ProgramPath({
				index: obj.programIndex,
			});
		} else {
			return new ProgramPath(obj);
		}
	}
}
const ADDRESS_PREFIXES = ["zb", "zd", "Qm", "ba", "k5"];

@variant(0)
export class Address {
	@field({ type: "string" })
	private _cid: string;

	@field({ type: option(ProgramPath) })
	private _path?: ProgramPath;

	constructor(properties: { cid: string; path?: ProgramPath }) {
		if (properties) {
			this._cid = properties.cid;
			this._path = properties.path;
		}
	}
	get cid(): string {
		return this._cid;
	}

	get path(): ProgramPath | undefined {
		return this._path;
	}

	get bytes(): Uint8Array {
		return serialize(this);
	}

	private _toString: string;

	toString() {
		return (
			this._toString || (this._toString = Address.join(this.cid, this.path))
		);
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
			for (const p of ADDRESS_PREFIXES) {
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
		if (
			cid.startsWith("/") ||
			cid.startsWith(" ") ||
			cid.endsWith("/") ||
			cid.endsWith(" ")
		) {
			throw new Error("Malformed CID");
		}
		if (!addressPath) {
			return "/peerbit/" + cid;
		}

		return "/peerbit/" + cid + "/" + addressPath.index.toString();
	}
}

export interface Saveable {
	save(
		store: BlockStore,
		options?: {
			format?: string;
			timeout?: number;
		}
	): Promise<Address>;

	delete(): Promise<void>;
}

export type OpenProgram = (program: Program) => Promise<Program>;
export type LogCallbackOptions = {
	onWrite?: (log: Log<any>, change: Entry<any>) => void;
	onChange?: (log: Log<any>, change: Change<any>) => void;
	onClose?: (log: Log<any>) => void;
};
export type ProgramInitializationOptions = {
	log?: LogProperties<any> & LogCallbackOptions;
	role: ReplicatorType | ObserverType | NoType;
	replicators: () => string[][] | undefined; // array of replicators in each shard
	parent?: AbstractProgram;
	onClose?: () => Promise<void> | void;
	onDrop?: () => Promise<void> | void;
	onSave?: (address: Address) => Promise<void> | void;
	open?: OpenProgram;
	openedBy?: AbstractProgram;
	replicator?: (gid: string) => Promise<boolean>;
};

@variant(0)
export abstract class AbstractProgram {
	@field({ type: option("u32") })
	_programIndex?: number; // Prevent duplicates for subprograms

	private _libp2p: Libp2pExtended;
	private _identity: Identity;
	private _encryption?: PublicKeyEncryptionResolver;
	private _onClose?: () => Promise<void> | void;
	private _onDrop?: () => Promise<void> | void;
	private _initialized?: boolean;
	private _role: SubscriptionType;
	private _logs: Log<any>[] | undefined;
	private _allLogs: Log<any>[] | undefined;
	private _allLogsMap: Map<string, Log<any>> | undefined;
	private _allPrograms: AbstractProgram[] | undefined;
	private _subprogramMap: Map<number, AbstractProgram> | undefined;

	open?: (program: Program) => Promise<Program>;
	programsOpened: Program[];
	parentProgram: Program;

	get initialized() {
		return this._initialized;
	}

	get programIndex(): number | undefined {
		return this._programIndex;
	}

	get role() {
		return this._role;
	}

	async init(
		libp2p: Libp2pExtended,
		identity: Identity,
		options: ProgramInitializationOptions
	): Promise<this> {
		if (this.initialized) {
			throw new Error("Already initialized");
		}
		this._libp2p = libp2p;
		this._identity = identity;
		this._encryption = options.log?.encryption;
		this._onClose = options.onClose;
		this._onDrop = options.onDrop;
		this._role = options.role;
		if (options.open) {
			this.programsOpened = [];
			this.open = async (program) => {
				if (!program.openedByPrograms) {
					program.openedByPrograms = [];
				}
				program.openedByPrograms.push(this);
				this.programsOpened.push(program);

				const opened = await options.open!(program);
				return opened;
			};
		}

		const nexts = this.programs;
		for (const next of nexts) {
			await next.init(libp2p, identity, {
				...options,
				parent: this,
			});
		}

		await Promise.all(
			this.logs.map((s) =>
				s.init(libp2p.services.blocks, identity, {
					cache: options.log?.cache,
					canAppend: options.log?.canAppend,
					clock: options.log?.clock,
					encoding: options.log?.encoding,
					encryption: options.log?.encryption,
					sortFn: options.log?.sortFn,
					trim: options.log?.trim,
					onChange: (change) => {
						return options.log?.onChange?.(s, change);
					},
					onClose: () => {
						return options.log?.onClose?.(s);
					},
					onWrite: (change) => {
						return options.log?.onWrite?.(s, change);
					},
				})
			)
		);

		this._initialized = true;
		return this;
	}

	private _clear() {
		this._logs = undefined;
		this._allLogs = undefined;
		this._allLogsMap = undefined;
		this._allPrograms = undefined;
		this._subprogramMap = undefined;
	}

	private async _end(
		type: "drop" | "close",
		onEvent?: () => void | Promise<void>
	) {
		if (this.initialized) {
			await onEvent?.();
			const promises: Promise<void | boolean>[] = [];
			for (const store of this.logs.values()) {
				promises.push(store[type]());
			}
			for (const program of this.programs.values()) {
				promises.push(program[type]());
			}
			if (this.programsOpened) {
				for (const program of this.programsOpened) {
					promises.push(program[type](this));
				}
				this.programsOpened = [];
			}
			await Promise.all(promises);

			this._clear();
			return true;
		} else {
			this._clear();
			return true;
		}
	}

	async close(): Promise<boolean> {
		return this._end("close", this._onClose);
	}

	async drop(): Promise<void> {
		await this._end("drop", this._onDrop);
		this._initialized = false;
	}

	get libp2p(): Libp2pExtended {
		return this._libp2p;
	}

	get identity(): Identity {
		return this._identity;
	}

	get encryption(): PublicKeyEncryptionResolver | undefined {
		return this._encryption;
	}

	get logs(): Log<any>[] {
		if (this._logs) {
			return this._logs;
		}
		this._logs = getValuesWithType(this, Log, AbstractProgram);
		return this._logs;
	}

	get allLogs(): Log<any>[] {
		if (this._allLogs) {
			return this._allLogs;
		}
		this._allLogs = getValuesWithType(this, Log);
		return this._allLogs;
	}

	get allLogsMap(): Map<string, Log<any>> {
		if (this._allLogsMap) {
			return this._allLogsMap;
		}
		const map = new Map<string, Log<any>>();
		getValuesWithType(this, Log).map((s) => map.set(s.idString, s));
		this._allLogsMap = map;
		return this._allLogsMap;
	}

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
		return getValuesWithType(this, AbstractProgram, Log);
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

export interface CanTrust {
	isTrusted(keyHash: string): Promise<boolean> | boolean;
}

@variant(0)
export abstract class Program
	extends AbstractProgram
	implements Addressable, Saveable
{
	@field({ type: fixedArray("u8", 32) })
	id: Uint8Array;

	private _address?: Address;

	private _closed: boolean;

	openedByPrograms: (AbstractProgram | undefined)[];

	constructor(properties?: { id?: Uint8Array }) {
		super();
		if (properties) {
			this.id = properties.id || randomBytes(32);
		} else {
			this.id = randomBytes(32);
		}
	}

	get closed() {
		return this._closed !== false;
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
		let prev = this.id;
		for (const [_ix, log] of this.allLogs.entries()) {
			log.id = sha256Sync(prev);
			prev = log.id;
		}
		// post setup
		// set parents of subprograms to this
		for (const [ix, program] of this.allPrograms.entries()) {
			program._programIndex = ix;
			program.parentProgram = this.parentProgram || this;
		}
	}

	async init(
		libp2p: Libp2pExtended,
		identity: Identity,
		options: ProgramInitializationOptions
	): Promise<this> {
		(this.openedByPrograms || (this.openedByPrograms = [])).push(
			options.openedBy
		);

		this._closed = false;

		if (this.initialized) {
			return this;
		}

		// TODO, determine whether setup should be called before or after save
		if (this.parentProgram === undefined) {
			const address = await this.save(libp2p.services.blocks);
			await options?.onSave?.(address);
		}
		await super.init(libp2p, identity, options);

		await this.setup();

		if (this.parentProgram != undefined && this._address) {
			throw new Error(
				"Expecting address to be undefined as this program is part of another program"
			);
		}

		return this;
	}

	async load() {
		this._closed = false;
		await Promise.all(this.allLogs.map((store) => store.load()));
	}

	async save(
		store: BlockStore,
		options?: {
			format?: string;
			timeout?: number;
		}
	): Promise<Address> {
		this.setupIndices();
		const existingAddress = this._address;
		const hash = await store.put(
			await createBlock(serialize(this), "raw"),
			options
		);

		this._address = Address.parse(Address.join(hash));
		if (!this.address) {
			throw new Error("Unexpected");
		}

		if (existingAddress && !existingAddress.equals(this.address)) {
			throw new Error(
				"Program properties has been changed after constructor so that the hash has changed. Make sure that the 'setup(...)' function does not modify any properties that are to be serialized"
			);
		}

		return this._address;
	}

	async delete(): Promise<void> {
		if (!this.address?.cid) {
			throw new Error("Can not delete, missing address");
		}
		return this.libp2p.services.blocks.rm(this.address.cid);
	}

	static async load<S extends Program>(
		store: BlockStore,
		address: Address | string,
		options?: {
			timeout?: number;
		}
	): Promise<S | undefined> {
		const addressObject =
			address instanceof Address ? address : Address.parse(address);
		const manifestBlock = await store.get<Uint8Array>(
			addressObject.cid,
			options
		);
		if (!manifestBlock) {
			return undefined;
		}
		const der = deserialize(manifestBlock.bytes, Program);
		der.address = Address.parse(Address.join(addressObject.cid));
		return der as S;
	}

	async close(from?: AbstractProgram): Promise<boolean> {
		if (from && this.openedByPrograms) {
			const ix = this.openedByPrograms.findIndex((x) => x == from);
			if (ix !== -1) {
				this.openedByPrograms.splice(ix, 1);
				if (this.openedByPrograms.length !== 0) {
					return false; // don't close, because someone else depends on this
				}
				// else close!
			} else {
				return false;
			}
		}
		this._closed = true;
		return super.close();
	}
	async drop(): Promise<void> {
		this._closed = true;
		await super.drop();
		return this.delete();
	}

	get topic(): string {
		if (!this.address) {
			throw new Error("Missing address");
		}
		return this.address.toString();
	}
}

/**
 * Building block, but not something you use as a standalone
 */
@variant(1)
export abstract class ComposableProgram extends AbstractProgram {}
