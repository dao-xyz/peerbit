import { BlockStore } from "@dao-xyz/libp2p-direct-block";
import { Libp2pExtended } from "@dao-xyz/peerbit-libp2p";
import { createBlock } from "@dao-xyz/libp2p-direct-block";
import { Change, Entry, Log, LogOptions } from "@dao-xyz/peerbit-log";
import { sha256 } from "@dao-xyz/peerbit-crypto";
import { field, getSchema, variant } from "@dao-xyz/borsh";
import { getValuesWithType } from "./utils.js";
import { serialize, deserialize } from "@dao-xyz/borsh";
import { CID } from "multiformats/cid";

import { NoType, Observer, Replicator, SubscriptionType } from "./role.js";
import { Identity, PublicKeyEncryptionResolver } from "@dao-xyz/peerbit-crypto";
import { PeerIds } from "@dao-xyz/libp2p-direct-sub";

export * from "./protocol-message.js";
export * from "./role.js";

const notEmpty = (e: string) => e !== "" && e !== " ";

export interface Addressable {
	address?: Address | undefined;
}

const ADDRESS_PREFIXES = ["zb", "zd", "Qm", "ba", "k5"];

@variant(0)
export class Address {
	@field({ type: "string" })
	private _cid: string;

	constructor(properties: { cid: string }) {
		if (properties) {
			this._cid = properties.cid;
		}
	}
	get cid(): string {
		return this._cid;
	}

	get bytes(): Uint8Array {
		return serialize(this);
	}

	private _toString: string;

	toString() {
		return this._toString || (this._toString = Address.join(this.cid));
	}

	equals(other: Address) {
		return this.cid === other.cid;
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

		if (parts.length > 1) {
			throw new Error("Expecting parts to have length 1");
		}
		return new Address({ cid: parts[0] });
	}

	static join(cid: string) {
		if (
			cid.startsWith("/") ||
			cid.startsWith(" ") ||
			cid.endsWith("/") ||
			cid.endsWith(" ")
		) {
			throw new Error("Malformed CID");
		}
		return "/peerbit/" + cid;
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
	log?: LogOptions<any> | ((log: Log<any>) => LogOptions<any>);
	role: Replicator | Observer | NoType;
	parent?: AbstractProgram;
	onClose?: () => Promise<void> | void;
	onDrop?: () => Promise<void> | void;
	onSave?: (address: Address) => Promise<void> | void;
	waitFor?: (other: PeerIds) => Promise<void>;
	open?: OpenProgram;
	openedBy?: AbstractProgram;
	encryption?: PublicKeyEncryptionResolver;
};

@variant(0)
export abstract class AbstractProgram {
	private _libp2p: Libp2pExtended;
	private _identity: Identity;
	private _onClose?: () => Promise<void> | void;
	private _onDrop?: () => Promise<void> | void;
	private _initialized?: boolean;
	private _role: SubscriptionType;
	private _logs: Log<any>[] | undefined;
	private _allLogs: Log<any>[] | undefined;
	private _allLogsMap: Map<string, Log<any>> | undefined;
	private _allPrograms: AbstractProgram[] | undefined;
	private _encryption?: PublicKeyEncryptionResolver;
	private _waitForPeer?: (other: PeerIds) => Promise<void>;

	open?: (program: Program) => Promise<Program>;
	programsOpened: Program[];
	parentProgram: Program;

	get initialized() {
		return this._initialized;
	}

	get role() {
		if (!this._role) {
			throw new Error("Role not defined");
		}
		return this._role;
	}

	get encryption() {
		return this._encryption;
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
		this._onClose = options.onClose;
		this._onDrop = options.onDrop;
		this._role = options.role;
		this._encryption = options.encryption;
		this._waitForPeer = options.waitFor;
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
				s.open(
					libp2p.services.blocks,
					identity,
					typeof options?.log === "function" ? options?.log(s) : options?.log
				)
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

	get programs(): AbstractProgram[] {
		return getValuesWithType(this, AbstractProgram, Log);
	}

	/**
	 * Wait for another peer to be 'ready' to talk with you for this particular program
	 * @param other
	 */
	async waitFor(other: PeerIds): Promise<void> {
		await this._waitForPeer?.(other);
		await Promise.all(this.programs.map((x) => x.waitFor(other)));
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
	private _address: Address;

	private _closed: boolean;

	openedByPrograms: (AbstractProgram | undefined)[];

	private ___seed: Uint8Array;
	constructor() {
		super();
	}

	get closed() {
		return this._closed !== false;
	}

	get address(): Address {
		return this._address;
	}

	set address(address: Address) {
		this._address = address;
	}

	/**
	 * Will be called before program init(...)
	 * This function can be used to connect different modules
	 */
	abstract setup(): Promise<void>;

	async initializeIds(): Promise<void> {
		let prev = await this.prehash();
		for (const [_ix, log] of this.allLogs.entries()) {
			log.id = await sha256(prev);
			prev = log.id;
		}
		// post setup
		// set parents of subprograms to this
		for (const [_ix, program] of this.allPrograms.entries()) {
			program.parentProgram = this.parentProgram || this;
		}
	}
	private prehash(): Promise<Uint8Array> {
		for (const [_ix, log] of this.allLogs.entries()) {
			log.id = undefined;
		}
		return sha256(serialize(this));
	}

	async init(
		libp2p: Libp2pExtended,
		identity: Identity,
		options: ProgramInitializationOptions
	): Promise<this> {
		// check that a  discriminator exist
		const schema = getSchema(this.constructor);
		if (!schema || typeof schema.variant !== "string") {
			throw new Error(
				`Expecting class to be decorated with a string variant. Example:\n\'import { variant } "@dao-xyz/borsh"\n@variant("example-db")\nclass ${this.constructor.name} { ...`
			);
		}

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

		// call setup before init, because init means "open" while "setup" is rather something we do to make everything ready for start
		await this.setup();

		await super.init(libp2p, identity, options);

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
		await this.initializeIds();

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
		if (this.address?.cid) {
			return this.libp2p.services.blocks.rm(this.address.cid);
		}
		// Not saved
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
