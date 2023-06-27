import {
	variant,
	vec,
	field,
	serialize,
	deserialize,
	fixedArray,
	option,
} from "@dao-xyz/borsh";
import { equals } from "uint8arrays";
import { Uint8ArrayList } from "uint8arraylist";
import {
	PublicSignKey,
	SignatureWithKey,
	verify,
	randomBytes,
	sha256Base64,
	sha256,
} from "@peerbit/crypto";

/**
 * The default msgID implementation
 * Child class can override this.
 */
export const getMsgId = async (msg: Uint8ArrayList | Uint8Array) => {
	// first bytes is discriminator,
	// next 32 bytes should be an id
	//return  Buffer.from(msg.slice(0, 33)).toString('base64');

	return sha256Base64(msg.subarray(0, 33)); // base64EncArr(msg, 0, ID_LENGTH + 1);
};

let concatBytes: (arr: Uint8Array[], totalLength: number) => Uint8Array;
if ((globalThis as any).Buffer) {
	concatBytes = (globalThis as any).Buffer.concat;
} else {
	concatBytes = (arrays, length) => {
		if (length == null) {
			let length = 0;
			for (const element of arrays) {
				length += element.length;
			}
		}
		const output = new Uint8Array(length);
		let offset = 0;
		for (const arr of arrays) {
			output.set(arr, offset);
			offset += arr.length;
		}
		return output;
	};
}

export const ID_LENGTH = 32;

const WEEK_MS = 7 * 24 * 60 * 60 + 1000;

@variant(0)
export class MessageHeader {
	@field({ type: fixedArray("u8", ID_LENGTH) })
	private _id: Uint8Array;

	@field({ type: "u64" })
	private _timestamp: bigint;

	@field({ type: "u64" })
	private _expires: bigint;

	constructor(properties?: { expires?: bigint; id?: Uint8Array }) {
		this._id = properties?.id || randomBytes(ID_LENGTH);
		this._expires = properties?.expires || BigInt(+new Date() + WEEK_MS);
		this._timestamp = BigInt(+new Date());
	}

	get id() {
		return this._id;
	}

	get expires() {
		return this._expires;
	}

	get timetamp() {
		return this._timestamp;
	}

	equals(other: MessageHeader) {
		return this._expires === other.expires && equals(this._id, other.id);
	}

	verify() {
		return this.expires >= +new Date();
	}
}

class PublicKeys {
	@field({ type: vec(PublicSignKey) })
	keys: PublicSignKey[];
	constructor(keys: PublicSignKey[]) {
		this.keys = keys;
	}
}

const SIGNATURES_SIZE_ENCODING = "u8"; // with 7 steps you know everyone in the world?, so u8 *should* suffice
@variant(0)
export class Signatures {
	@field({ type: vec(SignatureWithKey, SIGNATURES_SIZE_ENCODING) })
	signatures: SignatureWithKey[];

	constructor(signatures: SignatureWithKey[] = []) {
		this.signatures = signatures;
	}

	equals(other: Signatures) {
		return (
			this.signatures.length === other.signatures.length &&
			this.signatures.every((value, i) => other.signatures[i].equals(value))
		);
	}

	get publicKeys(): PublicSignKey[] {
		return this.signatures.map((x) => x.publicKey);
	}

	hashPublicKeys(): Promise<string> {
		return sha256Base64(serialize(new PublicKeys(this.publicKeys)));
	}
}

const keyMap: Map<string, PublicSignKey> = new Map();
interface Signed {
	get signatures(): Signatures;
}
interface Suffix {
	getSuffix(iteration: number): Uint8Array | Uint8Array[];
}

const verifyMultiSig = async (
	message: Suffix & Prefixed & Signed,
	expectSignatures: boolean
) => {
	const signatures = message.signatures.signatures;
	if (signatures.length === 0) {
		return !expectSignatures;
	}

	await message.createPrefix();

	const dataGenerator = getMultiSigDataToSignHistory(message, 0);
	let done: boolean | undefined = false;
	for (const signature of signatures) {
		if (done) {
			throw new Error(
				"Unexpected, the amount of signatures does not match the amount of data verify"
			);
		}
		const data = dataGenerator.next();
		done = data.done;
		if (!(await verify(signature, data.value!))) {
			return false;
		}
	}
	return true;
};
interface Prefixed {
	prefix: Uint8Array;
	createPrefix: () => Promise<Uint8Array>;
}

const emptySignatures = serialize(new Signatures());
function* getMultiSigDataToSignHistory(
	message: Suffix & Prefixed & Signed,
	from = 0
): Generator<Uint8Array, undefined, void> {
	if (from === 0) {
		yield concatBytes(
			[message.prefix, emptySignatures],
			message.prefix.length + emptySignatures.length
		);
	}

	for (
		let i = Math.max(from - 1, 0);
		i < message.signatures.signatures.length;
		i++
	) {
		const bytes = message.getSuffix(i); // TODO make more performant
		const concat = [message.prefix];
		let len = message.prefix.length;
		if (bytes instanceof Uint8Array) {
			concat.push(bytes);
			len += bytes.byteLength;
		} else {
			for (const arr of bytes) {
				concat.push(arr);
				len += arr.byteLength;
			}
		}
		yield concatBytes(concat, len);
	}
	return;
}

export abstract class Message {
	static deserialize(bytes: Uint8ArrayList) {
		if (bytes.get(0) === DATA_VARIANT) {
			// Data
			return DataMessage.deserialize(bytes);
		} else if (bytes.get(0) === HELLO_VARIANT) {
			// heartbeat
			return Hello.deserialize(bytes);
		} else if (bytes.get(0) === GOODBYE_VARIANT) {
			// heartbeat
			return Goodbye.deserialize(bytes);
		} else if (bytes.get(0) === PING_VARIANT) {
			return PingPong.deserialize(bytes);
		}

		throw new Error("Unsupported");
	}

	abstract serialize(): Uint8ArrayList | Uint8Array;
	abstract equals(other: Message): boolean;
	abstract verify(expectSignatures: boolean): Promise<boolean>;
}

// I pack data with this message
const DATA_VARIANT = 0;
@variant(DATA_VARIANT)
export class DataMessage extends Message {
	@field({ type: MessageHeader })
	private _header: MessageHeader;

	@field({ type: vec("string") })
	private _to: string[]; // not signed! TODO should we sign this?

	@field({ type: Signatures })
	private _signatures: Signatures;

	@field({ type: Uint8Array })
	private _data: Uint8Array;

	constructor(properties: {
		header?: MessageHeader;
		to?: string[];
		data: Uint8Array;
		signatures?: Signatures;
	}) {
		super();
		this._data = properties.data;
		this._header = properties.header || new MessageHeader();
		this._to = properties.to || [];
		this._signatures = properties.signatures || new Signatures();
	}

	get id(): Uint8Array {
		return this._header.id;
	}

	get signatures(): Signatures {
		return this._signatures;
	}

	get header(): MessageHeader {
		return this._header;
	}

	get to(): string[] {
		return this._to;
	}
	set to(to: string[]) {
		this._serialized = undefined;
		this._to = to;
	}

	get sender(): PublicSignKey {
		return this.signatures.signatures[0].publicKey;
	}

	get data(): Uint8Array {
		return this._data;
	}

	_serialized: Uint8Array | undefined;
	get serialized(): Uint8Array | undefined {
		return this.serialized;
	}

	_prefix: Uint8Array | undefined;
	get prefix(): Uint8Array {
		if (!this._prefix) {
			throw new Error("Prefix not created");
		}
		return this._prefix;
	}
	async createPrefix(): Promise<Uint8Array> {
		if (this._prefix) {
			return this._prefix;
		}
		const headerSer = serialize(this._header);
		const hashBytes = await sha256(this.data);
		this._prefix = concatBytes(
			[new Uint8Array([DATA_VARIANT]), headerSer, hashBytes],
			1 + headerSer.length + hashBytes.length
		);
		return this._prefix;
	}

	getSuffix(iteration: number): Uint8Array {
		return serialize(
			new Signatures(this.signatures.signatures.slice(0, iteration + 1))
		);
	}

	async sign(sign: (bytes: Uint8Array) => Promise<SignatureWithKey>) {
		this._serialized = undefined; // because we will change this object, so the serialized version will not be applicable anymore
		await this.createPrefix();
		this.signatures.signatures.push(
			await sign(
				getMultiSigDataToSignHistory(
					this,
					this.signatures.signatures.length
				).next().value!
			)
		);
		return this;
	}

	async verify(expectSignatures: boolean): Promise<boolean> {
		return this._header.verify() && verifyMultiSig(this, expectSignatures);
	}

	/** Manually ser/der for performance gains */
	serialize() {
		if (this._serialized) {
			return this._serialized;
		}
		return serialize(this);
	}

	static deserialize(bytes: Uint8ArrayList): DataMessage {
		if (bytes.get(0) !== 0) {
			throw new Error("Unsupported");
		}
		const arr = bytes.subarray();
		const ret = deserialize(arr, DataMessage);
		ret._serialized = arr;
		return ret;
	}

	equals(other: Message) {
		if (other instanceof DataMessage) {
			const a =
				equals(this.data, other.data) &&
				equals(this.id, other.id) &&
				this.to.length === other.to.length;
			if (!a) {
				return false;
			}
			for (let i = 0; i < this.to.length; i++) {
				if (this.to[i] !== other.to[i]) {
					return false;
				}
			}
			return this.signatures.equals(other.signatures);
		}
		return false;
	}
}
@variant(0)
export class NetworkInfo {
	@field({ type: vec("u32", SIGNATURES_SIZE_ENCODING) })
	pingLatencies: number[];

	constructor(pingLatencies: number[]) {
		this.pingLatencies = pingLatencies;
	}
}

// I send this too all my peers
const HELLO_VARIANT = 1;
@variant(HELLO_VARIANT)
export class Hello extends Message {
	@field({ type: MessageHeader })
	header: MessageHeader;

	@field({ type: vec("string") })
	multiaddrs: string[];

	@field({ type: option(Uint8Array) })
	data?: Uint8Array;

	@field({ type: NetworkInfo })
	networkInfo: NetworkInfo;

	@field({ type: Signatures })
	signatures: Signatures;

	constructor(options?: { multiaddrs?: string[]; data?: Uint8Array }) {
		super();
		this.header = new MessageHeader();
		this.data = options?.data;
		this.multiaddrs =
			options?.multiaddrs?.filter((x) => !x.includes("/p2p-circuit/")) || []; // don't forward relay addresess (TODO ?)
		this.signatures = new Signatures();
		this.networkInfo = new NetworkInfo([]);
	}

	get sender(): PublicSignKey {
		return this.signatures.signatures[0].publicKey;
	}

	serialize() {
		return serialize(this);
	}
	static deserialize(bytes: Uint8ArrayList): Hello {
		const result = deserialize(bytes.subarray(), Hello);
		if (result.signatures.signatures.length === 0) {
			throw new Error("Missing sender on Hello");
		}
		return result;
	}

	_prefix: Uint8Array | undefined;
	get prefix(): Uint8Array {
		if (!this._prefix) {
			throw new Error("Prefix not created");
		}
		return this._prefix;
	}
	async createPrefix(): Promise<Uint8Array> {
		if (this._prefix) {
			return this._prefix;
		}
		const headerSer = serialize(this.header);
		const hashBytes = this.data ? await sha256(this.data) : new Uint8Array();
		this._prefix = concatBytes(
			[new Uint8Array([HELLO_VARIANT]), headerSer, hashBytes],
			1 + headerSer.length + hashBytes.length
		);
		return this._prefix;
	}

	getSuffix(iteration: number): Uint8Array[] {
		return [
			serialize(
				new NetworkInfo(this.networkInfo.pingLatencies.slice(0, iteration + 1))
			),
			serialize(
				new Signatures(this.signatures.signatures.slice(0, iteration + 1))
			),
		];
	}

	async sign(sign: (bytes: Uint8Array) => Promise<SignatureWithKey>) {
		await this.createPrefix();
		const toSign = getMultiSigDataToSignHistory(
			this,
			this.signatures.signatures.length
		).next().value!;
		this.signatures.signatures.push(await sign(toSign));
		return this;
	}

	async verify(expectSignatures: boolean): Promise<boolean> {
		return (
			this.header.verify() &&
			this.networkInfo.pingLatencies.length ===
				this.signatures.signatures.length - 1 &&
			verifyMultiSig(this, expectSignatures)
		);
	}

	equals(other: Message) {
		if (other instanceof Hello) {
			const dataEquals =
				(!!this.data && !!other.data && equals(this.data, other.data)) ||
				!this.data === !other.data;
			if (!dataEquals) {
				return false;
			}

			return (
				this.header.equals(other.header) &&
				this.signatures.equals(other.signatures)
			);
		}
		return false;
	}
}

// Me or some my peer disconnected
const GOODBYE_VARIANT = 2;
@variant(GOODBYE_VARIANT)
export class Goodbye extends Message {
	@field({ type: MessageHeader })
	header: MessageHeader;

	@field({ type: "bool" })
	early?: boolean; // is early goodbye, I send this to my peers so when I disconnect, they can relay the message for me

	@field({ type: option(Uint8Array) })
	data?: Uint8Array; // not signed

	@field({ type: Signatures })
	signatures: Signatures;

	constructor(properties?: {
		header?: MessageHeader;
		data?: Uint8Array;
		early?: boolean;
	}) {
		// disconnected: PeerId | string,
		super();
		this.header = properties?.header || new MessageHeader();
		this.data = properties?.data;
		this.early = properties?.early;
		this.signatures = new Signatures();
	}

	get sender(): PublicSignKey {
		return this.signatures.signatures[0]!.publicKey;
	}

	serialize() {
		return serialize(this);
	}
	static deserialize(bytes: Uint8ArrayList): Goodbye {
		const result = deserialize(bytes.subarray(), Goodbye);
		if (result.signatures.signatures.length === 0) {
			throw new Error("Missing sender on Goodbye");
		}
		return result;
	}

	_prefix: Uint8Array | undefined;
	get prefix(): Uint8Array {
		if (!this._prefix) {
			throw new Error("Prefix not created");
		}
		return this._prefix;
	}

	async createPrefix(): Promise<Uint8Array> {
		if (this._prefix) {
			return this._prefix;
		}
		const headerSer = serialize(this.header);
		const hashBytes = this.data ? await sha256(this.data) : new Uint8Array();
		this._prefix = concatBytes(
			[new Uint8Array([GOODBYE_VARIANT]), headerSer, hashBytes],
			1 + headerSer.length + 1 + hashBytes.length
		);
		return this._prefix;
	}

	getSuffix(iteration: number): Uint8Array {
		return serialize(
			new Signatures(this.signatures.signatures.slice(0, iteration + 1))
		);
	}

	async sign(sign: (bytes: Uint8Array) => Promise<SignatureWithKey>) {
		await this.createPrefix();
		this.signatures.signatures.push(
			await sign(
				getMultiSigDataToSignHistory(
					this,
					this.signatures.signatures.length
				).next().value!
			)
		);
		return this;
	}

	async verify(expectSignatures: boolean): Promise<boolean> {
		return this.header.verify() && verifyMultiSig(this, expectSignatures);
	}

	equals(other: Message) {
		if (other instanceof Goodbye) {
			if (this.early !== other.early) {
				return false;
			}

			const dataEquals =
				(!!this.data && !!other.data && equals(this.data, other.data)) ||
				!this.data === !other.data;
			if (!dataEquals) {
				return false;
			}
			return (
				this.header.equals(other.header) &&
				this.signatures.equals(other.signatures)
			);
		}
		return false;
	}
}

const PING_VARIANT = 3;

@variant(PING_VARIANT)
export abstract class PingPong extends Message {
	static deserialize(bytes: Uint8ArrayList) {
		return deserialize(bytes.subarray(), PingPong);
	}

	serialize(): Uint8ArrayList | Uint8Array {
		return serialize(this);
	}

	verify(_expectSignatures: boolean): Promise<boolean> {
		return Promise.resolve(true);
	}

	abstract get pingBytes(): Uint8Array;
}

@variant(0)
export class Ping extends PingPong {
	@field({ type: fixedArray("u8", 32) })
	pingBytes: Uint8Array;

	constructor() {
		super();
		this.pingBytes = randomBytes(32);
	}
	equals(other: Message) {
		if (other instanceof Ping) {
			return equals(this.pingBytes, other.pingBytes);
		}
		return false;
	}
}

@variant(1)
export class Pong extends PingPong {
	@field({ type: fixedArray("u8", 32) })
	pingBytes: Uint8Array;

	constructor(pingBytes: Uint8Array) {
		super();
		this.pingBytes = pingBytes;
	}

	equals(other: Message) {
		if (other instanceof Pong) {
			return equals(this.pingBytes, other.pingBytes);
		}
		return false;
	}
}

@variant(0)
export class Connections {
	@field({ type: vec(fixedArray("string", 2)) })
	connections: [string, string][];

	constructor(connections: [string, string][]) {
		this.connections = connections;
	}

	equals(other: Connections) {
		if (this.connections.length !== other.connections.length) {
			return false;
		}
		for (let i = 0; i < this.connections.length; i++) {
			if (this.connections[i].length !== other.connections[i].length) {
				return false;
			}
			const a1 = this.connections[i][0];
			const a2 = this.connections[i][1];
			const b1 = other.connections[i][0];
			const b2 = other.connections[i][1];

			if (a1 === b1 && a2 === b2) {
				continue;
			}
			if (a1 === b2 && a2 === b1) {
				continue;
			}
			return false;
		}
		return true;
	}
}

// Share connections
/* const NETWORK_INFO_VARIANT = 3;
@variant(NETWORK_INFO_VARIANT)
export class NetworkInfo extends Message {

	@field({ type: MessageHeader })
	header: MessageHeader;

	@field({ type: Connections })
	connections: Connections;


	@field({ type: Signatures })
	signatures: Signatures


	constructor(connections: [string, string][]) {
		super();
		this.header = new MessageHeader();
		this.connections = new Connections(connections);
		this.signatures = new Signatures()
	}

	getDataToSign(): Uint8Array {
		return this.serialize()
	}

	_prefix: Uint8Array | undefined
	get prefix(): Uint8Array {
		if (this._prefix)
			return this._prefix
		const header = serialize(this.header);
		const connections = serialize(this.connections);
		this._prefix = concatBytes([new Uint8Array([NETWORK_INFO_VARIANT]), header, connections], 1 + header.length + connections.length);
		return this._prefix;
	}

	sign(sign: (bytes: Uint8Array) => SignatureWithKey) {
		this.signatures.signatures.push(sign(getMultiSigDataToSignHistory(this, this.signatures.signatures.length).next().value!));
		return this;
	}

	verify(): boolean {
		return this.header.verify() && verifyMultiSig(this)
	}


	serialize() {
		return serialize(this)
	}
	static deserialize(bytes: Uint8ArrayList): NetworkInfo {
		return deserialize(bytes.subarray(), NetworkInfo)
	}

	equals(other: Message) {
		if (other instanceof NetworkInfo) {
			if (!equals(this.header.id, other.header.id) || !this.header.equals(other.header)) { // TODO fix uneccessary copy
				return false;
			}

			if (!this.connections.equals(other.connections)) {
				return false;
			}
			return this.signatures.equals(other.signatures)
		}
		return false;
	}
}

 */
