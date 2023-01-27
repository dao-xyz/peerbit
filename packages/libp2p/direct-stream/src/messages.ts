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
} from "@dao-xyz/peerbit-crypto";
import crypto from "crypto";

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
		this._id = properties?.id || crypto.randomBytes(ID_LENGTH);
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

@variant(0)
export class Signatures {
	@field({ type: vec(SignatureWithKey, "u8") })
	signatures: SignatureWithKey[];
	constructor(signatures: SignatureWithKey[] = []) {
		this.signatures = signatures;
	}

	equals(other: Signatures) {
		return (
			this.signatures.length === other.signatures.length &&
			this.signatures.every((value, i) =>
				other.signatures[i].equals(value)
			)
		);
	}

	get publicKeys(): PublicSignKey[] {
		return this.signatures.map((x) => x.publicKey);
	}

	hashPublicKeys(): string {
		return crypto
			.createHash("sha256")
			.update(serialize(new PublicKeys(this.publicKeys)))
			.digest("base64");
	}
}

const keyMap: Map<string, PublicSignKey> = new Map();
interface Signed {
	get signatures(): Signatures;
}

const verifyMultiSig = (
	message: Signed & Prefixed,
	expectSignatures: boolean
) => {
	const signatures = message.signatures.signatures;

	if (signatures.length === 0) {
		return !expectSignatures;
	}

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
		if (!verify(signature.signature, signature.publicKey, data.value!)) {
			return false;
		}
	}
	return true;
};
interface Prefixed {
	prefix: Uint8Array;
}

const emptySignatures = serialize(new Signatures());
function* getMultiSigDataToSignHistory(
	message: Signed & Prefixed,
	from = 0
): Generator<Uint8Array, undefined, void> {
	if (from === 0) {
		yield concatBytes(
			[message.prefix, emptySignatures],
			message.prefix.length + emptySignatures.length
		);
	}

	const signatures = message.signatures.signatures;
	for (let i = Math.max(from, 1); i < signatures.length + 1; i++) {
		const bytes = serialize(new Signatures(signatures.slice(0, i))); // TODO make more performant
		yield concatBytes(
			[message.prefix, bytes],
			message.prefix.length + bytes.length
		);
	}
	return;
}

export abstract class Message {
	static deserialize(bytes: Uint8ArrayList) {
		if (bytes.get(0) === 0) {
			// Data
			return DataMessage.deserialize(bytes);
		} else if (bytes.get(0) === 1) {
			// heartbeat
			return Hello.deserialize(bytes);
		} else if (bytes.get(0) === 2) {
			// heartbeat
			return Goodbye.deserialize(bytes);
		}
		/* 	else if (bytes.get(0) === 3) // Connections
			{
				return NetworkInfo.deserialize(bytes)
			} */
		throw new Error("Unsupported");
	}

	abstract serialize(): Uint8ArrayList | Uint8Array;
	abstract equals(other: Message): boolean;
	abstract verify(expectSignatures: boolean): boolean;
	abstract get header(): MessageHeader;
	abstract get signatures(): Signatures;
}

// I pack data with this message
const DATA_VARIANT = 0;
@variant(DATA_VARIANT)
export class DataMessage extends Message {
	@field({ type: MessageHeader })
	_header: MessageHeader;

	@field({ type: vec("string") })
	private _to: string[]; // not signed! TODO should we sign this?

	@field({ type: Signatures })
	private _signatures: Signatures;

	@field({ type: Uint8Array })
	private _data: Uint8Array;

	constructor(properties: {
		header?: MessageHeader,
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

	get data(): Uint8Array {
		return this._data;
	}

	_serialized: Uint8Array | undefined;
	get serialized(): Uint8Array | undefined {
		return this.serialized;
	}

	_prefix: Uint8Array | undefined;
	get prefix(): Uint8Array {
		if (this._prefix) {
			return this._prefix;
		}
		const headerSer = serialize(this._header);
		const hashBytes = crypto
			.createHash("sha256")
			.update(this.data)
			.digest();
		this._prefix = concatBytes(
			[new Uint8Array([DATA_VARIANT]), headerSer, hashBytes],
			1 + headerSer.length + hashBytes.length
		);
		return this._prefix;
	}

	sign(sign: (bytes: Uint8Array) => SignatureWithKey) {
		this._serialized = undefined; // because we will change this object, so the serialized version will not be applicable anymore
		this.signatures.signatures.push(
			sign(
				getMultiSigDataToSignHistory(
					this,
					this.signatures.signatures.length
				).next().value!
			)
		);
		return this;
	}

	verify(expectSignatures: boolean): boolean {
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

// I send this too all my peers
const HELLO_VARIANT = 1;
@variant(HELLO_VARIANT)
export class Hello extends Message {
	@field({ type: MessageHeader })
	header: MessageHeader;

	@field({ type: option(Uint8Array) })
	data?: Uint8Array;

	@field({ type: Signatures })
	signatures: Signatures;

	constructor(options?: { data?: Uint8Array }) {
		super();
		this.header = new MessageHeader();
		this.data = options?.data;
		this.signatures = new Signatures();
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
			throw new Error("Missing sender on Hello")
		}
		return result;
	}

	_prefix: Uint8Array | undefined;
	get prefix(): Uint8Array {
		if (this._prefix) return this._prefix;
		const headerSer = serialize(this.header);
		const hashBytes = this.data
			? crypto.createHash("sha256").update(this.data).digest()
			: new Uint8Array();
		this._prefix = concatBytes(
			[new Uint8Array([HELLO_VARIANT]), headerSer, hashBytes],
			1 + headerSer.length + hashBytes.length
		);
		return this._prefix;
	}

	sign(sign: (bytes: Uint8Array) => SignatureWithKey) {
		const toSign = getMultiSigDataToSignHistory(
			this,
			this.signatures.signatures.length
		).next().value!;
		this.signatures.signatures.push(sign(toSign));
		return this;
	}

	verify(expectSignatures: boolean): boolean {
		return this.header.verify() && verifyMultiSig(this, expectSignatures);
	}

	equals(other: Message) {
		if (other instanceof Hello) {
			const dataEquals =
				(!!this.data &&
					!!other.data &&
					equals(this.data, other.data)) ||
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
			throw new Error("Missing sender on Goodbye")
		}
		return result;
	}

	_prefix: Uint8Array | undefined;
	get prefix(): Uint8Array {
		if (this._prefix) return this._prefix;
		const headerSer = serialize(this.header);
		const hashBytes = this.data
			? crypto.createHash("sha256").update(this.data).digest()
			: new Uint8Array();
		this._prefix = concatBytes(
			[
				new Uint8Array([GOODBYE_VARIANT]),
				headerSer,
				hashBytes,
			],
			1 + headerSer.length + 1 + hashBytes.length
		);
		return this._prefix;
	}

	sign(sign: (bytes: Uint8Array) => SignatureWithKey) {
		this.signatures.signatures.push(
			sign(
				getMultiSigDataToSignHistory(
					this,
					this.signatures.signatures.length
				).next().value!
			)
		);
		return this;
	}

	verify(expectSignatures: boolean): boolean {
		return this.header.verify() && verifyMultiSig(this, expectSignatures);
	}

	equals(other: Message) {
		if (other instanceof Goodbye) {
			if (this.early !== other.early) {
				return false;
			}

			const dataEquals =
				(!!this.data &&
					!!other.data &&
					equals(this.data, other.data)) ||
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

@variant(0)
export class NetworkInfoHeader {
	@field({ type: fixedArray("u8", 32) })
	id: Uint8Array;

	@field({ type: "u64" })
	timestamp: bigint;

	constructor() {
		this.id = crypto.randomBytes(ID_LENGTH);
		this.timestamp = BigInt(+new Date());
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
