import { variant, vec, field, serialize, deserialize, fixedArray, option } from "@dao-xyz/borsh";
import { equals } from 'uint8arrays'
import { Uint8ArrayList } from 'uint8arraylist'
import { Uint8ArrayView, viewAsArray } from './view.js';
import { PublicSignKey, SignatureWithKey, verify } from '@dao-xyz/peerbit-crypto';
import crypto from 'crypto';

let concatBytes: (arr: (Uint8Array)[], totalLength: number) => Uint8Array;
if ((globalThis as any).Buffer) {
	concatBytes = ((globalThis as any).Buffer.concat)
}
else {
	concatBytes = (arrays, length) => {
		if (length == null) {
			let length = 0;
			for (const element of arrays) {
				length += element.length;
			}
		}
		const output = new Uint8Array(length)
		let offset = 0
		for (const arr of arrays) {
			output.set(arr, offset)
			offset += arr.length
		}
		return output;
	}
}



export const ID_LENGTH = 32;

interface MessagHeader {
	get timestamp(): bigint;
}

const verifyHeader = (header: MessagHeader, options: { maxAhead: number, maxBehind: number } = { maxAhead: 100 * 1000, maxBehind: 300 * 1e3 }) => {
	const timestamp = header.timestamp;
	const thisTime = +new Date;
	if (timestamp > thisTime + options.maxAhead || timestamp < thisTime - options.maxBehind) {
		return false;
	}
	return true;
}

@variant(0)
export class Signatures {

	@field({ type: vec(SignatureWithKey, 'u8') })
	signatures: SignatureWithKey[]
	constructor(signatures: SignatureWithKey[] = []) {
		this.signatures = signatures;
	}

	equals(other: Signatures) {
		return this.signatures.every((value, i) => other.signatures[i].equals(value))
	}
}

interface Signed {
	get signatures(): Signatures
}

const verifyMultiSig = (message: Signed & Prefixed) => {
	const signatures = message.signatures.signatures;

	if (signatures == null) {
		throw new Error("Unexpected")
	}

	const dataGenerator = getMultiSigDataToSignHistory(message, 0);
	let done: boolean | undefined = false;
	for (const signature of signatures) {
		if (done) {
			throw new Error("Unexpected, the amount of signatures does not match the amount of data verify")
		}
		const data = dataGenerator.next();
		done = data.done;
		if (!verify(signature.signature, signature.publicKey, data.value!)) {
			return false;
		}
	}
	return true;
}
interface Prefixed {
	prefix: Uint8Array
}


const emptySignatures = serialize(new Signatures());
function* getMultiSigDataToSignHistory(message: Signed & Prefixed, from = 0): Generator<Uint8Array, undefined, void> {
	if (from === 0) {
		yield concatBytes([message.prefix, emptySignatures], message.prefix.length + emptySignatures.length);
	}

	const signatures = message.signatures.signatures;
	for (let i = Math.max(from, 1); i < signatures.length + 1; i++) {
		const bytes = serialize(new Signatures(signatures.slice(0, i))) // TODO make more performant 
		yield concatBytes([message.prefix, bytes], message.prefix.length + bytes.length);
	}
	return
}

export abstract class Message {


	static deserialize(bytes: Uint8ArrayList) {
		if (bytes.get(0) === 0) // Data
		{
			return DataMessage.deserialize(bytes)
		}
		else if (bytes.get(0) === 1) // heartbeat
		{
			return Hello.deserialize(bytes)
		}
		else if (bytes.get(0) === 2) // heartbeat
		{
			return Goodbye.deserialize(bytes)
		}
		else if (bytes.get(0) === 3) // Connections
		{
			return NetworkInfo.deserialize(bytes)
		}
		throw new Error("Unsupported")
	}

	abstract serialize(): Uint8ArrayList | Uint8Array
	abstract equals(other: Message): boolean;
	abstract verify(): boolean


}



@variant(0)
class DataMessageHeader {

	@field({ type: fixedArray('u8', ID_LENGTH) })
	private _id: Uint8Array | Uint8ArrayView

	@field({ type: 'u64' })
	private _timestamp: bigint

	@field({ type: vec(PublicSignKey) })
	private _to: PublicSignKey[]


	constructor(properties: { timestamp?: bigint, id?: Uint8Array, signatures?: Signatures, to?: PublicSignKey[] }) {
		this._id = properties.id || crypto.randomBytes(ID_LENGTH);
		this._to = properties?.to || [];
		this._timestamp = properties.timestamp || BigInt(+new Date);
	}

	get id() {
		return this._id;
	}

	get timestamp() {
		return this._timestamp;
	}

	get to() {
		return this._to;
	}
}


// I pack data with this message
const DATA_VARIANT = 0;
@variant(DATA_VARIANT)
export class DataMessage extends Message {

	@field({ type: DataMessageHeader })
	_header: DataMessageHeader

	@field({ type: Uint8Array })
	private _data: Uint8Array | Uint8ArrayView

	@field({ type: Signatures })
	private _signatures: Signatures

	constructor(properties: { to?: PublicSignKey[], data: Uint8Array | Uint8ArrayView, signatures?: Signatures }) {
		super();
		this._data = properties.data;
		this._header = new DataMessageHeader({ to: properties.to });
		this._signatures = properties.signatures || new Signatures()
		if (this.to.length > 255) {
			throw new Error("Can send to at most 255 peers")
		}
	}

	get id(): Uint8Array | Uint8ArrayView {
		return this._header.id;
	}

	get signatures(): Signatures {
		return this._signatures;
	}


	get to(): PublicSignKey[] {
		return this._header.to;
	}
	set to(to: PublicSignKey[]) {
		this._serialized = undefined;
		this.to = to;
	}


	get data(): Uint8Array | Uint8ArrayView {
		return this._data;
	}

	get dataBytes(): Uint8Array {
		const array = viewAsArray(this._data)
		this._data = array;
		return array;
	}


	_serialized: Uint8Array | undefined;
	get serialized(): Uint8Array | undefined {
		return this.serialized;
	}

	_prefix: Uint8Array | undefined
	get prefix(): Uint8Array {
		if (this._prefix)
			return this._prefix
		const headerSer = serialize(this._header);
		const hashBytes = crypto.createHash('sha256').update(this.dataBytes).digest();
		this._prefix = concatBytes([new Uint8Array([DATA_VARIANT]), headerSer, hashBytes], 1 + headerSer.length + hashBytes.length);
		return this._prefix;
	}

	sign(sign: (bytes: Uint8Array) => SignatureWithKey) {
		this._serialized = undefined; // because we will change this object, so the serialized version will not be applicable anymore
		this.signatures.signatures.push(sign(getMultiSigDataToSignHistory(this, this.signatures.signatures.length).next().value!));
	}

	verify(): boolean {
		return verifyHeader(this._header) && verifyMultiSig(this)
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
			throw new Error("Unsupported")
		}
		const arr = bytes.subarray()
		const ret = deserialize(arr, DataMessage);
		ret._serialized = arr;
		return ret;
	}

	equals(other: Message) {
		if (other instanceof DataMessage) {
			const a = equals(viewAsArray(this.data), viewAsArray(other.data)) && equals(viewAsArray(this.id), viewAsArray(other.id)) && this.to.length === other.to.length;
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
class HelloHeader {

	@field({ type: fixedArray('u8', ID_LENGTH) })
	id: Uint8Array

	@field({ type: 'u64' })
	timestamp: bigint

	constructor() {
		this.id = crypto.randomBytes(ID_LENGTH);
		this.timestamp = BigInt(+new Date);
	}
}


// I send this too all my peers
const HELLO_VARIANT = 1;
@variant(HELLO_VARIANT)
export class Hello extends Message {

	@field({ type: HelloHeader })
	header: HelloHeader

	@field({ type: option(Uint8Array) })
	data?: Uint8Array

	@field({ type: Signatures })
	signatures: Signatures

	constructor(options?: { data?: Uint8Array }) {
		super();
		this.header = new HelloHeader();
		this.data = options?.data;
		this.signatures = new Signatures()

	}

	serialize() {
		return serialize(this)
	}
	static deserialize(bytes: Uint8ArrayList): Hello {
		return deserialize(bytes.subarray(), Hello)
	}


	_prefix: Uint8Array | undefined
	get prefix(): Uint8Array {
		if (this._prefix)
			return this._prefix
		const headerSer = serialize(this.header);
		const hashBytes = this.data ? crypto.createHash('sha256').update(this.data).digest() : new Uint8Array();
		this._prefix = concatBytes([new Uint8Array([HELLO_VARIANT]), headerSer, hashBytes], 1 + headerSer.length + hashBytes.length);
		return this._prefix;
	}

	sign(sign: (bytes: Uint8Array) => SignatureWithKey) {
		const toSign = getMultiSigDataToSignHistory(this, this.signatures.signatures.length).next().value!;
		this.signatures.signatures.push(sign(toSign));
		return this;
	}

	verify(): boolean {
		return verifyHeader(this.header) && verifyMultiSig(this)
	}



	equals(other: Message) {
		if (other instanceof Hello) {
			const dataEquals = (!!this.data && !!other.data && equals(this.data, other.data) || !this.data === !other.data)
			if (!dataEquals) {
				return false;
			}
			if (!equals(this.header.id, other.header.id) || this.header.timestamp !== other.header.timestamp)
				return false;

			return this.signatures.equals(other.signatures);
		}
		return false;
	}
}


@variant(0)
class GoodbyeHeader {

	@field({ type: fixedArray('u8', ID_LENGTH) })
	id: Uint8Array

	@field({ type: 'u64' })
	timestamp: bigint

	constructor() {
		this.id = crypto.randomBytes(ID_LENGTH);
		this.timestamp = BigInt(+new Date);
	}
}



// Me or some my peer disconnected
const GOODBYE_VARIANT = 2;
@variant(GOODBYE_VARIANT)
export class Goodbye extends Message {

	@field({ type: GoodbyeHeader })
	header: GoodbyeHeader

	@field({ type: 'bool' })
	early?: boolean; // is early goodbye, I send this to my peers so when I disconnect, they can relay the message for me

	@field({ type: option(Uint8Array) })
	data?: Uint8Array


	@field({ type: Signatures })
	signatures: Signatures


	constructor(properties?: { header?: GoodbyeHeader, data?: Uint8Array, early?: boolean }) { // disconnected: PeerId | string,
		super();
		this.header = properties?.header || new GoodbyeHeader();
		this.data = properties?.data;
		this.early = properties?.early;
		this.signatures = new Signatures()

	}

	get disconnected(): PublicSignKey | undefined {
		return this.signatures.signatures[0]?.publicKey
	}

	serialize() {
		return serialize(this)
	}
	static deserialize(bytes: Uint8ArrayList): Goodbye {
		return deserialize(bytes.subarray(), Goodbye)
	}


	_prefix: Uint8Array | undefined
	get prefix(): Uint8Array {
		if (this._prefix)
			return this._prefix
		const headerSer = serialize(this.header);
		const hashBytes = this.data ? crypto.createHash('sha256').update(this.data).digest() : new Uint8Array();
		this._prefix = concatBytes([new Uint8Array([GOODBYE_VARIANT]), headerSer, new Uint8Array([this.early ? 1 : 0]), hashBytes], 1 + headerSer.length + 1 + hashBytes.length);
		return this._prefix;
	}

	sign(sign: (bytes: Uint8Array) => SignatureWithKey) {
		this.signatures.signatures.push(sign(getMultiSigDataToSignHistory(this, this.signatures.signatures.length).next().value!));
		return this;
	}

	verify(): boolean {
		return verifyHeader(this.header) && verifyMultiSig(this)
	}


	equals(other: Message) {
		if (other instanceof Goodbye) {
			if (this.early !== other.early) {
				return false;
			}

			const dataEquals = (!!this.data && !!other.data && equals(this.data, other.data) || !this.data === !other.data)
			if (!dataEquals) {
				return false;
			}
			if (!equals(this.header.id, other.header.id) || this.header.timestamp !== other.header.timestamp)
				return false;

			return this.signatures.equals(other.signatures);

		}
		return false;
	}

}



@variant(0)
export class NetworkInfoHeader {
	@field({ type: fixedArray('u8', 32) })
	id: Uint8Array

	@field({ type: 'u64' })
	timestamp: bigint;

	constructor() {
		this.id = crypto.randomBytes(ID_LENGTH);
		this.timestamp = BigInt(+new Date);
	}

}


@variant(0)
export class Connections {
	@field({ type: vec(fixedArray('string', 2)) })
	connections: [string, string][]

	constructor(connections: [string, string][]) {
		this.connections = connections
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
const NETWORK_INFO_VARIANT = 3;
@variant(NETWORK_INFO_VARIANT)
export class NetworkInfo extends Message {

	@field({ type: NetworkInfoHeader })
	header: NetworkInfoHeader;

	@field({ type: Connections })
	connections: Connections;


	@field({ type: Signatures })
	signatures: Signatures


	constructor(connections: [string, string][]) {
		super();
		this.header = new NetworkInfoHeader();
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
		return verifyHeader(this.header) && verifyMultiSig(this)
	}


	serialize() {
		return serialize(this)
	}
	static deserialize(bytes: Uint8ArrayList): NetworkInfo {
		return deserialize(bytes.subarray(), NetworkInfo)
	}

	equals(other: Message) {
		if (other instanceof NetworkInfo) {
			if (!equals(this.header.id, other.header.id) || this.header.timestamp !== other.header.timestamp) { // TODO fix uneccessary copy
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

