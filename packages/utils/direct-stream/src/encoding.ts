import type { PeerId } from '@libp2p/interface-peer-id'
import { variant, vec, field, serialize, deserialize, fixedArray, option } from "@dao-xyz/borsh";
import crypto from 'crypto';
import * as utf8 from './utf8.js';
import { equals } from 'uint8arrays'
import { Uint8ArrayList } from 'uint8arraylist'

export const ID_LENGTH = 32;
export abstract class Message {


	static deserialize(bytes: Uint8ArrayList) {
		if (bytes.get(0) === 0) // Data
		{
			return DataMessage.deserialize(bytes)
		}
		else if (bytes.get(0) === 1) // heartbeat
		{
			return Heartbeat.deserialize(bytes)
		}
		throw new Error("Unsupported")
	}

	abstract serialize(): Uint8ArrayList | Uint8Array
	abstract equals(other: Message): boolean;

}

export function writeUInt32LE(value: number, buf: Uint8Array, offset: number) {
	buf[offset++] = value;
	value = value >>> 8;
	buf[offset++] = value;
	value = value >>> 8;
	buf[offset++] = value;
	value = value >>> 8;
	buf[offset++] = value;
}


export const readUInt32LE = (buffer: Uint8ArrayList, offset: number) => {
	const first = buffer.get(offset);
	const last = buffer.get(offset + 3);
	if (first === undefined || last === undefined)
		throw new Error('Out of bounds');

	return first +
		buffer.get(++offset) * 2 ** 8 +
		buffer.get(++offset) * 2 ** 16 +
		last * 2 ** 24;
}



// I pack data with this message
export class DataMessage extends Message {

	private _id: Uint8Array | (() => Uint8Array)
	private _to: string[]
	private _data: Uint8Array | (() => Uint8Array)

	constructor(properties: { id?: Uint8Array | (() => Uint8Array), data: Uint8Array | (() => Uint8Array), to?: (string | PeerId)[] }) {
		super();
		this._data = properties.data;
		this._id = properties.id || crypto.randomBytes(ID_LENGTH);
		this._to = properties?.to?.map(t => typeof t === 'string' ? t : t.toString()) || [];
		if (this.to.length > 255) {
			throw new Error("Can send to at most 255 peers")
		}
	}

	get id(): Uint8Array {
		if (typeof this._id === 'function') {
			this._id = this._id();
		}
		return this._id;
	}
	get data(): Uint8Array {
		if (typeof this._data === 'function') {
			this._data = this._data();
		}
		return this._data;
	}
	get to(): string[] {
		return this._to;
	}
	_serialized: Uint8ArrayList;

	/** Manually ser/der for performance gains */
	serialize() {
		if (this._serialized) {
			return this._serialized;
		}

		let totalToLengths = 0;
		let toLengths: number[] = new Array(this.to.length)
		for (const [i, to] of this.to.entries()) {
			const len = utf8.length(to);
			totalToLengths += len;
			toLengths[i] = len;
		}
		const array = new Uint8Array(1 + ID_LENGTH + 1 + totalToLengths + 4 * this.to.length + this.data.length,)
		array.set(this.id, 1);
		array[ID_LENGTH + 1] = this.to.length; // 255 limit
		let offset = ID_LENGTH + 2;
		for (const [i, to] of this.to.entries()) {
			writeUInt32LE(toLengths[i], array, offset)
			offset += 4;
			offset += utf8.write(to, array, offset);
		}
		array.set(this.data, offset);
		return array
	}
	static deserialize(bytes: Uint8ArrayList): DataMessage {
		if (bytes.get(0) !== 0) {
			throw new Error("Unsupported")
		}
		let tosSize = bytes.get(ID_LENGTH + 1);
		let offset = ID_LENGTH + 2;
		let tos: string[] = new Array(tosSize);
		for (let i = 0; i < tosSize; i++) {
			let toSize = readUInt32LE(bytes, offset)
			offset += 4;
			tos[i] = utf8.read(bytes, offset, offset + toSize);
			offset += toSize;
		}
		const ret = new DataMessage({ data: () => bytes.slice(offset), id: () => bytes.slice(1, ID_LENGTH + 1), to: tos });
		ret._serialized = bytes;
		return ret;
	}

	equals(other: Message) {
		if (other instanceof DataMessage) {
			const a = equals(this.data, other.data) && equals(this.id, other.id) && this.to.length === other.to.length;
			if (!a) {
				return false;
			}
			for (let i = 0; i < this.to.length; i++) {
				if (this.to[i] !== other.to[i]) {
					return false;
				}
			}
			return true;
		}
		return false;
	}
}

// I send this too all my peers
@variant(1)
export class Heartbeat extends Message {

	@field({ type: fixedArray('u32', ID_LENGTH) })
	id: number[] | Uint8Array

	@field({ type: vec('string') })
	trace: string[]

	@field({ type: option(Uint8Array) })
	data?: Uint8Array

	constructor(options?: { data?: Uint8Array }) {
		super();
		this.id = crypto.randomBytes(ID_LENGTH);
		this.trace = [];
		this.data = options?.data;

	}

	serialize() {
		return serialize(this)
	}
	static deserialize(bytes: Uint8ArrayList): Heartbeat {
		return deserialize(bytes.subarray(), Heartbeat)
	}

	equals(other: Message) {
		if (other instanceof Heartbeat) {
			const a = (!!this.data && !!other.data && equals(this.data, other.data) || !this.data === !other.data) && equals(new Uint8Array(this.id), new Uint8Array(other.id)) && this.trace.length === other.trace.length;
			if (!a) {
				return false;
			}
			for (let i = 0; i < this.trace.length; i++) {
				if (this.trace[i] !== other.trace[i]) {
					return false;
				}
			}
			return true;
		}
		return false;
	}
}
/* 
// Some of my peer disconnected
@variant(2)
export class ConnectionClosed extends Message {
	@field({ type: fixedArray('u32', 32) })
	id: number[] | Uint8Array

	// Disconnected Peer id
	@field({ type: 'string' })
	disconnected: string;

	@field({ type: fixedArray('u32', 32) })
	session: number[] | Uint8Array

	@field({ type: vec('string') })
	trace: string[]

	@field({ type: vec(SignatureWithKey) })
	signatures: SignatureWithKey[]

	@field({ type: option(Uint8Array) })
	data?: Uint8Array

	constructor(disconnected: PeerId | string, session: Uint8Array, data?: Uint8Array) {
		super();
		this.id = crypto.randomBytes(32);
		this.trace = [];
		this.session = session;
		this.disconnected = disconnected.toString();
		this.data = data;
		this.signatures = [];
	}
}




// Share connections
@variant(3)
export class Connections extends Message {

	@field({ type: vec(fixedArray('string', 2)) })
	connections: [string, string][]

	constructor(connections: [string, string][]) {
		super();

		this.connections = connections;
	}
} */