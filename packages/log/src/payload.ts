import { field, variant } from "@dao-xyz/borsh";
import { type Encoding, NO_ENCODING } from "./encoding.js";
import { equals } from "./utils.js";

@variant(0)
export class Payload<T> {
	@field({ type: Uint8Array })
	data: Uint8Array;

	encoding: Encoding<T>;

	private _value?: T;

	constructor(props: { data: Uint8Array; value?: T; encoding: Encoding<T> }) {
		this.data = props.data;
		this._value = props.value;
		this.encoding = props?.encoding;
	}

	equals(other: Payload<T>): boolean {
		return equals(this.data, other.data);
	}

	get isDecoded(): boolean {
		return this._value != null;
	}

	get value(): T {
		if (this._value == null) {
			throw new Error("Value not decoded. Invoke: .getValue once");
		}
		return this._value;
	}
	getValue(encoding: Encoding<T> = this.encoding || NO_ENCODING): T {
		if (this._value !== undefined) {
			return this._value;
		}
		return encoding.decoder(this.data);
	}

	get byteLength() {
		return this.data.byteLength;
	}
}
