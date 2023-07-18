import { variant, field } from "@dao-xyz/borsh";
import { Message } from "./message.js";

@variant(0)
export abstract class NativeMessage extends Message {}

@variant(0)
export class RESP_Void extends NativeMessage {}

@variant(1)
export class RESP_Error extends NativeMessage {
	@field({ type: "string" })
	message: string;

	constructor(error: Error) {
		super();
		this.message = error.message;
	}

	private _error: Error;
	get error() {
		return this._error || (this._error = new Error(this.message));
	}
}
