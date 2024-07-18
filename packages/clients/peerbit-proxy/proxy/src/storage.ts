import { field, variant } from "@dao-xyz/borsh";
import * as api from "@peerbit/any-store-interface/messages";
import { Message } from "./message.js";

export { api };
@variant(10)
export class StorageMessage<T extends api.MemoryRequest> extends Message {
	@field({ type: api.MemoryRequest })
	message: T; // [] means root, ['x'] means sublevel named 'x'

	constructor(request: T) {
		super();
		this.message = request;
	}
}
