import { variant, field, fixedArray } from "@dao-xyz/borsh";

import { randomBytes } from "@peerbit/crypto";

@variant(0)
export abstract class Message {
	@field({ type: fixedArray("u8", 32) })
	messageId: Uint8Array;

	constructor(messageId?: Uint8Array) {
		this.messageId = messageId || randomBytes(32);
	}
}
