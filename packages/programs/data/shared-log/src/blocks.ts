import { field, variant } from "@dao-xyz/borsh";
import {
	BlockRequest,
	BlockResponse,
	BlockMessage as IBlockMessage,
} from "@peerbit/blocks";
import { TransportMessage } from "./message.js";

@variant([2, 0])
export class BlocksMessage extends TransportMessage {
	@field({ type: IBlockMessage })
	message: BlockRequest | BlockResponse;

	constructor(message: BlockRequest | BlockResponse) {
		super();
		this.message = message;
	}
}
