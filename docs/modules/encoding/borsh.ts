import {
	variant,
	field,
	fixedArray,
	serialize,
	deserialize,
} from "@dao-xyz/borsh";
import { randomBytes } from "@peerbit/crypto";
abstract class AbstractPost {}

@variant(0) // V0
class PostV0 extends AbstractPost {
	@field({ type: fixedArray("u8", 32) }) // id will always be a Uint8array with size 32
	id: Uint8Array;

	@field({ type: "string" })
	message: string;

	constructor(message: string) {
		super();
		this.id = randomBytes(32);
		this.message = message;
	}
}

@variant(1) // V1
class PostV1 extends AbstractPost {
	@field({ type: fixedArray("u8", 32) }) // id will always be a Uint8array with size 32
	id: Uint8Array;

	@field({ type: "string" })
	title: string;

	@field({ type: "string" })
	message: string;

	constructor(title: string, message: string) {
		super();
		this.id = randomBytes(32);
		this.title = title;
		this.message = message;
	}
}

const message = new PostV0("Hello world!");
const bytes = serialize(message); // [0, ... ] will start with 0 because @variant(0)
const post: AbstractPost = deserialize(bytes, AbstractPost);

if (post instanceof PostV0) {
	// To OLD behaviour
} else post instanceof PostV1;
{
	// Do V1 behaviour
}
