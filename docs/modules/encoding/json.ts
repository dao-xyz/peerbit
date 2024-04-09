import {
	variant,
	field,
	fixedArray,
	serialize,
	deserialize
} from "@dao-xyz/borsh";
import { randomBytes } from "@peerbit/crypto";
import assert from "node:assert";

interface Message {
	title: string;
	message: string;
}

@variant(0) // V0
class Post {
	@field({ type: fixedArray("u8", 32) }) // id will always be a Uint8array with size 32
	id: Uint8Array;

	@field({ type: "string" })
	private _messageJSON: string;

	private _message: Message;

	constructor(message: Message) {
		this.id = randomBytes(32);
		this._message = message;
		this._messageJSON = JSON.stringify(this._message);
	}

	get message(): Message {
		return this._message || (this._message = JSON.parse(this._messageJSON));
	}
}
const message = new Post({
	title: "Hello world!",
	message: "This is a JSON message"
});
const bytes = serialize(message); // [0, ... ] will start with 0 because @variant(0)
const post: Post = deserialize(bytes, Post);
assert.equal(post.message.title, "Hello world!");
