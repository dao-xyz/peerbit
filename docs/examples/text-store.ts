import { field, variant } from "@dao-xyz/borsh";
import { PublicSignKey } from "@peerbit/crypto";
import { Program } from "@peerbit/program";
import {
	DString,
	Range,
	StringOperation,
	type TransactionContext,
} from "@peerbit/string";
import assert from "node:assert";
import { Peerbit } from "peerbit";

@variant("collaborative_text") // You have to give the program a unique name
class CollaborativeText extends Program {
	@field({ type: DString })
	string: DString; // distributed string

	constructor() {
		super();
		this.string = new DString({});
	}

	async open() {
		await this.string.open({
			canPerform: this.canPerform,
			canRead: this.canRead,
		});
	}

	async canPerform(
		operation: StringOperation,
		context: TransactionContext,
	): Promise<boolean> {
		// .. acl logic writers
		return true;
	}

	async canRead(identity?: PublicSignKey): Promise<boolean> {
		// .. acl logic for readers
		return true;
	}
}

// ...

const peer = await Peerbit.create();
const document = await peer.open(new CollaborativeText());
console.log(document.address!.toString()); /// this address can be opened by another peer

//  ...
await document.string.add("hello", new Range({ offset: 0n, length: 5n }));
await document.string.add("world", new Range({ offset: 6n, length: 5n }));

assert.equal(await document.string.getValue(), "hello world");

await peer.stop();
