import { Peerbit } from "@dao-xyz/peerbit";
import { Program } from "@dao-xyz/peerbit-program";
import { PublicSignKey } from "@dao-xyz/peerbit-crypto";
import { Range, DString, StringOperation } from "@dao-xyz/peerbit-string";
import { field, variant } from "@dao-xyz/borsh";
import { Entry } from "@dao-xyz/peerbit-log";

@variant("collaborative_text") // You have to give the program a unique name
class CollaborativeText extends Program {
	@field({ type: DString })
	string: DString; // distributed string

	constructor() {
		super();
		this.string = new DString({});
	}

	async setup() {
		await this.string.setup({
			canAppend: this.canAppend,
			canRead: this.canRead,
		});
	}

	async canAppend(entry: Entry<StringOperation>): Promise<boolean> {
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
console.log(document.address); /// this address can be opened by another peer

//  ...
await document.string.add("hello", new Range({ offset: 0n, length: 5n }));
await document.string.add("world", new Range({ offset: 6n, length: 5n }));

expect(await document.string.toString()).toEqual("hello world");

await peer.stop();
