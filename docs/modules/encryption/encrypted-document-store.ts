import { field, variant } from "@dao-xyz/borsh";
import { Observer, Program } from "@dao-xyz/peerbit-program";
import { Peerbit } from "@dao-xyz/peerbit";
import { Documents } from "@dao-xyz/peerbit-document";
import { v4 as uuid } from "uuid";

class Message {
	@field({ type: "string" })
	id: string;

	@field({ type: "string" })
	message: string;

	constructor(message: string) {
		this.id = uuid();
		this.message = message;
	}
}
// This class extends Program which allows it to be replicated amongst peers

@variant("encrypted-document-store")
class DocumentStore extends Program {
	@field({ type: Documents })
	messages: Documents<Message>; // Documents<?> provide document store functionality around your Posts

	constructor() {
		super();
		this.messages = new Documents();
	}

	async setup(): Promise<void> {
		// We need to setup the store in the setup hook
		// we can also modify properties of our store here, for example set access control
		await this.messages.setup({
			type: Message,
		});
	}
}

const client = await Peerbit.create();

const client2 = await Peerbit.create();
await client2.dial(client);

const client3 = await Peerbit.create();
await client3.dial(client);

const store = await client.open(new DocumentStore());

const message = new Message("Hello world!");
await store.messages.put(message, {
	reciever: {
		// Who can read the log entry metadata (e.g. timestamps)
		metadata: [
			client.identity.publicKey,
			client2.identity.publicKey,
			client3.identity.publicKey,
		],

		// Who can read the references of the entry (next pointers)
		next: [
			client.identity.publicKey,
			client2.identity.publicKey,
			client3.identity.publicKey,
		],

		// Who can read the message?
		payload: [client.identity.publicKey, client2.identity.publicKey],

		// Who can read the signature ?
		// (In order to validate entries you need to be able to read the signature)
		signatures: [
			client.identity.publicKey,
			client2.identity.publicKey,
			client3.identity.publicKey,
		],

		// Omitting any of the fields below will make it unencrypted
	},
});

// A peer that can open
const store2 = await client2.open<DocumentStore>(store.address, {
	role: new Observer(),
});
await store2.waitFor(client.libp2p);

const messageRetrieved = await store2.messages.index.get(message.id);

// use .getPayload() instead of .payload to decrypt the payload
expect(messageRetrieved?.message).toEqual("Hello world!");

await client.stop();
await client2.stop();
await client3.stop();
