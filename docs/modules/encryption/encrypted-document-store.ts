import { field, variant } from "@dao-xyz/borsh";
import { Program } from "@peerbit/program";
import { Peerbit } from "peerbit";
import { Observer, Role, Documents } from "@peerbit/document";
import { v4 as uuid } from "uuid";
import { X25519Keypair } from "@peerbit/crypto";

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

type Args = { role?: Role };

// This class extends Program which allows it to be replicated amongst peers
@variant("encrypted-document-store")
class DocumentStore extends Program<Args> {
	@field({ type: Documents })
	messages: Documents<Message>; // Documents<?> provide document store functionality around your Posts

	constructor() {
		super();
		this.messages = new Documents();
	}

	async open(args?: Args): Promise<void> {
		// We need to setup the store in the open hook
		// we can also modify properties of our store here, for example set access control
		await this.messages.open({
			type: Message,
			role: args?.role,
		});
	}
}

const client = await Peerbit.create();

const client2 = await Peerbit.create();
await client2.dial(client.getMultiaddrs());

const client3 = await Peerbit.create();
await client3.dial(client.getMultiaddrs());

const store = await client.open(new DocumentStore());

const message = new Message("Hello world!");
await store.messages.put(message, {
	encryption: {
		keypair: await X25519Keypair.create(),
		reciever: {
			// Who can read the log entry metadata (e.g. timestamps), next pointers, and more location information
			meta: [
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
	},
});

// A peer that can open
const store2 = await client2.open<DocumentStore, Args>(store.address, {
	args: { role: new Observer() },
});
await store2.waitFor(client.peerId);

const messageRetrieved = await store2.messages.index.get(message.id);

// use .getPayload() instead of .payload to decrypt the payload
expect(messageRetrieved?.message).toEqual("Hello world!");

await client.stop();
await client2.stop();
await client3.stop();
