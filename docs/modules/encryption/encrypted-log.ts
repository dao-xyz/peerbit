import { field, variant } from "@dao-xyz/borsh";
import { Program } from "@peerbit/program";
import { Log } from "@peerbit/log";
import { Peerbit } from "peerbit";
import { waitForResolved } from "../../../packages/utils/time/src";

// This class extends Program which allows it to be replicated amongst peers
@variant("simple_store")
class SimpleStore extends Program {
	@field({ type: Log })
	log: Log<string>; // Documents<?> provide document store functionality around your Posts

	constructor() {
		super();
		this.log = new Log();
	}

	async setup(): Promise<void> {
		// We need to setup the store in the setup hook
		// we can also modify properties of our store here, for example set access control
		await this.log.setup();
	}
}

const client = await Peerbit.create();

const client2 = await Peerbit.create();
await client2.dial(client);

const client3 = await Peerbit.create();
await client3.dial(client);

const store = await client.open(new SimpleStore());

console.log(store.log.id);
await store.log.append("Hello world!", {
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
const store2 = await client2.open<SimpleStore>(store.address);
await waitForResolved(() => expect(store2.log.length).toEqual(1));
const entry = (await store2.log.values.toArray())[0];

// use .getPayload() instead of .payload to decrypt the payload
expect((await entry.getPayload()).getValue()).toEqual("Hello world!");

await client.stop();
await client2.stop();
await client3.stop();
