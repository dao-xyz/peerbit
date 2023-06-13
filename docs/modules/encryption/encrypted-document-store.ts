import { field, variant } from "@dao-xyz/borsh";
import { Program } from "@dao-xyz/peerbit-program";
import { Log } from "@dao-xyz/peerbit-log";
import { Peerbit } from "@dao-xyz/peerbit";

// This class extends Program which allows it to be replicated amongst peers
@variant("store")
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

const store = await client.open(new SimpleStore());
/* store.log.append('hello!', {
    reciever: {
        // Who can read the log entry metadata (e.g. timestamps)
        metadata: [client.identity.publicKey, client2.identity.publicKey],

        // Who can read the references of the entry (next pointers)
        next: [client.identity.publicKey, client2.identity.publicKey],

        // Who can read the message?
        payload: [client.identity.publicKey, client2.identity.publicKey],

        // Who can read the signature ? 
        // (In order to validate entries you need to be able to read the signature)
        signatures: [client.identity.publicKey, client2.identity.publicKey]

        // Omitting any of the fields below will make it unencrypted
    }
}) */
