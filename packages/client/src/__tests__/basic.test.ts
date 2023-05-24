import { field, variant } from "@dao-xyz/borsh";
import { Log } from "@dao-xyz/peerbit-log";
import { Program } from "@dao-xyz/peerbit-program";
import { Peerbit } from "../peer";
import { waitForResolved } from "@dao-xyz/peerbit-time";

it("default params are sufficient for dialing", async () => {
	@variant("some_store")
	class Store extends Program {
		@field({ type: Log })
		log: Log<string>;
		constructor() {
			super();
			this.log = new Log();
		}

		async setup(): Promise<void> {
			return this.log.setup();
		}
	}
	const peer = await Peerbit.create();
	const peer2 = await Peerbit.create();
	await peer.dial(peer2.libp2p.getMultiaddrs());

	const store = await peer.open(new Store());
	const store2 = await peer2.open<Store>(store.address);

	await store.log.append("hello");
	await waitForResolved(() => expect(store2.log.length).toEqual(1));

	await peer.stop();
	await peer2.stop();
});
