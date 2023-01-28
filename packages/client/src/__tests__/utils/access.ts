import { variant, field } from "@dao-xyz/borsh";
import { EventStore } from "./stores";
import { Program } from "@dao-xyz/peerbit-program";

@variant("test_simple")
export class SimpleStoreContract extends Program {
	@field({ type: EventStore })
	store: EventStore<string>;

	constructor(properties?: { store: EventStore<string> }) {
		super();
		if (properties) {
			this.store = properties.store;
		}
	}
	setup(option?: any): Promise<void> {
		return this.store.setup();
	}
}
