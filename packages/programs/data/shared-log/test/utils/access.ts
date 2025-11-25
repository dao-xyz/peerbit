import { field, variant } from "@dao-xyz/borsh";
import { Program } from "@peerbit/program";
import { EventStore } from "./stores/index.js";

@variant("test_simple")
export class SimpleStoreContract extends Program {
	@field({ type: EventStore })
	store!: EventStore<string, any>;

	constructor(properties?: { store: EventStore<string, any> }) {
		super();
		if (properties) {
			this.store = properties.store;
		}
	}
	open(option?: any): Promise<void> {
		return this.store.open();
	}
}
