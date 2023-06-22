import { variant, field } from "@dao-xyz/borsh";
import { Program } from "@peerbit/program";
import { randomBytes, sha256Base64Sync } from "@peerbit/crypto";

// TODO: generalize the Iterator functions and spin to its own module
export interface Operation<T> {
	op: string;
	key?: string;
	value?: T;
}

@variant("event_store")
export class EventStore extends Program {
	@field({ type: Uint8Array })
	id: Uint8Array;

	log: { hash: string; data: Uint8Array }[];

	constructor(properties?: { id: Uint8Array }) {
		super();
		this.id = properties?.id || randomBytes(32);
		this.log = [];
	}

	setup() {
		return;
	}

	add(data: Uint8Array) {
		this.log.push({ hash: sha256Base64Sync(data), data: data });
	}

	async get(hash: string) {
		return this.log.find((x) => x.hash === hash);
	}

	async iterator(options?: any) {
		return this.log;
	}
}
