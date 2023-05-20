import { field, variant, vec } from "@dao-xyz/borsh";
import { PublicSignKey } from "@dao-xyz/peerbit-crypto";
import { CanTrust, Program } from "@dao-xyz/peerbit-program";
import { EventStore } from "./event-store";
import type { PeerId } from "@libp2p/interface-peer-id";
import {} from "@libp2p/peer-id";
import { Ed25519PublicKey } from "@dao-xyz/peerbit-crypto";

@variant("permissioned_program")
export class PermissionedEventStore extends Program implements CanTrust {
	@field({ type: EventStore })
	_store: EventStore<string>;

	@field({ type: vec(PublicSignKey) })
	trusted: PublicSignKey[];

	constructor(properties: {
		store?: EventStore<string>;
		trusted: (PublicSignKey | PeerId)[];
	}) {
		super();
		this._store = properties.store || new EventStore();
		this.trusted = properties.trusted.map((x) =>
			x instanceof PublicSignKey ? x : Ed25519PublicKey.from(x)
		);
	}

	isTrusted(keyHash: string): boolean | Promise<boolean> {
		for (const t of this.trusted) {
			if (t.hashcode() == keyHash) {
				return true;
			}
		}
		return false;
	}

	get store(): EventStore<string> {
		return this._store;
	}

	async setup(): Promise<void> {
		await this._store.setup();
	}
}
