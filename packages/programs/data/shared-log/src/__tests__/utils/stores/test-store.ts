import { field, variant, vec } from "@dao-xyz/borsh";
import { PublicSignKey } from "@peerbit/crypto";
import { CanTrust, Program } from "@peerbit/program";
import { EventStore } from "./event-store";
import type { PeerId } from "@libp2p/interface-peer-id";
import { Ed25519PublicKey } from "@peerbit/crypto";
import { Role } from "../../../role.js";
import { TrimOptions } from "@peerbit/log";

export type SetupOptions = {
	role?: Role;
	trim?: TrimOptions;
	minReplicas?: number;
};

@variant("permissioned_program")
export class PermissionedEventStore
	extends Program<SetupOptions>
	implements CanTrust
{
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
			x instanceof PublicSignKey ? x : Ed25519PublicKey.fromPeerId(x)
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

	async open(options?: SetupOptions): Promise<void> {
		await this._store.open(options);
	}
}
