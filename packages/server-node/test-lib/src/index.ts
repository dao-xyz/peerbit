import { field, variant, vec } from "@dao-xyz/borsh";
import { Ed25519PublicKey, PublicSignKey } from "@dao-xyz/peerbit-crypto";
import { CanTrust, Program } from "@dao-xyz/peerbit-program";
import { DString } from "@dao-xyz/peerbit-string";
import { PeerId } from "@libp2p/interface-peer-id";

@variant("permissioned_string")
export class PermissionedString extends Program implements CanTrust {
	@field({ type: DString })
	_store: DString;

	@field({ type: vec(PublicSignKey) })
	trusted: PublicSignKey[];

	constructor(properties?: {
		store?: DString;
		trusted: (PublicSignKey | PeerId)[];
	}) {
		super();
		if (properties) {
			this._store = properties.store || new DString({});
			this.trusted = properties.trusted.map((x) =>
				x instanceof PublicSignKey ? x : Ed25519PublicKey.from(x)
			);
		}
	}

	get store(): DString {
		return this._store;
	}

	async setup(): Promise<void> {
		await this._store.setup();
	}

	isTrusted(keyHash: string): boolean | Promise<boolean> {
		for (const t of this.trusted) {
			if (t.hashcode() == keyHash) {
				return true;
			}
		}
		return false;
	}
}
