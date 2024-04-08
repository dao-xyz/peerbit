import { field, variant, vec } from "@dao-xyz/borsh";
import { Ed25519PublicKey, PublicSignKey } from "@peerbit/crypto";
import { Program } from "@peerbit/program";
import { DString } from "@peerbit/string";
import { type PeerId } from "@libp2p/interface";

@variant("permissioned_string")
export class PermissionedString extends Program {
	@field({ type: DString })
	_store: DString;

	@field({ type: vec(PublicSignKey) })
	trusted: PublicSignKey[];

	constructor(properties?: {
		store?: DString;
		trusted: (PublicSignKey | PeerId)[];
	}) {
		super();
		this._store = properties?.store || new DString({});
		this.trusted =
			properties?.trusted.map((x) =>
				x instanceof PublicSignKey ? x : Ed25519PublicKey.fromPeerId(x)
			) || [];
	}

	get store(): DString {
		return this._store;
	}

	async open(): Promise<void> {
		await this._store.open();
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
