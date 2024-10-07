import { deserialize, field, serialize, variant, vec } from "@dao-xyz/borsh";
import type { PeerId } from "@libp2p/interface";
import { type Multiaddr, multiaddr } from "@multiformats/multiaddr";
import {
	Ed25519Keypair,
	Ed25519PublicKey,
	type Identity,
	PublicSignKey,
	getPublicKeyFromPeerId,
} from "@peerbit/crypto";
import { Message } from "./message.js";

@variant(1)
export abstract class NetworkMessage extends Message {}

@variant(0)
export class REQ_PeerId extends NetworkMessage {}

@variant(1)
export class RESP_PeerId extends NetworkMessage {
	@field({ type: PublicSignKey })
	private _publicKey: PublicSignKey;

	private _peerId: PeerId;
	get peerId(): PeerId {
		return this._peerId || (this._peerId = this._publicKey.toPeerId());
	}
	constructor(peerId: PeerId) {
		super();
		this._publicKey = getPublicKeyFromPeerId(peerId);
	}
}

@variant(2)
export class REQ_Identity extends NetworkMessage {}

@variant(3)
export class RESP_Identity extends NetworkMessage {
	@field({ type: Uint8Array })
	private _keypair: Uint8Array;

	constructor(identity: Ed25519Keypair) {
		super();
		this._keypair = serialize(identity);
	}

	private _identity: Identity<Ed25519PublicKey>;
	get identity(): Identity<Ed25519PublicKey> {
		return (
			this._identity ||
			(this._identity = deserialize(this._keypair, Ed25519Keypair))
		);
	}
}

@variant(4)
export class REQ_GetMultiaddrs extends NetworkMessage {}

@variant(5)
export class RESP_GetMultiAddrs extends NetworkMessage {
	@field({ type: vec(Uint8Array) })
	private _addrs: Uint8Array[];

	private _multiaddr: Multiaddr[];
	get multiaddr(): Multiaddr[] {
		if (this._multiaddr) {
			return this._multiaddr;
		}

		const ret: Multiaddr[] = [];
		for (const bytes of this._addrs) {
			ret.push(multiaddr(bytes));
		}
		return (this._multiaddr = ret);
	}

	constructor(addrs: Multiaddr[]) {
		super();
		this._addrs = addrs.map((x) => x.bytes);
	}
}

@variant(6)
export class REQ_Dial extends NetworkMessage {
	@field({ type: vec(Uint8Array) })
	private _addrs: Uint8Array[];

	constructor(address: string | Multiaddr | Multiaddr[]) {
		super();
		let addresses: Uint8Array[] = [];
		if (typeof address === "string") {
			addresses.push(multiaddr(address).bytes);
		} else if (Array.isArray(address)) {
			addresses = address.map((x) => x.bytes);
		} else {
			addresses.push(address.bytes);
		}
		this._addrs = addresses;
	}

	private _multiaddr: Multiaddr[];
	get multiaddr(): Multiaddr[] {
		if (this._multiaddr) {
			return this._multiaddr;
		}

		const ret: Multiaddr[] = [];
		for (const bytes of this._addrs) {
			ret.push(multiaddr(bytes));
		}
		return (this._multiaddr = ret);
	}
}
@variant(7)
export class RESP_DIAL extends NetworkMessage {
	@field({ type: "bool" })
	value: boolean;

	constructor(value: boolean) {
		super();
		this.value = value;
	}
}
