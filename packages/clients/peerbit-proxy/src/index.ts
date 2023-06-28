import { SimpleLevel } from "@peerbit/lazy-level";
import { PeerId } from "@libp2p/interface-peer-id";
import { Multiaddr, multiaddr } from "@multiformats/multiaddr";
import { Blocks } from "@peerbit/blocks-interface";
import {
	Identity,
	Ed25519PublicKey,
	Keychain,
	Ed25519Keypair,
	getPublicKeyFromPeerId,
	Keypair,
	X25519PublicKey,
	X25519Keypair,
} from "@peerbit/crypto";
import { PubSub, PublishOptions } from "@peerbit/pubsub-interface";

import { field, variant, vec, option } from "@dao-xyz/borsh";
import { PublicSignKey } from "@peerbit/crypto";
import { Address } from "@peerbit/program";
import { ProgramClient } from "@peerbit/program";

class WindowClientMessage {
	type: "peerbit";
	bytes: Uint8Array;
}

@variant(0)
abstract class Message {}

@variant(1)
class RESP_IdentityMessage extends Message {
	@field({ type: Ed25519Keypair })
	keypair: Ed25519Keypair;
}

@variant([1, 0])
class REQ_GetMultiaddrs extends Message {
	@field({ type: Ed25519Keypair })
	keypair: Ed25519Keypair;
}

@variant([1, 1])
class RESP_GetMultiAddrs extends Message {
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
}

@variant([2, 0])
class REQ_Dial extends Message {
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
@variant([2, 1])
class RESP_DIAL extends Message {}

@variant(3)
abstract class PubSubMessage extends Message {}

@variant(0)
class REQ_GetSubscribers extends PubSubMessage {
	@field({ type: "string" })
	topic: string;

	constructor(topic: string) {
		super();
		this.topic = topic;
	}
}

@variant(0)
class SubscriptionData {
	timestamp: bigint;
	data?: Uint8Array;
}
@variant(1)
class RESP_GetSubscribers extends PubSubMessage {
	@field({ type: vec("string") })
	hashes: string[];

	@field({ type: vec(SubscriptionData) })
	data: SubscriptionData;

	_map: Map<string, SubscriptionData>;
	get map() {
		if (this._map) {
			return this._map;
		}
		const map = new Map();
		for (const [i, hash] of this.hashes.entries()) {
			map.set(hash, this.data[i]);
		}
		return (this._map = map);
	}
}

@variant(2)
class REQ_RequestSubscribers extends PubSubMessage {
	@field({ type: "string" })
	topic: string;

	constructor(topic: string) {
		super();
		this.topic = topic;
	}
}

@variant(3)
class RESP_RequestSubscribers extends PubSubMessage {}

class REQ_Publish extends PubSubMessage {
	@field({ type: Uint8Array })
	data: Uint8Array;

	@field({ type: option(vec("string")) })
	topics?: string[];

	@field({ type: option(vec("string")) })
	to?: string[]; // (string | PublicSignKey | Libp2pPeerId)[];

	@field({ type: "bool" })
	strict: boolean;

	constructor(data: Uint8Array, options?: PublishOptions) {
		super();
		this.data = data;
		this.topics = options?.topics;
		this.to = options?.to?.map((x) =>
			typeof x === "string"
				? x
				: x instanceof PublicSignKey
				? x.hashcode()
				: getPublicKeyFromPeerId(x).hashcode()
		);
		this.strict = options?.strict || false;
	}
}

class RESP_Publish extends PubSubMessage {}

@variant(4)
abstract class BlocksMessage extends Message {}

@variant(0)
class REQ_PutBlock extends BlocksMessage {
	@field({ type: Uint8Array })
	bytes: Uint8Array;

	constructor(bytes: Uint8Array) {
		super();
		this.bytes = bytes;
	}
}
@variant(1)
class RESP_PutBlock extends BlocksMessage {}

@variant(2)
class REQ_GetBlock extends BlocksMessage {
	@field({ type: "string" })
	cid: string;

	constructor(cid: string) {
		super();
		this.cid = cid;
	}
}
@variant(3)
class RESP_GetBlock extends BlocksMessage {
	@field({ type: Uint8Array })
	bytes: Uint8Array;

	constructor(bytes: Uint8Array) {
		super();
		this.bytes = bytes;
	}
}

@variant(4)
class REQ_HasBlock extends BlocksMessage {
	@field({ type: "string" })
	cid: string;

	constructor(cid: string) {
		super();
		this.cid = cid;
	}
}
@variant(5)
class RESP_HasBlock extends BlocksMessage {
	@field({ type: "bool" })
	has: boolean;

	constructor(has: boolean) {
		super();
		this.has = has;
	}
}

@variant(6)
class REQ_RmBlock extends BlocksMessage {
	@field({ type: "string" })
	cid: string;

	constructor(cid: string) {
		super();
		this.cid = cid;
	}
}
@variant(7)
class RESP_RmBlock extends BlocksMessage {}

@variant(5)
abstract class KeyChanMessage extends Message {}

@variant(0)
class REQ_ImportKey extends KeyChanMessage {
	@field({ type: Ed25519Keypair })
	keypair: Ed25519Keypair;

	@field({ type: Uint8Array })
	id: Uint8Array;

	constructor(keypair: Ed25519Keypair, id: Uint8Array) {
		super();
		this.keypair = keypair;
		this.id = id;
	}
}

@variant(1)
class RESP_ImportKey extends KeyChanMessage {}

@variant(2)
class REQ_ExportKeypairByKey extends KeyChanMessage {
	@field({ type: PublicSignKey })
	publicKey: Ed25519PublicKey | X25519PublicKey;
	constructor(publicKey: Ed25519PublicKey | X25519PublicKey) {
		super();
		this.publicKey = publicKey;
	}
}

@variant(3)
class RESP_ExportKeypairByKey extends KeyChanMessage {
	@field({ type: Keypair })
	keypair: X25519Keypair | Ed25519Keypair;

	constructor(keypair: X25519Keypair | Ed25519Keypair) {
		super();
		this.keypair = keypair;
	}
}

@variant(4)
class REQ_ExportKeypairById extends KeyChanMessage {
	@field({ type: Uint8Array })
	id: Uint8Array;
	constructor(id: Uint8Array) {
		super();
		this.id = id;
	}
}

@variant(5)
class RESP_ExportKeypairById extends KeyChanMessage {
	@field({ type: Keypair })
	keypair: X25519Keypair | Ed25519Keypair;

	constructor(keypair: X25519Keypair | Ed25519Keypair) {
		super();
		this.keypair = keypair;
	}
}

@variant(6)
abstract class MemoryMessage extends Message {}

@variant(0)
class REQ_Status extends MemoryMessage {}

@variant(1)
class RESP_Status extends MemoryMessage {
	@field({ type: "string" })
	status: "opening" | "open" | "closing" | "closed";
}

@variant(2)
class REQ_Open extends MemoryMessage {}

@variant(3)
class REQ_Close extends MemoryMessage {}

@variant(4)
class REQ_Get extends MemoryMessage {
	@field({ type: "string" })
	key: string;

	constructor(key: string) {
		super();
		this.key = key;
	}
}

@variant(5)
class RESP_Get extends MemoryMessage {
	@field({ type: Uint8Array })
	bytes: Uint8Array;

	constructor(bytes: Uint8Array) {
		super();
		this.bytes = bytes;
	}
}

@variant(6)
class REQ_Put extends MemoryMessage {
	@field({ type: "string" })
	key: string;

	@field({ type: Uint8Array })
	bytes: Uint8Array;

	constructor(key: string, bytes: Uint8Array) {
		super();
		this.key = key;
		this.bytes = bytes;
	}
}

@variant(7)
class RESP_Put extends MemoryMessage {}

@variant(8)
class REQ_Sublevel extends MemoryMessage {
	@field({ type: "string" })
	name: string;
	constructor(name: string) {
		super();
		this.name = name;
	}
}

@variant(9)
class RESP_Sublevel extends MemoryMessage {}

@variant(8)
class REQ_Clear extends MemoryMessage {}

@variant(9)
class RESP_Clear extends MemoryMessage {}

@variant(10)
class REQ_Idle extends MemoryMessage {}

@variant(11)
class RESP_Idle extends MemoryMessage {}

@variant(7)
abstract class LifeCycleMessage extends Message {}

@variant(0)
class Stop extends LifeCycleMessage {}

export class PeerbitProxy implements ProgramClient {
	peerId: PeerId;
	identity: Identity<Ed25519PublicKey>;
	getMultiaddrs: () => Multiaddr[];
	dial(address: string | Multiaddr | Multiaddr[]): Promise<boolean> {
		throw new Error("Method not implemented.");
	}

	services: { pubsub: PubSub; blocks: Blocks };
	keychain?: Keychain;
	memory?: SimpleLevel;
	start(): Promise<void> {
		throw new Error("Method not implemented.");
	}
	stop(): Promise<void> {
		throw new Error("Method not implemented.");
	}
	async open<T>(thing: T | Address, args): Promise<T> {
		throw new Error("Method not implemented.");
	}

	constructor(
		readonly properties: {
			subscribe: (message: Message) => void;
			publish: (message: Message) => void;
		}
	) {}

	onMessage(message: Message) {
		// TODO
	}
}
