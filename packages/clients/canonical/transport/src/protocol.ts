import { field, option, variant, vec } from "@dao-xyz/borsh";

export abstract class CanonicalFrame {}

@variant(0)
export class CanonicalControlRequest extends CanonicalFrame {
	@field({ type: "u32" })
	id: number;

	@field({ type: "string" })
	op: string;

	@field({ type: option("string") })
	name?: string;

	@field({ type: option(Uint8Array) })
	payload?: Uint8Array;

	constructor(properties: {
		id: number;
		op: string;
		name?: string;
		payload?: Uint8Array;
	}) {
		super();
		this.id = properties.id;
		this.op = properties.op;
		this.name = properties.name;
		this.payload = properties.payload;
	}
}

@variant(1)
export class CanonicalControlResponse extends CanonicalFrame {
	@field({ type: "u32" })
	id: number;

	@field({ type: "bool" })
	ok: boolean;

	@field({ type: option("string") })
	error?: string;

	@field({ type: option("string") })
	peerId?: string;

	@field({ type: option("u32") })
	channelId?: number;

	// Optional op-specific payload (e.g. serialized PublicSignKey / SignatureWithKey).
	@field({ type: option(Uint8Array) })
	payload?: Uint8Array;

	// Optional op-specific strings (e.g. multiaddrs).
	@field({ type: option(vec("string")) })
	strings?: string[];

	constructor(properties: {
		id: number;
		ok: boolean;
		error?: string;
		peerId?: string;
		channelId?: number;
		payload?: Uint8Array;
		strings?: string[];
	}) {
		super();
		this.id = properties.id;
		this.ok = properties.ok;
		this.error = properties.error;
		this.peerId = properties.peerId;
		this.channelId = properties.channelId;
		this.payload = properties.payload;
		this.strings = properties.strings;
	}
}

@variant(4)
export class CanonicalSignRequest {
	@field({ type: Uint8Array })
	data: Uint8Array;

	@field({ type: option("u8") })
	prehash?: number;

	constructor(properties: { data: Uint8Array; prehash?: number }) {
		this.data = properties.data;
		this.prehash = properties.prehash;
	}
}

@variant(5)
export class CanonicalBootstrapRequest {
	@field({ type: vec("string") })
	addresses: string[];

	constructor(properties?: { addresses?: string[] }) {
		this.addresses = properties?.addresses ?? [];
	}
}

@variant(6)
export class CanonicalLoadProgramRequest {
	@field({ type: option("u32") })
	timeoutMs?: number;

	constructor(properties?: { timeoutMs?: number }) {
		this.timeoutMs = properties?.timeoutMs;
	}
}

@variant(2)
export class CanonicalChannelMessage extends CanonicalFrame {
	@field({ type: "u32" })
	channelId: number;

	@field({ type: Uint8Array })
	payload: Uint8Array;

	constructor(properties: { channelId: number; payload: Uint8Array }) {
		super();
		this.channelId = properties.channelId;
		this.payload = properties.payload;
	}
}

@variant(3)
export class CanonicalChannelClose extends CanonicalFrame {
	@field({ type: "u32" })
	channelId: number;

	constructor(properties: { channelId: number }) {
		super();
		this.channelId = properties.channelId;
	}
}
