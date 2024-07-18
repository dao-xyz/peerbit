import { field, option, variant, vec } from "@dao-xyz/borsh";
import { v4 as uuid } from "uuid";

export const levelKey = (level: string[]) => JSON.stringify(level);

@variant(0)
export class MemoryRequest {
	@field({ type: "string" })
	messageId: string;

	constructor(messageId?: string) {
		this.messageId = messageId || uuid();
	}
}

@variant(0)
export abstract class MemoryMessage extends MemoryRequest {
	@field({ type: vec("string") })
	level: string[]; // [] means root, ['x'] means sublevel named 'x'

	constructor(properties: { level: string[] }) {
		super();
		this.level = properties.level;
	}
}

@variant(0)
export class REQ_Status extends MemoryMessage {}

@variant(1)
export class RESP_Status extends MemoryMessage {
	@field({ type: "string" })
	status: "opening" | "open" | "closing" | "closed";

	constructor(properties: {
		level: string[];
		status: "opening" | "open" | "closing" | "closed";
	}) {
		super(properties);
		this.status = properties.status;
	}
}

@variant(2)
export class REQ_Open extends MemoryMessage {}

@variant(3)
export class RESP_Open extends MemoryMessage {}

@variant(4)
export class REQ_Close extends MemoryMessage {}

@variant(5)
export class RESP_Close extends MemoryMessage {}

@variant(6)
export class REQ_Get extends MemoryMessage {
	@field({ type: "string" })
	key: string;

	constructor(properties: { level: string[]; key: string }) {
		super(properties);
		this.key = properties.key;
	}
}

@variant(7)
export class RESP_Get extends MemoryMessage {
	@field({ type: option(Uint8Array) })
	bytes?: Uint8Array;

	constructor(properties: { level: string[]; bytes?: Uint8Array }) {
		super(properties);
		this.bytes = properties.bytes;
	}
}

@variant(8)
export class REQ_Put extends MemoryMessage {
	@field({ type: "string" })
	key: string;

	@field({ type: Uint8Array })
	bytes: Uint8Array;

	constructor(properties: { level: string[]; key: string; bytes: Uint8Array }) {
		super(properties);
		this.key = properties.key;
		this.bytes = properties.bytes;
	}
}

@variant(9)
export class RESP_Put extends MemoryMessage {}

@variant(10)
export class REQ_Del extends MemoryMessage {
	@field({ type: "string" })
	key: string;

	constructor(properties: { level: string[]; key: string }) {
		super(properties);
		this.key = properties.key;
	}
}

@variant(11)
export class RESP_Del extends MemoryMessage {}

@variant(12)
export class REQ_Iterator_Next extends MemoryMessage {
	@field({ type: "string" })
	id: string;

	@field({ type: "u32" })
	step: number;

	constructor(properties: { id: string; level: string[] }) {
		super(properties);
		this.id = properties.id;
		this.step = 1;
	}
}

@variant(13)
export class RESP_Iterator_Next extends MemoryMessage {
	@field({ type: vec("string") })
	keys: string[];

	@field({ type: vec(Uint8Array) })
	values: Uint8Array[];

	constructor(properties: {
		level: string[];
		keys: string[];
		values: Uint8Array[];
	}) {
		super(properties);
		this.keys = properties.keys;
		this.values = properties.values;
	}
}

@variant(14)
export class REQ_Iterator_Stop extends MemoryMessage {
	@field({ type: "string" })
	id: string;

	constructor(properties: { id: string; level: string[] }) {
		super(properties);
		this.id = properties.id;
	}
}

@variant(15)
export class RESP_Iterator_Stop extends MemoryMessage {}

@variant(16)
export class REQ_Sublevel extends MemoryMessage {
	@field({ type: "string" })
	name: string;

	constructor(properties: { level: string[]; name: string }) {
		super(properties);
		this.name = properties.name;
	}
}

@variant(17)
export class RESP_Sublevel extends MemoryMessage {}

@variant(18)
export class REQ_Clear extends MemoryMessage {}

@variant(19)
export class RESP_Clear extends MemoryMessage {}

@variant(20)
export class REQ_Idle extends MemoryMessage {}

@variant(21)
export class RESP_Idle extends MemoryMessage {}

@variant(22)
export class REQ_Size extends MemoryMessage {}

@variant(23)
export class RESP_Size extends MemoryMessage {
	@field({ type: "u64" })
	private _size: bigint;

	constructor(properties: { level: string[]; size: number }) {
		super(properties);
		this._size = BigInt(properties.size);
	}
	get size(): number {
		return Number(this._size);
	}
}

@variant(24)
export class REQ_Persisted extends MemoryMessage {}

@variant(25)
export class RESP_Persisted extends MemoryMessage {
	@field({ type: "bool" })
	private _persisted: boolean;

	constructor(properties: { level: string[]; persisted: boolean }) {
		super(properties);
		this._persisted = properties.persisted;
	}
	get persisted(): boolean {
		return this._persisted;
	}
}

@variant(26)
export class RESP_Error extends MemoryMessage {
	@field({ type: "string" })
	error: string;

	constructor(properties: { level: string[]; error: string }) {
		super(properties);
		this.error = properties.error;
	}
}
