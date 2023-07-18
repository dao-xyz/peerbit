import { field, variant, vec, option } from "@dao-xyz/borsh";
import { Message } from "./message.js";

@variant(10)
export abstract class MemoryMessage extends Message {
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
export class REQ_Sublevel extends MemoryMessage {
	@field({ type: "string" })
	name: string;

	constructor(properties: { level: string[]; name: string }) {
		super(properties);
		this.name = properties.name;
	}
}

@variant(13)
export class RESP_Sublevel extends MemoryMessage {}

@variant(14)
export class REQ_Clear extends MemoryMessage {}

@variant(15)
export class RESP_Clear extends MemoryMessage {}

@variant(16)
export class REQ_Idle extends MemoryMessage {}

@variant(17)
export class RESP_Idle extends MemoryMessage {}
