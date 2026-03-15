export type OPFSStoreProtocol = "clone" | "legacy";

export type OPFSRequest =
	| {
			type: "open" | "close" | "clear" | "size" | "status";
			messageId: string;
			level: string[];
	  }
	| {
			type: "get" | "del";
			messageId: string;
			level: string[];
			key: string;
	  }
	| {
			type: "put";
			messageId: string;
			level: string[];
			key: string;
			bytes: Uint8Array;
	  }
	| {
			type: "sublevel";
			messageId: string;
			level: string[];
			name: string;
	  }
	| {
			type: "iterator-next" | "iterator-stop";
			messageId: string;
			level: string[];
			id: string;
	  };

export type OPFSResponse =
	| {
			type: "ack";
			messageId: string;
			level: string[];
	  }
	| {
			type: "status";
			messageId: string;
			level: string[];
			status: "opening" | "open" | "closing" | "closed";
	  }
	| {
			type: "get";
			messageId: string;
			level: string[];
			bytes?: Uint8Array;
	  }
	| {
			type: "size";
			messageId: string;
			level: string[];
			size: number;
	  }
	| {
			type: "iterator-next";
			messageId: string;
			level: string[];
			keys: string[];
			values: Uint8Array[];
	  }
	| {
			type: "error";
			messageId: string;
			level: string[];
			error: string;
	  };

const REQUEST_TYPES = new Set<OPFSRequest["type"]>([
	"open",
	"close",
	"clear",
	"size",
	"status",
	"get",
	"del",
	"put",
	"sublevel",
	"iterator-next",
	"iterator-stop",
]);

const RESPONSE_TYPES = new Set<OPFSResponse["type"]>([
	"ack",
	"status",
	"get",
	"size",
	"iterator-next",
	"error",
]);

export const isOPFSRequest = (value: unknown): value is OPFSRequest => {
	return (
		typeof value === "object" &&
		value !== null &&
		typeof (value as OPFSRequest).messageId === "string" &&
		Array.isArray((value as OPFSRequest).level) &&
		REQUEST_TYPES.has((value as OPFSRequest).type)
	);
};

export const isOPFSResponse = (value: unknown): value is OPFSResponse => {
	return (
		typeof value === "object" &&
		value !== null &&
		typeof (value as OPFSResponse).messageId === "string" &&
		Array.isArray((value as OPFSResponse).level) &&
		RESPONSE_TYPES.has((value as OPFSResponse).type)
	);
};

export const getTransferables = (
	message: OPFSRequest | OPFSResponse,
): Transferable[] => {
	if (message.type === "get" && "bytes" in message && message.bytes) {
		return [message.bytes.buffer as ArrayBuffer];
	}
	if (message.type === "iterator-next" && "values" in message) {
		return message.values.map(
			(value: Uint8Array) => value.buffer as ArrayBuffer,
		);
	}
	return [];
};
