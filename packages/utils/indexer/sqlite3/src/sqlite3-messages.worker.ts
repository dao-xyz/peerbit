import { fromBase64, toBase64 } from "@peerbit/crypto";

export type SqliteWorkerProtocol = "legacy" | "clone";
export type SQLiteSynchronousMode = "FULL" | "NORMAL" | "OFF";
export type SQLitePragmaOptions = {
	synchronous?: SQLiteSynchronousMode;
};

interface Message {
	id: string;
	databaseId: string;
	profile?: boolean;
	protocol?: SqliteWorkerProtocol;
}

// Database messages
interface CreateDatabase extends Message {
	type: "create";
	directory?: string;
	pragmas?: SQLitePragmaOptions;
}

interface Exec extends Message {
	type: "exec";
	sql: string;
}

interface Status extends Message {
	type: "status";
}

interface Close extends Message {
	type: "close";
}

interface Drop extends Message {
	type: "drop";
}

interface Open extends Message {
	type: "open";
}

interface Prepare extends Message {
	type: "prepare";
	sql: string;
}

type Uint8ArrayBase64Type = {
	type: "uint8array";
	encoding: "base64";
	base64: string;
};

type Uint8ArrayCloneType = {
	type: "uint8array";
	encoding: "clone";
	value: Uint8Array;
};

type SimpleType = { type: "simple"; value: any };

export type EncodedValue =
	| Uint8ArrayBase64Type
	| Uint8ArrayCloneType
	| SimpleType;

export type ClientEncodeMetrics = {
	encodeMs: number;
	valueCount: number;
	blobValueCount: number;
	blobBytes: number;
};

export type WorkerTiming = {
	decodeMs: number;
	execMs: number;
	totalMs: number;
	valueCount: number;
	blobValueCount: number;
	blobBytes: number;
};

export const resolveValue = (value: EncodedValue) => {
	if (value.type === "simple") {
		return value.value;
	}
	return value.encoding === "clone" ? value.value : fromBase64(value.base64);
};

export const encodeValue = (
	value: any,
	protocol: SqliteWorkerProtocol = "legacy",
): EncodedValue => {
	if (value instanceof Uint8Array) {
		return protocol === "clone"
			? { type: "uint8array", encoding: "clone", value }
			: { type: "uint8array", encoding: "base64", base64: toBase64(value) };
	}
	return { type: "simple", value };
};

export const encodeValues = (
	values: any[] | undefined,
	protocol: SqliteWorkerProtocol = "legacy",
): { values: EncodedValue[] | undefined; metrics: ClientEncodeMetrics } => {
	if (!values || values.length === 0) {
		return {
			values,
			metrics: {
				encodeMs: 0,
				valueCount: 0,
				blobValueCount: 0,
				blobBytes: 0,
			},
		};
	}

	let blobBytes = 0;
	let blobValueCount = 0;
	const startedAt = performance.now();
	const encodedValues = values.map((value) => {
		if (value instanceof Uint8Array) {
			blobValueCount++;
			blobBytes += value.byteLength;
		}
		return encodeValue(value, protocol);
	});

	return {
		values: encodedValues,
		metrics: {
			encodeMs: performance.now() - startedAt,
			valueCount: values.length,
			blobValueCount,
			blobBytes,
		},
	};
};

interface Run extends Statement {
	type: "run";
	sql: string;
	values: EncodedValue[];
}

// Statement messages
interface Statement extends Message {
	statementId: string;
}

interface Bind extends Statement {
	type: "bind";
	values: EncodedValue[];
}

interface Step extends Statement {
	type: "step";
}

interface Get extends Statement {
	type: "get";
	values?: EncodedValue[];
}

interface Reset extends Statement {
	type: "reset";
}

interface RunStatement extends Statement {
	type: "run-statement";
	values: EncodedValue[];
}

interface All extends Statement {
	type: "all";
	values: EncodedValue[];
}

interface Finalize extends Statement {
	type: "finalize";
}

// Response messages
interface ErrorResponse {
	type: "error";
	id: string;
	message: string;
	timing?: WorkerTiming;
}

interface Response {
	type: "response";
	id: string;
	result: any;
	timing?: WorkerTiming;
}

export type DatabaseMessages =
	| CreateDatabase
	| Exec
	| Prepare
	| Close
	| Drop
	| Open
	| Run
	| Status;
export type StatementMessages =
	| Bind
	| Step
	| Get
	| Reset
	| All
	| Finalize
	| RunStatement;
export type ResponseMessages = ErrorResponse | Response;

export type IsReady = { type: "ready" };
