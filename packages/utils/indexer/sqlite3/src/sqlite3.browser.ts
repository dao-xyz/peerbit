import pDefer from "p-defer";
import { v4 as uuid } from "uuid";
import type { BindableValue } from "./schema.js";
import * as messages from "./sqlite3-messages.worker.js";
import { create as createDatabase } from "./sqlite3.wasm.js";
import {
	type Database as IDatabase,
	type Statement as IStatement,
	type StatementGetResult,
} from "./types.js";

type RequestType =
	| messages.DatabaseMessages["type"]
	| messages.StatementMessages["type"];

export type SQLiteBrowserOptions = {
	protocol?: messages.SqliteWorkerProtocol;
	pragmas?: messages.SQLitePragmaOptions;
	profile?: boolean;
	onProfile?: (sample: SQLiteProfileSample) => void;
};

export type SQLiteProfileSample = {
	requestType: RequestType;
	protocol: messages.SqliteWorkerProtocol;
	databaseId: string;
	databaseDirectory?: string;
	sql?: string;
	clientEncodeMs: number;
	clientRoundTripMs: number;
	valueCount: number;
	blobValueCount: number;
	blobBytes: number;
	worker?: messages.WorkerTiming;
};

type SendMetrics = messages.ClientEncodeMetrics & {
	requestType: RequestType;
	sql?: string;
};

type ResponseMessage = Extract<messages.ResponseMessages, { type: "response" }>;
type ErrorMessage = Extract<messages.ResponseMessages, { type: "error" }>;

const DEFAULT_PROTOCOL: messages.SqliteWorkerProtocol = "clone";
const EMPTY_ENCODE_METRICS: messages.ClientEncodeMetrics = {
	encodeMs: 0,
	valueCount: 0,
	blobValueCount: 0,
	blobBytes: 0,
};

const getProtocol = (
	options?: SQLiteBrowserOptions,
): messages.SqliteWorkerProtocol => options?.protocol ?? DEFAULT_PROTOCOL;

class ProxyStatement implements IStatement {
	id: string;
	private needsReset = false;

	constructor(
		readonly send: <T>(
			message: messages.DatabaseMessages | messages.StatementMessages,
			metrics?: SendMetrics,
		) => Promise<T>,
		readonly databaseId: string,
		readonly statementId: string,
		readonly sql: string,
		readonly options?: SQLiteBrowserOptions,
	) {
		this.id = statementId;
	}

	async bind(values: any[]) {
		const encoded = messages.encodeValues(values, getProtocol(this.options));
		await this.send(
			{
				type: "bind",
				values: encoded.values ?? [],
				id: uuid(),
				databaseId: this.databaseId,
				statementId: this.statementId,
			},
				{
					requestType: "bind",
					sql: this.sql,
					...encoded.metrics,
				},
		);
		this.needsReset = true;
		return this;
	}

	async finalize() {
		await this.send(
			{
				type: "finalize",
				id: uuid(),
				databaseId: this.databaseId,
				statementId: this.statementId,
			},
			{
				requestType: "finalize",
				sql: this.sql,
				...EMPTY_ENCODE_METRICS,
			},
		);
		this.needsReset = false;
	}

	async get(values?: BindableValue[]) {
		const encoded = messages.encodeValues(values, getProtocol(this.options));
		const result = await this.send<StatementGetResult>(
			{
				type: "get",
				values: encoded.values,
				id: uuid(),
				databaseId: this.databaseId,
				statementId: this.statementId,
			},
				{
					requestType: "get",
					sql: this.sql,
					...encoded.metrics,
				},
		);
		this.needsReset = false;
		return result;
	}

	async run(values: BindableValue[]) {
		const encoded = messages.encodeValues(values, getProtocol(this.options));
		await this.send(
			{
				type: "run-statement",
				values: encoded.values ?? [],
				id: uuid(),
				databaseId: this.databaseId,
				statementId: this.statementId,
			},
				{
					requestType: "run-statement",
					sql: this.sql,
					...encoded.metrics,
				},
		);
		this.needsReset = false;
	}

	async reset() {
		if (!this.needsReset) {
			return this;
		}
		await this.send(
			{
				type: "reset",
				id: uuid(),
				databaseId: this.databaseId,
				statementId: this.statementId,
			},
			{
				requestType: "reset",
				sql: this.sql,
				...EMPTY_ENCODE_METRICS,
			},
		);
		this.needsReset = false;
		return this;
	}

	async all(values: BindableValue[]) {
		const encoded = messages.encodeValues(values, getProtocol(this.options));
		const result = await this.send(
			{
				type: "all",
				values: encoded.values ?? [],
				id: uuid(),
				databaseId: this.databaseId,
				statementId: this.statementId,
			},
				{
					requestType: "all",
					sql: this.sql,
					...encoded.metrics,
				},
		);
		this.needsReset = false;
		return result;
	}
}

class ProxyDatabase implements IDatabase {
	statements: Map<string, ProxyStatement> = new Map();
	databaseId!: string;
	private directory?: string;

	constructor(
		readonly postMessage: (
			message: messages.DatabaseMessages | messages.StatementMessages,
		) => Promise<ResponseMessage>,
		readonly options?: SQLiteBrowserOptions,
	) {}

	private async send<T>(
		message: messages.DatabaseMessages | messages.StatementMessages,
		metrics?: SendMetrics,
	): Promise<T> {
		const startedAt = performance.now();
		const protocol = getProtocol(this.options);
		const shouldProfile = Boolean(this.options?.profile || this.options?.onProfile);
		const requestType = metrics?.requestType ?? message.type;

		try {
			const response = await this.postMessage({
				...message,
				protocol,
				profile: shouldProfile,
			});
			this.options?.onProfile?.({
				requestType,
				protocol,
				databaseId: this.databaseId,
				databaseDirectory: this.directory,
				sql:
					metrics?.sql ??
					("sql" in message && typeof message.sql === "string"
						? message.sql
						: undefined),
				clientEncodeMs: metrics?.encodeMs ?? 0,
				clientRoundTripMs: performance.now() - startedAt,
				valueCount: metrics?.valueCount ?? 0,
				blobValueCount: metrics?.blobValueCount ?? 0,
				blobBytes: metrics?.blobBytes ?? 0,
				worker: response.timing,
			});
			return response.result as T;
		} catch (error: any) {
			const responseError = error as ErrorMessage | undefined;
			this.options?.onProfile?.({
				requestType,
				protocol,
				databaseId: this.databaseId,
				databaseDirectory: this.directory,
				sql:
					metrics?.sql ??
					("sql" in message && typeof message.sql === "string"
						? message.sql
						: undefined),
				clientEncodeMs: metrics?.encodeMs ?? 0,
				clientRoundTripMs: performance.now() - startedAt,
				valueCount: metrics?.valueCount ?? 0,
				blobValueCount: metrics?.blobValueCount ?? 0,
				blobBytes: metrics?.blobBytes ?? 0,
				worker: responseError?.timing,
			});
			if (responseError?.type === "error") {
				throw new Error(responseError.message);
			}
			throw error;
		}
	}

	async init(directory?: string) {
		this.databaseId = uuid();
		this.directory = directory;
		return this.send({
			type: "create",
			directory,
			pragmas: this.options?.pragmas,
			databaseId: this.databaseId,
			id: uuid(),
		});
	}

	async exec(sql: string) {
		return this.send({
			type: "exec",
			sql,
			id: uuid(),
			databaseId: this.databaseId,
		});
	}

	async prepare(sql: string, id?: string) {
		if (id != null) {
			const prev = this.statements.get(id);
			if (prev) {
				await prev.reset();
				return prev;
			}
		}
		const statementId = await this.send<string>({
			type: "prepare",
			sql,
			id: uuid(),
			databaseId: this.databaseId,
		});
		const statement = new ProxyStatement(
			this.send.bind(this),
			this.databaseId,
			statementId,
			sql,
			this.options,
		);
		this.statements.set(statementId, statement);

		if (id != null) {
			this.statements.set(id, statement);
		}

		return statement;
	}

	async open() {
		return this.send({ type: "open", id: uuid(), databaseId: this.databaseId });
	}

	async close() {
		return this.send({
			type: "close",
			id: uuid(),
			databaseId: this.databaseId,
		});
	}

	async drop() {
		return this.send({
			type: "drop",
			id: uuid(),
			databaseId: this.databaseId,
		});
	}

	async status() {
		return this.send<"open" | "closed">({
			type: "status",
			id: uuid(),
			databaseId: this.databaseId,
		});
	}
}

interface DatabaseCreator {
	create(directory?: string, options?: SQLiteBrowserOptions): Promise<ProxyDatabase>;
	close(): Promise<void> | void;
}

let initialized: DatabaseCreator | undefined = undefined;
const init = async (): Promise<DatabaseCreator> => {
	if (initialized) {
		return initialized;
	}

	const worker = new Worker(
		new URL("/peerbit/sqlite3/sqlite3.worker.min.js", import.meta.url),
		{ type: "module" },
	);
	const resolvers: Record<
		string,
		{
			resolve: (message: ResponseMessage) => void;
			reject: (message: ErrorMessage) => void;
		}
	> = {};

	const postMessage = (
		message: messages.DatabaseMessages | messages.StatementMessages,
	) => {
		const promise = new Promise<ResponseMessage>((resolve, reject) => {
			resolvers[message.id] = { resolve, reject };
		});
		worker.postMessage(message);

		return promise.finally(() => delete resolvers[message.id]);
	};

	const isReady = pDefer();

	worker.onmessage = async (ev) => {
		const message = ev.data as messages.ResponseMessages | messages.IsReady;

		if (message.type === "ready") {
			isReady.resolve();
			return;
		}

		const resolver = resolvers[message.id];
		if (!resolver) {
			return;
		}
		if (message.type === "error") {
			resolver.reject(message);
		} else {
			resolver.resolve(message);
		}
	};

	const create = async (
		directory?: string,
		options?: SQLiteBrowserOptions,
	) => {
		const db = new ProxyDatabase(postMessage, options);
		await isReady.promise;
		await db.init(directory);
		await db.open();
		return db;
	};
	return (initialized = {
		create,
		close: () => {
			initialized = undefined;
			worker.terminate();
		},
	});
};

const create = (
	directory?: string,
	options?: SQLiteBrowserOptions,
): Promise<IDatabase> => {
	if (directory) {
		// persist the database
		return init().then((creator) => creator.create(directory, options));
	}
	return createDatabase(directory, options);
};
export { create };
