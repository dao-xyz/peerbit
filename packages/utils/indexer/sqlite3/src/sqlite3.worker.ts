import * as messages from "./sqlite3-messages.worker.js";
import { create } from "./sqlite3.wasm.js";

const resolveValues = (
	values: messages.EncodedValue[] | undefined,
	profile = false,
) => {
	if (!values || values.length === 0) {
		return {
			values: undefined,
			timing: profile
				? {
						decodeMs: 0,
						valueCount: 0,
						blobValueCount: 0,
						blobBytes: 0,
					}
				: undefined,
		};
	}

	let blobBytes = 0;
	let blobValueCount = 0;
	const startedAt = profile ? performance.now() : 0;
	const resolvedValues = values.map((value) => {
		const resolved = messages.resolveValue(value);
		if (profile && resolved instanceof Uint8Array) {
			blobValueCount++;
			blobBytes += resolved.byteLength;
		}
		return resolved;
	});

	return {
		values: resolvedValues,
		timing: profile
			? {
					decodeMs: performance.now() - startedAt,
					valueCount: values.length,
					blobValueCount,
					blobBytes,
				}
			: undefined,
	};
};

class SqliteWorkerHandler {
	databases: Map<string, Awaited<ReturnType<typeof create>>> = new Map();

	async create(
		databaseId: string,
		directory?: string,
		options?: { pragmas?: messages.SQLitePragmaOptions },
	) {
		const db = await create(directory, options);
		this.databases.set(databaseId, db);
		return db;
	}

	async onMessage(
		message: messages.DatabaseMessages | messages.StatementMessages,
	) {
		const profile = Boolean(message.profile);
		const startedAt = profile ? performance.now() : 0;
		let decodeMs = 0;
		let valueCount = 0;
		let blobValueCount = 0;
		let blobBytes = 0;

		const execute = async () => {
			if (message.type === "create") {
				await this.create(message.databaseId, message.directory, {
					pragmas: message.pragmas,
				});
				return undefined;
			}

			const db = this.databases.get(message.databaseId);
			if (!db) {
				if (message.type === "close" || message.type === "drop") {
					return undefined;
				}
				if (message.type === "status") {
					return "closed";
				}

				throw new Error(
					"Database not found with id: " +
						message.databaseId +
						". For message type " +
						message.type,
				);
			}

			if (message.type === "exec") {
				return db.exec(message.sql);
			}
			if (message.type === "status") {
				return db.status();
			}
			if (message.type === "prepare") {
				const statementId = message.id;
				await db.prepare(message.sql, message.id);
				return statementId;
			}
			if (message.type === "prepare-many") {
				for (const statement of message.statements) {
					await db.prepare(statement.sql, statement.id);
				}
				return message.statements.map((statement) => statement.id);
			}
			if (message.type === "close") {
				await db.close();
				this.databases.delete(message.databaseId);
				return undefined;
			}
			if (message.type === "drop") {
				await db.drop();
				this.databases.delete(message.databaseId);
				return undefined;
			}
			if (message.type === "open") {
				await db.open();
				this.databases.set(message.databaseId, db);
				return undefined;
			}
			if (message.type === "run") {
				const resolved = resolveValues(message.values, profile);
				decodeMs = resolved.timing?.decodeMs ?? 0;
				valueCount = resolved.timing?.valueCount ?? 0;
				blobValueCount = resolved.timing?.blobValueCount ?? 0;
				blobBytes = resolved.timing?.blobBytes ?? 0;
				return db.run(message.sql, resolved.values ?? []);
			}

			const statement = db.statements.get(message.statementId);
			if (!statement) {
				throw new Error("Statement not found with id: " + message.statementId);
			}

			if (message.type === "bind") {
				const resolved = resolveValues(message.values, profile);
				decodeMs = resolved.timing?.decodeMs ?? 0;
				valueCount = resolved.timing?.valueCount ?? 0;
				blobValueCount = resolved.timing?.blobValueCount ?? 0;
				blobBytes = resolved.timing?.blobBytes ?? 0;
				return statement.bind(resolved.values ?? []);
			}
			if (message.type === "finalize") {
				return statement.finalize();
			}
			if (message.type === "reset") {
				return statement.reset();
			}
			if (message.type === "get") {
				const resolved = resolveValues(message.values, profile);
				decodeMs = resolved.timing?.decodeMs ?? 0;
				valueCount = resolved.timing?.valueCount ?? 0;
				blobValueCount = resolved.timing?.blobValueCount ?? 0;
				blobBytes = resolved.timing?.blobBytes ?? 0;
				return statement.get(resolved.values);
			}
			if (message.type === "step") {
				return statement.step();
			}
			if (message.type === "run-statement") {
				const resolved = resolveValues(message.values, profile);
				decodeMs = resolved.timing?.decodeMs ?? 0;
				valueCount = resolved.timing?.valueCount ?? 0;
				blobValueCount = resolved.timing?.blobValueCount ?? 0;
				blobBytes = resolved.timing?.blobBytes ?? 0;
				return statement.run(resolved.values ?? []);
			}
			if (message.type === "all") {
				const resolved = resolveValues(message.values, profile);
				decodeMs = resolved.timing?.decodeMs ?? 0;
				valueCount = resolved.timing?.valueCount ?? 0;
				blobValueCount = resolved.timing?.blobValueCount ?? 0;
				blobBytes = resolved.timing?.blobBytes ?? 0;
				return statement.all(resolved.values ?? []);
			}

			throw new Error("Unknown statement message type: " + message["type"]);
		};

		const execStart = profile ? performance.now() : 0;
		const result = await execute();
		const execMs = profile ? performance.now() - execStart - decodeMs : 0;

		return {
			result,
			timing: profile
				? {
						decodeMs,
						execMs,
						totalMs: performance.now() - startedAt,
						valueCount,
						blobValueCount,
						blobBytes,
					}
				: undefined,
		};
	}
}
const worker = new SqliteWorkerHandler();

self.onmessage = async (
	messageEvent: MessageEvent<
		messages.DatabaseMessages | messages.StatementMessages
	>,
) => {
	const profile = Boolean(messageEvent.data.profile);
	const startedAt = profile ? performance.now() : 0;
	try {
		const response = await worker.onMessage(messageEvent.data);
		self.postMessage({
			type: "response",
			id: messageEvent.data.id,
			result: response.result,
			timing: response.timing,
		});
	} catch (error: any) {
		self.postMessage({
			type: "error",
			id: messageEvent.data.id,
			message: error?.message,
			timing: profile
				? {
						decodeMs: 0,
						execMs: 0,
						totalMs: performance.now() - startedAt,
						valueCount: 0,
						blobValueCount: 0,
						blobBytes: 0,
					}
				: undefined,
		});
	}
};

self.postMessage({ type: "ready" });
