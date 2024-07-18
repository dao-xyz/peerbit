import * as messages from "./sqlite3-messages.worker.js";
import { create } from "./sqlite3.wasm.js";

class SqliteWorkerHandler {
	databases: Map<string, Awaited<ReturnType<typeof create>>> = new Map();

	async create(databaseId: string, directory?: string) {
		const db = await create(directory);
		this.databases.set(databaseId, db);
		return db;
	}

	async onMessage(
		message: messages.DatabaseMessages | messages.StatementMessages,
	) {
		if (message.type === "create") {
			await this.create(message.databaseId, message.directory);
		} else {
			const db = this.databases.get(message.databaseId);
			if (!db) {
				if (message.type === "close") {
					return; // ignore close message if database is not found
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
			} else if (message.type === "status") {
				return db.status();
			} else if (message.type === "prepare") {
				const statementId = message.sql;
				await db.prepare(message.sql);
				// db.statements.get(statementId) -> statement, because sqlite3.wasm stores the statement in a map like this
				return statementId;
			} else if (message.type === "close") {
				await db.close();
				this.databases.delete(message.databaseId);
			} else if (message.type === "open") {
				await db.open();
				this.databases.set(message.databaseId, db);
			} else if (message.type === "run") {
				return db.run(message.sql, message.values.map(messages.resolveValue));
			} else {
				const statement = db.statements.get(message.statementId);
				if (!statement) {
					throw new Error(
						"Statement not found with id: " + message.statementId,
					);
				}

				if (message.type === "bind") {
					return statement.bind(message.values.map(messages.resolveValue));
				} else if (message.type === "finalize") {
					return statement.finalize();
				} else if (message.type === "reset") {
					return statement.reset();
				} else if (message.type === "get") {
					return statement.get(
						message.values
							? message.values.map(messages.resolveValue)
							: undefined,
					);
				} else if (message.type === "step") {
					return statement.step();
				} else if (message.type === "run-statement") {
					return statement.run(message.values.map(messages.resolveValue));
				} else if (message.type === "all") {
					return statement.all(message.values.map(messages.resolveValue));
				} else {
					throw new Error("Unknown statement message type: " + message["type"]);
				}
			}
		}
	}
}
const worker = new SqliteWorkerHandler();

self.onmessage = async (
	messageEvent: MessageEvent<
		messages.DatabaseMessages | messages.StatementMessages
	>,
) => {
	try {
		const results = await worker.onMessage(messageEvent.data);
		self.postMessage({
			type: "response",
			id: messageEvent.data.id,
			result: results,
		});
	} catch (error: any) {
		self.postMessage({
			type: "error",
			id: messageEvent.data.id,
			message: error?.message,
		});
	}
};

self.postMessage({ type: "ready" });
