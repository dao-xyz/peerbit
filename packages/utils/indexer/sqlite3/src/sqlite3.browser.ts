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

class ProxyStatement implements IStatement {
	id: string;
	resolvers: {
		[hash in string]: {
			resolve: (...args: any) => void;
			reject: (...args: any) => void;
		};
	} = {};

	constructor(
		readonly send: <T>(
			message: messages.DatabaseMessages | messages.StatementMessages,
		) => Promise<T>,
		readonly databaseId: string,
		readonly statementId: string,
	) {
		this.id = statementId;
	}

	async bind(values: any[]) {
		await this.send({
			type: "bind",
			values: values.map(messages.encodeValue),
			id: uuid(),
			databaseId: this.databaseId,
			statementId: this.statementId,
		});
		return this;
	}

	async finalize() {
		await this.send({
			type: "finalize",
			id: uuid(),
			databaseId: this.databaseId,
			statementId: this.statementId,
		});
	}

	get(values?: BindableValue[]) {
		return this.send<StatementGetResult>({
			type: "get",
			values: values ? values.map(messages.encodeValue) : undefined,
			id: uuid(),
			databaseId: this.databaseId,
			statementId: this.statementId,
		});
	}

	async run(values: BindableValue[]) {
		await this.send({
			type: "run-statement",
			values: values.map(messages.encodeValue),
			id: uuid(),
			databaseId: this.databaseId,
			statementId: this.statementId,
		});
	}

	async reset() {
		await this.send({
			type: "reset",
			id: uuid(),
			databaseId: this.databaseId,
			statementId: this.statementId,
		});
		return this;
	}

	async all(values: BindableValue[]) {
		let id = uuid();
		const results = await this.send({
			type: "all",
			values: values.map(messages.encodeValue),
			id,
			databaseId: this.databaseId,
			statementId: this.statementId,
		});
		return results;
	}
}

class ProxyDatabase implements IDatabase {
	statements: Map<string, ProxyStatement> = new Map();

	resolvers: {
		[hash in string]: {
			resolve: (...args: any) => void;
			reject: (...args: any) => void;
		};
	} = {};
	databaseId!: string;
	constructor(
		readonly send: <T>(
			message: messages.DatabaseMessages | messages.StatementMessages,
		) => Promise<T>,
	) {}

	async init(directory?: string) {
		this.databaseId = uuid();
		return this.send({
			type: "create",
			directory,
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
			this.send,
			this.databaseId,
			statementId,
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

	async status() {
		return this.send<"open" | "closed">({
			type: "status",
			id: uuid(),
			databaseId: this.databaseId,
		});
	}

	/*    async get(sql: string) {
		   return this.send({ type: 'get', sql, id: uuid() });
	   }
   
	   async run(sql: string, bind: any[]) {
		   return this.send({ type: 'run', sql, bind, id: uuid() });
	   } */
}

interface DatabaseCreator {
	create(directory?: string): Promise<ProxyDatabase>;
	close(): Promise<void> | void;
}

let initialized: DatabaseCreator | undefined = undefined;
const init = async (): Promise<DatabaseCreator> => {
	if (initialized) {
		return initialized;
	}

	let worker = new Worker(
		new URL("/peerbit/sqlite3.worker.min.js", import.meta.url),
		{ type: "module" },
	);
	let resolvers: {
		[hash in string]: {
			resolve: (...args: any) => void;
			reject: (...args: any) => void;
		};
	} = {};

	let send = <T>(
		message: messages.DatabaseMessages | messages.StatementMessages,
	) => {
		const promise = new Promise<T>((resolve, reject) => {
			resolvers[message.id] = { resolve, reject };
		});
		worker.postMessage(message);

		return promise.finally(() => delete resolvers[message.id]);
	};

	let isReady = pDefer();

	worker.onmessage = async (ev) => {
		const message = ev.data as messages.ResponseMessages | messages.IsReady;

		if (message.type === "ready") {
			isReady.resolve();
			return;
		}

		const resolver = resolvers[message.id];
		if (message.type === "error") {
			resolver.reject(message.message);
		} else if (message.type === "response") {
			resolver.resolve(message.result);
		}
	};

	const create = async (directory?: string) => {
		const db = new ProxyDatabase(send);
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

const create = (directory?: string): Promise<IDatabase> => {
	if (directory) {
		// persist the database
		return init().then((creator) => creator.create(directory));
	} else {
		return createDatabase();
	}
};
export { create };
