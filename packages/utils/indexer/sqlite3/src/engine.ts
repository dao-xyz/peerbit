import { type AbstractType, type Constructor, getSchema } from "@dao-xyz/borsh";
import type {
	CloseIteratorRequest,
	CollectNextRequest,
	Index,
	IndexEngineInitProperties,
	IndexedResult,
	IndexedResults,
	SearchRequest,
	Shape,
} from "@peerbit/indexer-interface";
import * as types from "@peerbit/indexer-interface";
import { v4 as uuid } from "uuid";
import {
	type Table,
	buildJoin,
	convertCountRequestToQuery,
	convertDeleteRequestToQuery,
	convertSearchRequestToQuery,
	/* getTableName, */
	convertSumRequestToQuery,
	escapeColumnName,
	getInlineTableFieldName,
	getSQLTable,
	getTablePrefixedField,
	insert,
	resolveInstanceFromValue,
	resolveTable,
	selectAllFields,
	selectChildren,
} from "./schema.js";
import type { Database, Statement } from "./types.js";

const escapePathToSQLName = (path: string[]) => {
	return path.map((x) => x.replace(/[^a-zA-Z0-9]/g, "_"));
};

export class SQLLiteIndex<T extends Record<string, any>>
	implements Index<T, any>
{
	primaryKeyArr: string[];
	primaryKeyString: string;
	putStatement: Map<string, Statement>;
	replaceStatement: Map<string, Statement>;
	resolveChildrenStatement: Map<string, Statement>;
	private scopeString?: string;
	private _rootTables: Table[];
	private _tables: Map<string, Table>;
	private _cursor: Map<
		string,
		{
			kept: number;
			fetch: (
				amount: number,
			) => Promise<{ results: IndexedResult[]; kept: number }>;
			fetchStatement: Statement;
			/* countStatement: Statement; */
			timeout: ReturnType<typeof setTimeout>;
		}
	>; // TODO choose limit better

	iteratorTimeout: number;
	closed: boolean = true;

	id: string;
	constructor(
		readonly properties: {
			scope: string[];
			db: Database;
			schema: AbstractType<any>;
			start?: () => Promise<void> | void;
			stop?: () => Promise<void> | void;
		},
		options?: { iteratorTimeout?: number },
	) {
		this.closed = true;
		this.id = uuid();
		this.scopeString =
			properties.scope.length > 0
				? "_" + escapePathToSQLName(properties.scope).join("_")
				: undefined;
		this.iteratorTimeout = options?.iteratorTimeout || 1e4;
	}

	get tables() {
		if (this.closed) {
			throw new Error("Not started");
		}
		return this._tables;
	}

	get rootTables() {
		if (this.closed) {
			throw new Error("Not started");
		}
		return this._rootTables;
	}

	get cursor() {
		if (this.closed) {
			throw new Error("Not started");
		}
		return this._cursor;
	}

	init(properties: IndexEngineInitProperties<T, any>) {
		if (properties.indexBy) {
			this.primaryKeyArr = Array.isArray(properties.indexBy)
				? properties.indexBy
				: [properties.indexBy];
		} else {
			const indexBy = types.getIdProperty(properties.schema);

			if (!indexBy) {
				throw new Error(
					"No indexBy property defined nor schema has a property decorated with `id()`",
				);
			}

			this.primaryKeyArr = indexBy;
		}

		if (!this.properties.schema) {
			throw new Error("Missing schema");
		}

		this.primaryKeyString = getInlineTableFieldName(
			this.primaryKeyArr.slice(0, this.primaryKeyArr.length - 1),
			this.primaryKeyArr[this.primaryKeyArr.length - 1],
		);

		return this;
	}

	async start(): Promise<void> {
		if (this.closed === false) {
			return;
		}

		if (this.primaryKeyArr == null || this.primaryKeyArr.length === 0) {
			throw new Error("Not initialized");
		}

		await this.properties.start?.();

		this.putStatement = new Map();
		this.replaceStatement = new Map();
		this.resolveChildrenStatement = new Map();
		this._tables = new Map();
		this._cursor = new Map();

		const tables = getSQLTable(
			this.properties.schema!,
			this.scopeString ? [this.scopeString] : [],
			getInlineTableFieldName(
				this.primaryKeyArr.slice(0, -1),
				this.primaryKeyArr[this.primaryKeyArr.length - 1],
			), // TODO fix this, should be array
			false,
			undefined,
			false,
			/* getTableName(this.scopeString, this.properties.schema!) */
		);

		this._rootTables = tables.filter((x) => x.parent == null);

		if (this._rootTables.length > 1) {
			throw new Error("Multiple root tables not supported (yet)");
		}

		const allTables = tables;

		for (const table of allTables) {
			this._tables.set(table.name, table);

			for (const child of table.children) {
				allTables.push(child);
			}

			if (table.inline) {
				// this table does not 'really' exist as a separate table
				// but its fields are in the root table
				continue;
			}

			const sqlCreateTable = `create table if not exists ${table.name} (${[...table.fields, ...table.constraints].map((s) => s.definition).join(", ")}) strict`;
			const sqlCreateIndex = `create index if not exists ${table.name}_index on ${table.name} (${table.fields.map((field) => escapeColumnName(field.name)).join(", ")})`;

			this.properties.db.exec(sqlCreateTable);
			this.properties.db.exec(sqlCreateIndex);

			// put and return the id
			let sqlPut = `insert into ${table.name}  (${table.fields.map((field) => escapeColumnName(field.name)).join(", ")}) VALUES (${table.fields.map((_x) => "?").join(", ")}) RETURNING ${table.primary};`;

			// insert or replace with id already defined
			let sqlReplace = `insert or replace into ${table.name} (${table.fields.map((field) => escapeColumnName(field.name)).join(", ")}) VALUES (${table.fields.map((_x) => "?").join(", ")});`;

			this.putStatement.set(
				table.name,
				await this.properties.db.prepare(sqlPut),
			);
			this.replaceStatement.set(
				table.name,
				await this.properties.db.prepare(sqlReplace),
			);

			if (table.parent) {
				this.resolveChildrenStatement.set(
					table.name,
					await this.properties.db.prepare(selectChildren(table)),
				);
			}
		}

		this.closed = false;
	}

	private async clearStatements() {
		if ((await this.properties.db.status()) === "closed") {
			// TODO this should never be true, but if we remove this statement the tests faiL for browser tests?
			return;
		}

		for (const [_k, v] of this.putStatement) {
			await v.finalize?.();
		}

		for (const [_k, v] of this.replaceStatement) {
			await v.finalize?.();
		}

		for (const [_k, v] of this.resolveChildrenStatement) {
			await v.finalize?.();
		}
		this.putStatement.clear();
		this.replaceStatement.clear();
		this.resolveChildrenStatement.clear();
	}

	async stop(): Promise<void> {
		if (this.closed) {
			return;
		}
		this.closed = true;

		await this.clearStatements();

		this._tables.clear();

		for (const [k, _v] of this._cursor) {
			await this.clearupIterator(k);
		}
	}

	async drop(): Promise<void> {
		this.closed = true;

		await this.clearStatements();

		// drop root table and cascade
		// drop table faster by dropping constraints first
		for (const table of this._rootTables) {
			await this.properties.db.exec(`drop table if exists ${table.name}`);
		}

		this._tables.clear();

		for (const [k, _v] of this._cursor) {
			await this.clearupIterator(k);
		}
	}

	private async resolveDependencies(
		parentId: any,
		table: Table,
	): Promise<any[]> {
		const stmt = this.resolveChildrenStatement.get(table.name)!;
		const results = await stmt.all([parentId]);
		await stmt.reset?.();
		return results;
	}
	async get(
		id: types.IdKey,
		options?: { shape: Shape },
	): Promise<IndexedResult<T> | undefined> {
		for (const table of this._rootTables) {
			const { join: joinMap, query } = selectAllFields(table, options?.shape);
			const sql = `${query} ${buildJoin(joinMap, true)} where ${this.primaryKeyString} = ? `;
			const stmt = await this.properties.db.prepare(sql);
			const rows = await stmt.get([id.key]);
			await stmt.finalize?.();
			if (!rows) {
				continue;
			}
			return {
				value: (await resolveInstanceFromValue(
					rows,
					this.tables,
					table,
					this.resolveDependencies.bind(this),
					true,
					options?.shape,
				)) as unknown as T,
				id,
			};
		}
		return undefined;
	}

	async put(value: T, _id?: any): Promise<void> {
		const classOfValue = value.constructor as Constructor<T>;
		return insert(
			async (values, table) => {
				const preId = values[table.primaryIndex];

				if (preId != null) {
					const statement = this.replaceStatement.get(table.name)!;
					await statement.run(
						values.map((x) => (typeof x === "boolean" ? (x ? 1 : 0) : x)),
					);
					await statement.reset?.();
					return preId;
				} else {
					const statement = this.putStatement.get(table.name)!;
					const out = await statement.get(
						values.map((x) => (typeof x === "boolean" ? (x ? 1 : 0) : x)),
					);
					await statement.reset?.();

					// TODO types
					return out[table.primary as string];
				}
			},
			value,
			this.tables,
			resolveTable(
				this.scopeString ? [this.scopeString] : [],
				this.tables,
				classOfValue,
				true,
			),
			getSchema(classOfValue).fields,
			(_fn) => {
				throw new Error("Unexpected");
			},
		);
	}

	async query(
		request: SearchRequest,
		options?: { shape: Shape },
	): Promise<IndexedResults<T>> {
		// create a sql statement where the offset and the limit id dynamic and can be updated
		// TODO don't use offset but sort and limit 'next' calls by the last value of the sort
		let sqlFetch = convertSearchRequestToQuery(
			request,
			this.tables,
			this._rootTables,
			options?.shape,
		);

		const stmt = await this.properties.db.prepare(sqlFetch);
		/* 	const totalCountKey = "count"; */
		/* const sqlTotalCount = convertCountRequestToQuery(new types.CountRequest({ query: request.query }), this.tables, this.tables.get(this.rootTableName)!)
		const countStmt = await this.properties.db.prepare(sqlTotalCount); */

		let offset = 0;
		let first = false;

		const fetch = async (amount: number) => {
			if (!first) {
				stmt.reset?.();
				/* countStmt.reset?.(); */

				// Bump timeout timer
				clearTimeout(iterator.timeout);
				iterator.timeout = setTimeout(
					() => this.clearupIterator(request.idString),
					this.iteratorTimeout,
				);
			}

			first = true;
			const offsetStart = offset;
			const allResults: Record<string, any>[] = await stmt.all([
				amount,
				offsetStart,
			]);

			let results: IndexedResult<T>[] = await Promise.all(
				allResults.map(async (row: any) => {
					let selectedTable = this._rootTables.find(
						(table) =>
							row[getTablePrefixedField(table, this.primaryKeyString)] != null,
					)!;
					const value = await resolveInstanceFromValue<T>(
						row,
						this.tables,
						selectedTable,
						this.resolveDependencies.bind(this),
						true,
						options?.shape,
					);

					return {
						value,
						id: types.toId(
							row[getTablePrefixedField(selectedTable, this.primaryKeyString)],
						),
					};
				}),
			);

			offset += amount;

			if (results.length > 0) {
				const totalCount = await this.count(
					new types.CountRequest({ query: request.query }),
				); /*  (await countStmt.get())[totalCountKey] as number; */
				iterator.kept = totalCount - results.length - offsetStart;
			} else {
				iterator.kept = 0;
			}

			if (iterator.kept === 0) {
				await this.clearupIterator(request.idString);
				clearTimeout(iterator.timeout);
			}
			return { results, kept: iterator.kept };
		};
		const iterator = {
			kept: 0,
			fetch,
			fetchStatement: stmt,
			/* countStatement: countStmt, */
			timeout: setTimeout(
				() => this.clearupIterator(request.idString),
				this.iteratorTimeout,
			),
		};

		this.cursor.set(request.idString, iterator);
		return fetch(request.fetch);
	}

	next(query: CollectNextRequest): Promise<IndexedResults<T>> {
		const cache = this.cursor.get(query.idString);
		if (!cache) {
			throw new Error("No cursor found with id: " + query.idString);
		}

		// reuse statement
		return cache.fetch(query.amount) as Promise<IndexedResults<T>>;
	}

	close(query: CloseIteratorRequest): void | Promise<void> {
		return this.clearupIterator(query.idString);
	}

	private async clearupIterator(id: string) {
		const cache = this._cursor.get(id);
		if (!cache) {
			return; // already cleared
		}

		clearTimeout(cache.timeout);
		/* cache.countStatement.finalize?.(); */
		await cache.fetchStatement.finalize?.();
		this._cursor.delete(id);
	}

	async getSize(): Promise<number> {
		if (this.tables.size === 0) {
			return 0;
		}

		/* const stmt = await this.properties.db.prepare(`select count(*) as total from ${this.rootTableName}`);
		const result = await stmt.get()
		stmt.finalize?.();
		return result.total as number */
		return this.count(new types.CountRequest({ query: {} }));
	}

	async del(query: types.DeleteRequest): Promise<types.IdKey[]> {
		let ret: types.IdKey[] = [];
		for (const table of this._rootTables) {
			const stmt = await this.properties.db.prepare(
				convertDeleteRequestToQuery(query, this.tables, table),
			);
			const results: any[] = await stmt.all([]);
			await stmt.finalize?.();
			// TODO types
			for (const result of results) {
				ret.push(types.toId(result[table.primary as string]));
			}
		}
		return ret;
	}

	async sum(query: types.SumRequest): Promise<number | bigint> {
		let ret: number | bigint | undefined = undefined;
		for (const table of this._rootTables) {
			const stmt = await this.properties.db.prepare(
				convertSumRequestToQuery(query, this.tables, table),
			);
			const result = await stmt.get();
			await stmt.finalize?.();
			if (ret == null) {
				(ret as any) = result.sum as number;
			} else {
				(ret as any) += result.sum as number;
			}
		}
		return ret != null ? ret : 0;
	}

	async count(request: types.CountRequest): Promise<number> {
		let ret: number = 0;
		for (const table of this._rootTables) {
			const stmt = await this.properties.db.prepare(
				convertCountRequestToQuery(request, this.tables, table),
			);
			const result = await stmt.get();
			await stmt.finalize?.();
			ret += Number(result.count);
		}
		return ret;
	}

	getPending(cursorId: string): number | undefined {
		const cursor = this.cursor.get(cursorId);
		if (!cursor) {
			return;
		}
		return cursor.kept;
	}

	get cursorCount(): number {
		return this.cursor.size;
	}
}

export class SQLiteIndices implements types.Indices {
	private _scope: string[];
	private scopes: Map<string, SQLiteIndices>;
	private indices: { schema: any; index: Index<any, any> }[];
	private closed = true;

	constructor(
		readonly properties: {
			scope?: string[];
			db: Database;
			parent?: SQLiteIndices;
		},
	) {
		this._scope = properties.scope || [];
		this.scopes = new Map();
		this.indices = [];
	}

	async init<T extends Record<string, any>, NestedType>(
		properties: IndexEngineInitProperties<T, NestedType>,
	): Promise<Index<T, NestedType>> {
		const existing = this.indices.find((x) => x.schema === properties.schema);
		if (existing) {
			return existing.index;
		}

		const index: types.Index<T, any> = new SQLLiteIndex({
			db: this.properties.db,
			schema: properties.schema,
			scope: this._scope,
		});
		await index.init(properties);
		this.indices.push({ schema: properties.schema, index });

		if (!this.closed) {
			await index.start();
		}
		return index;
	}

	async scope(name: string): Promise<types.Indices> {
		if (!this.scopes.has(name)) {
			const scope = new SQLiteIndices({
				scope: [...this._scope, name],
				db: this.properties.db,
				parent: this,
			});

			if (!this.closed) {
				await scope.start();
			}
			this.scopes.set(name, scope);
		}
		return this.scopes.get(name)!;
	}

	async start(): Promise<void> {
		this.closed = false;

		await this.properties.db.open(); // TODO only open if parent is not defined ? or this method will not be the opposite of close

		for (const scope of this.scopes.values()) {
			await scope.start();
		}

		for (const index of this.indices) {
			await index.index.start();
		}
	}

	async stop(): Promise<void> {
		this.closed = true;
		for (const scope of this.scopes.values()) {
			await scope.stop();
		}

		for (const index of this.indices) {
			await index.index.stop();
		}

		if (!this.properties.parent) {
			await this.properties.db.close();
		}
	}

	async drop(): Promise<void> {
		for (const scope of this.scopes.values()) {
			await scope.drop();
		}

		for (const index of this.indices) {
			await index.index.drop();
		}

		this.scopes.clear();
	}
}
