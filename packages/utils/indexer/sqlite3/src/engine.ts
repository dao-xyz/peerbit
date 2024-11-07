import { type AbstractType, type Constructor, getSchema } from "@dao-xyz/borsh";
import type {
	Index,
	IndexEngineInitProperties,
	IndexedResult,
	Shape,
} from "@peerbit/indexer-interface";
import * as types from "@peerbit/indexer-interface";
import { v4 as uuid } from "uuid";
import {
	MissingFieldError,
	type Table,
	buildJoin,
	convertCountRequestToQuery,
	convertDeleteRequestToQuery,
	convertFromSQLType,
	convertSearchRequestToQuery,
	/* getTableName, */
	convertSumRequestToQuery,
	convertToSQLType,
	escapeColumnName,
	generateSelectQuery,
	getInlineTableFieldName,
	getSQLTable,
	getTablePrefixedField,
	insert,
	resolveInstanceFromValue,
	resolveTable,
	selectAllFieldsFromTable,
	selectChildren,
} from "./schema.js";
import type { Database, Statement } from "./types.js";

const escapePathToSQLName = (path: string[]) => {
	return path.map((x) => x.replace(/[^a-zA-Z0-9]/g, "_"));
};

const putStatementKey = (table: Table) => table.name + "_put";
const replaceStatementKey = (table: Table) => table.name + "_replicate";
const resolveChildrenStatement = (table: Table) =>
	table.name + "_resolve_children";

export class SQLLiteIndex<T extends Record<string, any>>
	implements Index<T, any>
{
	primaryKeyArr!: string[];
	primaryKeyString!: string;
	private scopeString?: string;
	private _rootTables!: Table[];
	private _tables!: Map<string, Table>;
	private _cursor!: Map<
		string,
		{
			fetch: (amount: number) => Promise<IndexedResult[]>;
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
		this.iteratorTimeout = options?.iteratorTimeout || 60e3;
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

		this.primaryKeyString = getInlineTableFieldName(this.primaryKeyArr);

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

		this._tables = new Map();
		this._cursor = new Map();

		const tables = getSQLTable(
			this.properties.schema!,
			this.scopeString ? [this.scopeString] : [],
			getInlineTableFieldName(this.primaryKeyArr), // TODO fix this, should be array
			false,
			undefined,
			false,
			/* getTableName(this.scopeString, this.properties.schema!) */
		);

		this._rootTables = tables.filter((x) => x.parent == null);

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

			await this.properties.db.prepare(sqlPut, putStatementKey(table));
			await this.properties.db.prepare(sqlReplace, replaceStatementKey(table));
			if (table.parent) {
				await this.properties.db.prepare(
					selectChildren(table),
					resolveChildrenStatement(table),
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
		const stmt = this.properties.db.statements.get(
			resolveChildrenStatement(table),
		)!;
		const results = await stmt.all([parentId]);
		await stmt.reset?.();
		return results;
	}
	async get(
		id: types.IdKey,
		options?: { shape: Shape },
	): Promise<IndexedResult<T> | undefined> {
		for (const table of this._rootTables) {
			const { join: joinMap, selects } = selectAllFieldsFromTable(
				table,
				options?.shape,
			);
			const sql = `${generateSelectQuery(table, selects)} ${buildJoin(joinMap, true)} where ${this.primaryKeyString} = ? limit 1`;
			const stmt = await this.properties.db.prepare(sql, sql);
			const rows = await stmt.get([
				table.primaryField?.from?.type
					? convertToSQLType(id.key, table.primaryField.from.type)
					: id.key,
			]);
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
					const statement = this.properties.db.statements.get(
						replaceStatementKey(table),
					)!;
					await statement.run(
						values.map((x) => (typeof x === "boolean" ? (x ? 1 : 0) : x)),
					);
					await statement.reset?.();
					return preId;
				} else {
					const statement = this.properties.db.statements.get(
						putStatementKey(table),
					)!;
					const out = await statement.get(
						values.map((x) => (typeof x === "boolean" ? (x ? 1 : 0) : x)),
					);
					await statement.reset?.();

					// TODO types
					if (out == null) {
						return undefined;
					}
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

	iterate<S extends Shape | undefined>(
		request?: types.IterateOptions,
		options?: { shape?: S; reference?: boolean },
	): types.IndexIterator<T, S> {
		// create a sql statement where the offset and the limit id dynamic and can be updated
		// TODO don't use offset but sort and limit 'next' calls by the last value of the sort

		/* 	const totalCountKey = "count"; */
		/* const sqlTotalCount = convertCountRequestToQuery(new types.CountRequest({ query: request.query }), this.tables, this.tables.get(this.rootTableName)!)
		const countStmt = await this.properties.db.prepare(sqlTotalCount); */

		let offset = 0;
		let once = false;
		let requestId = uuid();
		let hasMore = true;

		let stmt: Statement;
		let kept: number | undefined = undefined;
		let bindable: any[] = [];
		let sqlFetch: string | undefined = undefined;

		/* let totalCount: undefined | number = undefined; */
		const fetch = async (amount: number | "all") => {
			kept = undefined;
			if (!once) {
				let { sql, bindable: toBind } = convertSearchRequestToQuery(
					request,
					this.tables,
					this._rootTables,
					{
						shape: options?.shape,
						stable: typeof amount === "number", // if we are to fetch all, we dont need stable sorting
					},
				);
				sqlFetch = sql;
				bindable = toBind;

				stmt = await this.properties.db.prepare(sqlFetch, sqlFetch);
				// stmt.reset?.(); // TODO dont invoke reset if not needed
				/* countStmt.reset?.(); */

				// Bump timeout timer
				clearTimeout(iterator.timeout);
				iterator.timeout = setTimeout(
					() => this.clearupIterator(requestId),
					this.iteratorTimeout,
				);
			}

			once = true;

			const allResults: Record<string, any>[] = await stmt.all([
				...bindable,
				amount === "all" ? Number.MAX_SAFE_INTEGER : amount,
				offset,
			]);

			let results: IndexedResult<types.ReturnTypeFromShape<T, S>>[] =
				await Promise.all(
					allResults.map(async (row: any) => {
						let selectedTable = this._rootTables.find(
							(table /* row["table_name"] === table.name, */) =>
								row[getTablePrefixedField(table, this.primaryKeyString)] !=
								null,
						)!;

						const value = await resolveInstanceFromValue<T, S>(
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
								convertFromSQLType(
									row[
										getTablePrefixedField(selectedTable, this.primaryKeyString)
									],
									selectedTable.primaryField!.from!.type,
								),
							),
						};
					}),
				);

			offset += results.length;

			/* if (results.length > 0) {
				totalCount =
					totalCount ??
					(await this.count(
						request,
					));
				iterator.kept = totalCount - results.length - offsetStart;
			} else {
				iterator.kept = 0;
			} */

			if (amount === "all" || results.length < amount) {
				hasMore = false;
				await this.clearupIterator(requestId);
				clearTimeout(iterator.timeout);
			}
			return results;
		};

		const iterator = {
			fetch,
			/* countStatement: countStmt, */
			timeout: setTimeout(
				() => this.clearupIterator(requestId),
				this.iteratorTimeout,
			),
		};

		this.cursor.set(requestId, iterator);
		let totalCount: number | undefined = undefined;
		/* 			return fetch(request.fetch); */
		return {
			all: async () => {
				const results: IndexedResult<types.ReturnTypeFromShape<T, S>>[] = [];
				while (true) {
					const res = await fetch(100);
					results.push(...res);
					if (res.length === 0) {
						break;
					}
				}
				return results;
			},
			close: () => {
				hasMore = false;
				kept = 0;
				this.clearupIterator(requestId);
			},
			next: (amount: number) => fetch(amount),
			pending: async () => {
				if (!hasMore) {
					return 0;
				}
				if (kept != null) {
					return kept;
				}
				totalCount = totalCount ?? (await this.count(request));

				kept = totalCount - offset;
				hasMore = kept > 0;
				return kept;
			},
			done: () => (once ? !hasMore : undefined),
		};
	}

	private async clearupIterator(id: string) {
		const cache = this._cursor.get(id);
		if (!cache) {
			return; // already cleared
		}
		clearTimeout(cache.timeout);
		/* cache.countStatement.finalize?.(); */
		// 	await cache.fetchStatement.finalize?.();
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
		return this.count();
	}

	async del(query: types.DeleteOptions): Promise<types.IdKey[]> {
		let ret: types.IdKey[] = [];
		let once = false;
		let lastError: Error | undefined = undefined;
		for (const table of this._rootTables) {
			try {
				const { sql, bindable } = convertDeleteRequestToQuery(
					query,
					this.tables,
					table,
				);
				const stmt = await this.properties.db.prepare(sql, sql);
				const results: any[] = await stmt.all(bindable);

				// TODO types
				for (const result of results) {
					ret.push(types.toId(result[table.primary as string]));
				}
				once = true;
			} catch (error) {
				if (error instanceof MissingFieldError) {
					lastError = error;
					continue;
				}

				throw error;
			}
		}

		if (!once) {
			throw lastError!;
		}

		return ret;
	}

	async sum(query: types.SumOptions): Promise<number | bigint> {
		let ret: number | bigint | undefined = undefined;
		let once = false;
		let lastError: Error | undefined = undefined;

		let inlinedName = getInlineTableFieldName(query.key);
		for (const table of this._rootTables) {
			try {
				if (table.fields.find((x) => x.name === inlinedName) == null) {
					lastError = new MissingFieldError(
						"Missing field: " +
							(Array.isArray(query.key) ? query.key : [query.key]).join("."),
					);
					continue;
				}

				const { sql, bindable } = convertSumRequestToQuery(
					query,
					this.tables,
					table,
				);
				const stmt = await this.properties.db.prepare(sql, sql);
				const result = await stmt.get(bindable);
				if (result != null) {
					const value = result.sum as number;

					if (ret == null) {
						ret = value;
					} else {
						ret += value;
					}
					once = true;
				}
			} catch (error) {
				if (error instanceof MissingFieldError) {
					lastError = error;
					continue;
				}
				throw error;
			}
		}

		if (!once) {
			throw lastError!;
		}

		return ret != null ? ret : 0;
	}

	async count(request?: types.CountOptions): Promise<number> {
		let ret: number = 0;
		let once = false;
		let lastError: Error | undefined = undefined;
		for (const table of this._rootTables) {
			try {
				const { sql, bindable } = convertCountRequestToQuery(
					request,
					this.tables,
					table,
				);
				const stmt = await this.properties.db.prepare(sql, sql);
				const result = await stmt.get(bindable);
				if (result != null) {
					ret += Number(result.count);
					once = true;
				}
			} catch (error) {
				if (error instanceof MissingFieldError) {
					lastError = error;
					continue;
				}

				throw error;
			}
		}

		if (!once) {
			throw lastError!;
		}
		return ret;
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
