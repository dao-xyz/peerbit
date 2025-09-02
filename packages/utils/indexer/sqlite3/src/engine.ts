import { type AbstractType, type Constructor, getSchema } from "@dao-xyz/borsh";
import type {
	Index,
	IndexEngineInitProperties,
	IndexedResult,
	Shape,
} from "@peerbit/indexer-interface";
import * as types from "@peerbit/indexer-interface";
import { v4 as uuid } from "uuid";
import { PlannableQuery, QueryPlanner } from "./query-planner.js";
import {
	MissingFieldError,
	type Table,
	buildJoin,
	convertCountRequestToQuery,
	convertDeleteRequestToQuery,
	convertFromSQLType,
	convertSearchRequestToQuery,
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
import { isFKError } from "./utils.js";

const escapePathToSQLName = (path: string[]) => {
	return path.map((x) => x.replace(/[^a-zA-Z0-9]/g, "_"));
};

const putStatementKey = (table: Table) => table.name + "_put";
const replaceStatementKey = (table: Table) => table.name + "_replicate";
const resolveChildrenStatement = (table: Table) =>
	table.name + "_resolve_children";

type FKMode = "strict" | "race-tolerant";

async function safeReset(stmt?: Statement) {
	if (!stmt?.reset) return;
	try {
		await stmt.reset();
	} catch (e) {
		if (isFKError(e)) return; // swallow FK-reset noise
		throw e;
	}
}

async function runIgnoreFK(stmt: Statement, values: any[]) {
	try {
		await stmt.run(values);
		await safeReset(stmt); // success path: reset safely
		return;
	} catch (e) {
		if (isFKError(e)) {
			await safeReset(stmt); // swallow FK + swallow reset error
			return; // pretend no-op
		}
		// real error
		await safeReset(stmt); // best effort
		throw e;
	}
}

async function getIgnoreFK(stmt: Statement, values: any[]) {
	try {
		const out = await stmt.get(values);
		await safeReset(stmt); // success path
		return out;
	} catch (e) {
		if (isFKError(e)) {
			await safeReset(stmt); // swallow FK + reset error
			return undefined;
		}
		await safeReset(stmt);
		throw e;
	}
}

export class SQLLiteIndex<T extends Record<string, any>>
	implements Index<T, any>
{
	primaryKeyArr!: string[];
	primaryKeyString!: string;
	planner: QueryPlanner;
	private scopeString?: string;
	private _rootTables!: Table[];
	private _tables!: Map<string, Table>;
	private _cursor!: Map<
		string,
		{
			fetch: (amount: number) => Promise<IndexedResult[]>;
			/* countStatement: Statement; */
			expire: number;
		}
	>; // TODO choose limit better
	private cursorPruner: ReturnType<typeof setInterval> | undefined;

	iteratorTimeout: number;
	closed: boolean = true;
	private fkMode: FKMode;

	id: string;
	constructor(
		readonly properties: {
			scope: string[];
			db: Database;
			schema: AbstractType<any>;
			start?: () => Promise<void> | void;
			stop?: () => Promise<void> | void;
		},
		options?: { iteratorTimeout?: number; fkMode?: FKMode },
	) {
		this.fkMode = options?.fkMode || "race-tolerant";
		this.closed = true;
		this.id = uuid();
		this.scopeString =
			properties.scope.length > 0
				? "_" + escapePathToSQLName(properties.scope).join("_")
				: undefined;
		this.iteratorTimeout = options?.iteratorTimeout || 60e3;
		this.planner = new QueryPlanner({
			exec: this.properties.db.exec.bind(this.properties.db),
		});
	}

	get tables() {
		if (this.closed) {
			throw new types.NotStartedError();
		}
		return this._tables;
	}

	get rootTables() {
		if (this.closed) {
			throw new types.NotStartedError();
		}
		return this._rootTables;
	}

	get cursor() {
		if (this.closed) {
			throw new types.NotStartedError();
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
			this.properties.db.exec(sqlCreateTable);

			/* const fieldsToIndex = table.fields.filter(
				(field) =>
					field.key !== ARRAY_INDEX_COLUMN && field.key !== table.primary,
			);
			if (fieldsToIndex.length > 0) {
				let arr = fieldsToIndex.map((field) => escapeColumnName(field.name));
		
				const createIndex = async (columns: string[]) => {
					const key = createIndexKey(table.name, columns)
					const command = `create index if not exists ${key} on ${table.name} (${columns.map((n) => escapeColumnName(n)).join(", ")})`;
					await this.properties.db.exec(command);
					table.indices.add(key);
		
		
		
					const rev = columns.reverse()
					const key2 = createIndexKey(table.name, rev)
					const command2 = `create index if not exists ${key2} on ${table.name} (${rev.join(", ")})`;
					await this.properties.db.exec(command2);
					table.indices.add(key2);
				}
				await createIndex(fieldsToIndex.map(x => x.name));
				await createIndex([table.primary as string, ...fieldsToIndex.map(x => x.name)]);
		
				if (arr.length > 1) {
					for (const field of fieldsToIndex) {
						await createIndex([field.name]);
						await createIndex([table.primary as string, field.name]);
		
					}
				}
			} */

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

		this.cursorPruner = setInterval(() => {
			const now = Date.now();
			for (const [k, v] of this._cursor) {
				if (v.expire < now) {
					this.clearupIterator(k);
				}
			}
		}, this.iteratorTimeout);

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
		clearInterval(this.cursorPruner!);

		await this.clearStatements();

		this._tables.clear();

		for (const [k, _v] of this._cursor) {
			await this.clearupIterator(k);
		}

		await this.planner.stop();
	}

	async drop(): Promise<void> {
		if (this.closed) {
			throw new Error(`Already closed index ${this.id}, can not drop`);
		}

		this.closed = true;
		clearInterval(this.cursorPruner!);

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
		await this.planner.stop();
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
			const sql = `${generateSelectQuery(table, selects)} ${buildJoin(joinMap).join} where ${table.name}.${this.primaryKeyString} = ? limit 1`;
			try {
				const stmt = await this.properties.db.prepare(sql, sql);
				const rows = await stmt.get([
					table.primaryField?.from?.type
						? convertToSQLType(id.key, table.primaryField.from.type)
						: id.key,
				]);
				if (
					rows?.[getTablePrefixedField(table, table.primary as string)] == null
				) {
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
			} catch (error) {
				if (this.closed) {
					throw new types.NotStartedError();
				}
				throw error;
			}
		}
		return undefined;
	}

	async put(value: T, _id?: any): Promise<void> {
		const classOfValue = value.constructor as Constructor<T>;
		return insert(
			async (values, table) => {
				let preId = values[table.primaryIndex];
				let statement: Statement | undefined = undefined;
				try {
					if (preId != null) {
						statement = this.properties.db.statements.get(
							replaceStatementKey(table),
						)!;
						this.fkMode === "race-tolerant"
							? await runIgnoreFK(statement, values)
							: await statement.run(values);
						return preId;
					} else {
						statement = this.properties.db.statements.get(
							putStatementKey(table),
						)!;
						const out =
							this.fkMode === "race-tolerant"
								? await getIgnoreFK(statement, values)
								: await statement.get(values);

						// TODO types
						if (out == null) {
							return undefined;
						}
						return out[table.primary as string];
					}
				} finally {
					await statement?.reset?.();
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

		const normalizedQuery = new PlannableQuery({
			query: types.toQuery(request?.query),
			sort: request?.sort,
		});
		let planningScope: ReturnType<QueryPlanner["scope"]>;

		/* let totalCount: undefined | number = undefined; */
		const fetch = async (amount: number | "all") => {
			kept = undefined;
			if (!once) {
				planningScope = this.planner.scope(normalizedQuery);

				let { sql, bindable: toBind } = convertSearchRequestToQuery(
					normalizedQuery,
					this.tables,
					this._rootTables,
					{
						planner: planningScope,
						shape: options?.shape,
						fetchAll: amount === "all", // if we are to fetch all, we dont need stable sorting
					},
				);

				sqlFetch = sql;
				bindable = toBind;

				await planningScope.beforePrepare();

				stmt = await this.properties.db.prepare(sqlFetch, sqlFetch);

				// Bump timeout timer
				iterator.expire = Date.now() + this.iteratorTimeout;
			}

			once = true;

			const allResults = await planningScope.perform(async () => {
				const allResults: Record<string, any>[] = await stmt.all([
					...bindable,
					...(amount !== "all" ? [amount, offset] : []),
				]);
				return allResults;
			});

			/* const allResults: Record<string, any>[] = await stmt.all([
				...bindable,
				...(amount !== "all" ? [amount, 
					offset] : [])
			]);
	*/
			let results: IndexedResult<types.ReturnTypeFromShape<T, S>>[] =
				await Promise.all(
					allResults.map(async (row: any) => {
						let selectedTable = this._rootTables.find(
							(table) =>
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

			/* const uniqueIds = new Set(results.map((x) => x.id.primitive));
			if (uniqueIds.size !== results.length) {
				throw new Error("Duplicate ids in result set");
			} */

			if (amount === "all" || results.length < amount) {
				hasMore = false;
				await this.clearupIterator(requestId);
			}
			return results;
		};

		const iterator = {
			fetch,
			/* countStatement: countStmt, */
			expire: Date.now() + this.iteratorTimeout,
		};

		this.cursor.set(requestId, iterator);
		let totalCount: number | undefined = undefined;
		/* 			return fetch(request.fetch); */
		return {
			all: async () => {
				const results: IndexedResult<types.ReturnTypeFromShape<T, S>>[] = [];
				while (true) {
					const res = await fetch("all");
					results.push(...res);
					if (hasMore === false) {
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

				kept = Math.max(totalCount - offset, 0); // this could potentially be negative if new records are added and we iterate concurrently, so we do Math.max here
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
					ret.push(
						types.toId(
							convertFromSQLType(
								result[table.primary as string],
								table.primaryField!.from!.type,
							),
						),
					);
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
			return scope;
		}

		const scope = this.scopes.get(name)!;
		if (!this.closed) {
			// TODO test this code path
			await scope.start();
		}
		return scope;
	}

	async start(): Promise<void> {
		this.closed = false;

		if (!this.properties.parent) {
			await this.properties.db.open();
		}

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

		if (!this.properties.parent) {
			for (const index of this.indices) {
				await index.index.stop();
			}
			await this.properties.db.drop();
		} else {
			for (const index of this.indices) {
				await index.index.drop();
			}
		}
		this.scopes.clear();
	}
}
