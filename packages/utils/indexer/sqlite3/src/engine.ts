import { PublicSignKey } from "@peerbit/crypto";
import {
	type Index,
	type IndexedResult,
	type IndexedResults,
	SearchRequest,
	CollectNextRequest,
	CloseIteratorRequest,
	type IndexEngineInitProperties
} from "@peerbit/indexer-interface";
import * as types from "@peerbit/indexer-interface";
import {
	type Table,
	convertToSQLType,
	convertSearchRequestToQuery,
	getSQLTable,
	insert,
	resolveTable,
	/* getTableName, */
	convertSumRequestToQuery,
	convertCountRequestToQuery,
	resolveInstanceFromValue,
	selectChildren,
	selectAllFields,
	getTablePrefixedField,
	buildJoin
} from "./schema.js";
import type { Statement, Database } from "./types.js";
import type { Constructor } from '@dao-xyz/borsh';

export class SQLLiteIndex<T extends Record<string, any>> implements Index<T, any> {

	primaryKeyArr: string[];
	putStatement: Map<string, Statement>;
	replaceStatement: Map<string, Statement>;
	resolveChildrenStatement: Map<string, Statement>;
	_tables: Map<string, Table>;
	_cursor: Map<
		string,
		{
			kept: number;
			from: PublicSignKey;
			fetch: (
				amount: number
			) => Promise<{ results: IndexedResult[]; kept: number }>;
			fetchStatement: Statement;
			countStatement: Statement;
			timeout: ReturnType<typeof setTimeout>;
		}
	>; // TODO choose limit better

	iteratorTimeout: number;
	rootTableName = "root";
	closed: boolean = true;

	private scopeString: string;

	constructor(
		readonly properties: { scope: string[], db: Database, schema: Constructor<any>, start?: () => Promise<void> | void, stop?: () => Promise<void> | void },
		options?: { iteratorTimeout?: number }
	) {
		this.closed = true;
		this.scopeString = properties.scope.join("_");
		this.iteratorTimeout = options?.iteratorTimeout || 1e4;
	}

	get tables() {
		if (this.closed) {
			throw new Error("Not started");
		}
		return this._tables;
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
		}
		else {
			const indexBy = types.getIdProperty(properties.schema)

			if (!indexBy) {
				throw new Error("No indexBy property defined nor schema has a property decorated with `id()`")
			}

			this.primaryKeyArr = indexBy
		}



		if (this.primaryKeyArr.length > 1) {
			throw new Error("Indexed by property can only be a root property");
		}

		if (!this.properties.schema) {
			throw new Error("Missing schema");
		}

		return this;
	}

	async start(): Promise<void> {
		if (this.closed === false) {
			throw new Error("Already started");
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
			[this.scopeString],
			this.primaryKeyArr[0],
			/* getTableName(this.scopeString, this.properties.schema!) */
		);

		this.rootTableName = tables[0].name;

		const allTables = tables;

		for (const table of allTables) {
			const sql = `create table if not exists ${table.name} (${[...table.fields, ...table.constraints].map((s) => s.definition).join(", ")}) strict`
			this.properties.db.exec(sql);

			// put and return the id
			let sqlPut = `insert into ${table.name}  (${table.fields.map((field) => field.name).join(", ")}) VALUES (${table.fields.map((_x) => "?").join(", ")}) RETURNING ${table.primary};`
			let sqlReplace = `insert or replace into ${table.name} (${table.fields.map((field) => field.name).join(", ")}) VALUES (${table.fields.map((_x) => "?").join(", ")});`

			this.putStatement.set(table.name, await this.properties.db.prepare(sqlPut));
			this.replaceStatement.set(table.name, await this.properties.db.prepare(sqlReplace));

			if (this._tables.size > 0) {
				this.resolveChildrenStatement.set(table.name, await this.properties.db.prepare(selectChildren(table)))
			}
			this._tables.set(table.name, table);

			for (const child of table.children) {
				allTables.push(child)
			}
		}

		this.closed = false;

	}

	private async clearStatements() {
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

		await this.clearStatements()

		this.closed = true;

		this._tables.clear();

		for (const [k, _v] of this._cursor) {
			this.clearupIterator(k);
		}
	}

	async drop(): Promise<void> {

		await this.clearStatements()

		// drop root table and cascade
		/* for (const table of this.tables) {

			await this.properties.db.exec(`drop table if exists ${table[0]}`);
		} */

		await this.properties.db.exec(`drop table if exists ${this.rootTableName}`);

		this._tables.clear();

		for (const [k, _v] of this.cursor) {
			this.clearupIterator(k);
		}
	}

	/* async subindex(name: string): Promise<IndexEngine<any>> {
		const subIndex = new SQLLiteIndex({
			...this.properties,
			scope: `${this.properties.scope}_${name}`
		});
		this.subIndices.set(name, subIndex);
		return subIndex;

	} */


	private async resolveDependencies(parentId: any, table: Table): Promise<any[]> {
		/* const statement = await this.properties.db.prepare(sql);
		const result = await statement.all(bindable);
		return result */

		const stmt = this.resolveChildrenStatement.get(table.name)!
		const results = await stmt.all([parentId])
		await stmt.reset?.()
		return results
	}
	async get(id: types.IdKey): Promise<IndexedResult<T> | undefined> {
		const { join: joinMap, query } = selectAllFields(this.tables.get(this.rootTableName)!)
		const sql = `${query} ${buildJoin(joinMap, true)} where ${this.primaryKeyArr[0]} = ? `;
		const stmt = await this.properties.db.prepare(sql);
		const rows = await stmt.get([id.key]);
		await stmt.finalize?.();
		return rows ? {
			value: await resolveInstanceFromValue(
				rows, this.tables, this.tables.get(this.rootTableName)!,
				this.resolveDependencies.bind(this),
				true
			) as unknown as T, id
		} : undefined;
	}

	async put(value: T, _id: undefined): Promise<void> {
		await insert(
			async (values, table) => {
				const preId = values[table.primaryIndex]

				if (table.name === "__122__array__av0") {
					console.log("PUT", values, table.name)
				}

				if (table.name === "__122__array__av1") {
					console.log("PUT", values, table.name)
				}


				if (preId != null) {
					const statement = this.replaceStatement.get(table.name)!
					await statement.run(values.map(x => typeof x === 'boolean' ? (x ? 1 : 0) : x));
					await statement.reset?.()
					return preId
				}
				else {
					const statement = this.putStatement.get(table.name)!
					const out = await statement.get(values.map(x => typeof x === 'boolean' ? (x ? 1 : 0) : x));
					await statement.reset?.()
					return out[table.primary]
				}
			},
			value,
			this.tables,
			resolveTable([this.scopeString], this.tables, this.properties.schema!)
		);
	}

	async del(id: types.IdKey): Promise<void> {
		let statement = await this.properties.db.prepare(`delete from ${this.rootTableName} where ${this.primaryKeyArr[0]} = ?`)
		await statement.run([convertToSQLType(id.key)])
		await statement.finalize?.();
	}

	async query(
		request: SearchRequest,
		from: PublicSignKey
	): Promise<IndexedResults<T>> {
		// create a sql statement where the offset and the limit id dynamic and can be updated
		// TODO don't use offset but sort and limit 'next' calls by the last value of the sort
		let sqlFetch = convertSearchRequestToQuery(
			request,
			this.tables,
			this.tables.get(this.rootTableName)!
		);


		const stmt = await this.properties.db.prepare(sqlFetch);
		const totalCountKey = "count";
		const sqlTotalCount = convertCountRequestToQuery(new types.CountRequest({ query: request.query }), this.tables, this.tables.get(this.rootTableName)!)
		const countStmt = await this.properties.db.prepare(sqlTotalCount);

		let offset = 0;
		let first = false;

		const fetch = async (amount: number) => {
			if (!first) {
				stmt.reset?.();
				countStmt.reset?.();

				// Bump timeout timer
				clearTimeout(iterator.timeout);
				iterator.timeout = setTimeout(
					() => this.clearupIterator(request.idString),
					this.iteratorTimeout
				);
			}

			first = true;
			const offsetStart = offset;
			const allResults: Record<string, any>[] = await stmt.all([amount, offsetStart]);

			let results: IndexedResult<T>[] = await Promise.all(allResults.map(async (row: any) => {
				const value = await resolveInstanceFromValue<T>(row, this.tables, this.tables.get(this.rootTableName)!, this.resolveDependencies.bind(this), true)
				return {
					value,
					id: types.toId(row[getTablePrefixedField(this.tables.get(this.rootTableName)!, this.primaryKeyArr[0])])
				}
			}));

			offset += amount;

			if (results.length > 0) {
				const totalCount = (await countStmt.get())[totalCountKey] as number;
				iterator.kept = totalCount - results.length - offsetStart;
			} else {
				iterator.kept = 0;
			}

			if (iterator.kept === 0) {
				this.clearupIterator(request.idString);
				clearTimeout(iterator.timeout);
			}
			return { results, kept: iterator.kept };
		};
		const iterator = {
			kept: 0,
			fetch,
			from,
			fetchStatement: stmt,
			countStatement: countStmt,
			timeout: setTimeout(
				() => this.clearupIterator(request.idString),
				this.iteratorTimeout
			)
		};

		this.cursor.set(request.idString, iterator);
		return fetch(request.fetch);
	}

	async next(
		query: CollectNextRequest,
		from: PublicSignKey
	): Promise<IndexedResults<T>> {
		const cache = this.cursor.get(query.idString);
		if (!cache) {
			throw new Error("No statement found");
		}

		// reuse statement
		return cache.fetch(query.amount) as Promise<IndexedResults<T>>;
	}

	close(
		query: CloseIteratorRequest,
		from: PublicSignKey
	): void | Promise<void> {
		return this.clearupIterator(query.idString, from);
	}


	private clearupIterator(id: string, from?: PublicSignKey) {
		const cache = this._cursor.get(id);
		if (!cache) {
			return; // already cleared
		}
		if (from) {
			if (!cache.from.equals(from)) {
				return; // wrong sender
			}
		}
		clearTimeout(cache.timeout);
		cache.countStatement.finalize?.();
		cache.fetchStatement.finalize?.();
		this._cursor.delete(id);
	}

	async getSize(): Promise<number> {
		if (this.tables.size === 0) {
			return 0;
		}

		const stmt = await this.properties.db.prepare(`select count(*) as total from ${this.rootTableName}`);
		const result = await stmt.get()
		stmt.finalize?.();
		return result.total as number
	}

	async sum(query: types.SumRequest) {
		const stmt = await this.properties.db.prepare(convertSumRequestToQuery(query, this.tables, this.tables.get(this.rootTableName)!));
		const result = await stmt.get()
		await stmt.finalize?.();
		return result.sum as number
	}


	async count(request: types.CountRequest): Promise<number> {
		const stmt = await this.properties.db.prepare(convertCountRequestToQuery(
			request,
			this.tables,
			this.tables.get(this.rootTableName)!
		));
		const result = await stmt.get()
		await stmt.finalize?.();
		return result.count as number
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
	private indices: { schema: any, index: Index<any, any> }[]
	private closed = true;

	constructor(readonly properties: { scope?: string[], db: Database, parent?: SQLiteIndices }) {
		this._scope = properties.scope || [];
		this.scopes = new Map();
		this.indices = [];
	}

	async init<T extends Record<string, any>, NestedType>(properties: IndexEngineInitProperties<T, NestedType>): Promise<Index<T, NestedType>> {

		const existing = this.indices.find((x) => x.schema === properties.schema);
		if (existing) {
			return existing.index;
		}

		const index: types.Index<T, any> = new SQLLiteIndex({ db: this.properties.db, schema: properties.schema, scope: this._scope });
		await index.init(properties);
		this.indices.push({ schema: properties.schema, index });

		if (!this.closed) {
			await index.start();
		}
		return index;
	}
	async scope(name: string): Promise<types.Indices> {
		if (!this.scopes.has(name)) {

			const scope = new SQLiteIndices({ scope: [...this._scope, name], db: this.properties.db, parent: this })

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
			await scope.start()
		}

		for (const index of this.indices) {
			await index.index.start()
		}


	}

	async stop(): Promise<void> {

		this.closed = true;
		for (const scope of this.scopes.values()) {
			await scope.stop()
		}

		for (const index of this.indices) {
			await index.index.stop()
		}

		if (!this.properties.parent) {
			await this.properties.db.close();
		}
	}

	async drop(): Promise<void> {

		for (const scope of this.scopes.values()) {
			await scope.drop()
		}

		for (const index of this.indices) {
			await index.index.drop()
		}

		this.scopes.clear()
	}

}