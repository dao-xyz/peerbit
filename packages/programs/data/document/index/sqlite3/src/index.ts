import { PublicSignKey } from "@peerbit/crypto";
import {
	IndexEngine,
	IndexedResult,
	IndexedResults,
	SearchRequest,
	CollectNextRequest,
	CloseIteratorRequest,
	IndexEngineInitProperties
} from "@peerbit/document-interface";
import * as types from "@peerbit/document-interface";
import { type sqlite3, type Database, type Statement } from "sqlite3";
import defer from "p-defer";
import {
	Table,
	coerceSQLType,
	convertSearchRequestToQuery,
	getSQLTable,
	resolveFieldValues,
	resolveTable
} from "./schema.js";

export class SQLLiteEngine implements IndexEngine {
	db: Database;
	properties: IndexEngineInitProperties<any>;
	primaryKeyArr: string[];
	putStatement: Map<string, Statement>;
	tables: Map<string, Table>;
	cursor: Map<
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
	rootTableName = "test";
	closed = true;
	constructor(
		readonly sqllite: sqlite3,
		options?: { iteratorTimeout?: number }
	) {
		this.iteratorTimeout = options?.iteratorTimeout || 1e4;
	}

	async init(properties: IndexEngineInitProperties<any>): Promise<void> {
		this.properties = properties;
		this.primaryKeyArr = Array.isArray(properties.indexBy)
			? properties.indexBy
			: [properties.indexBy];

		if (this.primaryKeyArr.length > 1) {
			throw new Error("Indexed by property can only be a root property");
		}

		if (!this.properties.schema) {
			throw new Error("Missing schema");
		}
	}

	async start(): Promise<void> {
		if (this.closed === false) {
			throw new Error("Already started");
		}
		this.closed = false;
		const startPromise = defer();
		this.db = new this.sqllite.Database(":memory:", (err) => {
			if (err) startPromise.reject(err);
			else startPromise.resolve();
		});
		await startPromise.promise;

		const tables = getSQLTable(
			this.properties.schema!,
			[],
			this.primaryKeyArr[0]
		);

		this.rootTableName = tables[0].name;
		for (const table of tables) {
			const deferred = defer();

			this.db.exec(
				`create table if not exists ${table.name} (${[...table.fields, ...table.constraints].map((s) => s.definition).join(", ")})`,
				(err) => {
					if (err) {
						deferred.reject(err);
					} else {
						deferred.resolve();
					}
				}
			);

			await deferred.promise;
		}

		this.putStatement = new Map();
		this.tables = new Map();
		for (const table of tables) {
			const sqlPut = `insert or replace into ${table.name} (${table.fields.map((field) => field.name).join(", ")}) VALUES (${table.fields.map((_x) => "?").join(", ")});`;
			this.putStatement.set(table.name, this.db.prepare(sqlPut));
			this.tables.set(table.name, table);
		}
		this.cursor = new Map();
		// this.sqlInstance.open_v2('/mydb.sqlite3')
		/*  this.db = new this.sqlInstance.oo1.DB('/mydb.sqlite3', 'ct');
	   ; */
	}

	async stop(): Promise<void> {
		if (this.closed) {
			return;
		}
		this.closed = true;
		for (const [k, v] of this.putStatement) {
			v.finalize();
		}
		this.putStatement.clear();
		this.tables.clear();

		for (const [k, v] of this.cursor) {
			this.clearupIterator(k);
		}
		const deferred = defer();
		this.db.close((err) => {
			if (err) deferred.reject(err);
			else deferred.resolve();
		});
		await deferred.promise;
		/*  return this.sqllite.close(); */
	}

	async get(id: types.IdKey): Promise<IndexedResult | undefined> {
		const sql = `select * from ${this.rootTableName} where ${this.primaryKeyArr[0]} = ? `;
		const stmt = this.db.prepare(sql);
		const deferred = defer<IndexedResult | undefined>();
		stmt.get([coerceSQLType(id.key)], (err, row) => {
			if (err) {
				deferred.reject(err);
			} else {
				if (row) {
					deferred.resolve({ indexed: row, id, context: {} as any });
				} else {
					deferred.resolve(undefined);
				}
			}
		});
		stmt.finalize();
		return deferred.promise;
	}
	async put(value: types.IndexedValue<Record<string, any>>): Promise<void> {
		const deferred = defer();
		const valuesToPut = resolveFieldValues(
			value.indexed,
			[],
			this.tables,
			resolveTable(this.tables, this.properties.schema!)
		);

		for (const { table, values } of valuesToPut) {
			const statement = this.putStatement.get(table.name);
			if (!statement) {
				throw new Error("No statement found");
			}
			statement.run(values, (err) => {
				if (err) {
					deferred.reject(err);
				} else {
					deferred.resolve();
				}
			});
			statement.reset();
			await deferred.promise;
		}
	}
	async del(id: types.IdKey): Promise<void> {
		const deferred = defer<void>();
		this.db.run(
			`delete from ${this.rootTableName} where ${this.primaryKeyArr[0]} = ?`,
			[coerceSQLType(id.key)],
			(err) => {
				if (err) {
					deferred.reject(err);
				} else {
					deferred.resolve();
				}
			}
		);

		return deferred.promise;
	}
	async query(
		request: SearchRequest,
		from: PublicSignKey
	): Promise<IndexedResults> {
		// create a sql statement where the offset and the limit id dynamic and can be updated
		// TODO don't use offset but sort and limit 'next' calls by the last value of the sort
		const { where, join, orderBy } = convertSearchRequestToQuery(
			request,
			this.tables,
			this.tables.get(this.rootTableName)!
		);

		const query = `${join ? join : ""} ${where ? where : ""}`;
		const sqlFetch = `select ${this.rootTableName}.* from  ${this.rootTableName} ${query} ${orderBy ? orderBy : ""} limit ? offset ?`;
		const stmt = this.db.prepare(sqlFetch);
		const totalCountKey = "__total_count";
		const sqlTotalCount = `select count(*) as ${totalCountKey} from ${this.rootTableName} ${query}`;
		const countStmt = this.db.prepare(sqlTotalCount);

		let offset = 0;
		let first = false;

		const fetch = async (amount: number) => {
			if (!first) {
				stmt.reset();
				countStmt.reset();

				// Bump timeout timer
				clearTimeout(iterator.timeout);
				iterator.timeout = setTimeout(
					() => this.clearupIterator(request.idString),
					this.iteratorTimeout
				);
			}

			first = true;
			const offsetStart = offset;
			stmt.bind([amount, offsetStart]);
			const deferred = defer<IndexedResult[]>();
			stmt.all([], (err, rows: any) => {
				if (err) {
					deferred.reject(err);
				} else {
					deferred.resolve(
						rows.map((row) => ({
							indexed: row,
							id: types.toId(row.id),
							context: {} as any
						}))
					);
				}
			});
			const results = await deferred.promise;
			offset += amount;

			if (results.length > 0) {
				const deferred = defer<number>();
				countStmt.all([], (err, row: { [key: string]: number }) => {
					if (err) {
						deferred.reject(err);
					} else {
						deferred.resolve(row[0][totalCountKey]);
					}
				});
				const totalCount = await deferred.promise;
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
	): Promise<IndexedResults> {
		const cache = this.cursor.get(query.idString);
		if (!cache) {
			throw new Error("No statement found");
		}

		// reuse statement
		return cache.fetch(query.amount);
	}
	close(
		query: CloseIteratorRequest,
		from: PublicSignKey
	): void | Promise<void> {
		this.clearupIterator(query.idString, from);
	}

	private clearupIterator(id: string, from?: PublicSignKey) {
		const cache = this.cursor.get(id);
		if (!cache) {
			return; // already cleared
		}
		if (from) {
			if (!cache.from.equals(from)) {
				return; // wrong sender
			}
		}
		clearTimeout(cache.timeout);
		cache.countStatement.finalize();
		cache.fetchStatement.finalize();
		this.cursor.delete(id);
	}
	iterator(): IterableIterator<
		[types.IdPrimitive, types.IndexedValue<Record<string, any>>]
	> {
		throw new Error("Method not implemented.");
	}
	async getSize(): Promise<number> {
		const deferred = defer<number>();
		this.db.get(
			`select count(*) as total from ${this.rootTableName}`,
			(err, res: { total: number }) => {
				if (err) {
					deferred.reject(err);
				} else {
					deferred.resolve(res.total);
				}
			}
		);
		return deferred.promise;
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
