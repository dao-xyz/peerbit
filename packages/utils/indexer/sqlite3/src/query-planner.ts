// track timing for optimal index selection
import { field, serialize, vec } from "@dao-xyz/borsh";
import { sha256Base64Sync } from "@peerbit/crypto";
import {
	And,
	BigUnsignedIntegerValue,
	BoolQuery,
	ByteMatchQuery,
	Compare,
	IntegerCompare,
	IntegerValue,
	IsNull,
	Nested,
	Not,
	Or,
	Query,
	Sort,
	StringMatch,
	UnsignedIntegerValue,
} from "@peerbit/indexer-interface";
import { hrtime } from "@peerbit/time";
import { escapeColumnName } from "./schema.js";

export interface QueryIndexPlanner {
	// assumes withing a query, each index can be picked independently. For example if we are to join two tables, we can pick the best index for each table
	// sorted column names key to execution time for each index that was tried
	columnsToIndexes: Map<
		string,
		{
			results: {
				used: number;
				avg: number;
				times: number[];
				indexKey: string;
			}[];
		}
	>; //
}

type StmtStats = Map<string, QueryIndexPlanner>;

const getSortedNameKey = (tableName: string, names: string[]) =>
	[tableName, ...names.sort()].join(",");
const createIndexKey = (tableName: string, fields: string[]) =>
	`${tableName}_index_${fields.map((x) => x).join("_")}`;

const HALF_MAX_U32 = 2147483647; // rounded down
const HALF_MAX_U64 = 9223372036854775807n; // rounded down

export const flattenQuery = function* (props?: {
	query: Query[];
	sort?: Sort[] | Sort;
}): Generator<{ query: Query[]; sort?: Sort[] | Sort } | undefined> {
	if (!props) {
		return yield props;
	}
	// if query contains OR statements, split query into multiple queries so we can run each query with union and then sort

	// TODO this only works atm for one OR statement in the query
	let ors: Query[] = [];
	let ands: Query[] = [];
	let stack = [...props.query];
	let foundOr = false;
	for (const q of stack) {
		if (q instanceof Or) {
			if (foundOr) {
				// multiple ORs are not supported
				yield props;
				return;
			}

			ors = q.or;
			foundOr = true;
		} else if (q instanceof And) {
			for (const a of q.and) {
				stack.push(a);
			}
		} else {
			ands.push(q);
		}
	}

	let maxFlatten = 4; // max 4 ORs else the query will be too big
	if (ors.length === 0 || ors.length >= maxFlatten) {
		yield {
			query: ors.length === 0 ? ands : props.query,
			sort: props.sort,
		};
		return;
	}
	for (const or of ors) {
		yield {
			query: [...ands, ...(Array.isArray(or) ? or : [or])],
			sort: props.sort,
		};
	}
};

const reduceResolution = (value: IntegerValue): IntegerValue => {
	if (value instanceof UnsignedIntegerValue) {
		return value.number > HALF_MAX_U32
			? new UnsignedIntegerValue(HALF_MAX_U32)
			: new UnsignedIntegerValue(0);
	}

	if (value instanceof BigUnsignedIntegerValue) {
		return value.value > HALF_MAX_U64
			? new BigUnsignedIntegerValue(HALF_MAX_U64)
			: new BigUnsignedIntegerValue(0n);
	}

	throw new Error("Unknown integer value type: " + value?.constructor.name);
};
const nullifyQuery = (query: Query): Query => {
	if (query instanceof IntegerCompare) {
		return new IntegerCompare({
			compare: Compare.Equal,
			value: reduceResolution(query.value),
			key: query.key,
		});
	} else if (query instanceof StringMatch) {
		return new StringMatch({
			key: query.key,
			value: "",
			method: query.method,
		});
	} else if (query instanceof ByteMatchQuery) {
		return new ByteMatchQuery({
			key: query.key,
			value: new Uint8Array(),
		});
	} else if (query instanceof BoolQuery) {
		return new BoolQuery({
			key: query.key,
			value: false,
		});
	} else if (query instanceof And) {
		let and: Query[] = [];
		for (const condition of query.and) {
			and.push(nullifyQuery(condition));
		}
		return new And(and);
	} else if (query instanceof Or) {
		let or: Query[] = [];
		for (const condition of query.or) {
			or.push(nullifyQuery(condition));
		}
		return new Or(or);
	} else if (query instanceof Not) {
		return new Not(nullifyQuery(query.not));
	} else if (query instanceof IsNull) {
		return query;
	} else if (query instanceof Nested) {
		// TODO remove
		throw new Error("Unsupported query type, deprecated");
	}

	throw new Error("Unknown query type: " + query?.constructor.name);
};

export class PlannableQuery {
	@field({ type: vec(Query) })
	query: Query[];

	@field({ type: vec(Sort) })
	sort: Sort[];

	constructor(props: { query: Query[]; sort?: Sort[] | Sort }) {
		this.query = props.query;
		this.sort = Array.isArray(props.sort)
			? props.sort
			: props.sort
				? [props.sort]
				: [];
	}

	get key(): string {
		let query = this.query.map((x) => nullifyQuery(x));
		let nullifiedPlannableQuery = new PlannableQuery({
			query: query,
			sort: this.sort,
		});
		return sha256Base64Sync(serialize(nullifiedPlannableQuery));
	}
}
export type PlanningSession = ReturnType<QueryPlanner["scope"]>;

export class QueryPlanner {
	stats: StmtStats = new Map();

	pendingIndexCreation: Map<string, Promise<void>> = new Map();

	constructor(
		readonly props: { exec: (query: string) => Promise<any> | any },
	) {}

	async stop() {
		for (const promise of this.pendingIndexCreation.values()) {
			await promise.catch(() => {});
		}
		this.stats.clear();
	}

	scope(query: PlannableQuery) {
		let obj = this.stats.get(query.key);
		if (obj === undefined) {
			obj = {
				columnsToIndexes: new Map(),
			};
			this.stats.set(query.key, obj);
		}

		// returns a function that takes column names and return the index to use
		let indexCreateCommands: { key: string; cmd: string }[] | undefined =
			undefined;
		let pickedIndexKeys: Map<string, string> = new Map(); // index key to column names key
		return {
			beforePrepare: async () => {
				// create missing indices
				if (indexCreateCommands != null) {
					for (const { key, cmd } of indexCreateCommands) {
						if (this.pendingIndexCreation.has(key)) {
							await this.pendingIndexCreation.get(key);
						}
						const promise = this.props.exec(cmd);
						this.pendingIndexCreation.set(key, promise);
						await promise;
						this.pendingIndexCreation.delete(key);
					}
				}

				if (this.pendingIndexCreation.size > 0) {
					for (const picked of pickedIndexKeys.keys()) {
						await this.pendingIndexCreation.get(picked);
					}
				}
			},
			resolveIndex: (tableName: string, columns: string[]): string => {
				// first we figure out whether we want to reuse the fastest index or try a new one
				// only assume we either do forward or backward column order for now (not all n! permutations)
				const sortedNameKey = getSortedNameKey(tableName, columns);
				let indexStats = obj.columnsToIndexes.get(sortedNameKey);
				if (indexStats === undefined) {
					indexStats = {
						results: [],
					};
					obj.columnsToIndexes.set(sortedNameKey, indexStats);
				}

				if (indexStats.results.length === 0) {
					// create both forward and backward permutations
					const permutations = generatePermutations(columns);
					for (const columns of permutations) {
						const indexKey = createIndexKey(tableName, columns);
						const command = `create index if not exists ${indexKey} on ${tableName} (${columns.map((n) => escapeColumnName(n)).join(", ")})`;

						(indexCreateCommands || (indexCreateCommands = [])).push({
							cmd: command,
							key: indexKey,
						});

						indexStats.results.push({
							used: 0,
							times: [],
							avg: -1, // setting -1 will force the first time to be the fastest (i.e. new indices are always tested once)
							indexKey,
						});
					}
				}

				// find the fastest index
				let fastestIndex = indexStats.results[0];
				fastestIndex.used++;
				pickedIndexKeys.set(fastestIndex.indexKey, sortedNameKey);

				return fastestIndex.indexKey!;
			},
			perform: async <T>(fn: () => Promise<T>): Promise<T> => {
				// perform the query and meaasure time and updates stats for used indices
				let t0 = hrtime.bigint();
				const out = await fn();
				let t1 = hrtime.bigint();
				const time = Number(t1 - t0);

				for (const [indexKey, columnsKey] of pickedIndexKeys) {
					const indexStats = obj.columnsToIndexes.get(columnsKey);
					if (indexStats === undefined) {
						throw new Error("index stats not found");
					}
					const index = indexStats.results.find((x) => x.indexKey === indexKey);
					if (index === undefined) {
						throw new Error("index not found");
					}

					// recalculate the avg by updating the time array and calculating the average
					index.times.push(time);
					if (index.times.length > 20) {
						index.times.shift();
					}
					index.avg =
						index.times.reduce((a, b) => a + b, 0) / index.times.length;

					indexStats.results.sort((a, b) => a.avg - b.avg); // make sure fastest is first
				}

				return out;
			},
		};
	}
}

const generatePermutations = (list: string[]) => {
	if (list.length === 1) return [list];
	return [list, [...list].reverse()];
};
/* const generatePermutations = (list: string[]) => {
	const results: string[][] = [];

	function permute(arr: string[], start: number) {
		if (start === arr.length - 1) {
			results.push([...arr]); // Push a copy of the current permutation
			return;
		}

		for (let i = start; i < arr.length; i++) {
			[arr[start], arr[i]] = [arr[i], arr[start]]; // Swap
			permute(arr, start + 1); // Recurse
			[arr[start], arr[i]] = [arr[i], arr[start]]; // Swap back (backtrack)
		}
	}

	permute(list, 0);
	return results;
} */
