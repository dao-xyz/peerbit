// track timing for optimal index selection
import { field, serialize, vec } from "@dao-xyz/borsh";
import { sha256Base64Sync } from "@peerbit/crypto";
import { Query, Sort } from "@peerbit/indexer-interface";
import { hrtime } from "@peerbit/time";
import { escapeColumnName } from "./schema";

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
		return sha256Base64Sync(serialize(this));
	}
}
export type PlanningSession = ReturnType<QueryPlanner["scope"]>;

export class QueryPlanner {
	stats: StmtStats = new Map();

	constructor(
		readonly props: { exec: (query: string) => Promise<any> | any },
	) {}

	scope(query: PlannableQuery) {
		let obj = this.stats.get(query.key);
		if (obj === undefined) {
			obj = {
				columnsToIndexes: new Map(),
			};
			this.stats.set(query.key, obj);
		}

		// returns a function that takes column names and return the index to use
		let indexCreateCommands: string[] | undefined = undefined;
		let pickedIndexKeys: Map<string, string> = new Map(); // index key to column names key
		return {
			beforePrepare: async () => {
				// create missing indices
				if (indexCreateCommands != null) {
					for (const cmd of indexCreateCommands) {
						// console.log(cmd)
						await this.props.exec(cmd);
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

						(indexCreateCommands || (indexCreateCommands = [])).push(command);
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

				/*    console.log("INDEX STATS", indexStats.results.map(x => {
                       return {
                           key: x.indexKey,
                           used: x.used,
                           avg: x.avg,
                       }
                   }), columns); */

				//  console.log("FASTEST", fastestIndex.indexKey)
				return fastestIndex.indexKey!;
			},
			perform: async <T>(fn: () => Promise<T>): Promise<T> => {
				// perform the query and meaasure time and updates stats for used indices
				let t0 = hrtime.bigint();
				const out = await fn();
				let t1 = hrtime.bigint();
				const time = Number(t1 - t0);
				//  console.log("MEASURE TIME", time, "FOR", [...pickedIndexKeys.keys()]);

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
					if (index.times.length > 10) {
						index.times.shift();
					}
					index.avg =
						index.times.reduce((a, b) => a + b, 0) / index.times.length;

					indexStats.results.sort((a, b) => a.avg - b.avg); // make sure fastest is first
					//   console.log("INDEX STATS", indexStats.results.map(x => x.lastTime));
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
