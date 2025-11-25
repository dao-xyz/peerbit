import { field, vec } from "@dao-xyz/borsh";
import {
	And,
	BoolQuery,
	Compare,
	type Index,
	type IndexEngineInitProperties,
	type Indices,
	IntegerCompare,
	Or,
	Query,
	Sort,
	StringMatch,
	getIdProperty,
	id,
} from "@peerbit/indexer-interface";
import sodium from "libsodium-wrappers";
import * as B from "tinybench";
import { v4 as uuid } from "uuid";

const setup = async <T>(
	properties: Partial<IndexEngineInitProperties<T, any>> & { schema: any },
	createIndicies: (directory?: string) => Indices | Promise<Indices>,
	type: "transient" | "persist" = "transient",
): Promise<{ indices: Indices; store: Index<T, any>; directory?: string }> => {
	await sodium.ready;
	let directory =
		type === "persist" ? "./tmp/document-index/" + uuid() : undefined;
	const indices = await createIndicies(directory);
	await indices.start();
	const indexProps: IndexEngineInitProperties<T, any> = {
		...{
			indexBy: getIdProperty(properties.schema) || ["id"],
			iterator: { batch: { maxSize: 5e6, sizeProperty: ["__size"] } },
		},
		...properties,
	};
	const store = await indices.init(indexProps);
	return { indices, store, directory };
};

let preFillCount = 2e4;
const stringBenchmark = async (
	createIndicies: (directory?: string) => Indices | Promise<Indices>,
	type: "transient" | "persist" = "transient",
) => {
	class StringDocument {
		@id({ type: "string" })
		id: string;

		@field({ type: "string" })
		string: string;

		constructor(id: string, string: string) {
			this.id = id;
			this.string = string;
		}
	}

	const fs = await import("fs");

	const stringIndexPreFilled = await setup(
		{ schema: StringDocument },
		createIndicies,
		type,
	);
	let docCount = preFillCount;
	let fixed = uuid();
	for (let i = 0; i < docCount; i++) {
		await stringIndexPreFilled.store.put(
			new StringDocument(uuid(), i % 100 === 0 ? fixed : uuid()),
		);
	}

	const stringIndexEmpty = await setup(
		{ schema: StringDocument },
		createIndicies,
		type,
	);

	const suite = new B.Bench({ warmupIterations: 1000 });
	await suite
		.add("string put - " + type, async () => {
			await stringIndexEmpty.store.put(new StringDocument(uuid(), uuid()));
		})
		.add("string query matching - " + type, async () => {
			const iterator = stringIndexPreFilled.store.iterate({
				query: new StringMatch({ key: "string", value: fixed }),
			});
			await iterator.next(10);
			await iterator.close();
		})

		.add("string count matching - " + type, async () => {
			await stringIndexPreFilled.store.count({
				query: new StringMatch({ key: "string", value: fixed }),
			});
		})
		.add("string count no-matches - " + type, async () => {
			await stringIndexPreFilled.store.count({
				query: new StringMatch({ key: "string", value: uuid() }),
			});
		})
		.run();

	await stringIndexEmpty.indices.stop();
	stringIndexEmpty.directory &&
		fs.rmSync(stringIndexEmpty.directory, { recursive: true, force: true });

	await stringIndexPreFilled.indices.stop();
	stringIndexPreFilled.directory &&
		fs.rmSync(stringIndexPreFilled.directory, {
			recursive: true,
			force: true,
		});

	/* eslint-disable no-console */
	console.table(suite.table());
};

const boolQueryBenchmark = async (
	createIndicies: (directory?: string) => Indices | Promise<Indices>,
	type: "transient" | "persist" = "transient",
) => {
	class BoolQueryDocument {
		@id({ type: "string" })
		id: string;

		@field({ type: "bool" })
		bool: boolean;

		constructor(id: string, bool: boolean) {
			this.id = id;
			this.bool = bool;
		}
	}

	const fs = await import("fs");

	const boolIndexPrefilled = await setup(
		{ schema: BoolQueryDocument },
		createIndicies,
		type,
	);
	let docCount = preFillCount;
	for (let i = 0; i < docCount; i++) {
		await boolIndexPrefilled.store.put(
			new BoolQueryDocument(uuid(), Math.random() > 0.5 ? true : false),
		);
	}

	const boolIndexEmpty = await setup(
		{ schema: BoolQueryDocument },
		createIndicies,
		type,
	);

	const suite = new B.Bench({ warmupIterations: 1000 });
	let fetch = 10;
	await suite
		.add(`bool query fetch ${fetch} - ${type}`, async () => {
			const out = Math.random() > 0.5 ? true : false;
			const iterator = await boolIndexPrefilled.store.iterate({
				query: new BoolQuery({ key: "bool", value: out }),
			});
			await iterator.next(10);
			await iterator.close();
		})
		.add(`non bool query fetch ${fetch} - ${type}`, async () => {
			const iterator = await boolIndexPrefilled.store.iterate();
			await iterator.next(10);
			await iterator.close();
		})

		.add(`non bool query fetch with sort ${fetch} - ${type}`, async () => {
			const iterator = boolIndexPrefilled.store.iterate({
				sort: [new Sort({ key: "id" })],
			});
			await iterator.next(10);
			await iterator.close();
		})
		.add(`bool put - ${type}`, async () => {
			await boolIndexEmpty.store.put(
				new BoolQueryDocument(uuid(), Math.random() > 0.5 ? true : false),
			);
		})
		.run();

	await boolIndexEmpty.indices.stop();
	boolIndexEmpty.directory &&
		fs.rmSync(boolIndexEmpty.directory, { recursive: true, force: true });

	await boolIndexPrefilled.indices.stop();
	boolIndexPrefilled.directory &&
		fs.rmSync(boolIndexPrefilled.directory, {
			recursive: true,
			force: true,
		});

	/* eslint-disable no-console */
	console.table(suite.table());
};

const inequalityBenchmark = async (
	createIndicies: (directory?: string) => Indices | Promise<Indices>,
	type: "transient" | "persist" = "transient",
) => {
	class NumberQueryDocument {
		@id({ type: "string" })
		id: string;

		@field({ type: "u32" })
		number: number;

		constructor(id: string, number: number) {
			this.id = id;
			this.number = number;
		}
	}

	const fs = await import("fs");

	const numberIndexPrefilled = await setup(
		{ schema: NumberQueryDocument },
		createIndicies,
		type,
	);
	let docCount = 10e4;
	for (let i = 0; i < docCount; i++) {
		await numberIndexPrefilled.store.put(new NumberQueryDocument(uuid(), i));
	}

	const boolIndexEmpty = await setup(
		{ schema: NumberQueryDocument },
		createIndicies,
		type,
	);

	// warmup
	for (let i = 0; i < 1000; i++) {
		const iterator = numberIndexPrefilled.store.iterate({
			query: new IntegerCompare({
				key: "number",
				compare: Compare.Less,
				value: 11,
			}),
		});
		await iterator.next(10);
		await iterator.close();
	}

	const suite = new B.Bench({ warmupIterations: 1000 });
	let fetch = 10;
	await suite
		.add(`number query fetch ${fetch} - ${type}`, async () => {
			const iterator = numberIndexPrefilled.store.iterate({
				query: new IntegerCompare({
					key: "number",
					compare: Compare.Less,
					value: 11,
				}),
			});
			await iterator.next(10);
			await iterator.close();
		})

		.add(`non number query fetch ${fetch} - ${type}`, async () => {
			const iterator = numberIndexPrefilled.store.iterate();
			await iterator.next(10);
			await iterator.close();
		})

		.add(`number put - ${type}`, async () => {
			await boolIndexEmpty.store.put(
				new NumberQueryDocument(uuid(), Math.round(Math.random() * 0xffffffff)),
			);
		})
		.run();

	await boolIndexEmpty.indices.stop();
	boolIndexEmpty.directory &&
		fs.rmSync(boolIndexEmpty.directory, { recursive: true, force: true });

	await numberIndexPrefilled.indices.stop();
	numberIndexPrefilled.directory &&
		fs.rmSync(numberIndexPrefilled.directory, {
			recursive: true,
			force: true,
		});

	/* eslint-disable no-console */
	console.table(suite.table());
};

const getBenchmark = async (
	createIndicies: (directory?: string) => Indices | Promise<Indices>,
	type: "transient" | "persist" = "transient",
) => {
	class BoolQueryDocument {
		@id({ type: "string" })
		id: string;

		@field({ type: "bool" })
		bool: boolean;

		constructor(id: string, bool: boolean) {
			this.id = id;
			this.bool = bool;
		}
	}

	const fs = await import("fs");

	const boolIndexPrefilled = await setup(
		{ schema: BoolQueryDocument },
		createIndicies,
		type,
	);
	let docCount = preFillCount;
	let ids = [];
	for (let i = 0; i < docCount; i++) {
		let id = uuid();
		ids.push(id);
		await boolIndexPrefilled.store.put(
			new BoolQueryDocument(id, Math.random() > 0.5 ? true : false),
		);
	}

	const boolIndexEmpty = await setup(
		{ schema: BoolQueryDocument },
		createIndicies,
		type,
	);

	const suite = new B.Bench({ warmupIterations: 1000 });
	await suite
		.add("get by id - " + type, async () => {
			await boolIndexPrefilled.store.get(
				ids[Math.floor(Math.random() * ids.length)],
			);
		})
		.run();
	await boolIndexEmpty.indices.stop();
	boolIndexEmpty.directory &&
		fs.rmSync(boolIndexEmpty.directory, { recursive: true, force: true });

	await boolIndexPrefilled.indices.stop();
	boolIndexPrefilled.directory &&
		fs.rmSync(boolIndexPrefilled.directory, {
			recursive: true,
			force: true,
		});

	/* eslint-disable no-console */
	console.table(suite.table());
};

const nestedBoolQueryBenchmark = async (
	createIndicies: (directory?: string) => Indices | Promise<Indices>,
	type: "transient" | "persist" = "transient",
) => {
	class Nested {
		@field({ type: "bool" })
		bool: boolean;

		constructor(bool: boolean) {
			this.bool = bool;
		}
	}

	class NestedBoolQueryDocument {
		@id({ type: "string" })
		id: string;

		@field({ type: Nested })
		nested: Nested;

		constructor(id: string, bool: boolean) {
			this.id = id;
			this.nested = new Nested(bool);
		}
	}

	const fs = await import("fs");

	const boolIndexPrefilled = await setup(
		{ schema: NestedBoolQueryDocument },
		createIndicies,
		type,
	);

	let docCount = preFillCount;
	for (let i = 0; i < docCount; i++) {
		await boolIndexPrefilled.store.put(
			new NestedBoolQueryDocument(uuid(), i % 2 === 0 ? true : false),
		);
	}

	const boolIndexEmpty = await setup(
		{ schema: NestedBoolQueryDocument },
		createIndicies,
		type,
	);

	const suite = new B.Bench({ warmupIterations: 1000 });

	await suite
		.add("nested bool query - " + type, async () => {
			const out = Math.random() > 0.5 ? true : false;
			const iterator = await boolIndexPrefilled.store.iterate({
				query: new BoolQuery({ key: ["nested", "bool"], value: out }),
			});
			await iterator.next(10);
			await iterator.close();
		})
		.add("nested bool put - " + type, async () => {
			await boolIndexEmpty.store.put(
				new NestedBoolQueryDocument(uuid(), Math.random() > 0.5 ? true : false),
			);
		})
		.run();
	await boolIndexEmpty.indices.stop();
	boolIndexEmpty.directory &&
		fs.rmSync(boolIndexEmpty.directory, { recursive: true, force: true });

	await boolIndexPrefilled.indices.stop();
	boolIndexPrefilled.directory &&
		fs.rmSync(boolIndexPrefilled.directory, {
			recursive: true,
			force: true,
		});

	/* eslint-disable no-console */
	console.table(suite.table());
};

const shapedQueryBenchmark = async (
	createIndicies: (directory?: string) => Indices | Promise<Indices>,
	type: "transient" | "persist" = "transient",
) => {
	class Nested {
		@field({ type: "bool" })
		bool: boolean;

		constructor(bool: boolean) {
			this.bool = bool;
		}
	}

	class NestedBoolQueryDocument {
		@id({ type: "string" })
		id: string;

		@field({ type: vec(Nested) })
		nested: Nested[];

		constructor(id: string, nested: Nested[]) {
			this.id = id;
			this.nested = nested;
		}
	}

	const fs = await import("fs");

	const boolIndexPrefilled = await setup(
		{ schema: NestedBoolQueryDocument },
		createIndicies,
		type,
	);

	let docCount = 1e4;
	for (let i = 0; i < docCount; i++) {
		await boolIndexPrefilled.store.put(
			new NestedBoolQueryDocument(uuid(), [
				new Nested(i % 2 === 0 ? true : false),
			]),
		);
	}

	const suite = new B.Bench({ warmupIterations: 1000 });
	let fetch = 10;
	await suite
		.add("unshaped nested array query - " + type, async () => {
			const out = Math.random() > 0.5 ? true : false;
			let iterator = await boolIndexPrefilled.store.iterate({
				query: new BoolQuery({ key: ["nested", "bool"], value: out }),
			});
			const results = await iterator.next(fetch);
			await iterator.close();
			if (results.length !== fetch) {
				throw new Error("Missing results");
			}
		})
		.add("shaped nested array query - " + type, async () => {
			const out = Math.random() > 0.5 ? true : false;
			const iterator = boolIndexPrefilled.store.iterate(
				{
					query: new BoolQuery({ key: ["nested", "bool"], value: out }),
				},
				{ shape: { id: true } },
			);
			const results = await iterator.next(fetch);
			await iterator.close();
			if (results.length !== fetch) {
				throw new Error("Missing results");
			}
		})
		.add("nested fetch without query - " + type, async () => {
			const iterator = boolIndexPrefilled.store.iterate(
				{},
				{ shape: { id: true } },
			);
			const results = await iterator.next(fetch);
			await iterator.close();
			if (results.length !== fetch) {
				throw new Error("Missing results");
			}
		})
		.run();

	await boolIndexPrefilled.indices.stop();
	boolIndexPrefilled.directory &&
		fs.rmSync(boolIndexPrefilled.directory, {
			recursive: true,
			force: true,
		});

	/* eslint-disable no-console */
	console.table(suite.table());
};

const multiFieldQueryBenchmark = async (
	createIndicies: (directory?: string) => Indices | Promise<Indices>,
	type: "transient" | "persist" = "transient",
) => {
	class ReplicationRangeIndexableU32 {
		@id({ type: "string" })
		id: string;

		@field({ type: "string" })
		hash: string;

		@field({ type: "u64" })
		timestamp: bigint;

		@field({ type: "u32" })
		start1!: number;

		@field({ type: "u32" })
		end1!: number;

		@field({ type: "u32" })
		start2!: number;

		@field({ type: "u32" })
		end2!: number;

		@field({ type: "u32" })
		width!: number;

		@field({ type: "u8" })
		mode: number;

		constructor(properties: {
			id?: string;
			hash: string;
			timestamp: bigint;
			start1: number;
			end1: number;
			start2: number;
			end2: number;
			width: number;
			mode: number;
		}) {
			this.id = properties.id || uuid();
			this.hash = properties.hash;
			this.timestamp = properties.timestamp;
			this.start1 = properties.start1;
			this.end1 = properties.end1;
			this.start2 = properties.start2;
			this.end2 = properties.end2;
			this.width = properties.width;
			this.mode = properties.mode;
		}
	}

	const indexPrefilled = await setup(
		{ schema: ReplicationRangeIndexableU32 },
		createIndicies,
		type,
	);

	let docCount = 10e4; // This is very small, so we expect that the ops will be very fast (i.e a few amount to join)
	for (let i = 0; i < docCount; i++) {
		await indexPrefilled.store.put(
			new ReplicationRangeIndexableU32({
				hash: uuid(),
				timestamp: BigInt(i),
				start1: i,
				end1: i + 1,
				start2: i + 2,
				end2: i + 3,
				width: i + 4,
				mode: i % 3,
			}),
		);
	}

	const suite = new B.Bench({ warmupIterations: 500 });
	let fetch = 10;

	const fs = await import("fs");
	const ors: any[] = [];
	for (const point of [10, docCount - 4]) {
		ors.push(
			new And([
				new IntegerCompare({
					key: "start1",
					compare: Compare.LessOrEqual,
					value: point,
				}),
				new IntegerCompare({
					key: "end1",
					compare: Compare.Greater,
					value: point,
				}),
			]),
		);
		ors.push(
			new And([
				new IntegerCompare({
					key: "start2",
					compare: Compare.LessOrEqual,
					value: point,
				}),
				new IntegerCompare({
					key: "end2",
					compare: Compare.Greater,
					value: point,
				}),
			]),
		);
	}
	let complicatedQuery = [
		new Or(ors) /* ,
		new IntegerCompare({
			key: "timestamp",
			compare: Compare.Less,
			value: 100
		}) */,
	];

	const suites: { query: Query[]; name: string }[] = [
		/* {
			query: [
				new IntegerCompare({
					key: "start1",
					compare: Compare.LessOrEqual,
					value: 5,
				}),
				new IntegerCompare({
					key: "end1",
					compare: Compare.Greater,
					value: 5,
				})
			], name: "2-fields query"
		},
		{
			query: [

				new IntegerCompare({
					key: "start1",
					compare: Compare.LessOrEqual,
					value: 5,
				}),
				new IntegerCompare({
					key: "end1",
					compare: Compare.Greater,
					value: 5,
				}),
				new IntegerCompare({
					key: "timestamp",
					compare: Compare.Less,
					value: 10,
				}),
			], name: "3-fields query"
		}, */
		{ query: complicatedQuery, name: "3-fields or query" },
	];
	suites.forEach(({ query, name }) => {
		suite.add(`m-field ${name} query small fetch - ${type}`, async () => {
			const iterator = await indexPrefilled.store.iterate({
				query,
			});
			const results = await iterator.next(fetch);
			await iterator.close();

			if (results.length === 0) {
				throw new Error("No results");
			}
		});

		/* .add(`m-field ${name} query all fetch - ${type}`, async () => {
			const iterator = indexPrefilled.store.iterate({
				query,
			});
			const results = await iterator.all();

			if (results.length === 0) {
				throw new Error("No results");
			}
		}) */
	});

	/* suite.add("m-field no query small fetch - " + type, async () => {
		const iterator = await indexPrefilled.store.iterate();
		const results = await iterator.next(fetch);
		await iterator.close();

		if (results.length === 0) {
			throw new Error("No results");
		}
	}) */
	await suite.run();

	await indexPrefilled.indices.stop();
	indexPrefilled.directory &&
		fs.rmSync(indexPrefilled.directory, {
			recursive: true,
			force: true,
		});

	/* eslint-disable no-console */
	console.table(suite.table());
};

export const benchmarks = async (
	createIndicies: (directory?: string) => Indices | Promise<Indices>,
	type: "transient" | "persist" = "transient",
) => {
	await inequalityBenchmark(createIndicies, type);
	await multiFieldQueryBenchmark(createIndicies, type);
	await stringBenchmark(createIndicies, type);
	await shapedQueryBenchmark(createIndicies, type);
	await getBenchmark(createIndicies, type);
	await boolQueryBenchmark(createIndicies, type);
	await nestedBoolQueryBenchmark(createIndicies, type);
};
