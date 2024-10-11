import { field, vec } from "@dao-xyz/borsh";
import {
	BoolQuery,
	type Index,
	type IndexEngineInitProperties,
	type Indices,
	StringMatch,
	getIdProperty,
	id,
} from "@peerbit/indexer-interface";
import B from "benchmark";
import sodium from "libsodium-wrappers";
import pDefer from "p-defer";
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
const strinbBenchmark = async (
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

	let done = pDefer();
	const suite = new B.Suite({ delay: 100 });
	suite
		.add("string put - " + type, {
			fn: async (deferred: any) => {
				await stringIndexEmpty.store.put(new StringDocument(uuid(), uuid()));
				deferred.resolve();
			},
			defer: true,
			maxTime: 5,
		})
		.add("string query matching - " + type, {
			fn: async (deferred: any) => {
				const iterator = stringIndexPreFilled.store.iterate({
					query: new StringMatch({ key: "string", value: fixed }),
				});
				await iterator.next(10);
				await iterator.close();
				deferred.resolve();
			},
			defer: true,
			maxTime: 5,
		})

		.add("string count matching - " + type, {
			fn: async (deferred: any) => {
				await stringIndexPreFilled.store.count({
					query: new StringMatch({ key: "string", value: fixed }),
				});
				deferred.resolve();
			},
			defer: true,
			maxTime: 5,
		})
		.add("string count no-matches - " + type, {
			fn: async (deferred: any) => {
				const out = Math.random() > 0.5 ? true : false;
				await stringIndexPreFilled.store.count({
					query: new StringMatch({ key: "string", value: uuid() }),
				});
				deferred.resolve();
			},
			defer: true,
			maxTime: 5,
		})
		.on("cycle", async (event: any) => {
			// eslint-disable-next-line no-console
			console.log(String(event.target));
		})
		.on("error", (err: any) => {
			throw err;
		})
		.on("complete", async () => {
			await stringIndexEmpty.indices.stop();
			stringIndexEmpty.directory &&
				fs.rmSync(stringIndexEmpty.directory, { recursive: true, force: true });

			await stringIndexPreFilled.indices.stop();
			stringIndexPreFilled.directory &&
				fs.rmSync(stringIndexPreFilled.directory, {
					recursive: true,
					force: true,
				});

			done.resolve();
		})
		.on("error", (e) => {
			done.reject(e);
		})
		.run();
	return done.promise;
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

	let done = pDefer();
	const suite = new B.Suite({ delay: 100 });
	suite
		.add("bool query - " + type, {
			fn: async (deferred: any) => {
				const out = Math.random() > 0.5 ? true : false;
				const iterator = await boolIndexPrefilled.store.iterate({
					query: new BoolQuery({ key: "bool", value: out }),
				});
				await iterator.next(10);
				await iterator.close();
				deferred.resolve();
			},
			defer: true,
			maxTime: 5,
		})
		.add("bool put - " + type, {
			fn: async (deferred: any) => {
				await boolIndexEmpty.store.put(
					new BoolQueryDocument(uuid(), Math.random() > 0.5 ? true : false),
				);
				deferred.resolve();
			},
			defer: true,
			maxTime: 5,
		})
		.on("cycle", async (event: any) => {
			// eslint-disable-next-line no-console
			console.log(String(event.target));
		})
		.on("error", (err: any) => {
			throw err;
		})
		.on("complete", async () => {
			await boolIndexEmpty.indices.stop();
			boolIndexEmpty.directory &&
				fs.rmSync(boolIndexEmpty.directory, { recursive: true, force: true });

			await boolIndexPrefilled.indices.stop();
			boolIndexPrefilled.directory &&
				fs.rmSync(boolIndexPrefilled.directory, {
					recursive: true,
					force: true,
				});

			done.resolve();
		})
		.on("error", (e) => {
			done.reject(e);
		})
		.run();
	return done.promise;
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

	let done = pDefer();
	const suite = new B.Suite({ delay: 100 });

	suite
		.add("nested bool query - " + type, {
			fn: async (deferred: any) => {
				const out = Math.random() > 0.5 ? true : false;
				const iterator = await boolIndexPrefilled.store.iterate({
					query: new BoolQuery({ key: ["nested", "bool"], value: out }),
				});
				await iterator.next(10);
				await iterator.close();
				deferred.resolve();
			},
			defer: true,
			maxTime: 5,
			async: true,
		})
		.add("nested bool put - " + type, {
			fn: async (deferred: any) => {
				await boolIndexEmpty.store.put(
					new NestedBoolQueryDocument(
						uuid(),
						Math.random() > 0.5 ? true : false,
					),
				);
				deferred.resolve();
			},
			defer: true,
			maxTime: 5,
			async: true,
		})
		.on("cycle", async (event: any) => {
			// eslint-disable-next-line no-console
			console.log(String(event.target));
		})
		.on("error", (err: any) => {
			done.reject(err);
		})
		.on("complete", async () => {
			await boolIndexEmpty.indices.stop();
			boolIndexEmpty.directory &&
				fs.rmSync(boolIndexEmpty.directory, { recursive: true, force: true });

			await boolIndexPrefilled.indices.stop();
			boolIndexPrefilled.directory &&
				fs.rmSync(boolIndexPrefilled.directory, {
					recursive: true,
					force: true,
				});
			done.resolve();
		})
		.run();
	return done.promise;
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

	let done = pDefer();
	const suite = new B.Suite({ delay: 100 });
	let fetch = 10;
	suite
		.add("unshaped nested query - " + type, {
			fn: async (deferred: any) => {
				const out = Math.random() > 0.5 ? true : false;
				let iterator = await boolIndexPrefilled.store.iterate({
					query: new BoolQuery({ key: ["nested", "bool"], value: out }),
				});
				const results = await iterator.next(fetch);
				await iterator.close();
				if (results.length !== fetch) {
					throw new Error("Missing results");
				}
				deferred.resolve();
			},
			defer: true,
			maxTime: 5,
			async: true,
		})
		.add("shaped nested query - " + type, {
			fn: async (deferred: any) => {
				const out = Math.random() > 0.5 ? true : false;
				const iterator = await boolIndexPrefilled.store.iterate(
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
				deferred.resolve();
			},
			defer: true,
			maxTime: 5,
			async: true,
		})
		.on("cycle", async (event: any) => {
			// eslint-disable-next-line no-console
			console.log(String(event.target));
		})
		.on("error", (err: any) => {
			done.reject(err);
		})
		.on("complete", async () => {
			await boolIndexPrefilled.indices.stop();
			boolIndexPrefilled.directory &&
				fs.rmSync(boolIndexPrefilled.directory, {
					recursive: true,
					force: true,
				});
			done.resolve();
		})
		.run();
	return done.promise;
};

export const benchmarks = async (
	createIndicies: (directory?: string) => Indices | Promise<Indices>,
	type: "transient" | "persist" = "transient",
) => {
	await strinbBenchmark(createIndicies, type);
	await shapedQueryBenchmark(createIndicies, type);
	await boolQueryBenchmark(createIndicies, type);
	await nestedBoolQueryBenchmark(createIndicies, type);
};
