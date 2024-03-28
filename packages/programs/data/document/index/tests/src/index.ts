import { field, option, variant, vec } from "@dao-xyz/borsh";
import {
	IntegerCompare,
	StringMatch,
	Compare,
	MissingField,
	And,
	SearchRequest,
	StringMatchMethod,
	Or,
	ByteMatchQuery,
	BoolQuery,
	Sort,
	SortDirection,
	CloseIteratorRequest,
	IndexEngine,
	IndexEngineInitProperties,
	Context,
	IdPrimitive,
	toIdeable,
	IndexedResults,
	CollectNextRequest,
	extractFieldValue,
	toId
} from "@peerbit/document-interface";
import {
	PublicSignKey,
	randomBytes,
	Ed25519PublicKey,
	Ed25519Keypair
} from "@peerbit/crypto";
import { v4 as uuid } from "uuid";
import { delay, waitFor, waitForResolved } from "@peerbit/time";
import { serialize } from "@dao-xyz/borsh";
import sodium from "libsodium-wrappers";
import { expect } from "@jest/globals";

import type { MatcherFunction } from "expect";

const toEqualNumber: MatcherFunction<[value: unknown]> = function (
	received,
	expected
) {
	// parse both as numbers
	const pass = Number(received) === Number(expected);

	if (pass) {
		return {
			message: () =>
				`expected ${this.utils.printReceived(
					received
				)} not to equal ${this.utils.printExpected(expected)}`,
			pass: true
		};
	} else {
		return {
			message: () =>
				`expected ${this.utils.printReceived(
					received
				)} to equal ${this.utils.printExpected(expected)}`,
			pass: false
		};
	}
};
expect.extend({
	toEqualNumber
});

import * as matchers from "jest-extended";
type JestExtendedMatchers = typeof matchers & {
	toEqualNumber: (value: unknown) => void;
};

declare module "@jest/expect" {
	interface Matchers<R> extends JestExtendedMatchers {}
}

@variant(0)
class Document {
	@field({ type: "string" })
	id: string;

	@field({ type: option("string") })
	name?: string;

	@field({ type: option("u64") })
	number?: bigint;

	@field({ type: option(vec("string")) })
	tags?: string[];

	@field({ type: option("bool") })
	bool?: boolean;

	@field({ type: option(Uint8Array) })
	data?: Uint8Array;

	constructor(opts: Document) {
		this.id = opts.id;
		this.name = opts.name;
		this.number = opts.number;
		this.tags = opts.tags;
		this.bool = opts.bool;
		this.data = opts.data;
	}
}

const bigIntSort = <T extends number | bigint>(a: T, b: T): number =>
	a > b ? 1 : 0 || -(a < b);

class IndexWrapper {
	indexByResolver: (obj: any) => string | bigint | number;

	constructor(
		readonly index: IndexEngine,
		indexBy: string[] | string
	) {
		this.indexByResolver =
			typeof indexBy === "string"
				? (obj) => obj[indexBy as string]
				: (obj: any) => extractFieldValue(obj, indexBy as string[]);
	}
	getSize() {
		return this.index.getSize();
	}
	del(key: IdPrimitive) {
		return this.index.del(toId(key));
	}

	get(key: IdPrimitive) {
		return this.index.get(toId(key));
	}
	put(doc: any) {
		return this.index.put({
			id: toId(this.indexByResolver(doc)),
			indexed: doc,
			size: serialize(doc).length,
			context: new Context({
				created: BigInt(+new Date()),
				modified: BigInt(+new Date()),
				gid: uuid(),
				head: uuid()
			})
		});
	}
	stop() {
		return this.index.stop?.();
	}
	search(
		query: SearchRequest,
		from: PublicSignKey = new Ed25519PublicKey({ publicKey: randomBytes(32) })
	) {
		// fetch max u32
		query.fetch = 0xffffffff;
		return this.index.query(query, from);
	}

	iterate(
		query: SearchRequest,
		from: PublicSignKey = new Ed25519PublicKey({ publicKey: randomBytes(32) })
	) {
		let done = false;
		let fetchedOnce = false;
		return {
			next: async (count: number) => {
				let res: IndexedResults;
				if (!fetchedOnce) {
					fetchedOnce = true;
					query.fetch = count;
					res = await this.index.query(query, from);
				} else {
					res = await this.index.next(
						new CollectNextRequest({ id: query.id, amount: count }),
						from
					);
				}
				done = res.kept === 0;
				return res;
			},
			done: () => done,
			close: () => {
				return this.index.close(
					new CloseIteratorRequest({ id: query.id }),
					from
				);
			}
		};
	}
}

export const tests = (createIndex: () => IndexEngine) => {
	const setup = async (
		properties: Partial<IndexEngineInitProperties<IndexWrapper>> = {}
	) => {
		await sodium.ready;
		const index = createIndex();
		const initArgs = {
			...{
				indexBy: "id",
				maxBatchSize: 5e6,
				nested: {
					match: (obj): obj is IndexWrapper => obj instanceof IndexWrapper,
					query: (nested, query) => nested.search(query)
				}
			},
			...properties
		};
		await index.init(initArgs);
		await index.start?.();
		return new IndexWrapper(index, initArgs.indexBy);
	};

	return describe("index", () => {
		let store: Awaited<ReturnType<typeof setup>>;

		const setupDefault = async () => {
			// Create store
			store = await setup({ schema: Document });
			const doc = new Document({
				id: "1",
				name: "hello",
				number: 1n
			});

			const docEdit = new Document({
				id: "1",
				name: "hello world",
				number: 1n,
				bool: true,
				data: new Uint8Array([1])
			});

			const doc2 = new Document({
				id: "2",
				name: "hello world",
				number: 4n
			});

			const doc2Edit = new Document({
				id: "2",
				name: "Hello World",
				number: 2n,
				data: new Uint8Array([2])
			});

			const doc3 = new Document({
				id: "3",
				name: "foo",
				number: 3n,
				data: new Uint8Array([3])
			});

			const doc4 = new Document({
				id: "4",
				name: undefined,
				number: undefined
			});

			await store.put(doc);
			await waitForResolved(async () =>
				expect(await store.getSize()).toEqual(1)
			);
			await store.put(docEdit);
			await store.put(doc2);
			await waitForResolved(async () =>
				expect(await store.getSize()).toEqual(2)
			);
			await store.put(doc2Edit);
			await store.put(doc3);
			await store.put(doc4);
			await waitForResolved(async () =>
				expect(await store.getSize()).toEqual(4)
			);
		};

		afterEach(async () => {
			await store?.stop?.();
		});

		describe("indexBy", () => {
			const testIndex = async (
				store: Awaited<ReturnType<typeof setup>>,
				doc: any
			) => {
				await store.put(doc);
				let result = await store.get(doc.id);
				expect(result).toBeDefined();
				await store.del(doc.id);
				expect(await store.getSize()).toEqual(0);
				result = await store.get(doc.id);
				expect(result).toBeUndefined();
			};

			describe("string", () => {
				class SimpleDocument {
					@field({ type: "string" })
					id: string;

					@field({ type: "string" })
					value: string;

					constructor(properties: { id: string; value: string }) {
						this.id = properties.id;
						this.value = properties.value;
					}
				}

				it("will throw error if indexBy does not exist in document", async () => {
					try {
						store = await setup({
							indexBy: "__missing__",
							schema: SimpleDocument
						});
					} catch (error: any) {
						// some impl might want to throw here, since the schema is known in advance and the indexBy will be missing
						expect(error["message"]).toEqual(
							"Primary key __missing__ not found in schema"
						);
						return;
					}
					const doc = new SimpleDocument({
						id: "abc 123",
						value: "Hello world"
					});

					// else throw when putting the doc
					expect(() => store.put(doc)).toThrow(
						"Unexpected index key: undefined, expected: string, number, bigint or Uint8Array"
					);
				});

				it("index by another property", async () => {
					store = await setup({ indexBy: "value", schema: SimpleDocument });

					const helloWorld = "Hello world";
					const doc = new SimpleDocument({
						id: "abc 123",
						value: helloWorld
					});

					// put doc
					await store.put(doc);

					expect((await store.get(helloWorld))?.indexed.value).toEqual(
						helloWorld
					);
				});

				it("can StringQuery index", async () => {
					store = await setup({ indexBy: "value", schema: SimpleDocument });

					const doc = new SimpleDocument({
						id: "abc 123",
						value: "Hello world"
					});

					await store.put(doc);

					const results = await store.search(
						new SearchRequest({
							query: [
								new StringMatch({
									key: "id",
									value: "123",
									caseInsensitive: false,
									method: StringMatchMethod.contains
								})
							]
						})
					);
					expect(results.results).toHaveLength(1);
				});
			});

			describe("bytes", () => {
				class DocumentUint8arrayId {
					@field({ type: Uint8Array })
					id: Uint8Array;

					@field({ type: "string" })
					value: string;

					constructor(properties: { id: Uint8Array; value: string }) {
						this.id = properties.id;
						this.value = properties.value;
					}
				}

				it("index as Uint8array", async () => {
					store = await setup({ schema: DocumentUint8arrayId });

					const id = new Uint8Array([1, 2, 3]);
					const doc = new DocumentUint8arrayId({
						id,
						value: "Hello world"
					});
					await testIndex(store, doc);
				});
			});
			describe("number", () => {
				class DocumentNumberId {
					@field({ type: "u32" })
					id: number;

					@field({ type: "string" })
					value: string;

					constructor(properties: { id: number; value: string }) {
						this.id = properties.id;
						this.value = properties.value;
					}
				}

				it("index as number", async () => {
					store = await setup({ schema: DocumentNumberId });

					const id = 123456789;
					const doc = new DocumentNumberId({
						id,
						value: "Hello world"
					});

					await testIndex(store, doc);
				});
			});

			describe("bigint", () => {
				class DocumentBigintId {
					@field({ type: "u64" })
					id: bigint;

					@field({ type: "string" })
					value: string;

					constructor(properties: { id: bigint; value: string }) {
						this.id = properties.id;
						this.value = properties.value;
					}
				}

				it("index as bigint", async () => {
					store = await setup({ schema: DocumentBigintId });

					// make the id less than 2^53, but greater than u32 max
					const id = BigInt(2 ** 53 - 1);
					const doc = new DocumentBigintId({
						id,
						value: "Hello world"
					});
					await testIndex(store, doc);
				});
			});
		});

		describe("search", () => {
			describe("fields", () => {
				it("no-args", async () => {
					await setupDefault();

					const results = await store.search(new SearchRequest({ query: [] }));
					expect(results.results).toHaveLength(4);
				});

				describe("string", () => {
					beforeEach(async () => {
						await setupDefault();
					});
					it("exact", async () => {
						const responses = await store.search(
							new SearchRequest({
								query: [
									new StringMatch({
										key: "name",
										value: "hello world",
										caseInsensitive: true
									})
								]
							})
						);
						expect(
							responses.results.map((x) => x.id.primitive)
						).toContainAllValues(["1", "2"]);
					});

					it("exact-case-insensitive", async () => {
						const responses = await store.search(
							new SearchRequest({
								query: [
									new StringMatch({
										key: "name",
										value: "Hello World",
										caseInsensitive: true
									})
								]
							})
						);
						expect(responses.results).toHaveLength(2);
						expect(
							responses.results.map((x) => x.id.primitive)
						).toContainAllValues(["1", "2"]);
					});

					it("exact case sensitive", async () => {
						let responses = await store.search(
							new SearchRequest({
								query: [
									new StringMatch({
										key: "name",
										value: "Hello World",
										caseInsensitive: false
									})
								]
							})
						);
						expect(responses.results).toHaveLength(1);
						expect(
							responses.results.map((x) => x.id.primitive)
						).toContainAllValues(["2"]);
						responses = await store.search(
							new SearchRequest({
								query: [
									new StringMatch({
										key: "name",
										value: "hello world",
										caseInsensitive: false
									})
								]
							})
						);
						expect(
							responses.results.map((x) => x.id.primitive)
						).toContainAllValues(["1"]);
					});
					it("prefix", async () => {
						const responses = await store.search(
							new SearchRequest({
								query: [
									new StringMatch({
										key: "name",
										value: "hel",
										method: StringMatchMethod.prefix,
										caseInsensitive: true
									})
								]
							})
						);
						expect(responses.results).toHaveLength(2);
						expect(
							responses.results.map((x) => x.id.primitive)
						).toContainAllValues(["1", "2"]);
					});

					it("contains", async () => {
						const responses = await store.search(
							new SearchRequest({
								query: [
									new StringMatch({
										key: "name",
										value: "ello",
										method: StringMatchMethod.contains,
										caseInsensitive: true
									})
								]
							})
						);
						expect(responses.results).toHaveLength(2);
						expect(
							responses.results.map((x) => x.id.primitive)
						).toContainAllValues(["1", "2"]);
					});

					describe("arr", () => {
						const docArray1 = new Document({
							id: "a",
							name: "_",
							number: undefined,
							tags: ["Hello", "World"]
						});

						const docArray2 = new Document({
							id: "b",
							name: "__",
							number: undefined,
							tags: ["Hello"]
						});
						beforeEach(async () => {
							await store.put(docArray1);
							await store.put(docArray2);
						});
						afterEach(async () => {
							await store.del(toIdeable(docArray1.id));
							await store.del(toIdeable(docArray2.id));
						});
						it("arr", async () => {
							const responses = await store.search(
								new SearchRequest({
									query: [
										new StringMatch({
											key: "tags",
											value: "world",
											method: StringMatchMethod.contains,
											caseInsensitive: true
										})
									]
								})
							);
							expect(responses.results).toHaveLength(1);
							expect(
								responses.results.map((x) => x.id.primitive)
							).toContainAllValues(["a"]);
						});
					});
				});

				it("missing", async () => {
					await setupDefault();

					const responses = await store.search(
						new SearchRequest({
							query: [
								new MissingField({
									key: "name"
								})
							]
						})
					);
					expect(responses.results).toHaveLength(1);
					expect(responses.results.map((x) => x.id.primitive)).toEqual(["4"]);
				});

				describe("uint8arrays", () => {
					describe("bytematch", () => {
						it("matches", async () => {
							await setupDefault();

							const responses = await store.search(
								new SearchRequest({
									query: [
										new ByteMatchQuery({
											key: "data",
											value: Buffer.from([1])
										})
									]
								})
							);
							expect(responses.results).toHaveLength(1);
							expect(responses.results.map((x) => x.id.primitive)).toEqual([
								"1"
							]);
						});
						it("un-matches", async () => {
							await setupDefault();

							const responses = await store.search(
								new SearchRequest({
									query: [
										new ByteMatchQuery({
											key: "data",
											value: Buffer.from([199])
										})
									]
								})
							);
							expect(responses.results).toHaveLength(0);
						});
					});
					describe("integer", () => {
						it("exists", async () => {
							await setupDefault();

							const responses = await store.search(
								new SearchRequest({
									query: [
										new IntegerCompare({
											key: "data",
											compare: Compare.Equal,
											value: 1
										})
									]
								})
							);
							expect(responses.results).toHaveLength(1);
							expect(responses.results.map((x) => x.id.primitive)).toEqual([
								"1"
							]);
						});

						it("does not exist", async () => {
							await setupDefault();

							const responses = await store.search(
								new SearchRequest({
									query: [
										new IntegerCompare({
											key: "data",
											compare: Compare.Equal,
											value: 199
										})
									]
								})
							);
							expect(responses.results).toHaveLength(0);
						});
					});
				});
				it("bool", async () => {
					await setupDefault();

					const responses = await store.search(
						new SearchRequest({
							query: [
								new BoolQuery({
									key: "bool",
									value: true
								})
							]
						})
					);
					expect(responses.results).toHaveLength(1);
					expect(responses.results.map((x) => x.id.primitive)).toEqual(["1"]);
				});

				describe("array", () => {
					describe("uint8arrays", () => {
						class Uint8arraysVec {
							@field({ type: Uint8Array })
							id: Uint8Array;

							@field({ type: option(vec(Uint8Array)) })
							bytesArrays?: Uint8Array[];

							constructor(properties?: { bytesArrays: Uint8Array[] }) {
								this.id = randomBytes(32);
								this.bytesArrays = properties?.bytesArrays;
							}
						}

						it("uint8array[]", async () => {
							store = await setup({ schema: Uint8arraysVec });
							const d1 = new Uint8arraysVec({
								bytesArrays: [new Uint8Array([1]), new Uint8Array([2])]
							});
							await store.put(d1);
							await store.put(
								new Uint8arraysVec({
									bytesArrays: [new Uint8Array([3])]
								})
							);

							const results = await store.search(
								new SearchRequest({
									query: [
										new ByteMatchQuery({
											key: "bytesArrays",
											value: new Uint8Array([2])
										})
									]
								})
							);
							expect(results.results.map((x) => x.indexed.id)).toEqual([d1.id]);
						});
					});

					describe("documents", () => {
						class DocumentsVec {
							@field({ type: Uint8Array })
							id: Uint8Array;

							@field({ type: option(vec(Document)) })
							documents?: Document[];

							constructor(properties?: { documents: Document[] }) {
								this.id = randomBytes(32);
								this.documents = properties?.documents;
							}
						}

						it("can search", async () => {
							store = await setup({ schema: DocumentsVec });

							const d1 = new DocumentsVec({
								documents: [new Document({ id: uuid(), number: 123n })]
							});
							await store.put(d1);
							await store.put(
								new DocumentsVec({
									documents: [new Document({ id: uuid(), number: 124n })]
								})
							);

							const results = await store.search(
								new SearchRequest({
									query: new IntegerCompare({
										key: ["documents", "number"],
										compare: Compare.Equal,
										value: 123n
									})
								})
							);
							expect(results.results.map((x) => x.indexed.id)).toEqual([d1.id]);
						});
					});

					/* TODO how should we do this? Should nested arrays be supported?
					// For SQLLite this will be hard but for simple index it should be possible 
					
					describe("multi-dimensional", () => {
						class NestedVec {

							@field({ type: Uint8Array })
							id: Uint8Array;

							@field({ type: option(vec(vec("u32"))) })
							matrix?: number[][];

							constructor(properties?: {
								matrix?: number[][];
							}) {
								this.id = randomBytes(32);
								this.matrix = properties?.matrix;
							}
						}

						it("not supported", async () => {

							try {
								store = await setup({ schema: NestedVec });
							} catch (error: any) {

								expect(error.message).toEqual("vec(vec(...)) is not supported");
								return;
							}

							await expect(store.put(new NestedVec({ matrix: [[1, 2], [3]] }))).rejects.toThrow("vec(vec(...)) is not supported");
						});
					}); */
				});

				describe("logical", () => {
					beforeEach(async () => {
						await setupDefault();
					});
					it("and", async () => {
						const responses = await store.search(
							new SearchRequest({
								query: [
									new And([
										new StringMatch({
											key: "name",
											value: "hello",
											caseInsensitive: true,
											method: StringMatchMethod.contains
										}),
										new StringMatch({
											key: "name",
											value: "world",
											caseInsensitive: true,
											method: StringMatchMethod.contains
										})
									])
								]
							})
						);
						expect(responses.results).toHaveLength(2);
						expect(
							responses.results.map((x) => x.id.primitive)
						).toContainAllValues(["1", "2"]);
					});

					it("or", async () => {
						const responses = await store.search(
							new SearchRequest({
								query: [
									new Or([
										new StringMatch({
											key: "id",
											value: "1"
										}),
										new StringMatch({
											key: "id",
											value: "2"
										})
									])
								]
							})
						);
						expect(responses.results).toHaveLength(2);
						expect(
							responses.results.map((x) => x.id.primitive)
						).toContainAllValues(["1", "2"]);
					});
				});

				describe("number", () => {
					beforeEach(async () => {
						await setupDefault();
					});
					it("equal", async () => {
						const response = await store.search(
							new SearchRequest({
								query: [
									new IntegerCompare({
										key: "number",
										compare: Compare.Equal,
										value: 2n
									})
								]
							})
						);
						expect(response.results).toHaveLength(1);
						expect(response.results[0].indexed.number).toEqualNumber(2n);
					});

					it("gt", async () => {
						const response = await store.search(
							new SearchRequest({
								query: [
									new IntegerCompare({
										key: "number",
										compare: Compare.Greater,
										value: 2n
									})
								]
							})
						);
						expect(response.results).toHaveLength(1);
						expect(response.results[0].indexed.number).toEqualNumber(3n);
					});

					it("gte", async () => {
						const response = await store.search(
							new SearchRequest({
								query: [
									new IntegerCompare({
										key: "number",
										compare: Compare.GreaterOrEqual,
										value: 2n
									})
								]
							})
						);
						response.results.sort((a, b) =>
							bigIntSort(a.indexed.number as bigint, b.indexed.number as bigint)
						);
						expect(response.results).toHaveLength(2);
						expect(response.results[0].indexed.number).toEqualNumber(2n);
						expect(response.results[1].indexed.number).toEqualNumber(3n);
					});

					it("lt", async () => {
						const response = await store.search(
							new SearchRequest({
								query: [
									new IntegerCompare({
										key: "number",
										compare: Compare.Less,
										value: 2n
									})
								]
							})
						);
						expect(response.results).toHaveLength(1);
						expect(response.results[0].indexed.number).toEqualNumber(1n);
					});

					it("lte", async () => {
						const response = await store.search(
							new SearchRequest({
								query: [
									new IntegerCompare({
										key: "number",
										compare: Compare.LessOrEqual,
										value: 2n
									})
								]
							})
						);
						response.results.sort((a, b) =>
							bigIntSort(a.indexed.number as bigint, b.indexed.number as bigint)
						);
						expect(response.results).toHaveLength(2);
						expect(response.results[0].indexed.number).toEqualNumber(1n);
						expect(response.results[1].indexed.number).toEqualNumber(2n);
					});
				});

				describe("concurrently", () => {
					beforeEach(async () => {
						await setupDefault();
					});
					it("can query concurrently", async () => {
						// TODO add more concurrency
						const promises: Promise<IndexedResults>[] = [];
						const concurrency = 100;
						for (let i = 0; i < concurrency; i++) {
							if (i % 2 === 0) {
								promises.push(
									store.search(
										new SearchRequest({
											query: [
												new IntegerCompare({
													key: "number",
													compare: Compare.GreaterOrEqual,
													value: 2n
												})
											]
										})
									)
								);
							} else {
								promises.push(
									store.search(
										new SearchRequest({
											query: [
												new IntegerCompare({
													key: "number",
													compare: Compare.Less,
													value: 2n
												})
											]
										})
									)
								);
							}
						}

						const results = await Promise.all(promises);
						for (let i = 0; i < concurrency; i++) {
							if (i % 2 === 0) {
								// query1
								expect(results[i].results).toHaveLength(2);
								results[i].results.sort((a, b) =>
									Number(a.indexed.number! - b.indexed.number!)
								);
								expect(results[i].results[0].indexed.number).toEqualNumber(2n); // Jest can't seem to output BN if error, so we do equals manually
								expect(results[i].results[1].indexed.number).toEqualNumber(3n); // Jest can't seem to output BN if error, so we do equals manually
							} else {
								// query2
								expect(results[i].results).toHaveLength(1);
								expect(results[i].results[0].indexed.number).toEqualNumber(1n);
							}
						}
					});
				});
			});
		});

		describe("sort", () => {
			const peersCount = 3;

			const canRead: (
				| undefined
				| ((publicKey: PublicSignKey) => Promise<boolean>)
			)[] = [];

			const put = async (id: number) => {
				const doc = new Document({
					id: String(id),
					name: String(id),
					number: BigInt(id)
				});
				const resp = await store.put(doc);
				return resp;
			};

			const checkIterate = async (
				batches: bigint[][],
				query = new IntegerCompare({
					key: "number",
					compare: Compare.GreaterOrEqual,
					value: 0n
				})
			) => {
				await waitForResolved(async () => {
					const req = new SearchRequest({
						query: [query],
						sort: [new Sort({ direction: SortDirection.ASC, key: "number" })]
					});
					const iterator = store.iterate(req);

					if (batches.length === 0) {
						// No fetches has been made, so we don't know whether we are done yet
						expect(iterator.done()).toBeFalse();
					} else {
						for (const batch of batches) {
							expect(iterator.done()).toBeFalse();
							const next = await iterator.next(batch.length);
							expect(next.results.map((x) => Number(x.indexed.number))).toEqual(
								batch.map((x) => Number(x))
							);
						}
						expect(iterator.done()).toBeTrue();
					}
				});
			};

			beforeEach(async () => {
				store = await setup({ schema: Document });
			});

			it("empty", async () => {
				await checkIterate([]);
			});

			// TODO make sure documents are evenly distrubted before querye
			it("multiple batches", async () => {
				await put(0);
				await put(1);
				await put(2);
				expect(await store.getSize()).toEqual(3);
				await checkIterate([[0n], [1n], [2n]]);
				await checkIterate([[0n, 1n, 2n]]);
				await checkIterate([[0n, 1n], [2n]]);
				await checkIterate([[0n], [1n, 2n]]);
			});

			it("sorts by order", async () => {
				await put(0);
				await put(1);
				await put(2);
				{
					const iterator = await store.iterate(
						new SearchRequest({
							query: [],
							sort: [new Sort({ direction: SortDirection.ASC, key: "name" })]
						})
					);
					expect(iterator.done()).toBeFalse();
					const next = await iterator.next(3);
					expect(next.results.map((x) => x.indexed.name)).toEqual([
						"0",
						"1",
						"2"
					]);
					expect(iterator.done()).toBeTrue();
				}
				{
					const iterator = await store.iterate(
						new SearchRequest({
							query: [],
							sort: [new Sort({ direction: SortDirection.DESC, key: "name" })]
						})
					);
					expect(iterator.done()).toBeFalse();
					const next = await iterator.next(3);
					expect(next.results.map((x) => x.indexed.name)).toEqual([
						"2",
						"1",
						"0"
					]);
					expect(iterator.done()).toBeTrue();
				}
			});

			it("strings", async () => {
				await put(0);
				await put(1);
				await put(2);

				const iterator = await store.iterate(
					new SearchRequest({
						query: [],
						sort: [new Sort({ direction: SortDirection.ASC, key: "name" })]
					})
				);
				expect(iterator.done()).toBeFalse();
				const next = await iterator.next(3);
				expect(next.results.map((x) => x.indexed.name)).toEqual([
					"0",
					"1",
					"2"
				]);
				expect(iterator.done()).toBeTrue();
			});

			describe("close", () => {
				it("by invoking close()", async () => {
					await put(0);
					await put(1);
					await put(2);
					const request = new SearchRequest({
						query: []
					});
					const iterator = await store.iterate(request);
					expect(iterator.done()).toBeFalse();
					await iterator.next(2); // fetch some, but not all
					expect(store.index.getPending(request.idString)).toEqual(1);
					await iterator.close();
					await waitForResolved(
						() =>
							expect(store.index.getPending(request.idString)).toBeUndefined(),
						{ timeout: 3000, delayInterval: 50 }
					);
				});

				it("requires correct id", async () => {
					await put(0);
					await put(1);
					const request = new SearchRequest({
						query: []
					});
					const iteratorOwner = (await Ed25519Keypair.create()).publicKey;
					const iterator = await store.iterate(request, iteratorOwner);
					expect(iterator.done()).toBeFalse();
					await iterator.next(1); // fetch some, but not all
					expect(store.index.getPending(request.idString)).toEqual(1);

					const closeRequest = new CloseIteratorRequest({ id: request.id });

					// Try to send from another peer (that is not the owner of the iterator)
					await store.index.close(
						new CloseIteratorRequest({ id: request.id }),
						(await Ed25519Keypair.create()).publicKey
					);
					await delay(2000);
					expect(store.index.getPending(request.idString)).toBeDefined();

					// send from the owner
					await store.index.close(
						new CloseIteratorRequest({ id: request.id }),
						iteratorOwner
					);

					await waitForResolved(
						() =>
							expect(store.index.getPending(request.idString)).toBeUndefined(),
						{ timeout: 3000, delayInterval: 50 }
					);
				});

				it("end of iterator", async () => {
					await put(0);
					await put(1);
					await put(2);
					const request = new SearchRequest({
						query: []
					});
					const iterator = await store.iterate(request);
					expect(iterator.done()).toBeFalse();
					await iterator.next(3); // fetch all
					await waitForResolved(
						() =>
							expect(store.index.getPending(request.idString)).toBeUndefined(),
						{ timeout: 3000, delayInterval: 50 }
					);
				});

				it("end of iterator, multiple nexts", async () => {
					await put(0);
					await put(1);
					await put(2);
					const request = new SearchRequest({
						query: []
					});
					const iterator = await store.iterate(request);
					await iterator.next(2);
					await iterator.next(1);
					expect(iterator.done()).toBeTrue();
					await waitForResolved(
						() =>
							expect(store.index.getPending(request.idString)).toBeUndefined(),
						{ timeout: 3000, delayInterval: 50 }
					);
				});
			});

			// TODO test iterator.close() to stop pending promises

			// TODO deletion while sort

			// TODO session timeouts?
		});
	});
};
