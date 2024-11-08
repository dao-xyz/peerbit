import {
	deserialize,
	field,
	fixedArray,
	option,
	serialize,
	variant,
	vec,
} from "@dao-xyz/borsh";
import { randomBytes } from "@peerbit/crypto";
import {
	And,
	BoolQuery,
	ByteMatchQuery,
	Compare,
	type Index,
	type IndexEngineInitProperties,
	type IndexIterator,
	type Indices,
	IntegerCompare,
	IsNull,
	type IterateOptions,
	Nested,
	Not,
	Or,
	Query,
	type Shape,
	Sort,
	SortDirection,
	StringMatch,
	StringMatchMethod,
	extractFieldValue,
	getIdProperty,
	id,
	toId,
} from "@peerbit/indexer-interface";
import {
	/* delay,  */
	delay,
	waitForResolved,
} from "@peerbit/time";
import { expect } from "chai";
import sodium from "libsodium-wrappers";
import { equals } from "uint8arrays";
import { v4 as uuid } from "uuid";

@variant("nested_object")
class NestedValue {
	@field({ type: "u32" })
	number: number;

	constructor(properties: { number: number }) {
		this.number = properties.number;
	}
}

abstract class Base {}

@variant(0)
class Document extends Base {
	@field({ type: "string" })
	id: string;

	@field({ type: option("string") })
	name?: string;

	@field({ type: option("u64") })
	number?: bigint;

	@field({ type: option("bool") })
	bool?: boolean;

	@field({ type: option(Uint8Array) })
	data?: Uint8Array;

	@field({ type: option(fixedArray("u8", 32)) })
	fixedData?: Uint8Array;

	@field({ type: option(NestedValue) })
	nested?: NestedValue;

	@field({ type: vec("string") })
	tags: string[];

	@field({ type: vec(NestedValue) })
	nestedVec: NestedValue[];

	constructor(opts: Partial<Document>) {
		super();
		this.id = opts.id;
		this.name = opts.name;
		this.number = opts.number;
		this.tags = opts.tags || [];
		this.bool = opts.bool;
		this.data = opts.data;
		this.fixedData = opts.fixedData;
		this.nested = opts.nested;
		this.nestedVec = opts.nestedVec || [];
	}
}

// variant 1 (next version for migration testing)
@variant(1)
class DocumentNext extends Base {
	@field({ type: "string" })
	id: string;

	@field({ type: "string" })
	name: string;

	@field({ type: "string" })
	anotherField: string;

	constructor(opts: Partial<DocumentNext>) {
		super();
		this.id = opts.id || uuid();
		this.name = opts.name || uuid();
		this.anotherField = opts.anotherField || uuid();
	}
}

const bigIntSort = <T extends number | bigint>(a: T, b: T): number =>
	a > b ? 1 : 0 || -(a < b);

const search = <T, S extends Shape | undefined>(
	index: Index<T, any>,
	query: IterateOptions,
	options?: { shape: S },
) => {
	// fetch max u32
	return index.iterate<S>(query, options).all();
};

const assertIteratorIsDone = async (iterator: IndexIterator<any, any>) => {
	const next = await iterator.next(1);
	if (next.length > 0) {
		throw new Error(`Iterator is not done, got more results`);
	}
	expect(iterator.done()).to.be.true;
};

export const tests = (
	createIndicies: (directory?: string) => Indices | Promise<Indices>,
	type: "transient" | "persist" = "transient",
	properties: {
		shapingSupported: boolean;
		u64SumSupported: boolean;
	},
) => {
	return describe("index", () => {
		let store: Index<any, any>;
		let indices: Indices;
		let defaultDocs: Document[] = [];

		const setupDefault = async () => {
			// Create store
			const result = await setup({ schema: Base });

			const doc = new Document({
				id: "1",
				name: "hello",
				number: 1n,
				tags: [],
			});

			const docEdit = new Document({
				id: "1",
				name: "hello world",
				number: 1n,
				bool: true,
				data: new Uint8Array([1]),
				fixedData: new Uint8Array(32).fill(1),
				tags: [],
			});

			const doc2 = new Document({
				id: "2",
				name: "hello world",
				number: 4n,
				tags: [],
			});

			const doc2Edit = new Document({
				id: "2",
				name: "Hello World",
				number: 2n,
				data: new Uint8Array([2]),
				fixedData: new Uint8Array(32).fill(2),
				tags: ["Hello", "World"],
			});

			const doc3 = new Document({
				id: "3",
				name: "foo",
				number: 3n,
				data: new Uint8Array([3]),
				fixedData: new Uint8Array(32).fill(3),
				tags: ["Hello"],
			});

			const doc4 = new Document({
				id: "4",
				name: undefined,
				number: undefined,
				tags: [],
			});

			await store.put(doc);
			await waitForResolved(async () =>
				expect(await store.getSize()).equals(1),
			);
			await store.put(docEdit);
			await store.put(doc2);
			await waitForResolved(async () =>
				expect(await store.getSize()).equals(2),
			);

			await store.put(doc2Edit);
			await store.put(doc3);
			await store.put(doc4);
			await waitForResolved(async () => expect(await store.getSize()).equal(4));

			defaultDocs = [docEdit, doc2Edit, doc3, doc4];
			return result;
		};

		const checkDocument = (document: any, ...matchAny: any[]) => {
			const match = matchAny.find((x) =>
				x.id instanceof Uint8Array
					? equals(x.id, document.id)
					: x.id === document.id,
			);

			expect(match).to.exist;

			const keysMatch = Object.keys(match);
			const keysDocument = Object.keys(document);

			expect(keysMatch).to.have.members(keysDocument);
			expect(keysDocument).to.have.members(keysMatch);
			for (const key of keysMatch) {
				const value = document[key];
				const matchValue = match[key];
				if (value instanceof Uint8Array) {
					expect(equals(value, matchValue)).to.be.true;
				} else {
					expect(value).to.deep.equal(matchValue);
				}
			}

			// expect(document).to.deep.equal(match);
			expect(document).to.be.instanceOf(matchAny[0].constructor);
		};

		const setup = async <T>(
			properties: Partial<IndexEngineInitProperties<T, any>> & { schema: any },
			directory?: string,
		): Promise<{
			indices: Indices;
			store: Index<T, any>;
			directory: string;
		}> => {
			//	store && await store.stop()
			indices && (await indices.stop());

			await sodium.ready;
			directory = directory
				? directory
				: type === "persist"
					? "./tmp/document-index/" + uuid()
					: undefined;
			indices = await createIndicies(directory); // TODO add directory testsc
			await indices.start();
			const indexProps: IndexEngineInitProperties<T, any> = {
				...{
					indexBy: getIdProperty(properties.schema) || ["id"],
					iterator: { batch: { maxSize: 5e6, sizeProperty: ["__size"] } },
					/* nested: {
						match: (obj: any): obj is IndexWrapper => obj instanceof IndexWrapper,
						query: (nested: any, query: any) => nested.search(query)
					} */
				},
				...properties,
			};
			store = await indices.init(indexProps); // TODO add directory tests
			return { indices, store, directory };
			/* return new IndexWrapper(index, indexProps.indexBy, directory); */
		};

		afterEach(async () => {
			defaultDocs = [];
			await indices?.stop?.();
		});

		describe("indexBy", () => {
			const testIndex = async (
				store: Index<any, any>,
				doc: any,
				idProperty: string[] = ["id"],
			) => {
				await store.put(doc);
				let docId = extractFieldValue<any>(doc, idProperty);
				let result = await store.get(toId(docId));
				expect(result).to.exist;
				checkDocument(result.value, doc);
				let deleteQueryObject = {};
				let current = deleteQueryObject;
				for (const [i, path] of idProperty.entries()) {
					if (i < idProperty.length - 1) {
						current[path] = {};
						current = current[path];
					} else {
						current[path] = docId;
					}
				}

				await store.del({
					query: deleteQueryObject,
				});
				expect(await store.getSize()).equal(0);
				result = await store.get(toId(docId));

				expect(result).equal(undefined);
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
					let store: any;
					try {
						const out = await setup({
							indexBy: ["__missing__"],
							schema: SimpleDocument,
						});
						store = out.store;
					} catch (error: any) {
						// some impl might want to throw here, since the schema is known in advance and the indexBy will be missing
						expect(error["message"]).equal(
							"Primary key __missing__ not found in schema",
						);
						return;
					}
					const doc = new SimpleDocument({
						id: "abc 123",
						value: "Hello world",
					});

					// else throw when putting the doc
					try {
						await store.put(doc);
					} catch (error) {
						expect(error).to.haveOwnProperty(
							"message",
							"Unexpected index key: undefined, expected: string, number, bigint or Uint8Array",
						);
					}
				});

				it("index by another property", async () => {
					const { store } = await setup({
						indexBy: ["value"],
						schema: SimpleDocument,
					});

					const helloWorld = "Hello world";
					const doc = new SimpleDocument({
						id: "abc 123",
						value: helloWorld,
					});

					// put doc
					await store.put(doc);

					expect((await store.get(toId(helloWorld)))?.value.value).equal(
						helloWorld,
					);
				});

				it("can StringQuery index", async () => {
					const { store } = await setup({
						indexBy: ["value"],
						schema: SimpleDocument,
					});

					const doc = new SimpleDocument({
						id: "abc 123",
						value: "Hello world",
					});

					await store.put(doc);

					const results = await search(store, {
						query: [
							new StringMatch({
								key: "id",
								value: "123",
								caseInsensitive: false,
								method: StringMatchMethod.contains,
							}),
						],
					});
					expect(results).to.have.length(1);
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
					const { store } = await setup({ schema: DocumentUint8arrayId });

					const id = new Uint8Array([1, 2, 3]);
					const doc = new DocumentUint8arrayId({
						id,
						value: "Hello world",
					});
					await testIndex(store, doc);
				});

				class DocumentFixedUint8arrayId {
					@field({ type: fixedArray("u8", 32) })
					id: Uint8Array;

					@field({ type: "string" })
					value: string;

					constructor(properties: { id: Uint8Array; value: string }) {
						this.id = properties.id;
						this.value = properties.value;
					}
				}

				it("index as fixed Uint8array", async () => {
					const { store } = await setup({ schema: DocumentFixedUint8arrayId });

					const id = new Uint8Array(32).fill(1);
					const doc = new DocumentFixedUint8arrayId({
						id,
						value: "Hello world",
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
					const { store } = await setup({ schema: DocumentNumberId });

					const id = 123456789;
					const doc = new DocumentNumberId({
						id,
						value: "Hello world",
					});

					await testIndex(store, doc);
				});
			});

			/* TMP renable with sqlite support u64
			 describe("bigint", () => {
				class DocumentBigintId {
					@field({ type: "u64" })
					id: bigint;

					@field({ type: "u64" })
					value: bigint;

					constructor(properties: { id: bigint; value: bigint }) {
						this.id = properties.id;
						this.value = properties.value;
					}
				}

				it("index as bigint", async () => {
					const { store } = await setup({ schema: DocumentBigintId });

					// make the id less than 2^53, but greater than u32 max
					const id = BigInt(2 ** 63 - 1);
					const doc = new DocumentBigintId({
						id,
						value: id,
					});
					await testIndex(store, doc);
				});
			}); */

			describe("by decorator", () => {
				class DocumentWithDecoratedId {
					@id({ type: "string" })
					xyz: string;

					constructor(properties: { xyz: string }) {
						this.xyz = properties.xyz;
					}
				}

				it("can index by decorated id", async () => {
					const { store } = await setup({ schema: DocumentWithDecoratedId });
					const doc = new DocumentWithDecoratedId({
						xyz: "abc",
					});

					await testIndex(store, doc, getIdProperty(DocumentWithDecoratedId));
				});
			});

			describe("nested by decorator ", () => {
				class Nested {
					@id({ type: "string" })
					id: string;

					constructor(id: string) {
						this.id = id;
					}
				}

				class NestedDocument {
					@field({ type: Nested })
					nested: Nested;

					constructor(nested: Nested) {
						this.nested = nested;
					}
				}

				it("can index by nested decorated id", async () => {
					const { store } = await setup({ schema: NestedDocument });
					const doc = new NestedDocument(new Nested("abc"));
					await testIndex(store, doc, getIdProperty(NestedDocument));
				});
			});
		});

		describe("search", () => {
			describe("fields", () => {
				it("no-args", async () => {
					await setupDefault();

					const results = await search(store, { query: [] });
					expect(results).to.have.length(4);
					for (const result of results) {
						checkDocument(result.value, ...defaultDocs);
					}
				});

				describe("string", () => {
					beforeEach(async () => {
						await setupDefault();
					});
					it("exact", async () => {
						const responses = await search(store, {
							query: [
								new StringMatch({
									key: "name",
									value: "hello world",
									caseInsensitive: true,
								}),
							],
						});
						expect(responses.map((x) => x.id.primitive)).to.have.members([
							"1",
							"2",
						]);
					});

					it("exact-case-insensitive", async () => {
						const responses = await search(store, {
							query: [
								new StringMatch({
									key: "name",
									value: "Hello World",
									caseInsensitive: true,
								}),
							],
						});
						expect(responses).to.have.length(2);
						expect(responses.map((x) => x.id.primitive)).to.have.members([
							"1",
							"2",
						]);
					});

					it("exact case sensitive", async () => {
						let responses = await search(store, {
							query: [
								new StringMatch({
									key: "name",
									value: "Hello World",
									caseInsensitive: false,
								}),
							],
						});
						expect(responses).to.have.length(1);
						expect(responses.map((x) => x.id.primitive)).to.have.members(["2"]);
						responses = await search(store, {
							query: [
								new StringMatch({
									key: "name",
									value: "hello world",
									caseInsensitive: false,
								}),
							],
						});
						expect(responses.map((x) => x.id.primitive)).to.have.members(["1"]);
					});
					it("prefix", async () => {
						const responses = await search(store, {
							query: [
								new StringMatch({
									key: "name",
									value: "hel",
									method: StringMatchMethod.prefix,
									caseInsensitive: true,
								}),
							],
						});
						expect(responses).to.have.length(2);
						expect(responses.map((x) => x.id.primitive)).to.have.members([
							"1",
							"2",
						]);
					});

					it("contains", async () => {
						const responses = await search(store, {
							query: [
								new StringMatch({
									key: "name",
									value: "ello",
									method: StringMatchMethod.contains,
									caseInsensitive: true,
								}),
							],
						});
						expect(responses).to.have.length(2);
						expect(responses.map((x) => x.id.primitive)).to.have.members([
							"1",
							"2",
						]);
					});

					describe("arr", () => {
						it("arr", async () => {
							const responses = await search(store, {
								query: [
									new StringMatch({
										key: "tags",
										value: "world",
										method: StringMatchMethod.contains,
										caseInsensitive: true,
									}),
								],
							});
							expect(responses).to.have.length(1);
							expect(responses.map((x) => x.id.primitive)).to.have.members([
								"2",
							]);

							checkDocument(responses[0].value, ...defaultDocs);
						});
					});
				});

				it("missing", async () => {
					await setupDefault();

					const responses = await search(store, {
						query: [
							new IsNull({
								key: "name",
							}),
						],
					});

					expect(responses).to.have.length(1);
					expect(responses.map((x) => x.id.primitive)).to.deep.equal(["4"]);
				});

				describe("uint8arrays", () => {
					describe("dynamic", () => {
						describe("bytematch", () => {
							it("matches", async () => {
								await setupDefault();

								const responses = await search(store, {
									query: [
										new ByteMatchQuery({
											key: "data",
											value: new Uint8Array([1]),
										}),
									],
								});
								expect(responses).to.have.length(1);
								expect(responses.map((x) => x.id.primitive)).to.deep.equal([
									"1",
								]);
							});
							it("un-matches", async () => {
								await setupDefault();

								const responses = await search(store, {
									query: [
										new ByteMatchQuery({
											key: "data",
											value: new Uint8Array([199]),
										}),
									],
								});
								expect(responses).to.be.empty;
							});
						});
						describe("integer", () => {
							it("exists", async () => {
								await setupDefault();

								const responses = await search(store, {
									query: [
										new IntegerCompare({
											key: "data",
											compare: Compare.Equal,
											value: 1,
										}),
									],
								});
								expect(responses).to.have.length(1);
								expect(responses.map((x) => x.id.primitive)).to.deep.equal([
									"1",
								]);
							});

							it("does not exist", async () => {
								await setupDefault();

								const responses = await search(store, {
									query: [
										new IntegerCompare({
											key: "data",
											compare: Compare.Equal,
											value: 199,
										}),
									],
								});
								expect(responses).to.be.empty;
							});
						});
					});

					describe("fixed", () => {
						describe("bytematch", () => {
							it("matches", async () => {
								await setupDefault();

								const responses = await search(store, {
									query: [
										new ByteMatchQuery({
											key: "fixedData",
											value: new Uint8Array(32).fill(1),
										}),
									],
								});
								expect(responses).to.have.length(1);
								expect(responses.map((x) => x.id.primitive)).to.deep.equal([
									"1",
								]);
							});
							it("un-matches", async () => {
								await setupDefault();

								const responses = await search(store, {
									query: [
										new ByteMatchQuery({
											key: "data",
											value: new Uint8Array(32).fill(99),
										}),
									],
								});
								expect(responses).to.be.empty;
							});
						});
						describe("integer", () => {
							it("exists", async () => {
								await setupDefault();

								const responses = await search(store, {
									query: [
										new IntegerCompare({
											key: "data",
											compare: Compare.Equal,
											value: 1,
										}),
									],
								});
								expect(responses).to.have.length(1);
								expect(responses.map((x) => x.id.primitive)).to.deep.equal([
									"1",
								]);
							});

							it("does not exist", async () => {
								await setupDefault();

								const responses = await search(store, {
									query: [
										new IntegerCompare({
											key: "data",
											compare: Compare.Equal,
											value: 199,
										}),
									],
								});
								expect(responses).to.be.empty;
							});
						});
					});
				});

				it("bool", async () => {
					await setupDefault();

					const responses = await search(store, {
						query: [
							new BoolQuery({
								key: "bool",
								value: true,
							}),
						],
					});
					expect(responses).to.have.length(1);
					expect(responses.map((x) => x.id.primitive)).to.deep.equal(["1"]);
				});

				describe("array", () => {
					describe("uint8arrays", () => {
						class Uint8arraysVec {
							@field({ type: Uint8Array })
							id: Uint8Array;

							@field({ type: vec(Uint8Array) })
							bytesArrays: Uint8Array[];

							constructor(properties?: { bytesArrays: Uint8Array[] }) {
								this.id = randomBytes(32);
								this.bytesArrays = properties?.bytesArrays || [];
							}
						}

						it("uint8array[]", async () => {
							const out = await setup({ schema: Uint8arraysVec });
							store = out.store;
							const d1 = new Uint8arraysVec({
								bytesArrays: [new Uint8Array([1]), new Uint8Array([2])],
							});
							await store.put(d1);
							await store.put(
								new Uint8arraysVec({
									bytesArrays: [new Uint8Array([3])],
								}),
							);

							const results = await search(store, {
								query: [
									new ByteMatchQuery({
										key: "bytesArrays",
										value: new Uint8Array([2]),
									}),
								],
							});
							expect(results.map((x) => x.value.id)).to.deep.equal([d1.id]);
						});
					});

					describe("documents", () => {
						class DocumentsVec {
							@field({ type: Uint8Array })
							id: Uint8Array;

							@field({ type: vec(Document) })
							documents: Document[];

							constructor(properties?: { documents: Document[] }) {
								this.id = randomBytes(32);
								this.documents = properties?.documents || [];
							}
						}

						it("can search", async () => {
							const out = await setup({ schema: DocumentsVec });
							store = out.store;

							const d1 = new DocumentsVec({
								documents: [
									new Document({ id: uuid(), number: 123n, tags: [] }),
								],
							});
							await store.put(d1);
							await store.put(
								new DocumentsVec({
									documents: [
										new Document({ id: uuid(), number: 124n, tags: [] }),
									],
								}),
							);

							const results = await search(store, {
								query: new IntegerCompare({
									key: ["documents", "number"],
									compare: Compare.Equal,
									value: d1.documents[0]!.number,
								}),
							});
							expect(results.map((x) => x.value.id)).to.deep.equal([d1.id]);
						});

						it("update array", async () => {
							const out = await setup({ schema: DocumentsVec });
							store = out.store;

							const d1 = new DocumentsVec({
								documents: [
									new Document({ id: uuid(), number: 123n, tags: [] }),
								],
							});
							await store.put(d1);

							d1.documents = [
								new Document({ id: uuid(), number: 124n, tags: [] }),
							];

							await store.put(d1);

							// should have update results
							expect(
								(
									await search(store, {
										query: new IntegerCompare({
											key: ["documents", "number"],
											compare: Compare.Equal,
											value: 123n,
										}),
									})
								).length,
							).to.equal(0);

							expect(
								(
									await search(store, {
										query: new IntegerCompare({
											key: ["documents", "number"],
											compare: Compare.Equal,
											value: 124n,
										}),
									})
								).map((x) => x.value.id),
							).to.deep.equal([d1.id]);
						});

						it("put delete put", async () => {
							const { store } = await setup({ schema: DocumentsVec });

							const d1 = new DocumentsVec({
								documents: [
									new Document({ id: uuid(), number: 123n, tags: [] }),
								],
							});

							await store.put(d1);
							const [deleted] = await store.del({
								query: {
									id: d1.id,
								},
							});

							expect(deleted.key).to.deep.equal(d1.id);

							expect(
								(
									await search(store, {
										query: new IntegerCompare({
											key: ["documents", "number"],
											compare: Compare.Equal,
											value: 123n,
										}),
									})
								).length,
							).to.equal(0);

							d1.documents = [
								new Document({ id: uuid(), number: 124n, tags: [] }),
							];
							await store.put(d1);

							expect(
								(
									await search(store, {
										query: new IntegerCompare({
											key: ["documents", "number"],
											compare: Compare.Equal,
											value: 124n,
										}),
									})
								).map((x) => x.value.id),
							).to.deep.equal([d1.id]);

							expect(
								(
									await search(store, {
										query: new IntegerCompare({
											key: ["documents", "number"],
											compare: Compare.Equal,
											value: 123n,
										}),
									})
								).length,
							).to.equal(0);
						});
					});
				});

				describe("logical", () => {
					beforeEach(async () => {
						await setupDefault();
					});

					it("and", async () => {
						const responses = await search(store, {
							query: [
								new And([
									new StringMatch({
										key: "name",
										value: "hello",
										caseInsensitive: true,
										method: StringMatchMethod.contains,
									}),
									new StringMatch({
										key: "name",
										value: "world",
										caseInsensitive: true,
										method: StringMatchMethod.contains,
									}),
								]),
							],
						});
						expect(responses).to.have.length(2);
						expect(responses.map((x) => x.id.primitive)).to.have.members([
							"1",
							"2",
						]);
					});

					it("or", async () => {
						const responses = await search(store, {
							query: [
								new Or([
									new StringMatch({
										key: "id",
										value: "1",
									}),
									new StringMatch({
										key: "id",
										value: "2",
									}),
								]),
							],
						});
						expect(responses).to.have.length(2);
						expect(responses.map((x) => x.id.primitive)).to.have.members([
							"1",
							"2",
						]);
					});

					it("not", async () => {
						const responses = await search(store, {
							query: [
								new And([
									new Not(
										new IntegerCompare({
											key: "number",
											compare: Compare.Greater,
											value: 1n,
										}),
									),
								]),
							],
						});
						expect(responses).to.have.length(1);
						expect(responses.map((x) => x.id.primitive)).to.have.members(["1"]);
					});
				});

				describe("number", () => {
					beforeEach(async () => {
						await setupDefault();
					});
					it("equal", async () => {
						const response = await search(store, {
							query: [
								new IntegerCompare({
									key: "number",
									compare: Compare.Equal,
									value: 2n,
								}),
							],
						});
						expect(response).to.have.length(1);
						expect(response[0].value.number).to.be.oneOf([2n, 2]);
					});

					it("gt", async () => {
						const response = await search(store, {
							query: [
								new IntegerCompare({
									key: "number",
									compare: Compare.Greater,
									value: 2n,
								}),
							],
						});
						expect(response).to.have.length(1);
						expect(response[0].value.number).to.be.oneOf([3n, 3]);
					});

					it("gte", async () => {
						const response = await search(store, {
							query: [
								new IntegerCompare({
									key: "number",
									compare: Compare.GreaterOrEqual,
									value: 2n,
								}),
							],
						});
						response.sort((a, b) =>
							bigIntSort(a.value.number as bigint, b.value.number as bigint),
						);
						expect(response).to.have.length(2);
						expect(response[0].value.number).to.be.oneOf([2n, 2]);
						expect(response[1].value.number).to.be.oneOf([3n, 3]);
					});

					it("lt", async () => {
						const response = await search(store, {
							query: [
								new IntegerCompare({
									key: "number",
									compare: Compare.Less,
									value: 2n,
								}),
							],
						});
						expect(response).to.have.length(1);
						expect(response[0].value.number).to.be.oneOf([1n, 1]);
					});

					it("lte", async () => {
						const response = await search(store, {
							query: [
								new IntegerCompare({
									key: "number",
									compare: Compare.LessOrEqual,
									value: 2n,
								}),
							],
						});
						response.sort((a, b) =>
							bigIntSort(a.value.number as bigint, b.value.number as bigint),
						);
						expect(response).to.have.length(2);
						expect(response[0].value.number).to.be.oneOf([1n, 1]);
						expect(response[1].value.number).to.be.oneOf([2n, 2]);
					});
				});

				describe("bigint", () => {
					@variant(0)
					class BigInt {
						@id({ type: "string" })
						id: string;

						@field({ type: "u64" })
						bigint: bigint;

						constructor(id: string, bigint: bigint) {
							this.id = id;
							this.bigint = bigint;
						}
					}
					let first = 1720600661484958580n;
					let second = first + 1n;
					let third = first + 2n;
					beforeEach(async () => {
						await setup({ schema: BigInt });
						await store.put(new BigInt("0", first));
						await store.put(new BigInt("1", second));
						await store.put(new BigInt("2", third));

						const ser = deserialize(
							serialize(new BigInt("0", first)),
							BigInt,
						).bigint;
						expect(ser).to.equal(first);
					});

					it("equal", async () => {
						const response = await search(store, {
							query: [
								new IntegerCompare({
									key: "bigint",
									compare: Compare.Equal,
									value: first,
								}),
							],
						});
						expect(response).to.have.length(1);
						expect(response[0].value.bigint).to.equal(first);
					});

					it("gt", async () => {
						const response = await search(store, {
							query: [
								new IntegerCompare({
									key: "bigint",
									compare: Compare.Greater,
									value: second,
								}),
							],
						});
						expect(response).to.have.length(1);
						expect(response[0].value.bigint).to.equal(third);
					});

					it("gte", async () => {
						const response = await search(store, {
							query: [
								new IntegerCompare({
									key: "bigint",
									compare: Compare.GreaterOrEqual,
									value: second,
								}),
							],
						});
						response.sort((a, b) =>
							bigIntSort(a.value.bigint as bigint, b.value.bigint as bigint),
						);
						expect(response).to.have.length(2);
						expect(response[0].value.bigint).to.equal(second);
						expect(response[1].value.bigint).to.equal(third);
					});

					it("lt", async () => {
						const response = await search(store, {
							query: [
								new IntegerCompare({
									key: "bigint",
									compare: Compare.Less,
									value: second,
								}),
							],
						});
						expect(response).to.have.length(1);
						expect(response[0].value.bigint).to.equal(first);
					});

					it("lte", async () => {
						const response = await search(store, {
							query: [
								new IntegerCompare({
									key: "bigint",
									compare: Compare.LessOrEqual,
									value: second,
								}),
							],
						});
						response.sort((a, b) =>
							bigIntSort(a.value.number as bigint, b.value.number as bigint),
						);

						expect(response).to.have.length(2);
						expect(response[0].value.bigint).to.equal(first);
						expect(response[1].value.bigint).to.equal(second);
					});
				});

				describe("nested", () => {
					describe("one level", () => {
						class Nested {
							@field({ type: "u64" })
							number: bigint;

							@field({ type: "bool" })
							bool: boolean;

							constructor(opts: Nested) {
								this.number = opts.number;
								this.bool = opts.bool;
							}
						}

						@variant(0)
						class DocumentWithNesting {
							@id({ type: "string" })
							id: string;

							@field({ type: option(Nested) })
							nested?: Nested;

							constructor(opts: DocumentWithNesting) {
								this.id = opts.id;
								this.nested = opts.nested;
							}
						}

						beforeEach(async () => {
							await setup({ schema: DocumentWithNesting });
						});

						it("number", async () => {
							await store.put(
								new DocumentWithNesting({
									id: "1",
									nested: new Nested({ number: 1n, bool: false }),
								}),
							);
							const doc2 = new DocumentWithNesting({
								id: "2",
								nested: new Nested({ number: 2n, bool: true }),
							});
							await store.put(doc2);

							const response = await search(store, {
								query: [
									new IntegerCompare({
										key: ["nested", "number"],
										compare: Compare.GreaterOrEqual,
										value: 2n,
									}),
								],
							});
							expect(response).to.have.length(1);
							expect(response[0].value.id).to.equal("2");

							checkDocument(response[0].value, doc2);
						});

						it("bool", async () => {
							const doc1 = new DocumentWithNesting({
								id: "1",
								nested: new Nested({ number: 1n, bool: false }),
							});

							await store.put(doc1);
							const doc2 = new DocumentWithNesting({
								id: "2",
								nested: new Nested({ number: 2n, bool: true }),
							});

							await store.put(doc2);
							let response = await search(store, {
								query: [
									new BoolQuery({
										key: ["nested", "bool"],
										value: true,
									}),
								],
							});
							expect(response).to.have.length(1);
							expect(response[0].value.id).to.equal("2");
							checkDocument(response[0].value, doc2);

							response = await search(store, {
								query: [
									new BoolQuery({
										key: ["nested", "bool"],
										value: false,
									}),
								],
							});
							expect(response).to.have.length(1);
							expect(response[0].value.id).to.equal("1");
							checkDocument(response[0].value, doc1);
						});
					});

					describe("one level flat constructor", () => {
						class Nested {
							@field({ type: "bool" })
							bool: boolean;

							constructor(bool: boolean) {
								this.bool = bool;
							}
						}

						@variant(0)
						class DocumentWithNesting {
							@id({ type: "string" })
							id: string;

							@field({ type: option(Nested) })
							nested?: Nested;

							constructor(opts: DocumentWithNesting) {
								this.id = opts.id;
								this.nested = opts.nested;
							}
						}

						beforeEach(async () => {
							await setup({ schema: DocumentWithNesting });
						});

						it("bool", async () => {
							const doc1 = new DocumentWithNesting({
								id: "1",
								nested: new Nested(false),
							});

							await store.put(doc1);
							const doc2 = new DocumentWithNesting({
								id: "2",
								nested: new Nested(true),
							});

							await store.put(doc2);
							let response = await search(store, {
								query: [
									new BoolQuery({
										key: ["nested", "bool"],
										value: true,
									}),
								],
							});
							expect(response).to.have.length(1);
							expect(response[0].value.id).to.equal("2");
							checkDocument(response[0].value, doc2);

							response = await search(store, {
								query: [
									new BoolQuery({
										key: ["nested", "bool"],
										value: false,
									}),
								],
							});
							expect(response).to.have.length(1);
							expect(response[0].value.id).to.equal("1");
							checkDocument(response[0].value, doc1);
						});
					});

					describe("2-level-variants", () => {
						class L1 {
							@field({ type: option("u64") })
							number?: bigint;

							constructor(opts: L1) {
								this.number = opts.number;
							}
						}

						class L0 {
							@field({ type: option(L1) })
							nestedAgain?: L1;

							constructor(opts: L0) {
								this.nestedAgain = opts.nestedAgain;
							}
						}

						@variant("DocumentWithNestedNesting")
						class DocumentWithNestedNesting {
							@id({ type: "string" })
							id: string;

							@field({ type: option(L0) })
							nested?: L0;

							constructor(opts: DocumentWithNestedNesting) {
								this.id = opts.id;
								this.nested = opts.nested;
							}
						}

						beforeEach(async () => {
							await setup({ schema: DocumentWithNestedNesting });
						});

						it("nested", async () => {
							await store.put(
								new DocumentWithNestedNesting({
									id: "1",
									nested: new L0({
										nestedAgain: new L1({ number: 1n }),
									}),
								}),
							);
							const doc2 = new DocumentWithNestedNesting({
								id: "2",
								nested: new L0({
									nestedAgain: new L1({ number: 2n }),
								}),
							});
							await store.put(doc2);

							const response = await search(store, {
								query: [
									new IntegerCompare({
										key: ["nested", "nestedAgain", "number"],
										compare: Compare.GreaterOrEqual,
										value: 2n,
									}),
								],
							});
							expect(response).to.have.length(1);
							expect(response[0].value.id).to.equal("2");

							checkDocument(response[0].value, doc2);
						});
					});

					describe("3-level-variants", () => {
						class L2 {
							@field({ type: option("u64") })
							number?: bigint;

							constructor(opts: L2) {
								this.number = opts.number;
							}
						}

						class L1 {
							@field({ type: option(L2) })
							nestedAgainAgain?: L2;

							constructor(opts: L1) {
								this.nestedAgainAgain = opts.nestedAgainAgain;
							}
						}

						class L0 {
							@field({ type: option(L1) })
							nestedAgain?: L1;

							constructor(opts: L0) {
								this.nestedAgain = opts.nestedAgain;
							}
						}

						@variant("DocumentWithNestedNesting")
						class DocumentWithNestedNesting {
							@id({ type: "string" })
							id: string;

							@field({ type: option(L0) })
							nested?: L0;

							constructor(opts: DocumentWithNestedNesting) {
								this.id = opts.id;
								this.nested = opts.nested;
							}
						}

						beforeEach(async () => {
							await setup({ schema: DocumentWithNestedNesting });
						});

						it("nested", async () => {
							await store.put(
								new DocumentWithNestedNesting({
									id: "1",
									nested: new L0({
										nestedAgain: new L1({
											nestedAgainAgain: new L2({ number: 1n }),
										}),
									}),
								}),
							);
							const doc2 = new DocumentWithNestedNesting({
								id: "2",
								nested: new L0({
									nestedAgain: new L1({
										nestedAgainAgain: new L2({ number: 2n }),
									}),
								}),
							});
							await store.put(doc2);

							const response = await search(store, {
								query: [
									new IntegerCompare({
										key: [
											"nested",
											"nestedAgain",
											"nestedAgainAgain",
											"number",
										],
										compare: Compare.GreaterOrEqual,
										value: 2n,
									}),
								],
							});

							expect(response).to.have.length(1);
							expect(response[0].value.id).to.equal("2");
							checkDocument(response[0].value, doc2);
						});
					});

					describe("poly-morphism", () => {
						describe("non-array", () => {
							abstract class Base {}

							@variant("v0")
							class V0 extends Base {
								@field({ type: option("u64") })
								number?: bigint;

								constructor(opts: V0) {
									super();
									this.number = opts.number;
								}
							}

							@variant("v1")
							class V1 extends Base {
								@field({ type: option("string") })
								string?: string;

								constructor(opts: V1) {
									super();
									this.string = opts.string;
								}
							}

							@variant("PolymorphDocument")
							class PolymorphDocument {
								@id({ type: "string" })
								id: string;

								@field({ type: option(Base) })
								nested?: Base;

								constructor(opts: PolymorphDocument) {
									this.id = opts.id;
									this.nested = opts.nested;
								}
							}

							beforeEach(async () => {
								await setup({ schema: PolymorphDocument });
							});

							it("can query multiple versions at once", async () => {
								await store.put(
									new PolymorphDocument({
										id: "1",
										nested: new V0({
											number: 1n,
										}),
									}),
								);
								const doc2 = new PolymorphDocument({
									id: "2",
									nested: new V1({
										string: "hello",
									}),
								});
								await store.put(doc2);

								const response = await search(store, {
									query: [
										new StringMatch({
											key: ["nested", "string"],
											value: "hello",
										}),
									],
								});

								expect(response).to.have.length(1);
								expect(response[0].value.id).to.equal("2");

								checkDocument(response[0].value, doc2);
							});
						});

						describe("non-array-nested", () => {
							abstract class Base {}

							@variant("v0")
							class V0 extends Base {
								@field({ type: option("u64") })
								number?: bigint;

								constructor(opts: V0) {
									super();
									this.number = opts.number;
								}
							}

							@variant("v1")
							class V1 extends Base {
								@field({ type: option("string") })
								string?: string;

								constructor(opts: V1) {
									super();
									this.string = opts.string;
								}
							}

							class Nested {
								@field({ type: option(Base) })
								nestedAgain?: Base;

								constructor(opts: Nested) {
									this.nestedAgain = opts.nestedAgain;
								}
							}

							@variant("PolymorphDocument")
							class PolymorphDocument {
								@id({ type: "string" })
								id: string;

								@field({ type: Nested })
								nested?: Nested;

								constructor(opts: PolymorphDocument) {
									this.id = opts.id;
									this.nested = opts.nested;
								}
							}

							beforeEach(async () => {
								await setup({ schema: PolymorphDocument });
							});

							it("can query multiple versions at once", async () => {
								await store.put(
									new PolymorphDocument({
										id: "1",
										nested: new Nested({
											nestedAgain: new V0({
												number: 1n,
											}),
										}),
									}),
								);

								const doc2 = new PolymorphDocument({
									id: "2",
									nested: new Nested({
										nestedAgain: new V1({
											string: "hello",
										}),
									}),
								});
								await store.put(doc2);

								const response = await search(store, {
									query: [
										new StringMatch({
											key: ["nested", "nestedAgain", "string"],
											value: "hello",
										}),
									],
								});

								expect(response).to.have.length(1);
								expect(response[0].value.id).to.equal("2");

								checkDocument(response[0].value, doc2);
							});
						});

						describe("array", () => {
							describe("polymorphism-simple-base", () => {
								abstract class Base {}

								@variant("av0")
								class AV0 extends Base {
									@field({ type: option("u64") })
									number?: bigint;

									constructor(opts: AV0) {
										super();
										this.number = opts.number;
									}
								}

								@variant("av1")
								class AV1 extends Base {
									@field({ type: option("string") })
									string?: string;

									constructor(opts: AV1) {
										super();
										this.string = opts.string;
									}
								}

								@variant("PolymorpArrayDocument")
								class PolymorpArrayDocument {
									@id({ type: "string" })
									id: string;

									@field({ type: vec(Base) })
									array: Base[];

									constructor(opts: PolymorpArrayDocument) {
										this.id = opts.id;
										this.array = opts.array;
									}
								}

								beforeEach(async () => {
									await setup({ schema: PolymorpArrayDocument });
								});

								it("can query multiple versions at once", async () => {
									await store.put(
										new PolymorpArrayDocument({
											id: "1",
											array: [
												new AV0({
													number: 0n,
												}),
												new AV1({
													string: "hello",
												}),
											],
										}),
									);

									const doc2 = new PolymorpArrayDocument({
										id: "2",
										array: [
											new AV1({
												string: "world",
											}),
											new AV0({
												number: 2n,
											}),
										],
									});

									await store.put(doc2);

									const response = await search(store, {
										query: [
											new StringMatch({
												key: ["array", "string"],
												value: "world",
											}),
										],
									});

									expect(response).to.have.length(1);
									expect(response[0].value.id).to.equal("2");
									checkDocument(response[0].value, doc2);
								});
							});

							describe("polymorphism-variant-base", () => {
								@variant(0)
								abstract class Base {}

								@variant("bv0")
								class V0 extends Base {
									@field({ type: option("u64") })
									number?: bigint;

									constructor(opts: V0) {
										super();
										this.number = opts.number;
									}
								}

								@variant("bv1")
								class V1 extends Base {
									@field({ type: option("string") })
									string?: string;

									constructor(opts: V1) {
										super();
										this.string = opts.string;
									}
								}

								@variant("PolymorpArrayDocument")
								class PolymorpDocument {
									@id({ type: "string" })
									id: string;

									@field({ type: Base })
									base: Base;

									constructor(opts: PolymorpDocument) {
										this.id = opts.id;
										this.base = opts.base;
									}
								}

								beforeEach(async () => {
									await setup({ schema: PolymorpDocument });
								});

								it("can query multiple versions at once", async () => {
									await store.put(
										new PolymorpDocument({
											id: "1",
											base: new V0({
												number: 0n,
											}),
										}),
									);

									const doc2 = new PolymorpDocument({
										id: "2",
										base: new V1({
											string: "world",
										}),
									});

									await store.put(doc2);

									const response = await search(store, {
										query: [
											new StringMatch({
												key: ["base", "string"],
												value: "world",
											}),
										],
									});

									expect(response).to.have.length(1);
									expect(response[0].value.id).to.equal("2");
									checkDocument(response[0].value, doc2);
								});
							});

							describe("nested-string-array", () => {
								class Nested {
									@field({ type: vec("string") })
									arr: string[];

									constructor(opts: Nested) {
										this.arr = opts.arr;
									}
								}

								@variant("NestedArrayDocument")
								class NestedArrayDocument {
									@id({ type: "string" })
									id: string;

									@field({ type: Nested })
									nested: Nested;

									constructor(opts: NestedArrayDocument) {
										this.id = opts.id;
										this.nested = opts.nested;
									}
								}

								beforeEach(async () => {
									await setup({ schema: NestedArrayDocument });
								});

								it("can query nested array", async () => {
									await store.put(
										new NestedArrayDocument({
											id: "1",
											nested: new Nested({
												arr: ["hello", "world"],
											}),
										}),
									);

									const doc2 = new NestedArrayDocument({
										id: "2",
										nested: new Nested({
											arr: ["hello", "värld"],
										}),
									});
									await store.put(doc2);

									const response = await search(store, {
										query: [
											new StringMatch({
												key: ["nested", "arr"],
												value: "värld",
											}),
										],
									});

									expect(response).to.have.length(1);
									expect(response.map((x) => x.value.id)).to.have.members([
										"2",
									]);
									checkDocument(response[0].value, doc2);
								});
							});

							describe("nested multiple fields", () => {
								class NestedMultipleFieldsDocument {
									@field({ type: "string" })
									a: string;

									@field({ type: "string" })
									b: string;

									constructor(opts: NestedMultipleFieldsDocument) {
										this.a = opts.a;
										this.b = opts.b;
									}
								}

								@variant("NestedMultipleFieldsArrayDocument")
								class NestedMultipleFieldsArrayDocument {
									@id({ type: "string" })
									id: string;

									@field({ type: vec(NestedMultipleFieldsDocument) })
									array: NestedMultipleFieldsDocument[];

									constructor(opts: NestedMultipleFieldsArrayDocument) {
										this.id = opts.id;
										this.array = opts.array;
									}
								}

								beforeEach(async () => {
									await setup({ schema: NestedMultipleFieldsArrayDocument });
								});

								it("combined query", async () => {
									const doc1 = new NestedMultipleFieldsArrayDocument({
										id: "1",
										array: [
											new NestedMultipleFieldsDocument({
												a: "hello",
												b: "world",
											}),
										],
									});
									await store.put(doc1);
									await store.put(
										new NestedMultipleFieldsArrayDocument({
											id: "2",
											array: [
												new NestedMultipleFieldsDocument({
													a: "hello",
													b: "värld",
												}),
												new NestedMultipleFieldsDocument({
													a: "hej",
													b: "world",
												}),
											],
										}),
									);

									const response = await search(store, {
										query: [
											new Nested({
												path: "array",
												query: new And([
													new StringMatch({
														key: "a",
														value: "hello",
													}),
													new StringMatch({
														key: "b",
														value: "world",
													}),
												]),
											}),
										],
									});

									expect(response).to.have.length(1);
									expect(response[0].value.id).to.equal("1");
									checkDocument(response[0].value, doc1);
								});

								it("nested partial match", async () => {
									// query nested without Nested query to match either or

									const doc1 = new NestedMultipleFieldsArrayDocument({
										id: "1",
										array: [
											new NestedMultipleFieldsDocument({
												a: "hello",
												b: "world",
											}),
										],
									});
									await store.put(doc1);
									await store.put(
										new NestedMultipleFieldsArrayDocument({
											id: "2",
											array: [
												new NestedMultipleFieldsDocument({
													a: "hello",
													b: "värld",
												}),
												new NestedMultipleFieldsDocument({
													a: "hej",
													b: "world",
												}),
											],
										}),
									);

									const response = await search(store, {
										query: [
											new StringMatch({
												key: ["array", "a"],
												value: "hello",
											}),
											new StringMatch({
												key: ["array", "b"],
												value: "world",
											}),
										],
									});

									expect(response).to.have.length(2);
									checkDocument(response[0].value, doc1);
								});
							});
						});
					});
				});

				describe("polymorph-root", () => {
					beforeEach(async () => {
						await setup({ schema: Base });
					});

					it("can query one of the version", async () => {
						await store.put(new DocumentNext({ anotherField: "hello" }));

						const result = await search(store, {
							query: new StringMatch({ key: "anotherField", value: "hello" }),
						});
						expect(result).to.have.length(1);

						const [doc] = result;
						expect(doc.value).to.be.instanceOf(DocumentNext);
					});

					it("can query multiple versions at once", async () => {
						let name = uuid();
						await store.put(new DocumentNext({ name }));
						await store.put(new DocumentNext({ name }));

						const result = await search(store, {
							query: new StringMatch({ key: "name", value: name }),
						});

						expect(result).to.have.length(2);
						for (const doc of result) {
							expect(doc.value).to.be.instanceOf(DocumentNext);
						}
					});
				});
			});
			describe("shape", () => {
				describe("simple", () => {
					beforeEach(async () => {
						await setupDefault();
					});

					it("filter field", async () => {
						const results = await search(
							store,
							{ query: [] },
							{ shape: { id: true } },
						);
						expect(results).to.have.length(4);
						for (const result of results) {
							if (properties.shapingSupported) {
								expect(Object.keys(result.value)).to.have.length(1);
								expect(result.value["id"]).to.exist;
							} else {
								expect(
									Object.keys(result.value).length,
								).to.be.greaterThanOrEqual(1);
								expect(result.value["id"]).to.exist;
							}
						}
					});

					it("nested field", async () => {
						const results = await search(
							store,
							{ query: [] },
							{ shape: { nestedVec: [{ number: true }] } },
						);
						expect(results).to.have.length(4);
						for (const value of results) {
							const arr = value.value["nestedVec"];
							expect(arr).to.be.exist;

							if (arr.length > 0) {
								for (const element of arr) {
									expect(element.number).to.exist;
									if (properties.shapingSupported) {
										expect(Object.keys(element)).to.have.length(1);
									}
								}
							}
						}
					});
				});

				describe("nested", () => {
					class MultifieldNested {
						@field({ type: "bool" })
						bool: boolean;

						@field({ type: "u32" })
						number: number;

						@field({ type: vec("string") })
						string: string[];

						constructor(bool: boolean, number: number, string: string[]) {
							this.bool = bool;
							this.number = number;
							this.string = string;
						}
					}

					class NestedBoolQueryDocument {
						@id({ type: "string" })
						id: string;

						@field({ type: MultifieldNested })
						nested: MultifieldNested;

						constructor(id: string, nested: MultifieldNested) {
							this.id = id;
							this.nested = nested;
						}
					}

					let index: Awaited<ReturnType<typeof setup<NestedBoolQueryDocument>>>;

					afterEach(async () => {
						await index.store.stop();
					});

					it("nested", async () => {
						index = await setup({ schema: NestedBoolQueryDocument });

						await index.store.put(
							new NestedBoolQueryDocument(
								"1",
								new MultifieldNested(true, 1, ["1"]),
							),
						);
						await index.store.put(
							new NestedBoolQueryDocument(
								"2",
								new MultifieldNested(false, 2, ["2"]),
							),
						);

						const shapedResults = await index.store
							.iterate(
								{
									query: new BoolQuery({
										key: ["nested", "bool"],
										value: false,
									}),
								},
								{ shape: { id: true } },
							)
							.all();
						expect(shapedResults).to.have.length(1);
						expect(shapedResults[0].value.id).to.equal("2");

						if (properties.shapingSupported) {
							expect(shapedResults[0].value["nested"]).to.be.undefined;
						} else {
							expect(shapedResults[0].value["nested"]).to.exist;
						}

						const unshapedResults = await index.store
							.iterate({
								query: new BoolQuery({ key: ["nested", "bool"], value: false }),
							})
							.all();
						expect(unshapedResults).to.have.length(1);
						expect(unshapedResults[0].value.id).to.equal("2");
						expect(unshapedResults[0].value.nested).to.exist;
					});

					it("nested-filtering", async () => {
						index = await setup({ schema: NestedBoolQueryDocument });

						const d1 = new NestedBoolQueryDocument(
							"1",
							new MultifieldNested(true, 1, ["1"]),
						);
						const d2 = new NestedBoolQueryDocument(
							"2",
							new MultifieldNested(false, 2, ["2"]),
						);

						await index.store.put(d1);
						await index.store.put(d2);

						const unshapedResults = await index.store
							.iterate({
								query: new StringMatch({ key: ["id"], value: "2" }),
							})
							.all();
						expect(unshapedResults).to.have.length(1);
						expect(unshapedResults[0].value.id).to.equal(d2.id);
						expect(unshapedResults[0].value.nested).to.deep.equal(d2.nested);

						const shapedResults = await index.store
							.iterate(
								{
									query: new StringMatch({ key: ["id"], value: "2" }),
								},
								{ shape: { id: true, nested: { bool: true } } },
							)
							.all();

						expect(shapedResults).to.have.length(1);
						expect(shapedResults[0].value.id).to.equal(d2.id);

						if (properties.shapingSupported) {
							expect({ ...shapedResults[0].value.nested }).to.deep.equal({
								bool: false,
							});
						} else {
							expect(shapedResults[0].value.nested).to.deep.equal(d2.nested);
						}
					});
				});

				describe("nested-poly", () => {
					abstract class Base {}

					@variant(0)
					class MultifieldNested extends Base {
						@field({ type: "bool" })
						bool: boolean;

						@field({ type: "u32" })
						number: number;

						@field({ type: vec("string") })
						string: string[];

						constructor(bool: boolean, number: number, string: string[]) {
							super();
							this.bool = bool;
							this.number = number;
							this.string = string;
						}
					}

					@variant(1)
					class _AnotherMultifieldNested extends Base {
						@field({ type: "bool" })
						bool: boolean;

						@field({ type: "u32" })
						number: number;

						@field({ type: vec("string") })
						string: string[];

						constructor(bool: boolean, number: number, string: string[]) {
							super();
							this.bool = bool;
							this.number = number;
							this.string = string;
						}
					}

					class NestedBoolQueryDocument {
						@id({ type: "string" })
						id: string;

						@field({ type: Base })
						nested: Base;

						constructor(id: string, nested: Base) {
							this.id = id;
							this.nested = nested;
						}
					}

					let index: Awaited<ReturnType<typeof setup<NestedBoolQueryDocument>>>;

					afterEach(async () => {
						await index.store.stop();
					});

					it("nested", async () => {
						index = await setup({ schema: NestedBoolQueryDocument });

						await index.store.put(
							new NestedBoolQueryDocument(
								"1",
								new MultifieldNested(true, 1, ["1"]),
							),
						);
						await index.store.put(
							new NestedBoolQueryDocument(
								"2",
								new MultifieldNested(false, 2, ["2"]),
							),
						);

						const shapedResults = await index.store
							.iterate(
								{
									query: new BoolQuery({
										key: ["nested", "bool"],
										value: false,
									}),
								},
								{ shape: { id: true } },
							)
							.all();
						expect(shapedResults).to.have.length(1);
						expect(shapedResults[0].value.id).to.equal("2");

						if (properties.shapingSupported) {
							expect(shapedResults[0].value["nested"]).to.be.undefined;
						} else {
							expect(shapedResults[0].value["nested"]).to.exist;
						}

						const unshapedResults = await index.store
							.iterate({
								query: new BoolQuery({ key: ["nested", "bool"], value: false }),
							})
							.all();

						expect(unshapedResults).to.have.length(1);
						expect(unshapedResults[0].value.id).to.equal("2");

						expect(unshapedResults[0].value.nested).to.exist;
					});

					it("nested-filtering", async () => {
						index = await setup({ schema: NestedBoolQueryDocument });

						const d1 = new NestedBoolQueryDocument(
							"1",
							new MultifieldNested(true, 1, ["1"]),
						);
						const d2 = new NestedBoolQueryDocument(
							"2",
							new MultifieldNested(false, 2, ["2"]),
						);

						await index.store.put(d1);
						await index.store.put(d2);

						const unshapedResults = await index.store
							.iterate({
								query: new StringMatch({ key: ["id"], value: "2" }),
							})
							.all();
						expect(unshapedResults).to.have.length(1);
						expect(unshapedResults[0].value.id).to.equal(d2.id);
						expect(unshapedResults[0].value.nested).to.deep.equal(d2.nested);

						const shapedResults = await index.store
							.iterate(
								{
									query: new StringMatch({ key: ["id"], value: "2" }),
								},
								{ shape: { id: true, nested: { bool: true } } },
							)
							.all();
						expect(shapedResults).to.have.length(1);
						expect(shapedResults[0].value.id).to.equal(d2.id);

						if (properties.shapingSupported) {
							expect({ ...shapedResults[0].value.nested }).to.deep.equal({
								bool: false,
							});
						} else {
							expect(shapedResults[0].value.nested).to.deep.equal(d2.nested);
						}
					});
				});

				describe("nested-array", () => {
					class MultifieldNested {
						@field({ type: "bool" })
						bool: boolean;

						@field({ type: "u32" })
						number: number;

						@field({ type: vec("string") })
						string: string[];

						constructor(bool: boolean, number: number, string: string[]) {
							this.bool = bool;
							this.number = number;
							this.string = string;
						}
					}

					class NestedBoolQueryDocument {
						@id({ type: "string" })
						id: string;

						@field({ type: vec(MultifieldNested) })
						nested: MultifieldNested[];

						constructor(id: string, nested: MultifieldNested) {
							this.id = id;
							this.nested = [nested];
						}
					}

					let index: Awaited<ReturnType<typeof setup<NestedBoolQueryDocument>>>;

					afterEach(async () => {
						await index.store.stop();
					});

					it("nested", async () => {
						index = await setup({ schema: NestedBoolQueryDocument });

						await index.store.put(
							new NestedBoolQueryDocument(
								"1",
								new MultifieldNested(true, 1, ["1"]),
							),
						);
						await index.store.put(
							new NestedBoolQueryDocument(
								"2",
								new MultifieldNested(false, 2, ["2"]),
							),
						);

						const shapedResults = await index.store
							.iterate(
								{
									query: new BoolQuery({
										key: ["nested", "bool"],
										value: false,
									}),
								},
								{ shape: { id: true } },
							)
							.all();
						expect(shapedResults).to.have.length(1);
						expect(shapedResults[0].value.id).to.equal("2");

						if (properties.shapingSupported) {
							expect(shapedResults[0].value["nested"]).to.be.undefined;
						} else {
							expect(shapedResults[0].value["nested"]).to.exist;
						}

						const unshapedResults = await index.store
							.iterate({
								query: new BoolQuery({ key: ["nested", "bool"], value: false }),
							})
							.all();
						expect(unshapedResults).to.have.length(1);
						expect(unshapedResults[0].value.id).to.equal("2");
						expect(unshapedResults[0].value.nested).to.exist;
					});

					it("nested-filtering", async () => {
						index = await setup({ schema: NestedBoolQueryDocument });

						const d1 = new NestedBoolQueryDocument(
							"1",
							new MultifieldNested(true, 1, ["1"]),
						);
						const d2 = new NestedBoolQueryDocument(
							"2",
							new MultifieldNested(false, 2, ["2"]),
						);

						await index.store.put(d1);
						await index.store.put(d2);

						const shapedResults = await index.store
							.iterate(
								{
									query: new StringMatch({ key: ["id"], value: "2" }),
								},
								{ shape: { id: true, nested: [{ bool: true }] } },
							)
							.all();
						expect(shapedResults).to.have.length(1);
						expect(shapedResults[0].value.id).to.equal(d2.id);

						if (properties.shapingSupported) {
							expect({ ...shapedResults[0].value.nested[0] }).to.deep.equal({
								bool: false,
							});
						} else {
							expect(shapedResults[0].value.nested[0]).to.deep.equal(
								d2.nested[0],
							);
						}

						const unshapedResults = await index.store
							.iterate({
								query: new StringMatch({ key: ["id"], value: "2" }),
							})
							.all();
						expect(unshapedResults).to.have.length(1);
						expect(unshapedResults[0].value.id).to.equal(d2.id);
						expect(unshapedResults[0].value.nested[0]).to.deep.equal(
							d2.nested[0],
						);
					});

					it("true resolves fully", async () => {
						index = await setup({ schema: NestedBoolQueryDocument });

						const d1 = new NestedBoolQueryDocument(
							"1",
							new MultifieldNested(true, 1, ["1"]),
						);

						await index.store.put(d1);

						const shapedResults = await index.store
							.iterate(
								{
									query: [],
								},
								{ shape: { id: true, nested: true } },
							)
							.all();
						expect(shapedResults).to.have.length(1);
						expect(shapedResults[0].value.id).to.equal(d1.id);

						expect(shapedResults[0].value.nested[0]).to.deep.equal(
							d1.nested[0],
						);
					});
				});
			});
		});

		describe("sort", () => {
			const put = async (id: number, stringId?: string) => {
				const doc = new Document({
					id: stringId ?? String(id),
					name: String(id),
					number: BigInt(id),
					tags: [],
				});
				const resp = await store.put(doc);
				return resp;
			};

			const checkIterate = async (
				batches: bigint[][],
				query: Query[] = [
					new IntegerCompare({
						key: "number",
						compare: Compare.GreaterOrEqual,
						value: 0n,
					}),
				],
				sort: Sort[] = [
					new Sort({ direction: SortDirection.ASC, key: "number" }),
				],
			) => {
				await waitForResolved(async () => {
					const req = {
						query,
						sort,
					};
					const iterator = store.iterate(req);

					// No fetches has been made, so we don't know whether we are done yet
					expect(iterator.done()).to.be.undefined;

					if (batches.length === 0) {
						await assertIteratorIsDone(iterator);
					} else {
						let first = true;
						for (const batch of batches) {
							first
								? expect(iterator.done()).to.be.undefined
								: expect(iterator.done()).to.be.false;
							first = false;
							const next = await iterator.next(batch.length);
							expect(next.map((x) => Number(x.value.number))).to.deep.equal(
								batch.map((x) => Number(x)),
							);
						}
						await assertIteratorIsDone(iterator);
					}
				});
			};

			beforeEach(async () => {
				const results = await setup({ schema: Document });
				store = results.store;
			});

			it("empty", async () => {
				await checkIterate([]);
			});

			// TODO make sure documents are evenly distrubted before query
			it("multiple batches", async () => {
				await put(0);
				await put(1);
				await put(2);
				expect(await store.getSize()).equal(3);
				await checkIterate([[0n], [1n], [2n]]);
				await checkIterate([[0n, 1n, 2n]]);
				await checkIterate([[0n, 1n], [2n]]);
				await checkIterate([[0n], [1n, 2n]]);
			});

			it("sorts by order", async () => {
				await put(0);
				await put(1);
				await put(2);
				const f1 = async () => {
					const iterator = store.iterate({
						query: [],
						sort: [new Sort({ direction: SortDirection.ASC, key: "name" })],
					});
					expect(iterator.done()).to.be.undefined;
					const next = await iterator.next(3);
					expect(next.map((x) => x.value.name)).to.deep.equal(["0", "1", "2"]);
					await assertIteratorIsDone(iterator);
				};
				const f2 = async () => {
					const iterator = store.iterate({
						query: [],
						sort: [new Sort({ direction: SortDirection.DESC, key: "name" })],
					});
					expect(iterator.done()).to.be.undefined;
					const next = await iterator.next(3);
					expect(next.map((x) => x.value.name)).to.deep.equal(["2", "1", "0"]);
					await assertIteratorIsDone(iterator);
				};
				await f1();
				await f2();
			});

			it("sorts by order", async () => {
				await put(0);
				await put(1);
				await put(2);
				const f1 = async () => {
					const iterator = store.iterate({
						query: [],
						sort: [new Sort({ direction: SortDirection.ASC, key: "name" })],
					});
					expect(iterator.done()).to.be.undefined;
					const next = await iterator.next(3);
					expect(next.map((x) => x.value.name)).to.deep.equal(["0", "1", "2"]);
					await assertIteratorIsDone(iterator);
				};
				const f2 = async () => {
					const iterator = store.iterate({
						query: [],
						sort: [new Sort({ direction: SortDirection.DESC, key: "name" })],
					});
					expect(iterator.done()).to.be.undefined;
					const next = await iterator.next(3);
					expect(next.map((x) => x.value.name)).to.deep.equal(["2", "1", "0"]);
					await assertIteratorIsDone(iterator);
				};
				const f3 = async () => {
					const iterator = store.iterate({
						query: [],
						sort: [new Sort({ direction: SortDirection.ASC, key: "name" })],
					});
					expect(iterator.done()).to.be.undefined;
					let next = await iterator.next(2);
					expect(next.map((x) => x.value.name)).to.deep.equal(["0", "1"]);
					next = await iterator.next(1);
					expect(next.map((x) => x.value.name)).to.deep.equal(["2"]);
					await assertIteratorIsDone(iterator);
				};
				const f4 = async () => {
					const iterator = store.iterate({
						query: [],
						sort: [new Sort({ direction: SortDirection.DESC, key: "name" })],
					});
					expect(iterator.done()).to.be.undefined;
					let next = await iterator.next(2);
					expect(next.map((x) => x.value.name)).to.deep.equal(["2", "1"]);
					next = await iterator.next(1);
					expect(next.map((x) => x.value.name)).to.deep.equal(["0"]);
					await assertIteratorIsDone(iterator);
				};
				const f5 = async () => {
					const iterator = store.iterate({
						query: [],
						sort: [new Sort({ direction: SortDirection.ASC, key: "name" })],
					});
					expect(iterator.done()).to.be.undefined;
					let next = await iterator.next(1);
					expect(next.map((x) => x.value.name)).to.deep.equal(["0"]);
					next = await iterator.next(1);
					expect(next.map((x) => x.value.name)).to.deep.equal(["1"]);
					next = await iterator.next(1);
					expect(next.map((x) => x.value.name)).to.deep.equal(["2"]);
					await assertIteratorIsDone(iterator);
				};
				await f1();
				await f2();
				await f3();
				await f4();
				await f5();
			});

			/* it("no sort is stable", async () => {
				// TODO this test is actually not a good predictor of stability

				const insertCount = 500;
				for (let i = 0; i < insertCount; i++) {
					await put(i, uuid());
				}

				const resolvedValues: Set<number> = new Set()
				const batchSize = 123;
				const iterator = store.iterate();
				while (!iterator.done()) {
					const next = await iterator.next(batchSize);
					next.map((x) => resolvedValues.add(Number(x.value.number)));
				}
				expect(resolvedValues.size).to.equal(insertCount);
			}); */

			it("strings", async () => {
				await put(0);
				await put(1);
				await put(2);

				const iterator = store.iterate({
					query: [],
					sort: [new Sort({ direction: SortDirection.ASC, key: "name" })],
				});
				expect(iterator.done()).to.be.undefined;
				const next = await iterator.next(3);
				expect(next.map((x) => x.value.name)).to.deep.equal(["0", "1", "2"]);
				await assertIteratorIsDone(iterator);
			});

			describe("nested", () => {
				it("variants", async () => {
					const doc1 = new Document({
						id: "1",
						nested: new NestedValue({ number: 1 }),
					});
					const doc2 = new Document({
						id: "2",
						nested: new NestedValue({ number: 2 }),
					});
					await store.put(doc1);
					await store.put(doc2);

					const iterator = store.iterate({
						sort: [
							new Sort({
								direction: SortDirection.DESC,
								key: ["nested", "number"],
							}),
						],
					});
					expect(iterator.done()).to.be.undefined;
					const next = await iterator.next(2);
					expect(next.map((x) => x.value.id)).to.deep.equal(["2", "1"]);
					await assertIteratorIsDone(iterator);
				});

				describe("nested-nested-invariant", () => {
					class V0 {
						@field({ type: "u64" })
						number: bigint;

						constructor(number: bigint) {
							this.number = number;
						}
					}

					class NestedValue {
						@field({ type: V0 })
						v0?: V0;

						constructor(v0?: V0) {
							this.v0 = v0;
						}
					}

					class Document {
						@id({ type: "string" })
						id: string;

						@field({ type: NestedValue })
						nested: NestedValue;

						constructor(id: string, nested: NestedValue) {
							this.id = id;
							this.nested = nested;
						}
					}

					const doc1 = new Document("1", new NestedValue(new V0(1n)));
					const doc2 = new Document("2", new NestedValue(new V0(2n)));

					beforeEach(async () => {
						await setup({ schema: Document });
						await store.put(doc1);
						await store.put(doc2);
					});

					it("nested-variants", async () => {
						const iterator = store.iterate({
							sort: [
								new Sort({
									direction: SortDirection.DESC,
									key: ["nested", "v0", "number"],
								}),
							],
						});
						expect(iterator.done()).to.be.undefined;
						const next = await iterator.next(2);
						expect(next.map((x) => x.value.id)).to.deep.equal(["2", "1"]);
						await assertIteratorIsDone(iterator);
					});
				});

				describe("variant-nested-invariant", () => {
					class V0 {
						@field({ type: "u64" })
						number: bigint;

						constructor(number: bigint) {
							this.number = number;
						}
					}

					class NestedValue {
						@field({ type: V0 })
						v0?: V0;

						constructor(v0?: V0) {
							this.v0 = v0;
						}
					}

					@variant(0)
					class DocumentV0 {
						@id({ type: "string" })
						id: string;

						@field({ type: NestedValue })
						nested: NestedValue;

						constructor(id: string, nested: NestedValue) {
							this.id = id;
							this.nested = nested;
						}
					}

					const doc1 = new DocumentV0("1", new NestedValue(new V0(1n)));
					const doc2 = new DocumentV0("2", new NestedValue(new V0(2n)));

					beforeEach(async () => {
						await setup({ schema: DocumentV0 });
						await store.put(doc1);
						await store.put(doc2);
					});

					it("nested-variants", async () => {
						const iterator = store.iterate({
							sort: [
								new Sort({
									direction: SortDirection.DESC,
									key: ["nested", "v0", "number"],
								}),
							],
						});
						expect(iterator.done()).to.be.undefined;
						const next = await iterator.next(2);
						expect(next.map((x) => x.value.id)).to.deep.equal(["2", "1"]);
					});
				});
				/*  TODO (requires sort join interleaving)
				
				describe("nested-nested-variant", () => {
		
					abstract class Base { }
		
					@variant(0)
					class V0 extends Base {
		
						@field({ type: 'u64' })
						number: bigint;
		
						constructor(number: bigint) {
							super()
							this.number = number;
						}
					}
		
					@variant(1)
					class V1 extends Base {
		
						@field({ type: 'u64' })
						number: bigint;
		
						constructor(number: bigint) {
							super()
							this.number = number;
						}
					}
		
					class NestedValue {
						@field({ type: Base })
						v0: Base;
		
						constructor(v0?: Base) {
							this.v0 = v0;
						}
					}
		
					class Document {
						@id({ type: 'string' })
						id: string;
		
						@field({ type: NestedValue })
						nested: NestedValue;
		
						constructor(id: string, nested: NestedValue) {
							this.id = id;
							this.nested = nested;
						}
					}
		
					const doc1 = new Document("1", new NestedValue(new V0(1n)));
					const doc2 = new Document("2", new NestedValue(new V0(2n)));
					const doc3 = new Document("3", new NestedValue(new V1(3n)));
					const doc4 = new Document("4", new NestedValue(new V1(4n)));
		
					beforeEach(async () => {
						await setup({ schema: Document });
						await store.put(doc1);
						await store.put(doc2);
						await store.put(doc3);
						await store.put(doc4);
					});
		
					it("nested-variants", async () => {
						const iterator = iterate(store, ({ sort: [new Sort({ direction: SortDirection.DESC, key: ["nested", "v0", "number"] })] }));
						expect(iterator.done()).to.be.false;
						const next = await iterator.next(4);
						expect(next.results.map((x) => x.value.id)).to.deep.equal(["4", "3", "2", "1"]);
		
					})
		
				})*/

				/* TODO 
				it("array sort", async () => {
		
					const doc1 = new Document({
						id: "1",
						number: 101n,
						nestedVec: [new NestedValue({ number: 1 }), new NestedValue({ number: 300 })]
					});
					const doc2 = new Document({
						id: "2",
						number: 102n,
						nestedVec: [new NestedValue({ number: 2 }), new NestedValue({ number: 200 })]
					});
		
					const doc3 = new Document({
						id: "3",
						number: 103n,
						nestedVec: [new NestedValue({ number: 3 }), new NestedValue({ number: 100 })]
					});
		
					await store.put(doc1);
					await store.put(doc2);
					await store.put(doc3);
		
					const iterator = iterate(store, ({ query: [new IntegerCompare({ key: 'number', compare: 'gte', value: 102n }), new Nested({ path: 'nestedVec', id: 'path-to-nested', query: [new IntegerCompare({ key: 'number', compare: 'gte', value: 200 })] })], sort: [new Sort({ direction: SortDirection.DESC, key: ["path-to-nested", "number"] })] }));
					expect(iterator.done()).to.be.false;
					const next = await iterator.next(2);
					expect(next.results.map((x) => x.value.id)).to.deep.equal(["2", "1"]);
					expect(iterator.done()).to.be.true;
				})*/
			});

			describe("close", () => {
				it("by invoking close()", async () => {
					await put(0);
					await put(1);
					await put(2);
					const request = {
						query: [],
					};
					const iterator = store.iterate(request);
					expect(iterator.done()).to.be.undefined;
					await iterator.next(2); // fetch some, but not all
					expect(iterator.done()).to.be.false;
					expect(await iterator.pending()).equal(1);
					await iterator.close();
					expect(await iterator.pending()).equal(0);
					expect(iterator.done()).to.be.true;
				});

				it("end of iterator", async () => {
					await put(0);
					await put(1);
					await put(2);
					const request = {
						query: [],
					};
					const iterator = store.iterate(request);
					expect(iterator.done()).to.be.undefined;
					await iterator.next(3); // fetch all
					expect(await iterator.pending()).equal(0);
					expect(iterator.done()).to.be.true;
				});

				it("end of iterator, multiple nexts", async () => {
					await put(0);
					await put(1);
					await put(2);
					const request = {
						query: [],
					};
					const iterator = store.iterate(request);
					await iterator.next(2);
					await iterator.next(1);
					assertIteratorIsDone(iterator);
					expect(await iterator.pending()).equal(0);
				});
			});

			// TODO test iterator.close() to stop pending promises

			// TODO deletion while sort

			// TODO session timeouts?
		});

		describe("sum", () => {
			class SummableDocument {
				@field({ type: "string" })
				id: string;

				@field({ type: option("u32") })
				value?: number;

				constructor(opts: SummableDocument) {
					this.id = opts.id;
					this.value = opts.value;
				}
			}
			it("it returns sum", async () => {
				await setup({ schema: SummableDocument });
				await store.put(
					new SummableDocument({
						id: "1",
						value: 1,
					}),
				);
				await store.put(
					new SummableDocument({
						id: "2",
						value: 2,
					}),
				);
				const sum = await store.sum({ key: "value" });
				typeof sum === "bigint"
					? expect(sum).to.equal(3n)
					: expect(sum).to.equal(3);
			});

			if (properties.u64SumSupported) {
				it("u64", async () => {
					await setupDefault();
					const sum = await store.sum({ key: "number" });
					typeof sum === "bigint"
						? expect(sum).to.equal(6n)
						: expect(sum).to.equal(6);
				});
			}

			it("it returns sum with query", async () => {
				await setup({ schema: SummableDocument });
				await store.put(
					new SummableDocument({
						id: "1",
						value: 1,
					}),
				);
				await store.put(
					new SummableDocument({
						id: "2",
						value: 2,
					}),
				);

				const sum = await store.sum({
					key: "value",
					query: [
						new IntegerCompare({
							key: "value",
							compare: Compare.Greater,
							value: 1,
						}),
					],
				});
				typeof sum === "bigint"
					? expect(sum).to.equal(2n)
					: expect(sum).to.equal(2);
			});

			it("nested", async () => {
				await setup({ schema: Document });

				const doc1 = new Document({
					id: "1",
					nested: new NestedValue({ number: 1 }),
				});

				const doc2 = new Document({
					id: "2",
					nested: new NestedValue({ number: 2 }),
				});

				const doc3 = new Document({
					id: "3",
				});

				await store.put(doc1);
				await store.put(doc2);
				await store.put(doc3);

				const sum = await store.sum({
					key: ["nested", "number"],
				});

				typeof sum === "bigint"
					? expect(sum).to.equal(3n)
					: expect(sum).to.equal(3);
			});
		});

		describe("count", () => {
			it("it returns count", async () => {
				await setupDefault();
				const sum = await store.count();
				expect(sum).to.equal(4);
			});

			it("it returns count with query", async () => {
				await setupDefault();
				const sum = await store.count({
					query: [
						new StringMatch({
							key: "tags",
							value: "world",
							method: StringMatchMethod.contains,
							caseInsensitive: true,
						}),
					],
				});
				expect(sum).to.equal(1);
			});
		});

		describe("delete", () => {
			it("delete with query", async () => {
				await setupDefault();
				await store.del({
					query: [
						new StringMatch({
							key: "tags",
							value: "world",
							method: StringMatchMethod.contains,
							caseInsensitive: true,
						}),
					],
				});
				expect(await store.getSize()).to.equal(3);
			});
		});

		describe("persistance", () => {
			if (type === "persist") {
				it("persists across restarts", async () => {
					const {
						store: documentStore,
						indices,
						directory,
					} = await setupDefault();
					expect(await documentStore.getSize()).equal(4);
					await indices.stop();
					const { store } = await setup({ schema: Document }, directory);
					expect(await store.getSize()).equal(4);
				});
			} else {
				it("should not persist", async () => {
					let { store } = await setup({ schema: Document });
					await store.stop();
					store = (await setup({ schema: Document })).store;
					expect(await store.getSize()).equal(0);
				});
			}
		});

		describe("concurrency", () => {
			it("can handle concurrent counts", async () => {
				let results: number[] = [];
				let promises: Promise<void>[] = [];
				await setupDefault();
				for (let i = 0; i < 100; i++) {
					promises.push(
						(async () => {
							results.push(await store.count());
						})(),
					);
				}
				await Promise.all(promises);
				expect(results).to.have.length(100);
				expect(results.every((x) => x === 4)).to.be.true;
			});
		});
		describe("drop", () => {
			it("store", async () => {
				let { directory, indices, store } = await setupDefault();
				expect(await store.getSize()).equal(4);
				await store.drop();
				await store.start();
				expect(await store.getSize()).equal(0);
				await indices.stop();
				store = (await setup({ schema: Document }, directory)).store;
				expect(await store.getSize()).equal(0);
			});

			it("indices", async () => {
				let { directory, indices } = await setupDefault();

				let subindex = await indices.scope("x");

				store = await subindex.init({ indexBy: ["id"], schema: Document });
				await store.put(
					new Document({ id: "1", name: "hello", number: 1n, tags: [] }),
				);
				await store.put(
					new Document({ id: "2", name: "hello", number: 1n, tags: [] }),
				);
				await store.put(
					new Document({ id: "3", name: "hello", number: 1n, tags: [] }),
				);
				await store.put(
					new Document({ id: "4", name: "hello", number: 1n, tags: [] }),
				);
				await store.start();
				expect(await store.getSize()).equal(4);

				await indices.drop();

				await store.start();

				expect(await store.getSize()).equal(0);

				await store.stop(); /// TODO why do w
				await indices.stop();

				store = (await setup({ schema: Document }, directory)).store;

				await store.start();
				expect(await store.getSize()).equal(0);
			});
		});

		describe("scopes", () => {
			it("stop after stop", async () => {
				let { indices, store } = await setupDefault();
				expect(await store.getSize()).equal(4);
				let subindex = await indices.scope("x");
				store = await subindex.init({ indexBy: ["id"], schema: Document });
				await indices.stop();
				store = (await setup({ schema: Document })).store;
				await store.stop();
			});

			it("re-drop", async () => {
				const scope = await createIndicies();
				await scope.start();
				const subScope = await scope.scope("subindex");
				await subScope.init({ indexBy: ["id"], schema: Document });
				await scope.drop();
				await scope.drop();
			});

			it("isolates", async () => {
				const scope = await createIndicies();
				await scope.start();
				const scopeA = await scope.scope("a");
				const scopeB = await scope.scope("b");
				const indexA = await scopeA.init({ indexBy: ["id"], schema: Document });
				const indexB = await scopeB.init({ indexBy: ["id"], schema: Document });
				await indexA.put(
					new Document({ id: "1", name: "hello", number: 1n, tags: [] }),
				);
				await indexB.put(
					new Document({ id: "2", name: "hello", number: 1n, tags: [] }),
				);
				expect(await indexA.getSize()).equal(1);
				expect(await indexB.get(toId("1"))).to.not.exist;
			});

			it("scope name can contain any character", async () => {
				const scope = await createIndicies();
				await scope.start();
				const scopeA = await scope.scope("a/=b");
				const indexA = await scopeA.init({ indexBy: ["id"], schema: Document });
				await indexA.put(
					new Document({ id: "1", name: "hello", number: 1n, tags: [] }),
				);
				expect(await indexA.getSize()).equal(1);
			});

			it("drops sub scopes", async () => {
				const scope = await createIndicies();
				let subScope = await scope.scope("subindex");
				await scope.start();
				let subIndex = await subScope.init({
					indexBy: ["id"],
					schema: Document,
				});
				await subIndex.put(
					new Document({ id: "1", name: "hello", number: 1n, tags: [] }),
				);
				expect(await subIndex.getSize()).equal(1);
				await scope.drop();

				// re-init the scope
				subScope = await scope.scope("subindex");
				subIndex = await subScope.init({ indexBy: ["id"], schema: Document });

				expect(await subIndex.getSize()).equal(0);
			});

			it("starts on init if scope is started", async () => {
				const scope = await createIndicies();
				await scope.start();
				const subScope = await scope.scope("subindex");
				const subIndex = await subScope.init({
					indexBy: ["id"],
					schema: Document,
				});
				await subIndex.put(
					new Document({ id: "1", name: "hello", number: 1n, tags: [] }),
				);
			});

			it("can restart", async () => {
				const scope = await createIndicies();
				await scope.start();
				await scope.stop();
				await scope.start();
				const subIndex = await scope.init({
					indexBy: ["id"],
					schema: Document,
				});
				await subIndex.put(
					new Document({ id: "1", name: "hello", number: 1n, tags: [] }),
				);
			});

			it("multi-scope insertion", async () => {
				class AnotherDocument {
					@id({ type: "string" })
					id: string;

					@field({ type: "string" })
					string: string;

					constructor(string: string) {
						this.id = string;
						this.string = string;
					}
				}

				const scope = await createIndicies();
				await scope.start();

				const a = await scope.scope("a");
				const aIndex = await a.init({ indexBy: ["id"], schema: Document });

				const b = await scope.scope("b");
				const bIndex = await b.init({
					indexBy: ["id"],
					schema: AnotherDocument,
				});

				await aIndex.put(
					new Document({ id: "a", name: "hello", number: 1n, tags: [] }),
				);
				expect(await aIndex.count({ query: { id: "a" } })).to.eq(1);

				await bIndex.put(new AnotherDocument("b"));
				expect(await bIndex.count({ query: { id: "b" } })).to.eq(1);
			});
		});
	});
};

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

			expect(error.message).equal("vec(vec(...)) is not supported");
			return;
		}

		await expect(store.put(new NestedVec({ matrix: [[1, 2], [3]] }))).rejectedWith("vec(vec(...)) is not supported");
	});
}); */

/* 	describe("concurrently", () => {
						beforeEach(async () => {
							await setupDefault();
						});
						it("can query concurrently", async () => {
							// TODO add more concurrency
							const promises: MaybePromise<IndexedResults>[] = [];
							const concurrency = 100;
							for (let i = 0; i < concurrency; i++) {
								if (i % 2 === 0) {
									promises.push(
										search(
											store,
											({
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
										search(
											store,
											({
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
									expect(results[i].results).to.have.length(2);
									results[i].results.sort((a, b) =>
										Number(a.value.number! - b.value.number!)
									);
									expect(results[i].results[0].value.number).to.be.oneOf([2n, 2]);
									expect(results[i].results[1].value.number).to.be.oneOf([3n, 3]);
								} else {
									// query2
									expect(results[i].results).to.have.length(1);
									expect(results[i].results[0].value.number).to.be.oneOf([1n, 1]);
								}
							}
						});
					}); */
