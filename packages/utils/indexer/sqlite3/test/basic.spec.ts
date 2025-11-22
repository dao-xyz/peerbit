// @ts-nocheck
import {
	deserialize,
	field,
	fixedArray,
	option,
	serialize,
	variant,
	vec,
} from "@dao-xyz/borsh";
import { randomBytes, sha256Base64Sync } from "@peerbit/crypto";
import {
	And,
	BoolQuery,
	ByteMatchQuery,
	Compare,
	type Index,
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
	id,
	toId,
} from "@peerbit/indexer-interface";
import { /* delay,  */ delay, waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import sodium from "libsodium-wrappers";
import { equals } from "uint8arrays";
import { v4 as uuid } from "uuid";
import { setup } from "./utils.js";

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
		this.id = opts.id || uuid();
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

describe("basic", () => {
	let store: Index<any, any>;
	let indices: Indices;
	let defaultDocs: Document[] = [];

	const setupDefault = async () => {
		await sodium.ready;
		const result = await setup<Base>({
			schema: Base,
			iterator: { batch: { maxSize: 5e6, sizeProperty: ["__size"] } },
		});
		indices = result.indices;
		store = result.store;

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
		await waitForResolved(async () => expect(await store.getSize()).equals(1));
		await store.put(docEdit);
		await store.put(doc2);
		await waitForResolved(async () => expect(await store.getSize()).equals(2));

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

	it("all", async () => {
		await setupDefault();

		const results = await store.iterate().all();
		expect(results).to.have.length(4);
		for (const result of results) {
			checkDocument(result.value, ...defaultDocs);
		}
	});
});
