import { deserialize, field, serialize, variant } from "@dao-xyz/borsh";
import {
	Context,
	ResultIndexedValue,
	ResultValue,
	Results,
} from "@peerbit/document-interface";
import { expect } from "chai";
import {
	initializeResultType,
	isResultIndexedValue,
	isResultValue,
	isResults,
} from "../src/result-shape.js";

@variant("result_shape_document")
class Document {
	@field({ type: "string" })
	id: string;

	@field({ type: "string" })
	body: string;

	constructor(properties: { id: string; body: string }) {
		this.id = properties.id;
		this.body = properties.body;
	}
}

@variant("result_shape_indexed")
class IndexedDocument {
	@field({ type: "string" })
	id: string;

	constructor(properties: { id: string }) {
		this.id = properties.id;
	}
}

const context = new Context({
	created: 0n,
	modified: 0n,
	head: "head",
	gid: "gid",
	size: 0,
});

describe("result shape guards", () => {
	it("classifies result values by shape across package identity boundaries", () => {
		const local = new ResultValue({
			source: serialize(new Document({ id: "1", body: "manifest" })),
			context,
		});
		const foreign: any = {
			_source: serialize(new Document({ id: "2", body: "remote manifest" })),
			context,
			init(type: unknown) {
				this._type = type;
			},
			get value() {
				return deserialize(this._source, this._type);
			},
		};

		expect(isResultValue(local)).to.be.true;
		expect(isResultValue(foreign)).to.be.true;
		expect(isResultIndexedValue(foreign)).to.be.false;

		initializeResultType(
			foreign as ResultValue<Document>,
			Document,
			IndexedDocument,
		);

		expect(foreign._type).to.equal(Document);
		expect(foreign.value).to.be.instanceOf(Document);
		expect(foreign.value.body).to.equal("remote manifest");
	});

	it("classifies indexed results and result batches by shape", () => {
		const indexed = new ResultIndexedValue({
			source: serialize(new IndexedDocument({ id: "1" })),
			indexed: new IndexedDocument({ id: "1" }),
			entries: [],
			context,
		});
		const foreignIndexed: any = {
			_source: serialize(new IndexedDocument({ id: "2" })),
			context,
			entries: [],
			init(type: unknown) {
				this._type = type;
			},
			get value() {
				return deserialize(this._source, this._type);
			},
		};
		const foreignResults = {
			results: [foreignIndexed],
			kept: 0n,
		};
		const entryBearingNonResult = {
			context,
			entries: [],
			init() {
				// This looks similar to a result but is missing the serialized
				// source field that ResultIndexedValue carries.
			},
		};

		expect(isResultIndexedValue(indexed)).to.be.true;
		expect(isResultIndexedValue(foreignIndexed)).to.be.true;
		expect(isResultIndexedValue(entryBearingNonResult)).to.be.false;
		expect(isResultValue(foreignIndexed)).to.be.false;
		expect(isResults(new Results({ results: [indexed], kept: 0n }))).to.be.true;
		expect(isResults(foreignResults)).to.be.true;

		initializeResultType(
			foreignIndexed as ResultIndexedValue<IndexedDocument>,
			Document,
			IndexedDocument,
		);

		expect(foreignIndexed._type).to.equal(IndexedDocument);
		expect(foreignIndexed.value).to.be.instanceOf(IndexedDocument);
	});
});
