import { field, serialize, variant } from "@dao-xyz/borsh";
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

	constructor(properties: { id: string }) {
		this.id = properties.id;
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
			source: serialize(new Document({ id: "1" })),
			context,
		});
		const foreign: any = {
			_source: serialize(new Document({ id: "2" })),
			context,
			init(type: unknown) {
				this._type = type;
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
		};
		const foreignResults = {
			results: [foreignIndexed],
			kept: 0n,
		};

		expect(isResultIndexedValue(indexed)).to.be.true;
		expect(isResultIndexedValue(foreignIndexed)).to.be.true;
		expect(isResultValue(foreignIndexed)).to.be.false;
		expect(isResults(new Results({ results: [indexed], kept: 0n }))).to.be.true;
		expect(isResults(foreignResults)).to.be.true;

		initializeResultType(
			foreignIndexed as ResultIndexedValue<IndexedDocument>,
			Document,
			IndexedDocument,
		);

		expect(foreignIndexed._type).to.equal(IndexedDocument);
	});
});
