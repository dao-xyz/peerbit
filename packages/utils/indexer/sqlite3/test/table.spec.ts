import { field } from "@dao-xyz/borsh";
import { id } from "@peerbit/indexer-interface";
import { expect } from "chai";
import { SQLLiteIndex } from "../src/engine.js";
import { create } from "../src/index.js";
import { setup } from "./utils.js";

describe("table", () => {
	let index: Awaited<ReturnType<typeof setup<any>>>;

	afterEach(async () => {
		await index.store.stop();
	});

	// TODO what is expected? if we do this, we can not migrate, on the other hand we get performance benefits
	it("can use reserved words", async () => {
		class DocumentWithFromProperty {
			@id({ type: "string" })
			id: string;

			@field({ type: "string" })
			from: string;

			constructor(id: string, from: string) {
				this.id = id;
				this.from = from;
			}
		}

		index = await setup({ schema: DocumentWithFromProperty }, create);
		const store = index.store as SQLLiteIndex<DocumentWithFromProperty>;
		expect(store.tables.size).to.equal(1);
		await store.put(new DocumentWithFromProperty("1", "from"));

		const results = await store.iterate().all();
		expect(results.length).to.equal(1);
	});
});
