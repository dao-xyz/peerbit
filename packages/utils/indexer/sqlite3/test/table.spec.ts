import { field } from "@dao-xyz/borsh";
import {
	type Index,
	type IndexEngineInitProperties,
	type Indices,
	getIdProperty,
	id,
} from "@peerbit/indexer-interface";
import { expect } from "chai";
import { SQLLiteIndex } from "../src/engine.js";
import { create } from "../src/index.js";

const setup = async <T extends Record<string, any>>(
	properties: Partial<IndexEngineInitProperties<T, any>> & { schema: any },
	createIndicies: (directory?: string) => Indices | Promise<Indices>,
): Promise<{ indices: Indices; store: Index<T, any>; directory?: string }> => {
	const indices = await createIndicies();
	await indices.start();
	const indexProps: IndexEngineInitProperties<T, any> = {
		...{
			indexBy: getIdProperty(properties.schema) || ["id"],
			iterator: { batch: { maxSize: 5e6, sizeProperty: ["__size"] } },
		},
		...properties,
	};
	const store = await indices.init(indexProps);
	return { indices, store };
};

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
