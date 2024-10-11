import { field, variant } from "@dao-xyz/borsh";
import { SearchRequest } from "@peerbit/document-interface";
import { Program } from "@peerbit/program";
import { TestSession } from "@peerbit/test-utils";
import { delay, waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import { v4 as uuid } from "uuid";
import { type CustomDomain, createDocumentDomain } from "../src/domain.js";
import { Documents, type SetupOptions } from "../src/program.js";

class Document {
	@field({ type: "string" })
	id: string;
	@field({ type: "u32" })
	property: number;

	constructor(properties: { id?: string; property: number }) {
		this.id = properties.id || uuid();
		this.property = properties.property;
	}
}

@variant("StoreWithCustomDomain")
export class StoreWithCustomDomain extends Program {
	@field({ type: Documents })
	docs: Documents<Document, Document, CustomDomain>;

	constructor(properties?: {
		docs?: Documents<Document, Document, CustomDomain>;
	}) {
		super();
		this.docs =
			properties?.docs || new Documents<Document, Document, CustomDomain>();
	}

	async open(
		args?: Partial<SetupOptions<Document, Document, CustomDomain>>,
	): Promise<void> {
		return this.docs.open({
			...(args || {}),
			domain: createDocumentDomain(this.docs, {
				fromValue: (value) => value.property,
			}),
			type: Document,
		});
	}
}

describe("domain", () => {
	let session: TestSession;

	before(async () => {
		session = await TestSession.connected(2);
	});

	after(async () => {
		await session.stop();
	});

	it("custom domain", async () => {
		const store = await session.peers[0].open(new StoreWithCustomDomain(), {
			args: {
				replicate: {
					normalized: false,
					factor: 1,
					offset: 1,
					strict: true,
				},
			},
		});
		const store2 = await session.peers[1].open(store.clone(), {
			args: {
				replicate: {
					normalized: false,
					offset: 2,
					factor: 2,
					strict: true,
				},
			},
		});

		await store.docs.put(new Document({ id: "1", property: 1 }));
		await store2.docs.put(new Document({ id: "2", property: 2 }));
		await store2.docs.put(new Document({ id: "3", property: 3 }));

		await delay(3000); // wait for sometime so that potential replication could have happened

		expect(await store.docs.index.getSize()).to.equal(1);
		expect(await store2.docs.index.getSize()).to.equal(2);

		// test querying with the same domain but different peers and assert results are correct
		await waitForResolved(async () => {
			const resultsWithRemoteRightDomain = await store.docs.index.search(
				new SearchRequest(),
				{
					remote: {
						domain: {
							from: 2,
							to: 3,
						},
					},
				},
			);

			expect(resultsWithRemoteRightDomain).to.have.length(3);
		});

		const resultsWhenRemoteDoesNotHaveRightDomain =
			await store.docs.index.search(new SearchRequest(), {
				remote: {
					domain: {
						from: 4,
						to: 5,
					},
				},
			});

		expect(resultsWhenRemoteDoesNotHaveRightDomain).to.have.length(1); // only the loal result
		expect(resultsWhenRemoteDoesNotHaveRightDomain[0].id).to.equal("1");
	});
});
