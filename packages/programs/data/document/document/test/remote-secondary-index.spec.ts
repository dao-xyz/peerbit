import { field, variant } from "@dao-xyz/borsh";
import { SearchRequest } from "@peerbit/document-interface";
import { StringMatch } from "@peerbit/indexer-interface";
import { TestSession } from "@peerbit/test-utils";
import { expect } from "chai";
import { Documents } from "../src/program.js";
import { Document, TestStore } from "./data.js";

@variant("remote_indexed_document")
class IndexedDocument {
	@field({ type: "string" })
	id: string;

	@field({ type: "string" })
	name: string;

	constructor(document: Document) {
		this.id = document.id;
		this.name = document.name ?? "";
	}
}

describe("remote secondary index lookup", () => {
	let session: TestSession;

	beforeEach(async () => {
		session = await TestSession.connected(2);
	});

	afterEach(async () => {
		await session.stop();
	});

	it("finds a post-join document by indexed field from a non-replicating peer", async () => {
		const writerStore = await session.peers[0].open(
			new TestStore({ docs: new Documents<Document>() }),
			{
				args: {
					replicate: { factor: 1 },
				},
			},
		);
		const readerStore = await session.peers[1].open(writerStore.clone(), {
			args: {
				replicate: false,
			},
		});

		await readerStore.docs.index.waitFor(writerStore.node.identity.publicKey);
		expect([...(await readerStore.docs.log.getReplicators()).keys()]).to.include(
			writerStore.node.identity.publicKey.hashcode(),
		);

		await writerStore.docs.put(
			new Document({
				id: "metadata-1",
				name: "sync-pointer-1",
			}),
		);

		const localOnly = await readerStore.docs.index
			.iterate(
				new SearchRequest({
					query: new StringMatch({
						key: "name",
						value: "sync-pointer-1",
					}),
				}),
				{ local: true, remote: false },
			)
			.all();

		expect(localOnly).to.have.length(0);

		const remote = await readerStore.docs.index
			.iterate(
				new SearchRequest({
					query: new StringMatch({
						key: "name",
						value: "sync-pointer-1",
					}),
				}),
				{
					local: true,
					remote: {
						timeout: 10_000,
					},
				},
			)
			.all();

		expect(remote.map((entry) => entry.id)).to.deep.equal(["metadata-1"]);
	});

	it("discovers a post-join indexed field query without an explicit waitFor", async () => {
		const writerStore = await session.peers[0].open(
			new TestStore({ docs: new Documents<Document>() }),
			{
				args: {
					replicate: { factor: 1 },
				},
			},
		);
		const readerStore = await session.peers[1].open(writerStore.clone(), {
			args: {
				replicate: false,
			},
		});

		await writerStore.docs.put(
			new Document({
				id: "metadata-2",
				name: "sync-pointer-2",
			}),
		);

		const remote = await readerStore.docs.index
			.iterate(
				new SearchRequest({
					query: new StringMatch({
						key: "name",
						value: "sync-pointer-2",
					}),
				}),
				{
					local: true,
					remote: {
						timeout: 10_000,
					},
				},
			)
			.all();

		expect(remote.map((entry) => entry.id)).to.deep.equal(["metadata-2"]);
	});

	it("discovers a post-join indexed field query with remote wait", async () => {
		const writerStore = await session.peers[0].open(
			new TestStore({ docs: new Documents<Document>() }),
			{
				args: {
					replicate: { factor: 1 },
				},
			},
		);
		const readerStore = await session.peers[1].open(writerStore.clone(), {
			args: {
				replicate: false,
			},
		});

		await writerStore.docs.put(
			new Document({
				id: "metadata-3",
				name: "sync-pointer-3",
			}),
		);

		const remote = await readerStore.docs.index
			.iterate(
				new SearchRequest({
					query: new StringMatch({
						key: "name",
						value: "sync-pointer-3",
					}),
				}),
				{
					local: true,
					remote: {
						timeout: 10_000,
						wait: {
							timeout: 10_000,
							behavior: "block",
						},
					},
				},
			)
			.all();

		expect(remote.map((entry) => entry.id)).to.deep.equal(["metadata-3"]);
	});

	it("discovers a post-join indexed field query from a cold replicator", async () => {
		const writerStore = await session.peers[0].open(
			new TestStore({ docs: new Documents<Document>() }),
			{
				args: {
					replicate: { factor: 1 },
				},
			},
		);
		const readerStore = await session.peers[1].open(writerStore.clone(), {
			args: {
				replicate: { factor: 1 },
			},
		});

		await writerStore.docs.put(
			new Document({
				id: "metadata-4",
				name: "sync-pointer-4",
			}),
		);

		const remote = await readerStore.docs.index
			.iterate(
				new SearchRequest({
					query: new StringMatch({
						key: "name",
						value: "sync-pointer-4",
					}),
				}),
				{
					local: true,
					remote: {
						timeout: 10_000,
						wait: {
							timeout: 10_000,
							behavior: "block",
						},
					},
				},
			)
			.all();

		expect(remote.map((entry) => entry.id)).to.deep.equal(["metadata-4"]);
	});

	it("discovers a post-join indexed field query from a cold replicator without remote wait", async () => {
		const writerStore = await session.peers[0].open(
			new TestStore({ docs: new Documents<Document>() }),
			{
				args: {
					replicate: { factor: 1 },
				},
			},
		);
		const readerStore = await session.peers[1].open(writerStore.clone(), {
			args: {
				replicate: { factor: 1 },
			},
		});

		await writerStore.docs.put(
			new Document({
				id: "metadata-4-nowait",
				name: "sync-pointer-4-nowait",
			}),
		);

		const remote = await readerStore.docs.index
			.iterate(
				new SearchRequest({
					query: new StringMatch({
						key: "name",
						value: "sync-pointer-4-nowait",
					}),
				}),
				{
					local: true,
					remote: {
						timeout: 10_000,
					},
				},
			)
			.all();

		expect(remote.map((entry) => entry.id)).to.deep.equal(["metadata-4-nowait"]);
	});

	it("falls back to known replicators when the cold cover is self-only", async () => {
		const writerStore = await session.peers[0].open(
			new TestStore({ docs: new Documents<Document>() }),
			{
				args: {
					replicate: { factor: 1 },
				},
			},
		);
		const readerStore = await session.peers[1].open(writerStore.clone(), {
			args: {
				replicate: { factor: 1 },
			},
		});

		await readerStore.docs.index.waitFor(writerStore.node.identity.publicKey);

		await writerStore.docs.put(
			new Document({
				id: "metadata-4-cover-self",
				name: "sync-pointer-4-cover-self",
			}),
		);

		const originalGetCover = readerStore.docs.log.getCover.bind(
			readerStore.docs.log,
		);
		const originalGetReplicators = readerStore.docs.log.getReplicators.bind(
			readerStore.docs.log,
		);
		let usedKnownReplicators = false;

		readerStore.docs.log.getCover = async () => {
			return [readerStore.node.identity.publicKey.hashcode()];
		};
		readerStore.docs.log.getReplicators = async () => {
			usedKnownReplicators = true;
			return originalGetReplicators();
		};

		try {
			const remote = await readerStore.docs.index
				.iterate(
					new SearchRequest({
						query: new StringMatch({
							key: "name",
							value: "sync-pointer-4-cover-self",
						}),
					}),
					{
						local: true,
						remote: {
							timeout: 10_000,
						},
					},
				)
				.all();

			expect(remote.map((entry) => entry.id)).to.deep.equal([
				"metadata-4-cover-self",
			]);
			expect(usedKnownReplicators).to.equal(true);
		} finally {
			readerStore.docs.log.getCover = originalGetCover;
			readerStore.docs.log.getReplicators = originalGetReplicators;
		}
	});

	it("discovers a cold replicator query with a custom indexed type", async () => {
		const writerStore = await session.peers[0].open(
			new TestStore<IndexedDocument>({
				docs: new Documents<Document, IndexedDocument>(),
			}),
			{
				args: {
					replicate: { factor: 1 },
					index: {
						type: IndexedDocument,
					},
				},
			},
		);
		const readerStore = await session.peers[1].open(writerStore.clone(), {
			args: {
				replicate: { factor: 1 },
				index: {
					type: IndexedDocument,
				},
			},
		});

		await writerStore.docs.put(
			new Document({
				id: "metadata-5",
				name: "sync-pointer-5",
			}),
		);

		const remote = await readerStore.docs.index
			.iterate(
				new SearchRequest({
					query: new StringMatch({
						key: "name",
						value: "sync-pointer-5",
					}),
				}),
				{
					local: true,
					remote: {
						timeout: 10_000,
						wait: {
							timeout: 10_000,
							behavior: "block",
						},
					},
				},
			)
			.all();

		expect(remote.map((entry) => entry.id)).to.deep.equal(["metadata-5"]);
	});

	it("discovers a cold replicator query with a large resolved document", async () => {
		const writerStore = await session.peers[0].open(
			new TestStore<IndexedDocument>({
				docs: new Documents<Document, IndexedDocument>(),
			}),
			{
				args: {
					replicate: { factor: 1 },
					index: {
						type: IndexedDocument,
					},
				},
			},
		);
		const readerStore = await session.peers[1].open(writerStore.clone(), {
			args: {
				replicate: { factor: 1 },
				index: {
					type: IndexedDocument,
				},
			},
		});

		await writerStore.docs.put(
			new Document({
				id: "metadata-6",
				name: "sync-pointer-6",
				tags: Array.from({ length: 128 }, (_, index) => `chunk-${index}`),
			}),
		);

		const remote = await readerStore.docs.index
			.iterate(
				new SearchRequest({
					query: new StringMatch({
						key: "name",
						value: "sync-pointer-6",
					}),
				}),
				{
					local: true,
					remote: {
						timeout: 10_000,
						wait: {
							timeout: 10_000,
							behavior: "block",
						},
					},
				},
			)
			.all();

		expect(remote.map((entry) => entry.id)).to.deep.equal(["metadata-6"]);
		expect(remote[0].tags).to.have.length(128);
	});

	it("discovers the latest value after a cold replicated overwrite", async () => {
		const writerStore = await session.peers[0].open(
			new TestStore<IndexedDocument>({
				docs: new Documents<Document, IndexedDocument>(),
			}),
			{
				args: {
					replicate: { factor: 1 },
					index: {
						type: IndexedDocument,
					},
				},
			},
		);
		const readerStore = await session.peers[1].open(writerStore.clone(), {
			args: {
				replicate: { factor: 1 },
				index: {
					type: IndexedDocument,
				},
			},
		});

		await writerStore.docs.put(
			new Document({
				id: "metadata-7",
				name: "sync-pointer-7",
				bool: false,
			}),
		);
		await writerStore.docs.put(
			new Document({
				id: "metadata-7",
				name: "sync-pointer-7",
				bool: true,
				tags: Array.from({ length: 128 }, (_, index) => `chunk-${index}`),
			}),
		);

		const remote = await readerStore.docs.index
			.iterate(
				new SearchRequest({
					query: new StringMatch({
						key: "name",
						value: "sync-pointer-7",
					}),
				}),
				{
					local: true,
					remote: {
						timeout: 10_000,
						wait: {
							timeout: 10_000,
							behavior: "block",
						},
					},
				},
			)
			.all();

		expect(remote.map((entry) => entry.id)).to.deep.equal(["metadata-7"]);
		expect(remote[0].bool).to.equal(true);
		expect(remote[0].tags).to.have.length(128);
	});
});
