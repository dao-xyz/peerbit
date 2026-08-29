import { field, variant, vec } from "@dao-xyz/borsh";
import { toId } from "@peerbit/indexer-interface";
import { TestSession } from "@peerbit/test-utils";
import { expect } from "chai";
import sinon from "sinon";
import { Documents } from "../src/program.js";
import type { DocumentTransformFacts } from "../src/transform.js";
import { Document, TestStore } from "./data.js";

@variant("ordered_remote_projection")
class ProjectedDocument {
	@field({ type: "string" })
	id: string;

	@field({ type: "string" })
	name: string;

	@field({ type: vec("string") })
	tags: string[];

	constructor(properties: { id: string; name: string; tags: string[] }) {
		this.id = properties.id;
		this.name = properties.name;
		this.tags = properties.tags;
	}
}

const expectRejected = async (promise: Promise<unknown>) => {
	try {
		await promise;
	} catch (error) {
		return error;
	}
	throw new Error("Expected promise to reject");
};

describe("ordered remote document projection", () => {
	let session: TestSession;

	beforeEach(async () => {
		session = await TestSession.connected(2);
	});

	afterEach(async () => {
		await session.stop();
	});

	it("preserves transform facts, order, events, scalar replacements, and failure prefixes", async () => {
		const source = await session.peers[0].open(
			new TestStore<ProjectedDocument>({
				docs: new Documents<Document, ProjectedDocument>(),
			}),
			{
				args: {
					replicate: false,
					index: {
						type: ProjectedDocument,
						transform: (document: Document) =>
							new ProjectedDocument({
								id: document.id,
								name: document.name ?? "",
								tags: document.tags,
							}),
					},
				},
			},
		);
		const transformOrder: string[] = [];
		const signerFacts: boolean[] = [];
		let transformFailureId: string | undefined;
		const transformFailure = new Error("projection transform failed");
		const target = await session.peers[1].open(source.clone(), {
			args: {
				replicate: false,
				index: {
					type: ProjectedDocument,
					transform: async (
						document: Document,
						_context,
						facts?: DocumentTransformFacts,
					) => {
						transformOrder.push(document.id);
						signerFacts.push(
							facts?.entryPublicKeys?.[0]?.equals(
								source.node.identity.publicKey,
							) === true,
						);
						if (document.id === transformFailureId) {
							throw transformFailure;
						}
						await Promise.resolve();
						return new ProjectedDocument({
							id: document.id,
							name: document.name ?? "",
							tags: document.tags,
						});
					},
				},
			},
		});
		const backend = target.docs.index.index as any;
		expect(backend.constructor.name).to.equal("SQLiteIndex");
		expect(backend.withOrderedWriteSession).to.be.a("function");
		const orderedSpy = sinon.spy(backend, "withOrderedWriteSession");
		const scalarPutSpy = sinon.spy(backend, "put");
		const scalarDeleteSpy = sinon.spy(backend, "del");
		const events: string[][] = [];
		const removedEvents: string[][] = [];
		target.docs.events.addEventListener("change", (event) => {
			events.push(event.detail.added.map((document) => document.id));
			removedEvents.push(event.detail.removed.map((document) => document.id));
		});

		const fresh = [
			new Document({ id: "meta-1", name: "metadata", tags: ["head"] }),
			new Document({ id: "chunk-1", name: "chunk", tags: ["0", "1"] }),
			new Document({ id: "link-1", name: "link", tags: ["meta-1"] }),
		];
		const freshAppend = await source.docs.putMany(fresh, {
			unique: true,
			replicate: false,
			target: "none",
		});
		await (target.docs as any).handleChanges({
			added: freshAppend.entries.map((entry) => ({ head: true, entry })),
			removed: [],
		});

		expect(transformOrder).to.deep.equal(fresh.map((document) => document.id));
		expect(signerFacts).to.deep.equal([true, true, true]);
		expect(events).to.deep.equal([["meta-1", "chunk-1", "link-1"]]);
		expect(await backend.count()).to.equal(3);
		expect(orderedSpy.callCount).to.equal(1);
		expect(scalarPutSpy.callCount).to.equal(0);

		transformOrder.length = 0;
		signerFacts.length = 0;
		events.length = 0;
		orderedSpy.resetHistory();
		scalarPutSpy.resetHistory();
		const mixed = [
			new Document({ id: "mixed-before", tags: ["ordered"] }),
			new Document({
				id: "meta-1",
				name: "metadata-mixed-replacement",
				tags: ["scalar"],
			}),
			new Document({ id: "mixed-after", tags: ["ordered"] }),
		];
		const mixedAppend = await source.docs.putMany(mixed, {
			replicate: false,
			target: "none",
		});
		await (target.docs as any).handleChanges({
			added: mixedAppend.entries.map((entry) => ({ head: true, entry })),
			removed: [],
		});
		expect(transformOrder).to.deep.equal(mixed.map((document) => document.id));
		expect(orderedSpy.callCount).to.equal(1);
		expect(scalarPutSpy.callCount).to.equal(1);
		expect((await backend.get(toId("meta-1")))?.value.name).to.equal(
			"metadata-mixed-replacement",
		);
		expect(await backend.get(toId("mixed-before"))).not.to.equal(undefined);
		expect(await backend.get(toId("mixed-after"))).not.to.equal(undefined);
		expect(events).to.deep.equal([["mixed-before", "meta-1", "mixed-after"]]);

		transformOrder.length = 0;
		signerFacts.length = 0;
		events.length = 0;
		orderedSpy.resetHistory();
		scalarPutSpy.resetHistory();
		const singleton = new Document({
			id: "singleton-fresh",
			name: "singleton",
			tags: ["scalar"],
		});
		const singletonAppend = await source.docs.put(singleton, {
			unique: true,
			replicate: false,
			target: "none",
		});
		await (target.docs as any).handleChanges({
			added: [{ head: true, entry: singletonAppend.entry }],
			removed: [],
		});
		expect(transformOrder).to.deep.equal(["singleton-fresh"]);
		expect(orderedSpy.callCount).to.equal(0);
		expect(scalarPutSpy.callCount).to.equal(1);
		expect(events).to.deep.equal([["singleton-fresh"]]);

		events.length = 0;
		removedEvents.length = 0;
		orderedSpy.resetHistory();
		scalarDeleteSpy.resetHistory();
		await (target.docs as any).handleChanges({
			added: [],
			removed: [singletonAppend.entry],
		});
		expect(orderedSpy.callCount).to.equal(0);
		expect(scalarDeleteSpy.callCount).to.equal(1);
		expect(events).to.deep.equal([[]]);
		expect(removedEvents).to.deep.equal([["singleton-fresh"]]);
		expect(await backend.get(toId("singleton-fresh"))).to.equal(undefined);

		transformOrder.length = 0;
		signerFacts.length = 0;
		events.length = 0;
		removedEvents.length = 0;
		orderedSpy.resetHistory();
		scalarPutSpy.resetHistory();
		const duplicateFirst = await source.docs.put(
			new Document({ id: "duplicate", name: "first", tags: ["first"] }),
			{ unique: true, replicate: false, target: "none" },
		);
		const duplicateSecond = await source.docs.put(
			new Document({ id: "duplicate", name: "second", tags: ["second"] }),
			{ replicate: false, target: "none" },
		);
		await (target.docs as any).handleChanges({
			added: [
				{ head: true, entry: duplicateFirst.entry },
				{ head: true, entry: duplicateSecond.entry },
			],
			removed: [],
		});
		expect(transformOrder).to.deep.equal(["duplicate"]);
		expect(orderedSpy.callCount).to.equal(1);
		expect(scalarPutSpy.callCount).to.equal(0);
		expect((await backend.get(toId("duplicate")))?.value.name).to.equal(
			"first",
		);
		expect(events).to.deep.equal([["duplicate"]]);

		transformOrder.length = 0;
		signerFacts.length = 0;
		events.length = 0;
		orderedSpy.resetHistory();
		scalarPutSpy.resetHistory();
		const replacement = new Document({
			id: "meta-1",
			name: "metadata-updated",
			tags: ["head", "new"],
		});
		const replacementAppend = await source.docs.put(replacement, {
			replicate: false,
			target: "none",
		});
		await (target.docs as any).handleChanges({
			added: [{ head: true, entry: replacementAppend.entry }],
			removed: [],
		});
		expect(orderedSpy.callCount).to.equal(0);
		expect(scalarPutSpy.callCount).to.equal(1);
		expect((await backend.get(toId("meta-1")))?.value.name).to.equal(
			"metadata-updated",
		);
		expect(events).to.deep.equal([["meta-1"]]);

		transformOrder.length = 0;
		signerFacts.length = 0;
		events.length = 0;
		transformFailureId = "transform-fail";
		const transformDocs = [
			new Document({ id: "before-transform-fail", tags: ["kept"] }),
			new Document({ id: "transform-fail", tags: ["rejected"] }),
			new Document({ id: "after-transform-fail", tags: ["never"] }),
		];
		const transformAppend = await source.docs.putMany(transformDocs, {
			unique: true,
			replicate: false,
			target: "none",
		});
		const transformError = await expectRejected(
			(target.docs as any).handleChanges({
				added: transformAppend.entries.map((entry) => ({ head: true, entry })),
				removed: [],
			}),
		);
		expect(transformError).to.equal(transformFailure);
		expect(transformOrder).to.deep.equal([
			"before-transform-fail",
			"transform-fail",
		]);
		expect(await backend.get(toId("before-transform-fail"))).not.to.equal(
			undefined,
		);
		expect(await backend.get(toId("transform-fail"))).to.equal(undefined);
		expect(await backend.get(toId("after-transform-fail"))).to.equal(undefined);
		expect(events).to.deep.equal([]);

		transformFailureId = undefined;
		transformOrder.length = 0;
		signerFacts.length = 0;
		events.length = 0;
		const storageFailure = new Error("injected sqlite row failure");
		const originalPutUnlocked = backend.putUnlocked.bind(backend);
		backend.putUnlocked = async (value: any, options: unknown) => {
			await originalPutUnlocked(value, options);
			if (value.id === "storage-fail") {
				throw storageFailure;
			}
		};
		try {
			const storageDocs = [
				new Document({ id: "before-storage-fail", tags: ["kept"] }),
				new Document({ id: "storage-fail", tags: ["rolled", "back"] }),
				new Document({ id: "after-storage-fail", tags: ["never"] }),
			];
			const storageAppend = await source.docs.putMany(storageDocs, {
				unique: true,
				replicate: false,
				target: "none",
			});
			const storageError = await expectRejected(
				(target.docs as any).handleChanges({
					added: storageAppend.entries.map((entry) => ({ head: true, entry })),
					removed: [],
				}),
			);
			expect(storageError).to.equal(storageFailure);
			expect(transformOrder).to.deep.equal([
				"before-storage-fail",
				"storage-fail",
			]);
			expect(await backend.get(toId("before-storage-fail"))).not.to.equal(
				undefined,
			);
			expect(await backend.get(toId("storage-fail"))).to.equal(undefined);
			expect(await backend.get(toId("after-storage-fail"))).to.equal(undefined);
			expect(events).to.deep.equal([]);
		} finally {
			backend.putUnlocked = originalPutUnlocked;
			orderedSpy.restore();
			scalarPutSpy.restore();
			scalarDeleteSpy.restore();
		}
	});
});
