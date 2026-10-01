import { deserialize, serialize } from "@dao-xyz/borsh";
import { Entry } from "@peerbit/log";
import { TestSession } from "@peerbit/test-utils";
import { expect } from "chai";
import type { DocumentsChange } from "../src/events.js";
import type { Operation } from "../src/operation.js";
import { Documents, type SetupOptions } from "../src/program.js";
import { Document } from "./data.js";

describe("document change listener initialization", () => {
	let session: TestSession;
	const args: SetupOptions<Document> = {
		type: Document,
		replicate: false,
	};

	beforeEach(async () => {
		session = await TestSession.disconnected(2);
	});

	afterEach(async () => {
		await session.stop();
	});

	const cases = [
		{
			name: "constructed before open",
			create: () => new Documents<Document>(),
			beforeOpen: true,
			alias: false,
		},
		{
			name: "cloned before open",
			create: () => new Documents<Document>().clone(),
			beforeOpen: true,
			alias: false,
		},
		{
			name: "deserialized before open through changes",
			create: () =>
				deserialize(serialize(new Documents<Document>()), Documents<Document>),
			beforeOpen: true,
			alias: true,
		},
		{
			name: "cloned after open",
			create: () => new Documents<Document>().clone(),
			beforeOpen: false,
			alias: false,
		},
	];

	for (const test of cases) {
		it(`delivers puts and deletes when ${test.name}`, async () => {
			const docs = test.create();
			const events = test.alias ? docs.changes : docs.events;
			const changes: DocumentsChange<Document, Document>[] = [];
			const listener = (
				event: CustomEvent<DocumentsChange<Document, Document>>,
			) => changes.push(event.detail);
			if (test.beforeOpen) {
				events.addEventListener("change", listener);
			}
			expect(await session.peers[0].open(docs, { args })).to.equal(docs);
			expect(docs.events).to.equal(events);
			expect(docs.changes).to.equal(events);
			if (!test.beforeOpen) {
				events.addEventListener("change", listener);
			}

			const result = await docs.put(new Document({ id: "one", name: "value" }));
			expect(result.entry.hash).to.be.a("string");
			expect(docs.log.log.length).to.equal(1);
			// Assert immediately after the write: a pending query would mask the
			// missing listener by enabling document change materialization itself.
			expect(
				changes.map((change) => change.added.map((doc) => doc.id)),
			).to.deep.equal([["one"]]);
			expect(changes[0].added[0].name).to.equal("value");
			expect(changes[0].removed).to.have.length(0);

			await docs.del("one");
			expect(changes).to.have.length(2);
			expect(changes[1].added).to.have.length(0);
			expect(changes[1].removed.map((doc) => doc.id)).to.deep.equal(["one"]);
		});
	}

	it("keeps tracking idempotent across repeated access, duplicate adds and removal", async () => {
		const docs = new Documents<Document>().clone();
		const events = docs.events;
		const add = events.addEventListener;
		const remove = events.removeEventListener;
		const ids: string[] = [];
		const listener = (
			event: CustomEvent<DocumentsChange<Document, Document>>,
		) => ids.push(...event.detail.added.map((doc) => doc.id));
		events.addEventListener("change", listener);
		events.addEventListener("change", listener);
		await session.peers[0].open(docs, { args });
		expect(docs.events.addEventListener).to.equal(add);
		expect(docs.changes.removeEventListener).to.equal(remove);
		await docs.put(new Document({ id: "first" }));
		expect(ids).to.deep.equal(["first"]);

		events.removeEventListener("change", listener);
		await docs.put(new Document({ id: "removed" }));
		expect(ids).to.deep.equal(["first"]);
		events.addEventListener("change", listener);
		await docs.put(new Document({ id: "re-added" }));
		expect(ids).to.deep.equal(["first", "re-added"]);
	});

	it("preserves once delivery and subsequent registration", async () => {
		const docs = new Documents<Document>().clone();
		const ids: string[] = [];
		const listener = (
			event: CustomEvent<DocumentsChange<Document, Document>>,
		) => ids.push(...event.detail.added.map((doc) => doc.id));
		docs.events.addEventListener("change", listener, { once: true });
		await session.peers[0].open(docs, { args });
		await docs.put(new Document({ id: "first" }));
		await docs.put(new Document({ id: "second" }));
		expect(ids).to.deep.equal(["first"]);

		docs.events.addEventListener("change", listener);
		await docs.put(new Document({ id: "re-added" }));
		expect(ids).to.deep.equal(["first", "re-added"]);
	});

	it("preserves the emitter and listener across same-instance close and reopen", async () => {
		const docs = await session.peers[0].open(
			new Documents<Document>().clone(),
			{ args },
		);
		const events = docs.events;
		const add = events.addEventListener;
		const ids: string[] = [];
		events.addEventListener("change", (event) => {
			ids.push(...event.detail.added.map((doc) => doc.id));
		});
		await docs.put(new Document({ id: "before-close" }));
		expect(ids).to.deep.equal(["before-close"]);
		expect(await docs.close()).to.equal(true);
		expect(await session.peers[0].open(docs, { args })).to.equal(docs);
		expect(docs.events).to.equal(events);
		expect(docs.events.addEventListener).to.equal(add);
		const beforePut = ids.length;
		await docs.put(new Document({ id: "after-reopen" }));
		expect(ids.slice(beforePut)).to.deep.equal(["after-reopen"]);
	});

	it("delivers signed joined entries to a listener registered before open", async () => {
		const source = await session.peers[0].open(new Documents<Document>(), {
			args,
		});
		const target = source.clone();
		const added: Document[] = [];
		target.events.addEventListener("change", (event) => {
			added.push(...event.detail.added);
		});
		expect(await session.peers[1].open(target, { args })).to.equal(target);
		const first = await source.put(new Document({ id: "first", name: "one" }));
		const second = await source.put(
			new Document({ id: "second", name: "two" }),
		);
		// Receive the committed signed images, not the writer's append-result objects.
		const entries = await Promise.all(
			[first.entry, second.entry].map(async ({ hash }) =>
				(
					await Entry.fromMultihash<Operation>(source.log.log.blocks, hash, {
						remote: false,
					})
				).init(source.log.log),
			),
		);
		await target.log.join(entries, {
			verifySignatures: true,
		});
		expect(target.log.log.length).to.equal(2);
		expect(added.map((doc) => [doc.id, doc.name])).to.have.deep.members([
			["first", "one"],
			["second", "two"],
		]);
		await target.log.join(entries, {
			verifySignatures: true,
		});
		expect(added).to.have.length(2);
	});
});
