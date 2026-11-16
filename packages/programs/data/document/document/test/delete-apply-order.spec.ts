import { serialize } from "@dao-xyz/borsh";
import { create as createSimpleIndexer } from "@peerbit/indexer-simple";
import {
	type Entry,
	EntryType,
	LamportClock,
	Timestamp,
	createEntry,
} from "@peerbit/log";
import {
	NativeBackboneMemoryCoordinatePersistenceStore,
	NativeBackboneNodeCoordinatePersistence,
} from "@peerbit/native-backbone";
import { TestSession } from "@peerbit/test-utils";
import { expect } from "chai";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import pDefer from "p-defer";
import { createRustPeerbitOptions } from "peerbit/rust";
import sinon from "sinon";
import {
	type CanPerform,
	DeleteOperation,
	Documents,
	type IdKey,
	type Operation,
	PutOperation,
	policy,
	toId,
	transform,
} from "../src/index.js";
import { Document, TestStore } from "./data.js";

// The compare-and-delete this suite exercises. Reached through a narrow view so
// the suite also compiles, and fails, against an index without it.
type CompareAndDeleteIndex = {
	delIfHead(key: IdKey, head: string): Promise<boolean>;
};
const compareAndDelete = (store: TestStore) =>
	store.docs.index as unknown as CompareAndDeleteIndex;

// A received delete may only remove the version it names, or a version that
// descends from it. These tests hold the receiver at the points where a newer,
// independent version can be indexed between the delete's admission and its
// application.
describe("delete apply order", () => {
	let session: TestSession | undefined;
	let source: TestStore;
	let target: TestStore;

	const openArgs = {
		mode: "compat" as const,
		replicate: false as const,
		keep: "self" as const,
		nativeGraph: false,
		nativeBackbone: false as const,
		nativeRangePlanner: false as const,
	};

	const openPeers = async (
		targetArgs?: Partial<{ canPerform: CanPerform<Document> }>,
		beforeTargetOpen?: () => void,
	) => {
		session = await TestSession.disconnected(2);
		source = new TestStore({ docs: new Documents<Document>() });
		target = source.clone();
		beforeTargetOpen?.();
		await session.peers[0].open(source, { args: openArgs });
		await session.peers[1].open(target, {
			args: { ...openArgs, ...targetArgs },
		});
	};

	// Resolves when a change handled by `target` adds `hash`, without a document
	// change listener (so the store keeps its listener-free change path).
	const resolveWhenHandled = (
		hash: () => string | undefined,
		resolve: () => void,
	) => {
		const handleChanges = target.docs.handleChanges.bind(target.docs);
		target.docs.handleChanges = async (...args) => {
			await handleChanges(...args);
			if (args[0].added.some(({ entry }) => entry.hash === hash())) {
				resolve();
			}
		};
	};

	const getName = async (store: TestStore, id: string) =>
		(await store.docs.index.get(id, { local: true, remote: false }))?.name;

	// On the source: put O, delete it (C, next = [O]), then put an independent
	// replacement R (next = []) under the same id.
	const writeRecreatedDocument = async (id: string) => {
		const original = await source.docs.put(
			new Document({ id, name: "original" }),
			{ target: "none" },
		);
		const cut = await source.docs.del(id, { target: "none" });
		const replacement = await source.docs.put(
			new Document({ id, name: "replacement" }),
			{ target: "none", unique: true, meta: { next: [] } },
		);
		expect(cut.entry.meta.type).equal(EntryType.CUT);
		expect(cut.entry.meta.next).deep.equal([original.entry.hash]);
		expect(replacement.entry.meta.next).deep.equal([]);
		expect(await source.docs.log.log.has(original.entry.hash)).equal(false);
		return { original, cut, replacement };
	};

	// On the source: put T, then update it (U, next = [T]), and sign a CUT for the
	// same id that names T, the ancestor of the indexed head.
	const writeAncestorDelete = async (id: string) => {
		const original = await source.docs.put(
			new Document({ id, name: "original" }),
			{ target: "none" },
		);
		const update = await source.docs.put(new Document({ id, name: "update" }), {
			target: "none",
		});
		expect(update.entry.meta.next).deep.equal([original.entry.hash]);
		const cut = await createEntry<Operation>({
			store: source.docs.log.log.blocks,
			encoding: source.docs.log.log.encoding,
			identity: source.node.identity,
			data: new DeleteOperation({ key: toId(id) }),
			// meta.data carries the replica count the shared log requires.
			meta: {
				type: EntryType.CUT,
				next: [original.entry],
				data: original.entry.meta.data,
			},
		});
		expect(cut.meta.next).deep.equal([original.entry.hash]);
		return { original, update, cut };
	};

	afterEach(async () => {
		sinon.restore();
		await session?.stop();
		session = undefined;
	});

	describe("straddle", () => {
		// The delete C is admitted while the id has no row ("already deleted"), then
		// the replacement R is indexed before C is applied. C names O, which R does
		// not descend from, so R must stay indexed.
		for (const listener of [true, false]) {
			it(`keeps an independent replacement indexed after an older delete is admitted (${listener ? "change listener" : "no change listener"})`, async () => {
				const id = "recreated-document";
				let entries: Awaited<ReturnType<typeof writeRecreatedDocument>>;
				const cutEntered = pDefer<void>();
				const replacementIndexed = pDefer<void>();
				let cutJoin: Promise<void> | undefined;
				const canPerform: CanPerform<Document> = async (properties) => {
					if (properties.type === "delete") {
						cutEntered.resolve();
						await replacementIndexed.promise;
					} else if (properties.value.name === "replacement") {
						cutJoin = target.docs.log.join([entries.cut.entry]);
						await cutEntered.promise;
					}
					return true;
				};
				await openPeers({ canPerform }, () => {
					if (!listener) {
						resolveWhenHandled(
							() => entries?.replacement.entry.hash,
							() => replacementIndexed.resolve(),
						);
					}
				});
				const removed: string[] = [];
				if (listener) {
					target.docs.events.addEventListener("change", (event) => {
						removed.push(...event.detail.removed.map((doc) => doc.name!));
						if (event.detail.added.some((doc) => doc.name === "replacement")) {
							replacementIndexed.resolve();
						}
					});
				}
				entries = await writeRecreatedDocument(id);

				await target.docs.log.join([entries.replacement.entry]);
				expect(
					cutJoin,
					"the replacement's put reached the gate",
				).to.be.instanceOf(Promise);
				await cutJoin;

				expect(await target.docs.log.log.has(entries.cut.entry.hash)).equal(
					true,
				);
				expect(
					await target.docs.log.log.has(entries.replacement.entry.hash),
				).equal(true);
				expect(await getName(target, id)).equal("replacement");
				if (listener) {
					expect(removed).deep.equal([]);
				}
			});
		}
	});

	it("removes a descendant when a delete names its ancestor", async () => {
		await openPeers();
		const id = "ancestor-delete";
		const { original, update, cut } = await writeAncestorDelete(id);
		await target.docs.log.join([original.entry]);
		await target.docs.log.join([update.entry]);
		expect(await getName(target, id)).equal("update");

		await target.docs.log.join([cut]);

		expect(await target.docs.log.log.has(cut.hash)).equal(true);
		expect(await getName(target, id)).equal(undefined);
	});

	describe("head moves before the delete", () => {
		// The delete names T and the indexed head U descends from T, so the delete
		// applies. A newer independent replacement R is indexed after that decision
		// and before the delete runs. The delete must miss R.
		it("keeps a replacement indexed while the delete walks the lineage", async () => {
			const id = "lineage-moved";
			let replacementHash: string | undefined;
			const replacementIndexed = pDefer<void>();
			await openPeers(undefined, () =>
				resolveWhenHandled(
					() => replacementHash,
					() => replacementIndexed.resolve(),
				),
			);
			const { original, update, cut } = await writeAncestorDelete(id);
			const replacement = await source.docs.put(
				new Document({ id, name: "replacement" }),
				{ target: "none", unique: true, meta: { next: [] } },
			);
			replacementHash = replacement.entry.hash;
			await target.docs.log.join([original.entry]);
			await target.docs.log.join([update.entry]);

			const lineageHeld = pDefer<void>();
			const release = pDefer<void>();
			const docs = target.docs as any;
			const lineageReaches = docs.lineageReaches.bind(docs);
			const lineage = sinon
				.stub(docs, "lineageReaches")
				.callsFake(async (...args: unknown[]) => {
					const reaches = await lineageReaches(...args);
					lineageHeld.resolve();
					await release.promise;
					return reaches;
				});
			const delIfHead = sinon.spy(compareAndDelete(target), "delIfHead");

			const cutJoin = target.docs.log.join([cut]);
			await lineageHeld.promise;
			await target.docs.log.join([replacement.entry]);
			await replacementIndexed.promise;
			release.resolve();
			await cutJoin;

			expect(lineage.firstCall.args).deep.equal([
				update.entry.hash,
				original.entry.hash,
			]);
			expect(await lineage.firstCall.returnValue).equal(true);
			expect(delIfHead.callCount).equal(1);
			expect(delIfHead.firstCall.args[1]).equal(update.entry.hash);
			expect(await delIfHead.firstCall.returnValue).equal(false);
			expect(lineage.secondCall.args).deep.equal([
				replacement.entry.hash,
				original.entry.hash,
			]);
			expect(await lineage.secondCall.returnValue).equal(false);
			expect(await target.docs.log.log.has(cut.hash)).equal(true);
			expect(await getName(target, id)).equal("replacement");
		});

		it("keeps a replacement indexed while the removed value is read", async () => {
			const id = "value-read-moved";
			let deleteAdmitted = false;
			await openPeers({
				canPerform: (properties) => {
					if (properties.type === "delete") {
						deleteAdmitted = true;
					}
					return true;
				},
			});
			const removed: string[] = [];
			const replacementIndexed = pDefer<void>();
			target.docs.events.addEventListener("change", (event) => {
				removed.push(...event.detail.removed.map((doc) => doc.name!));
				if (event.detail.added.some((doc) => doc.name === "replacement")) {
					replacementIndexed.resolve();
				}
			});
			const { original, update, cut } = await writeAncestorDelete(id);
			const replacement = await source.docs.put(
				new Document({ id, name: "replacement" }),
				{ target: "none", unique: true, meta: { next: [] } },
			);
			await target.docs.log.join([original.entry]);
			await target.docs.log.join([update.entry]);

			// The first index read after the delete is admitted is the read of the
			// value it removes.
			const valueRead = pDefer<void>();
			const release = pDefer<void>();
			const get = target.docs.index.get.bind(target.docs.index);
			sinon
				.stub(target.docs.index, "get")
				.callsFake(async (...args: Parameters<typeof get>) => {
					if (deleteAdmitted) {
						deleteAdmitted = false;
						valueRead.resolve();
						await release.promise;
					}
					return get(...args);
				});

			const cutJoin = target.docs.log.join([cut]);
			await valueRead.promise;
			await target.docs.log.join([replacement.entry]);
			await replacementIndexed.promise;
			release.resolve();
			await cutJoin;

			expect(await target.docs.log.log.has(cut.hash)).equal(true);
			expect(await getName(target, id)).equal("replacement");
			expect(removed).deep.equal([]);
		});
	});

	describe("native mode with a persisted index", () => {
		let directory: string | undefined;

		afterEach(async () => {
			await session?.stop();
			session = undefined;
			if (directory) {
				await fs.rm(directory, { recursive: true, force: true });
				directory = undefined;
			}
		});

		// A persisted Rust index applies each mutation in a queue, after its snapshot
		// append. A received replacement R that waits in that queue is not indexed
		// yet, so a delete of the older version is admitted and decided on that
		// version. The delete must check the head where it removes the row, after R,
		// and keep R.
		it("keeps a replacement indexed whose put is queued ahead of the delete", async () => {
			directory = await fs.mkdtemp(
				path.join(os.tmpdir(), "peerbit-delete-apply-order-"),
			);
			session = await TestSession.disconnected(1, {
				...createRustPeerbitOptions(),
				directory,
			});
			const peer = session.peers[0];
			const store = await peer.open(
				new TestStore({ docs: new Documents<Document>() }),
				{
					args: {
						mode: "native",
						replicate: false,
						nativeGraph: true,
						nativeBackbone: {
							optional: false,
							documentIndex: true,
							coordinatePersistence:
								new NativeBackboneNodeCoordinatePersistence(
									path.join(directory, "coordinate-wal"),
									{ flushOnAppend: true },
								),
						},
						canPerform: policy.allowAll<Document>(),
						index: {
							type: Document,
							transform: transform.identity<Document>(),
						},
					},
				},
			);
			const engine = store.docs.index.index as any;
			expect(await engine.persisted()).equal(true);

			// meta.data carries the replica count the shared log requires.
			const { entry: template } = await store.docs.put(
				new Document({ id: "template" }),
				{ target: "none" },
			);
			let tick = 0n;
			const write = (
				data: Operation,
				type: EntryType,
				next: Entry<Operation>[],
			) =>
				createEntry<Operation>({
					store: store.docs.log.log.blocks,
					encoding: store.docs.log.log.encoding,
					identity: peer.identity,
					data,
					meta: {
						type,
						next,
						data: template.meta.data,
						clock: new LamportClock({
							id: peer.identity.publicKey.bytes,
							timestamp: new Timestamp({
								wallTime:
									template.meta.clock.timestamp.wallTime + ++tick * 1_000_000n,
							}),
						}),
					},
				});
			const id = "queued-replacement";
			const putOf = (name: string) =>
				write(
					new PutOperation({ data: serialize(new Document({ id, name })) }),
					EntryType.APPEND,
					[],
				);
			const original = await putOf("original");
			await store.docs.log.join([original]);
			expect(await getName(store, id)).equal("original");
			const cut = await write(
				new DeleteOperation({ key: toId(id) }),
				EntryType.CUT,
				[original],
			);
			const replacement = await putOf("replacement");

			// Hold the replacement's put in the index queue, at its snapshot append.
			const appendHeld = pDefer<void>();
			const releaseAppend = pDefer<void>();
			const appendPut = engine.appendPut.bind(engine);
			sinon.stub(engine, "appendPut").callsFake(async (...args: unknown[]) => {
				appendHeld.resolve();
				await releaseAppend.promise;
				return appendPut(...args);
			});
			// Resolves when the delete reaches the index, whichever removal it uses.
			const deleteReachedIndex = pDefer<void>();
			for (const method of [
				"delIdIfContextHead",
				"delIdsNoReturn",
				"delIds",
				"del",
			]) {
				if (typeof engine[method] !== "function") {
					continue;
				}
				const remove = engine[method].bind(engine);
				sinon.stub(engine, method).callsFake((...args: unknown[]) => {
					deleteReachedIndex.resolve();
					return remove(...args);
				});
			}

			const replacementJoin = store.docs.log.join([replacement]);
			await appendHeld.promise;
			expect(
				await getName(store, id),
				"the replacement waits in the index queue",
			).equal("original");
			const cutJoin = store.docs.log.join([cut]);
			await deleteReachedIndex.promise;
			releaseAppend.resolve();
			await replacementJoin;
			await cutJoin;

			expect(await store.docs.log.log.has(cut.hash)).equal(true);
			expect(await store.docs.log.log.has(replacement.hash)).equal(true);
			expect(await getName(store, id)).equal("replacement");
		});
	});

	describe("compare-and-delete", () => {
		const engines = [
			{ name: "sqlite3", options: undefined, args: {} },
			{
				name: "simple",
				options: { indexer: createSimpleIndexer },
				args: {},
			},
			{ name: "rust", options: createRustPeerbitOptions(), args: {} },
			{
				name: "rust with native backbone document index",
				options: createRustPeerbitOptions(),
				args: {
					nativeGraph: true,
					nativeBackbone: {
						optional: false,
						documentIndex: true,
						coordinatePersistence: {
							store: new NativeBackboneMemoryCoordinatePersistenceStore(),
							buffered: true,
							flushOnAppend: false,
						},
					},
				},
			},
		];
		for (const engine of engines) {
			it(`removes a row only while its head is the named head (${engine.name})`, async () => {
				session = await TestSession.disconnected(1, engine.options);
				const store = await session.peers[0].open(
					new TestStore({ docs: new Documents<Document>() }),
					{ args: { replicate: false, ...engine.args } as any },
				);
				const id = "compare-and-delete";
				const first = await store.docs.put(
					new Document({ id, name: "first" }),
					{ target: "none" },
				);
				const second = await store.docs.put(
					new Document({ id, name: "second" }),
					{ target: "none" },
				);
				expect(second.entry.meta.next).deep.equal([first.entry.hash]);

				expect(
					await compareAndDelete(store).delIfHead(toId(id), first.entry.hash),
				).equal(false);
				expect(await getName(store, id)).equal("second");

				expect(
					await compareAndDelete(store).delIfHead(toId(id), second.entry.hash),
				).equal(true);
				expect(await getName(store, id)).equal(undefined);
			});
		}
	});
});
