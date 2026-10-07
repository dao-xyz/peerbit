import { field, serialize, vec } from "@dao-xyz/borsh";
import {
	type IdPrimitive,
	type Index,
	type IndexKeyScanOptions,
	NotStartedError,
	StringMatch,
	id,
	toId,
} from "@peerbit/indexer-interface";
import { expect } from "chai";
import sinon from "sinon";
import { RustIndex } from "../src/index.js";

class ScanDocument {
	@id({ type: "string" })
	id: string;

	@field({ type: "string" })
	tag: string;

	constructor(id: string, tag = "original") {
		this.id = id;
		this.tag = tag;
	}
}

class ScanCoordinate {
	@id({ type: "string" }) hash!: string;
	@field({ type: "u64" }) hashNumber!: bigint;
	@field({ type: "string" }) gid!: string;
	@field({ type: vec("u64") }) coordinates!: bigint[];
	@field({ type: "u64" }) wallTime!: bigint;
	@field({ type: "bool" }) assignedToRangeBoundary!: boolean;
	@field({ type: Uint8Array }) _meta!: Uint8Array;
}

const scan = <T extends Record<string, any>>(
	index: RustIndex<T>,
	options: IndexKeyScanOptions,
) => {
	const capability = (index as Index<T>).scanKeyPrimitives;
	expect(capability, "native owner has bounded key inventory").to.be.a(
		"function",
	);
	return capability!.call(index, options);
};

describe("raw key inventory", () => {
	const opened: RustIndex<any>[] = [];
	const open = async (directory?: string) => {
		const index = new RustIndex<ScanDocument>(directory);
		await index.init({ schema: ScanDocument });
		index.start();
		opened.push(index);
		return index;
	};

	afterEach(async () => {
		sinon.restore();
		for (const index of opened.splice(0)) await index.drop();
	});

	for (const pageSize of [1, 2, 8]) {
		for (const count of [0, pageSize, pageSize + 1]) {
			it(`returns ${count} keys with page size ${pageSize} without repeating the last page`, async () => {
				const index = await open();
				const keys = Array.from({ length: count }, (_, i) => `key-${i}`);
				await index.putBatch(keys.map((key) => new ScanDocument(key)));
				const cursor = scan(index, { pageSize });
				expect("all" in cursor).to.be.false;
				expect("pending" in cursor).to.be.false;
				const received: IdPrimitive[] = [];
				const pages = Math.max(1, Math.ceil(count / pageSize));
				for (let i = 0; i < pages; i++) {
					const page = await cursor.next();
					expect(page).to.deep.equal({
						status: i === pages - 1 ? "complete" : "more",
						keys: keys.slice(i * pageSize, (i + 1) * pageSize),
					});
					received.push(...page.keys);
				}
				expect(received).to.deep.equal(keys);
				await cursor.close();
				await index.put(new ScanDocument("later"));
				expect(await cursor.next()).to.deep.equal({
					status: "complete",
					keys: [],
				});
			});
		}
	}

	it("validates bounds and snapshots the fixed page size", async () => {
		const index = await open();
		for (const pageSize of [0, -1, 0.5, NaN, Infinity, -Infinity, 4097]) {
			expect(() => scan(index, { pageSize })).to.throw(RangeError);
		}
		expect(await scan(index, { pageSize: 4096 }).next()).to.deep.equal({
			status: "complete",
			keys: [],
		});
		await index.putBatch([new ScanDocument("a"), new ScanDocument("b")]);
		const options = { pageSize: 1 };
		const cursor = scan(index, options);
		options.pageSize = 4096;
		expect(await cursor.next()).to.deep.equal({ status: "more", keys: ["a"] });
		await index.stop();
		expect(() => scan(index, { pageSize: 1 })).to.throw(NotStartedError);
		expect(await cursor.next()).to.deep.equal({ status: "closed", keys: [] });
	});

	for (const mutation of [
		"put",
		"replace",
		"delete",
		"empty batch",
		"absent delete",
		"encoded put",
		"encoded batch",
	]) {
		it(`invalidates existing scans on ${mutation}`, async () => {
			const index = await open();
			await index.putBatch([new ScanDocument("a"), new ScanDocument("b")]);
			const cursor = scan(index, { pageSize: 1 });
			expect(await cursor.next()).to.deep.equal({
				status: "more",
				keys: ["a"],
			});
			if (mutation === "put") await index.put(new ScanDocument("c"));
			else if (mutation === "replace")
				await index.put(new ScanDocument("b", "changed"));
			else if (mutation === "empty batch") await index.putBatch([]);
			else if (mutation === "delete") await index.delIds(["b"]);
			else if (mutation === "absent delete")
				await index.del({
					query: new StringMatch({ key: "id", value: "absent" }),
				});
			else {
				const encodedValueParts = {
					prefix: serialize(new ScanDocument("b", "encoded")),
					suffix: new Uint8Array(),
				};
				if (mutation === "encoded put")
					await index.putStoredContextualEncodedValue(
						toId("b"),
						encodedValueParts,
					);
				else
					await index.putStoredContextualEncodedValueBatch([
						{ id: toId("b"), encodedValueParts },
					]);
			}
			expect(await cursor.next()).to.deep.equal({
				status: "invalidated",
				keys: [],
			});
			await cursor.close();
			expect(await cursor.next()).to.deep.equal({
				status: "invalidated",
				keys: [],
			});
		});
	}

	for (const transition of ["stop/start", "reinitialize", "drop/start"]) {
		it(`invalidates old storage after ${transition}`, async () => {
			const index = await open();
			await index.putBatch([new ScanDocument("a"), new ScanDocument("b")]);
			const cursor = scan(index, { pageSize: 1 });
			await cursor.next();
			if (transition === "stop/start") {
				await index.stop();
				index.start();
			} else if (transition === "reinitialize")
				await index.init({ schema: ScanDocument });
			else {
				await index.drop();
				index.start();
			}
			expect(await cursor.next()).to.deep.equal({
				status: "invalidated",
				keys: [],
			});
		});
	}

	it("aborts and closes without changing sticky terminal results", async () => {
		const index = await open();
		await index.putBatch([new ScanDocument("a"), new ScanDocument("b")]);
		const controller = new AbortController();
		const cursor = scan(index, { pageSize: 1, signal: controller.signal });
		await cursor.next();
		controller.abort();
		await cursor.close();
		expect(await cursor.next()).to.deep.equal({ status: "aborted", keys: [] });
		const closed = scan(index, { pageSize: 1 });
		await closed.close();
		expect(await closed.next()).to.deep.equal({ status: "closed", keys: [] });
		expect(
			await scan(index, { pageSize: 1, signal: controller.signal }).next(),
		).to.deep.equal({ status: "aborted", keys: [] });
	});

	it("pages canonical keys directly without document hydration or a growing prefix walk", async () => {
		const index = await open();
		const ids = [
			toId("a:colon"),
			toId(123),
			toId(18446744073709551615n),
			toId(new Uint8Array([1, 2, 3])),
		];
		for (const id of ids) await index.put(new ScanDocument("ignored"), id);
		const internal = index as any;
		const page = sinon.spy(internal.native, "key_page");
		for (const method of ["entries", "query", "query_page", "get"]) {
			sinon
				.stub(internal.native, method)
				.throws(new Error("document access forbidden"));
		}
		sinon
			.stub(internal, "decodeNativeStoredValue")
			.throws(new Error("decode forbidden"));
		sinon.stub(index, "iterate").throws(new Error("query fallback forbidden"));
		const cursor = scan(index, { pageSize: 2 });
		expect(await cursor.next()).to.deep.equal({
			status: "more",
			keys: ids.slice(0, 2).map((id) => id.primitive),
		});
		expect(await cursor.next()).to.deep.equal({
			status: "complete",
			keys: ids.slice(2).map((id) => id.primitive),
		});
		expect(page.args).to.deep.equal([
			[0, 2],
			[2, 2],
		]);
	});

	for (const shortcut of [
		"value",
		"ids",
		"hashes",
		"no-return",
		"encoded",
		"value-batch",
		"ids-batch",
		"hashes-batch",
		"batch-no-return",
	]) {
		it(`fences the coordinate ${shortcut} shortcut without decoding coordinate values`, async () => {
			const index = new RustIndex<ScanCoordinate>();
			await index.init({ schema: ScanCoordinate });
			index.start();
			opened.push(index);
			const before = scan(index, { pageSize: 1 });
			const fields = {
				hash: "h",
				hashNumber: 1n,
				gid: "g",
				coordinates: [2n],
				wallTime: 3n,
				assignedToRangeBoundary: false,
				metaBytes: new Uint8Array([4]),
			};
			const value = Object.assign(new ScanCoordinate(), fields, {
				_meta: fields.metaBytes,
			});
			if (shortcut === "value")
				await index.putSharedLogCoordinateAndDeleteIds(value, fields);
			else if (shortcut === "ids")
				await index.putSharedLogCoordinateFieldsAndDeleteIds(fields);
			else if (shortcut === "hashes")
				await index.putSharedLogCoordinateFieldsAndDeleteHashes(fields);
			else if (shortcut === "no-return")
				await index.putSharedLogCoordinateFieldsAndDeleteHashesNoReturn(fields);
			else if (shortcut === "encoded")
				await index.putSharedLogCoordinateFieldsEncodedAndDeleteHashesNoReturn(
					fields,
				);
			else if (shortcut === "value-batch")
				await index.putSharedLogCoordinatesAndDeleteIdsBatch([
					{ value, fields },
				]);
			else if (shortcut === "ids-batch")
				await index.putSharedLogCoordinateFieldsAndDeleteIdsBatch([{ fields }]);
			else if (shortcut === "hashes-batch")
				await index.putSharedLogCoordinateFieldsAndDeleteHashesBatch([
					{ fields },
				]);
			else
				await index.putSharedLogCoordinateFieldsAndDeleteHashesBatchNoReturn([
					{ fields },
				]);
			expect(await before.next()).to.deep.equal({
				status: "invalidated",
				keys: [],
			});
			sinon
				.stub(index as any, "decodeNativeStoredValue")
				.throws(new Error("coordinate decode forbidden"));
			expect(await scan(index, { pageSize: 1 }).next()).to.deep.equal({
				status: "complete",
				keys: ["h"],
			});
		});
	}

	it("keeps an older admitted batch busy across native owner replacement", async () => {
		const index = await open();
		const original = index.putWithContext.bind(index);
		let release!: () => void;
		const gate = new Promise<void>((resolve) => {
			release = resolve;
		});
		sinon.stub(index, "putWithContext").callsFake(async (...args) => {
			await gate;
			return original(...args);
		});
		const work = index
			.putWithContextBatch([
				{
					value: new ScanDocument("b"),
					id: toId("b"),
					context: {},
					options: { replace: true },
				},
			])
			.then(
				() => undefined,
				(error: unknown) => error,
			);
		try {
			await index.init({ schema: ScanDocument });
			const blocked = scan(index, { pageSize: 1 });
			const unread = scan(index, { pageSize: 1 });
			expect(await blocked.next()).to.deep.equal({
				status: "invalidated",
				keys: [],
			});
			release();
			expect(await work).to.equal(undefined);
			expect(await unread.next()).to.deep.equal({
				status: "invalidated",
				keys: [],
			});
			expect(await scan(index, { pageSize: 1 }).next()).to.deep.equal({
				status: "complete",
				keys: ["b"],
			});
		} finally {
			release();
			await work;
		}
	});

	it("reports a missing native ABI as unsupported rather than falling back", async () => {
		const index = await open();
		const native = (index as any).native;
		const original = native.key_page;
		try {
			native.key_page = undefined;
			expect((index as Index<ScanDocument>).scanKeyPrimitives).to.equal(
				undefined,
			);
		} finally {
			native.key_page = original;
		}
	});

	it("observes suppressed abort delivery and detaches its listener", async () => {
		const index = await open();
		await index.putBatch([new ScanDocument("a"), new ScanDocument("b")]);
		const controller = new AbortController();
		controller.signal.addEventListener("abort", (event) =>
			event.stopImmediatePropagation(),
		);
		const remove = sinon.spy(controller.signal, "removeEventListener");
		const cursor = scan(index, { pageSize: 1, signal: controller.signal });
		await cursor.next();
		controller.abort();
		expect(await cursor.next()).to.deep.equal({ status: "aborted", keys: [] });
		expect(remove.calledOnce).to.equal(true);
		await cursor.close();
		expect(remove.calledOnce).to.equal(true);
	});

	it("reserves before getters, preserves synchronous errors and releases the reservation", async () => {
		const index = await open();
		await index.put(new ScanDocument("a"));
		const before = scan(index, { pageSize: 1 });
		const sentinel = new Error("key getter failed");
		const invalid = new ScanDocument("invalid");
		let nested: ReturnType<typeof scan> | undefined;
		Object.defineProperty(invalid, "id", {
			get: () => {
				nested = scan(index, { pageSize: 1 });
				expect(nested.next()).to.deep.equal({
					status: "invalidated",
					keys: [],
				});
				throw sentinel;
			},
		});
		expect(() => index.put(invalid)).to.throw(sentinel);
		expect(await before.next()).to.deep.equal({
			status: "invalidated",
			keys: [],
		});
		expect(await nested!.next()).to.deep.equal({
			status: "invalidated",
			keys: [],
		});
		expect(await scan(index, { pageSize: 8 }).next()).to.deep.equal({
			status: "complete",
			keys: ["a"],
		});
		expect(index.put(new ScanDocument("b"))).to.equal(undefined);
	});

	it("invalidates on a failed contextual batch after its successful prefix", async () => {
		const index = await open();
		await index.put(new ScanDocument("a"));
		const before = scan(index, { pageSize: 1 });
		const sentinel = new Error("later batch encoding failed");
		const internal = index as any;
		const original = internal.putWithEncodedValue.bind(index);
		sinon
			.stub(internal, "putWithEncodedValue")
			.callsFake((...args: unknown[]) => {
				if ((args[1] as { primitive: string }).primitive === "c")
					throw sentinel;
				return original(...args);
			});
		let failure: unknown;
		try {
			await index.putWithContextBatch(
				["b", "c"].map((id) => ({
					value: new ScanDocument(id),
					id: toId(id),
					context: {},
					options: {
						replace: true,
						encodedValue: serialize(new ScanDocument(id)),
					},
				})),
			);
		} catch (error) {
			failure = error;
		}
		expect(failure).to.equal(sentinel);
		expect(await before.next()).to.deep.equal({
			status: "invalidated",
			keys: [],
		});
		expect(await scan(index, { pageSize: 8 }).next()).to.deep.equal({
			status: "complete",
			keys: ["a", "b"],
		});
	});

	for (const fail of [false, true]) {
		it(`fences an admitted queued persisted write until ${fail ? "rejection" : "success"}`, async () => {
			const directory = `key-inventory-rust-${Date.now()}-${Math.random().toString(16).slice(2)}`;
			const index = await open(directory);
			await index.put(new ScanDocument("a"));
			const internal = index as any;
			const snapshot = internal.snapshotFile;
			const original = snapshot.appendPut.bind(snapshot);
			let enter!: () => void;
			const entered = new Promise<void>((resolve) => {
				enter = resolve;
			});
			let release!: () => void;
			const gate = new Promise<void>((resolve) => {
				release = resolve;
			});
			const sentinel = new Error("journal rejected");
			const stub = sinon
				.stub(snapshot, "appendPut")
				.callsFake(async (...args: unknown[]) => {
					enter();
					await gate;
					if (fail) throw sentinel;
					return original(...args);
				});
			const before = scan(index, { pageSize: 1 });
			const work = Promise.resolve(index.put(new ScanDocument("b"))).then(
				() => undefined,
				(error: unknown) => error,
			);
			try {
				const during = scan(index, { pageSize: 1 });
				expect(await before.next()).to.deep.equal({
					status: "invalidated",
					keys: [],
				});
				expect(await during.next()).to.deep.equal({
					status: "invalidated",
					keys: [],
				});
				await entered;
				const unread = scan(index, { pageSize: 1 });
				release();
				expect(await work).to.equal(fail ? sentinel : undefined);
				expect(await unread.next()).to.deep.equal({
					status: "invalidated",
					keys: [],
				});
				expect(await scan(index, { pageSize: 8 }).next()).to.deep.equal({
					status: "complete",
					keys: fail ? ["a"] : ["a", "b"],
				});
			} finally {
				release();
				await work;
				stub.restore();
				await index.drop();
				if ((globalThis as any).process?.versions?.node) {
					const module = "node:fs/promises";
					const { rm } = (await import(
						module
					)) as typeof import("node:fs/promises");
					await rm(directory, { recursive: true, force: true });
				}
			}
		});
	}

	it("does not expose a stale mirror after primary backbone attachment, detach or reopen", async () => {
		const index = await open();
		await index.put(new ScanDocument("a"));
		const extracted = (index as Index<ScanDocument>).scanKeyPrimitives!;
		const before = scan(index, { pageSize: 1 });
		const backbone = {
			documentIndexLength: 0,
			configureDocumentSchemaIr: () => ({
				rootFields: 0,
				nodeCount: 0,
				genericNodes: 0,
			}),
			clearDocumentIndex: () => {},
			putDocumentEncodedPartsStored: () => {},
			documentEntry: () => undefined,
			documentQuery: () => [],
			documentQueryPage: () => [],
			documentCount: () => 0,
			documentSum: () => ["none", "0"] as ["none", string],
			deleteDocument: () => false,
		};
		expect(index.attachNativeBackboneDocumentIndex(backbone)).to.equal(true);
		expect((index as Index<ScanDocument>).scanKeyPrimitives).to.equal(
			undefined,
		);
		expect(await before.next()).to.deep.equal({
			status: "invalidated",
			keys: [],
		});
		expect(await extracted.call(index, { pageSize: 1 }).next()).to.deep.equal({
			status: "invalidated",
			keys: [],
		});
		index.attachNativeBackboneDocumentIndex(undefined);
		expect((index as Index<ScanDocument>).scanKeyPrimitives).to.equal(
			undefined,
		);
		await index.stop();
		index.start();
		expect((index as Index<ScanDocument>).scanKeyPrimitives).to.equal(
			undefined,
		);
		await index.init({ schema: ScanDocument });
		expect(await scan(index, { pageSize: 1 }).next()).to.deep.equal({
			status: "complete",
			keys: [],
		});
	});

	it("does not restore mirror authority when reinitialized with a primary backbone still attached", async () => {
		const index = await open();
		const extracted = (index as Index<ScanDocument>).scanKeyPrimitives!;
		const values = new Map<string, Uint8Array>();
		const backbone = {
			get documentIndexLength() {
				return values.size;
			},
			configureDocumentSchemaIr: () => ({
				rootFields: 0,
				nodeCount: 0,
				genericNodes: 0,
			}),
			clearDocumentIndex: () => values.clear(),
			putDocumentEncodedPartsStored: (
				key: string,
				prefix: Uint8Array,
				suffix: Uint8Array,
			) => {
				values.set(key, new Uint8Array([...prefix, ...suffix]));
			},
			documentEntry: (key: string): [string, Uint8Array] | undefined => {
				const bytes = values.get(key);
				return bytes ? [key, bytes] : undefined;
			},
			documentQuery: () => [],
			documentQueryPage: () => [],
			documentCount: () => values.size,
			documentSum: () => ["none", "0"] as ["none", string],
			deleteDocument: (key: string) => values.delete(key),
		};
		expect(index.attachNativeBackboneDocumentIndex(backbone)).to.equal(true);
		await index.init({ schema: ScanDocument });
		const document = new ScanDocument("backbone-only");
		await index.put(document);
		expect(values.has("string:backbone-only")).to.equal(true);
		expect(index.get(toId(document.id))?.value).to.deep.equal(document);
		expect(
			(index as any).native.len(),
			"native mirror did not receive the write",
		).to.equal(0);
		index.attachNativeBackboneDocumentIndex(undefined);
		expect((index as Index<ScanDocument>).scanKeyPrimitives).to.equal(
			undefined,
		);
		expect(await extracted.call(index, { pageSize: 1 }).next()).to.deep.equal({
			status: "invalidated",
			keys: [],
		});
		await index.stop();
		index.start();
		expect((index as Index<ScanDocument>).scanKeyPrimitives).to.equal(
			undefined,
		);
		await index.init({ schema: ScanDocument });
		expect(await scan(index, { pageSize: 1 }).next()).to.deep.equal({
			status: "complete",
			keys: [],
		});
	});

	for (const interference of ["abort", "close", "mutation", "throw"]) {
		it(`discards an interrupted native page on ${interference}`, async () => {
			const index = await open();
			await index.putBatch([new ScanDocument("a"), new ScanDocument("b")]);
			const controller = new AbortController();
			const cursor = scan(index, { pageSize: 1, signal: controller.signal });
			const native = (index as any).native;
			const original = native.key_page.bind(native);
			const sentinel = new Error("native key page failed");
			sinon.stub(native, "key_page").callsFake((...args: unknown[]) => {
				const keys = original(...args);
				if (interference === "abort") controller.abort();
				else if (interference === "close") cursor.close();
				else if (interference === "mutation") index.put(new ScanDocument("c"));
				else throw sentinel;
				return keys;
			});
			const status =
				interference === "abort"
					? "aborted"
					: interference === "close"
						? "closed"
						: interference === "mutation"
							? "invalidated"
							: "failed";
			if (interference === "throw")
				expect(() => cursor.next()).to.throw(sentinel);
			else expect(await cursor.next()).to.deep.equal({ status, keys: [] });
			expect(await cursor.next()).to.deep.equal({ status, keys: [] });
		});
	}
});
