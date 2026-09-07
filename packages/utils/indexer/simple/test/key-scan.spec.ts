import { field } from "@dao-xyz/borsh";
import {
	type IdPrimitive,
	type IndexEngineInitProperties,
	NotStartedError,
	StringKey,
	StringMatch,
	Uint8ArrayKey,
	id,
	toId,
} from "@peerbit/indexer-interface";
import { expect } from "chai";
import sinon from "sinon";
import { HashmapIndex, create } from "../src/index.js";

class ScanDocument {
	@id({ type: "string" })
	id: string;

	@field({ type: "string" })
	tag: string;

	nested?: object;

	constructor(id: string, tag = "original") {
		this.id = id;
		this.tag = tag;
	}
}

const gate = () => {
	let resolve!: () => void;
	const promise = new Promise<void>((settle) => {
		resolve = settle;
	});
	return { promise, resolve };
};

describe("raw key inventory", () => {
	const opened: HashmapIndex<ScanDocument, object>[] = [];
	const open = async (
		properties: Partial<IndexEngineInitProperties<ScanDocument, object>> = {},
	) => {
		const index = new HashmapIndex<ScanDocument, object>();
		index.init({ schema: ScanDocument, ...properties });
		await index.start();
		opened.push(index);
		return index;
	};
	const remove = (index: HashmapIndex<ScanDocument, object>, key: string) =>
		index.del({ query: new StringMatch({ key: "id", value: key }) });

	afterEach(() => {
		for (const index of opened.splice(0)) index.drop();
		sinon.restore();
	});

	for (const pageSize of [1, 2, 8]) {
		for (const count of [0, pageSize, pageSize + 1]) {
			it(`returns ${count} keys with page size ${pageSize} and no repeated final page`, async () => {
				const index = await open();
				const keys = Array.from({ length: count }, (_, i) => `key-${i}`);
				index.putBatch(keys.map((key) => new ScanDocument(key)));
				const scan = index.scanKeyPrimitives({ pageSize });
				expect("all" in scan).to.be.false;
				expect("pending" in scan).to.be.false;
				const received: IdPrimitive[] = [];
				const pages = Math.max(1, Math.ceil(count / pageSize));
				for (let i = 0; i < pages; i++) {
					const page = await scan.next();
					expect(page).to.deep.equal({
						status: i === pages - 1 ? "complete" : "more",
						keys: keys.slice(i * pageSize, (i + 1) * pageSize),
					});
					received.push(...page.keys);
				}
				expect(received).to.deep.equal(keys);
				await scan.close();
				index.put(new ScanDocument("later"));
				expect(await scan.next()).to.deep.equal({
					status: "complete",
					keys: [],
				});
			});
		}
	}

	it("rejects non-positive, fractional, non-finite, and oversized page bounds", async () => {
		const index = await open();
		for (const pageSize of [0, -1, 0.5, NaN, Infinity, -Infinity, 4097]) {
			expect(() => index.scanKeyPrimitives({ pageSize })).to.throw(RangeError);
		}
		expect(
			await index.scanKeyPrimitives({ pageSize: 4096 }).next(),
		).to.deep.equal({
			status: "complete",
			keys: [],
		});
		await index.stop();
		expect(() => index.scanKeyPrimitives({ pageSize: 1 })).to.throw(
			NotStartedError,
		);
	});

	it("captures membership at creation and snapshots the fixed page bound", async () => {
		const index = await open();
		index.putBatch([new ScanDocument("a"), new ScanDocument("b")]);
		const old = index.scanKeyPrimitives({ pageSize: 1 });
		index.put(new ScanDocument("c"));
		expect(await old.next()).to.deep.equal({ status: "invalidated", keys: [] });
		const options = { pageSize: 1 };
		const current = index.scanKeyPrimitives(options);
		options.pageSize = 4096;
		expect(await current.next()).to.deep.equal({ status: "more", keys: ["a"] });
		await current.close();
	});

	for (const mutation of [
		"put",
		"replace",
		"empty batch",
		"absent delete",
		"delete/reinsert",
	]) {
		it(`invalidates a partially consumed scan after ${mutation}`, async () => {
			const index = await open();
			index.putBatch([new ScanDocument("a"), new ScanDocument("b")]);
			const scan = index.scanKeyPrimitives({ pageSize: 1 });
			expect(await scan.next()).to.deep.equal({ status: "more", keys: ["a"] });
			if (mutation === "put") index.put(new ScanDocument("c"));
			else if (mutation === "replace")
				index.put(new ScanDocument("b", "replacement"));
			else if (mutation === "empty batch") index.putBatch([]);
			else if (mutation === "absent delete") await remove(index, "absent");
			else {
				await remove(index, "b");
				index.put(new ScanDocument("b"));
				expect(index.getSize()).to.equal(2);
			}
			expect(await scan.next()).to.deep.equal({
				status: "invalidated",
				keys: [],
			});
			await scan.close();
			await index.stop();
			await index.start();
			expect(await scan.next()).to.deep.equal({
				status: "invalidated",
				keys: [],
			});
		});
	}

	it("retains a successful batch prefix while invalidating on a later key failure", async () => {
		const index = await open();
		index.put(new ScanDocument("a"));
		const scan = index.scanKeyPrimitives({ pageSize: 1 });
		const sentinel = new Error("key extraction failed");
		const invalid = new ScanDocument("invalid");
		Object.defineProperty(invalid, "id", {
			get: () => {
				throw sentinel;
			},
		});
		expect(() => index.putBatch([new ScanDocument("b"), invalid])).to.throw(
			sentinel,
		);
		expect(await scan.next()).to.deep.equal({
			status: "invalidated",
			keys: [],
		});
		expect(await index.scanKeyPrimitives({ pageSize: 8 }).next()).to.deep.equal(
			{
				status: "complete",
				keys: ["a", "b"],
			},
		);
	});

	for (const transition of ["none", "stop/start", "init"]) {
		for (const fails of [false, true]) {
			it(`reserves an async delete across ${transition} until ${fails ? "failure" : "success"}`, async () => {
				const entered = gate();
				const release = gate();
				const nested = {};
				const sentinel = new Error("nested delete failed");
				const index = await open({
					nested: {
						match: (value): value is object => value === nested,
						iterate: async () => {
							entered.resolve();
							await release.promise;
							if (fails) throw sentinel;
							return [{ tag: "match" }];
						},
					},
				});
				const document = new ScanDocument("a");
				document.nested = nested;
				index.put(document);
				const before = index.scanKeyPrimitives({ pageSize: 1 });
				const deletion = index.del({
					query: new StringMatch({ key: ["nested", "tag"], value: "match" }),
				});
				// Observe rejection immediately; the gate is always released in finally.
				const outcome = deletion.then(
					(value) => ({ value, error: undefined }),
					(error: unknown) => ({ value: undefined, error }),
				);
				try {
					// No await of the predicate: admission already invalidates both scans.
					const during = index.scanKeyPrimitives({ pageSize: 1 });
					expect(await before.next()).to.deep.equal({
						status: "invalidated",
						keys: [],
					});
					expect(await during.next()).to.deep.equal({
						status: "invalidated",
						keys: [],
					});
					await entered.promise;
					if (transition === "stop/start") {
						await index.stop();
						await index.start();
					} else if (transition === "init") {
						index.init({ schema: ScanDocument });
						index.put(new ScanDocument("a", "replacement map"));
					}
					const blocked = index.scanKeyPrimitives({ pageSize: 1 });
					const unreadWhileBusy = index.scanKeyPrimitives({ pageSize: 1 });
					expect(await blocked.next()).to.deep.equal({
						status: "invalidated",
						keys: [],
					});
					release.resolve();
					const result = await outcome;
					expect(await unreadWhileBusy.next()).to.deep.equal({
						status: "invalidated",
						keys: [],
					});
					expect(result.error).to.equal(fails ? sentinel : undefined);
					if (!fails)
						expect(result.value?.map((key) => key.primitive)).to.deep.equal([
							"a",
						]);
					expect(await blocked.next()).to.deep.equal({
						status: "invalidated",
						keys: [],
					});
					expect(
						await index.scanKeyPrimitives({ pageSize: 1 }).next(),
					).to.deep.equal({
						status: "complete",
						keys: fails ? ["a"] : [],
					});
				} finally {
					release.resolve();
					await outcome;
				}
			});
		}
	}

	it("keeps idempotent start and cached scope init in the same generation", async () => {
		const indices = create();
		try {
			await indices.start();
			const index = await indices.init({ schema: ScanDocument });
			index.putBatch([new ScanDocument("a"), new ScanDocument("b")]);
			const scan = index.scanKeyPrimitives({ pageSize: 1 });
			expect(await scan.next()).to.deep.equal({ status: "more", keys: ["a"] });
			await index.start();
			await indices.start();
			expect(await indices.init({ schema: ScanDocument })).to.equal(index);
			expect(await scan.next()).to.deep.equal({
				status: "complete",
				keys: ["b"],
			});
		} finally {
			await indices.drop();
		}
	});

	for (const transition of ["stop", "stop/start", "drop", "init"]) {
		it(`does not certify an old scan after ${transition}`, async () => {
			const index = await open();
			index.put(new ScanDocument("a"));
			const scan = index.scanKeyPrimitives({ pageSize: 1 });
			if (transition === "init") index.init({ schema: ScanDocument });
			else if (transition === "drop") index.drop();
			else {
				await index.stop();
				if (transition === "stop/start") await index.start();
			}
			const status =
				transition === "stop" || transition === "drop"
					? "closed"
					: "invalidated";
			expect(await scan.next()).to.deep.equal({ status, keys: [] });
			await index.start();
			expect(await scan.next()).to.deep.equal({ status, keys: [] });
		});
	}

	it("returns canonical primitive keys independently of mutable ID and value aliases", async () => {
		const index = await open();
		const stringKey = new StringKey("stored-string");
		const bytes = new Uint8Array([3, 4, 5]);
		const byteKey = new Uint8ArrayKey(bytes);
		const storedBytes = byteKey.primitive;
		const document = new ScanDocument("value-id");
		index.put(document, stringKey);
		index.put(new ScanDocument("byte-value"), byteKey);
		index.put(new ScanDocument("number-value"), toId(7));
		index.put(new ScanDocument("bigint-value"), toId(9n));
		const scan = index.scanKeyPrimitives({ pageSize: 8 });
		stringKey.key = "changed-string";
		bytes[0] = 255;
		byteKey.key = new Uint8Array([9]);
		document.id = "changed-value";
		const stored = index.iterator().next().value![1];
		stored.id = new StringKey("changed-wrapper");
		stored.value = new ScanDocument("another-value");
		// These are this backend's equality primitives, not portable typed IDs.
		expect(await scan.next()).to.deep.equal({
			status: "complete",
			keys: ["stored-string", storedBytes, 7, 9n],
		});
	});

	it("does not read document values, ID getters, predicates, or query iterators", async () => {
		const sentinel = new Error("raw inventory must not read values");
		const index = await open({
			nested: {
				match: (_value): _value is object => {
					throw sentinel;
				},
				iterate: async () => {
					throw sentinel;
				},
			},
		});
		for (const key of ["a", "b"]) {
			index.put(new ScanDocument(key));
		}
		for (const [, stored] of index.iterator()) {
			Object.defineProperty(stored, "id", {
				get: () => {
					throw sentinel;
				},
			});
			Object.defineProperty(stored, "value", {
				get: () => {
					throw sentinel;
				},
			});
		}
		sinon.stub(index, "iterate").throws(sentinel);
		sinon.stub(index, "get").throws(sentinel);
		sinon.stub(index, "count").throws(sentinel);
		const scan = index.scanKeyPrimitives({ pageSize: 1 });
		expect(await scan.next()).to.deep.equal({ status: "more", keys: ["a"] });
		expect(await scan.next()).to.deep.equal({
			status: "complete",
			keys: ["b"],
		});
	});

	for (const terminal of [
		"complete",
		"closed",
		"aborted",
		"invalidated",
	] as const) {
		it(`detaches the abort listener and keeps ${terminal} terminal`, async () => {
			const index = await open();
			index.put(new ScanDocument("a"));
			const controller = new AbortController();
			const add = sinon.spy(controller.signal, "addEventListener");
			const remove = sinon.spy(controller.signal, "removeEventListener");
			const scan = index.scanKeyPrimitives({
				pageSize: 1,
				signal: controller.signal,
			});
			expect(add.calledOnce).to.be.true;
			if (terminal === "closed") await scan.close();
			else if (terminal === "aborted") controller.abort();
			else if (terminal === "invalidated") index.put(new ScanDocument("b"));
			expect(await scan.next()).to.deep.equal({
				status: terminal,
				keys: terminal === "complete" ? ["a"] : [],
			});
			expect(remove.calledOnce).to.be.true;
			expect(remove.firstCall.args[0]).to.equal("abort");
			expect(remove.firstCall.args[1]).to.equal(add.firstCall.args[1]);
			controller.abort();
			await scan.close();
			expect(await scan.next()).to.deep.equal({ status: terminal, keys: [] });
			expect(remove.calledOnce).to.be.true;
		});
	}

	it("returns an already-aborted scan without attaching a listener", async () => {
		const index = await open();
		index.put(new ScanDocument("a"));
		const controller = new AbortController();
		controller.abort();
		const add = sinon.spy(controller.signal, "addEventListener");
		const scan = index.scanKeyPrimitives({
			pageSize: 1,
			signal: controller.signal,
		});
		expect(add.called).to.be.false;
		expect(await scan.next()).to.deep.equal({ status: "aborted", keys: [] });
		await scan.close();
		expect(await scan.next()).to.deep.equal({ status: "aborted", keys: [] });
	});

	for (const readFirst of [false, true]) {
		it(`observes cancellation with suppressed abort delivery ${readFirst ? "between pages" : "before reading"}`, async () => {
			const index = await open();
			index.putBatch([new ScanDocument("a"), new ScanDocument("b")]);
			const controller = new AbortController();
			controller.signal.addEventListener(
				"abort",
				(event) => event.stopImmediatePropagation(),
				{ once: true },
			);
			const scan = index.scanKeyPrimitives({
				pageSize: 1,
				signal: controller.signal,
			});
			if (readFirst)
				expect(await scan.next()).to.deep.equal({
					status: "more",
					keys: ["a"],
				});
			controller.abort();
			expect(await scan.next()).to.deep.equal({ status: "aborted", keys: [] });
			await scan.close();
			expect(await scan.next()).to.deep.equal({ status: "aborted", keys: [] });
		});
	}

	it("advances only the requested page without eager collection or a lookahead", async () => {
		const index = await open();
		index.putBatch(
			Array.from({ length: 32 }, (_, i) => new ScanDocument(String(i))),
		);
		const backing = (index as unknown as { _index: Map<IdPrimitive, unknown> })
			._index;
		const cursor = backing.keys();
		const next = sinon.spy(cursor, "next");
		sinon.stub(backing, "keys").returns(cursor);
		const scan = index.scanKeyPrimitives({ pageSize: 2 });
		expect(next.callCount).to.equal(0);
		expect(await scan.next()).to.deep.equal({
			status: "more",
			keys: ["0", "1"],
		});
		expect(next.callCount).to.equal(2);
		await scan.close();
		expect(await scan.next()).to.deep.equal({ status: "closed", keys: [] });
		expect(next.callCount).to.equal(2);
	});

	it("rethrows a cursor failure unchanged, discards its partial page, and releases its listener", async () => {
		const index = await open();
		index.putBatch([new ScanDocument("a"), new ScanDocument("b")]);
		const controller = new AbortController();
		const remove = sinon.spy(controller.signal, "removeEventListener");
		const sentinel = new Error("cursor failed on second key");
		// Narrow fault injection: no public API makes a native Map cursor throw.
		const backing = (index as unknown as { _index: Map<IdPrimitive, unknown> })
			._index;
		const cursor = backing.keys();
		const next = cursor.next.bind(cursor);
		let reads = 0;
		sinon.stub(cursor, "next").callsFake(() => {
			if (++reads === 2) throw sentinel;
			return next();
		});
		sinon.stub(backing, "keys").returns(cursor);
		const scan = index.scanKeyPrimitives({
			pageSize: 2,
			signal: controller.signal,
		});
		let failure: unknown;
		try {
			await scan.next();
		} catch (error) {
			failure = error;
		}
		expect(failure).to.equal(sentinel);
		expect(reads).to.equal(2);
		expect(remove.calledOnce).to.be.true;
		controller.abort();
		await scan.close();
		expect(await scan.next()).to.deep.equal({ status: "failed", keys: [] });
		expect(reads).to.equal(2);
		expect(remove.calledOnce).to.be.true;
	});
});
