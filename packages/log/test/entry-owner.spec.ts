import { AnyBlockStore } from "@peerbit/blocks";
import { HashmapIndices } from "@peerbit/indexer-simple";
import { expect } from "chai";
import sinon from "sinon";
import { EntryV0 } from "../src/entry-v0.js";
import { Entry } from "../src/entry.js";
import { Log } from "../src/log.js";
import { signKey } from "./fixtures/privateKey.js";

const deferred = () => {
	let resolve!: () => void;
	const promise = new Promise<void>((done) => (resolve = done));
	return { promise, resolve };
};

// Routing/ownership tests only. The test owner deliberately does not implement
// proof authorization or commits; those belong to the document protocol suite.
describe("private versioned entry owner routing", () => {
	let store: AnyBlockStore;
	let log: Log<Uint8Array>;
	let entry: Entry<Uint8Array>;
	let owner: { append: sinon.SinonStub; join: sinon.SinonStub };
	const open = () =>
		log.open(store, signKey, { indexer: new HashmapIndices() });
	const bind = () => (log as any).bindEntryOwner(owner);
	beforeEach(async () => {
		store = new AnyBlockStore();
		await store.start();
		log = new Log();
		await open();
		entry = await EntryV0.create<Uint8Array>({
			store,
			identity: signKey,
			data: Uint8Array.of(9),
			deferStore: true,
		});
		owner = {
			append: sinon.stub().callsFake(async (_data, _options, onLocalCommit) => {
				onLocalCommit(entry);
				return { entry, removed: [] };
			}),
			join: sinon.stub().resolves(),
		};
	});
	afterEach(async () => {
		sinon.restore();
		await log.close();
		await store.stop();
	});

	it("leaves an ordinary log append unchanged", async () => {
		const result = await log.append(Uint8Array.of(1));
		expect(await log.has(result.entry.hash)).equal(true);
		expect(owner.append.called).equal(false);
	});

	it("routes append and snapshots its owner methods", async () => {
		bind();
		const captured = owner.append;
		owner.append = sinon.stub().rejects(new Error("replaced"));
		const data = Uint8Array.of(1);
		const options = { meta: { next: [] } };
		expect((await log.append(data, options)).entry).equal(entry);
		expect(
			captured.calledOnceWithExactly(data, options, sinon.match.func),
		).equal(true);
		expect(owner.append.called).equal(false);
		expect(log.length).equal(0);
		expect(() => bind()).throw("Cannot bind entry owner");
	});

	it("routes normalized joins before head, CUT, signature and parent planning", async () => {
		bind();
		const next = sinon.stub(entry, "getNext").throws("legacy parent planning");
		const plan = sinon.spy(log.entryIndex, "planJoin");
		const options = {
			verifySignatures: true,
			__peerbitBatchIndependent: true,
			__peerbitCanAppendAlreadyValidated: true,
		};
		await log.join([entry], options);
		expect(owner.join.calledOnce).equal(true);
		expect(owner.join.firstCall.args[0]).deep.equal([entry]);
		expect(owner.join.firstCall.args[1].references.get(entry.hash)).equal(
			entry,
		);
		expect(owner.join.firstCall.args[1].options).equal(options);
		expect(plan.called).equal(false);
		expect(next.called).equal(false);
		expect(log.length).equal(0);
	});

	it("captures owned local commit evidence before notifying change observers", async () => {
		bind();
		const hash = entry.hash;
		const order: string[] = [];
		const evidence = sinon.spy(
			(hashes: string[], entries: Entry<Uint8Array>[]) => {
				expect(hashes).deep.equal([hash]);
				expect(entries[0]!.hash).equal(hash);
				order.push("evidence");
			},
		);
		await log.append(Uint8Array.of(1), {
			__peerbitOnLocalCommit: evidence,
			onChange: async (change: { added: { entry: Entry<Uint8Array> }[] }) => {
				order.push("change");
				change.added[0]!.entry.hash = "observer mutation";
			},
		} as any);
		expect(order).deep.equal(["evidence", "change"]);
		expect(evidence.calledOnce).equal(true);
	});

	it("preserves owned commit evidence when later completion fails", async () => {
		bind();
		const evidence = sinon.spy();
		owner.append.callsFake(async (_data, _options, onLocalCommit) => {
			onLocalCommit(entry);
			throw new Error("projection failed after local commit");
		});
		await expect(
			log.append(Uint8Array.of(1), {
				__peerbitOnLocalCommit: evidence,
			} as any),
		).rejectedWith("projection failed");
		expect(evidence.calledOnceWithExactly([entry.hash], [entry])).true;
	});

	it("passes the configured change observer to an owned join", async () => {
		const change = sinon.stub().resolves();
		(log as any)._onChange = change;
		bind();
		owner.join.callsFake(async (_entries, context) => {
			await context.options.onChange({ added: [], removed: [] });
		});
		await log.join([entry]);
		expect(change.calledOnce).equal(true);
	});

	it("refuses mandatory fast paths, batches and generic history removal before effects", async () => {
		bind();
		const data = Uint8Array.of(1);
		const change = sinon.stub();
		(log as any)._onChange = change;
		for (const operation of [
			() => log.appendMany([data]),
			() => (log as any).appendLocallyPrepared(data),
			() => log.remove(entry),
			() => log.remove(entry, { recursively: true }),
			() => log.deleteRecursively(entry),
			() => log.prepareDeleteRecursively(entry),
			() => log.prepareDelete(entry.hash),
			() => log.delete(entry.hash),
			() => log.trim({ type: "length", to: 0 }),
			() => log.recover(),
			() => log.load({ reset: true, heads: [entry] }),
		]) {
			await expect(operation()).rejectedWith(
				"requires the versioned store entry owner",
			);
		}
		await log.trim(); // Unconfigured membership-triggered trim is harmless.
		expect(change.called).equal(false);
		expect(log.length).equal(0);
	});

	it("declines optional fused paths without calling native preparation", async () => {
		bind();
		const prepare = sinon.stub().throws("must not prepare");
		const internal = log as any;
		const options = {
			__peerbitCanAppendAlreadyValidated: true,
			meta: { next: [] },
		};
		for (const method of [
			"appendLocallyPreparedCommitOnly",
			"appendLocallyPreparedNativeNoNextCommitOnly",
			"appendLocallyPreparedNativeKnownNoNextCommitOnly",
			"appendLocallyPreparedNativeCommitOnly",
			"appendLocallyPreparedManyIndependent",
			"appendLocallyPreparedNativeKnownNoNextCommitOnlyBatch",
		]) {
			expect(
				await internal[method](Uint8Array.of(1), options, {}, prepare),
			).equal(undefined);
		}
		expect(await internal.joinPreparedAppendFactsBatch([], options)).equal(
			false,
		);
		expect(prepare.called).equal(false);
		expect(owner.append.called).equal(false);
	});

	for (const mutation of ["append", "join"] as const) {
		for (const terminal of ["close", "drop"] as const) {
			it(`drains an owned ${mutation} before ${terminal}`, async () => {
				const entered = deferred();
				const release = deferred();
				owner[mutation].callsFake(async () => {
					entered.resolve();
					await release.promise;
					return mutation === "append" ? { entry, removed: [] } : undefined;
				});
				bind();
				const pending =
					mutation === "append"
						? log.append(Uint8Array.of(1))
						: log.join([entry]);
				await entered.promise;
				const stop = sinon.spy((log as any)._indexer, "stop");
				let settled = false;
				const closing = log[terminal]().then(() => {
					settled = true;
				});
				try {
					await Promise.resolve();
					await Promise.resolve();
					expect(settled).equal(false);
					expect(stop.called).equal(false);
				} finally {
					release.resolve();
					await pending.catch(() => undefined);
					await closing;
				}
				expect(settled).equal(true);
				expect(stop.called).equal(true);
			});
		}
	}

	it("keeps direct lower reopen blocked until a new owner is bound", async () => {
		bind();
		await log.close();
		await open();
		await expect(log.append(Uint8Array.of(1))).rejectedWith(
			"must recover the new log generation",
		);
		await expect(log.join([entry])).rejectedWith(
			"must recover the new log generation",
		);
		await expect(log.trim()).rejectedWith(
			"must recover the new log generation",
		);
		expect(owner.append.called).equal(false);
		bind();
		await log.append(Uint8Array.of(1));
		expect(owner.append.calledOnce).equal(true);
	});

	it("rejects a resolved join whose owner generation was replaced during fetch", async () => {
		bind();
		const entered = deferred();
		const release = deferred();
		const bytes = Entry.getPreparedStorageBytes(entry)!;
		const get = sinon.stub(store, "get").callsFake(async () => {
			entered.resolve();
			await release.promise;
			return bytes;
		});
		const joining = log.join([entry.hash]).catch((error) => error);
		await entered.promise;
		await log.close();
		await open();
		bind();
		release.resolve();
		expect(await joining).instanceOf(Error);
		expect(owner.join.called).equal(false);
		get.restore();
	});

	it("refuses a late owner binding once an ordinary join starts resolving", async () => {
		const entered = deferred();
		const release = deferred();
		const bytes = Entry.getPreparedStorageBytes(entry)!;
		const get = sinon.stub(store, "get").callsFake(async () => {
			entered.resolve();
			await release.promise;
			return bytes;
		});
		const joining = log.join([entry.hash]).catch((error) => error);
		await entered.promise;
		expect(() => bind()).throw("Cannot bind entry owner");
		release.resolve();
		expect(await joining).equal(undefined);
		expect(log.length).equal(1);
		expect(owner.join.called).equal(false);
		get.restore();
	});

	it("never retrofits an ordinary runtime object, including after reopen", async () => {
		await log.append(Uint8Array.of(1));
		expect(() => bind()).throw("Cannot bind entry owner");
		await log.close();
		await open();
		expect(() => bind()).throw("Cannot bind entry owner");
	});

	for (const terminal of ["close", "drop"] as const) {
		for (const mutation of ["append", "join"] as const) {
			it(`rejects ${terminal} awaited from an owned ${mutation} callback before teardown`, async () => {
				const callback = async () => {
					await expect(log[terminal]()).rejectedWith("mutation callback");
					expect(log.closed).equal(false);
				};
				owner[mutation].callsFake(async (_data, context) => {
					await (mutation === "append" ? context : context.options).onChange({
						added: [],
						removed: [],
					});
					return mutation === "append" ? { entry, removed: [] } : undefined;
				});
				bind();
				if (mutation === "append")
					await log.append(Uint8Array.of(1), { onChange: callback });
				else await log.join([entry], { onChange: callback });
				expect(log.closed).equal(false);
			});
		}
	}
});
