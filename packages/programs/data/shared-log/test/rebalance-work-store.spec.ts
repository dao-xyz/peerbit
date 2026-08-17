import { sha256Sync, toHexString } from "@peerbit/crypto";
import { expect } from "chai";
import deterministicStringify from "json-stringify-deterministic";
import { MAX_U64 } from "../src/integers.js";
import { type RebalanceScanPlan, ReplicationIntent } from "../src/ranges.js";
import {
	DEFAULT_REBALANCE_WORK_LIMITS,
	REBALANCE_WORK_FILES,
	type RebalanceWorkPersistence,
	RebalanceWorkStore,
} from "../src/rebalance-work-store.js";

type BarrierGate = {
	entered: Promise<void>;
	release: () => void;
};

class MemoryPersistence implements RebalanceWorkPersistence {
	readonly writes: string[] = [];
	readonly barriers: string[] = [];
	readonly flushes: string[] = [];
	closeCalls = 0;
	private writeFailureArmed = false;
	private writeFailure: unknown;
	private barrierFailureArmed = false;
	private barrierFailure: unknown;
	private closeFailureArmed = false;
	private closeFailure: unknown;
	private readGate?: {
		name: string;
		entered: () => void;
		wait: Promise<void>;
	};
	private readonly readFailures = new Map<string, unknown>();
	private barrierGate?: {
		entered: () => void;
		wait: Promise<void>;
	};

	constructor(readonly files = new Map<string, Uint8Array>()) {}

	async read(name: string, maxBytes: number) {
		const gate = this.readGate;
		if (gate?.name === name) {
			this.readGate = undefined;
			gate.entered();
			await gate.wait;
		}
		if (this.readFailures.has(name)) {
			const error = this.readFailures.get(name);
			this.readFailures.delete(name);
			throw error;
		}
		const value = this.files.get(name);
		if (value && value.byteLength > maxBytes) {
			throw new Error("Rebalance work persistence read exceeds byte bound");
		}
		return value ? new Uint8Array(value) : undefined;
	}

	async write(name: string, bytes: Uint8Array) {
		if (this.writeFailureArmed) {
			this.writeFailureArmed = false;
			throw this.writeFailure;
		}
		this.writes.push(name);
		this.files.set(name, new Uint8Array(bytes));
	}

	async durableBarrier(name?: string) {
		this.barriers.push(name!);
		const gate = this.barrierGate;
		if (gate) {
			this.barrierGate = undefined;
			gate.entered();
			await gate.wait;
		}
		if (this.barrierFailureArmed) {
			this.barrierFailureArmed = false;
			throw this.barrierFailure;
		}
	}

	async flush(name?: string) {
		this.flushes.push(name!);
	}

	async close() {
		this.closeCalls++;
		if (this.closeFailureArmed) {
			this.closeFailureArmed = false;
			throw this.closeFailure;
		}
	}

	blockNextBarrier(): BarrierGate {
		let markEntered!: () => void;
		let release!: () => void;
		const entered = new Promise<void>((resolve) => {
			markEntered = resolve;
		});
		const wait = new Promise<void>((resolve) => {
			release = resolve;
		});
		this.barrierGate = { entered: markEntered, wait };
		return { entered, release };
	}

	blockNextRead(name: string): BarrierGate {
		let markEntered!: () => void;
		let release!: () => void;
		const entered = new Promise<void>((resolve) => {
			markEntered = resolve;
		});
		const wait = new Promise<void>((resolve) => {
			release = resolve;
		});
		this.readGate = { name, entered: markEntered, wait };
		return { entered, release };
	}

	failNextReadWith(name: string, error: unknown) {
		this.readFailures.set(name, error);
	}

	failNextBarrierWith(error: unknown) {
		this.barrierFailureArmed = true;
		this.barrierFailure = error;
	}

	failNextWriteWith(error: unknown) {
		this.writeFailureArmed = true;
		this.writeFailure = error;
	}

	failNextCloseWith(error: unknown) {
		this.closeFailureArmed = true;
		this.closeFailure = error;
	}

	fork() {
		return new MemoryPersistence(this.files);
	}
}

const viewId = (character: string) => character.repeat(64);
const textEncoder = new TextEncoder();

const readFrame = (bytes: Uint8Array) => {
	const outer = JSON.parse(new TextDecoder().decode(bytes));
	return { outer, payload: JSON.parse(outer.payload) };
};

const encodeFramePayload = (payload: unknown) => {
	const canonicalPayload = deterministicStringify(payload);
	return textEncoder.encode(
		JSON.stringify({
			payload: canonicalPayload,
			checksum: toHexString(sha256Sync(textEncoder.encode(canonicalPayload))),
		}),
	);
};

const makePlanU32 = (offset = 10): RebalanceScanPlan<"u32"> => ({
	boundary: false,
	geometryRanges: [
		{
			start1: offset,
			end1: offset + 10,
			start2: offset,
			end2: offset + 10,
			mode: ReplicationIntent.Strict,
		},
	],
	ownedIntervals: [
		{ start: BigInt(offset), end: BigInt(offset + 10), geometryTask: 0 },
	],
	taskCount: 1,
	historyMutations: [{ rangeHash: `range-${offset}`, present: true }],
});

const makePlanU64 = (offset = 10n): RebalanceScanPlan<"u64"> => ({
	boundary: true,
	geometryRanges: [
		{
			start1: offset,
			end1: offset + 10n,
			start2: offset,
			end2: offset + 10n,
			mode: ReplicationIntent.Strict,
		},
	],
	ownedIntervals: [{ start: offset, end: offset + 10n, geometryTask: 0 }],
	taskCount: 2,
	historyMutations: [{ rangeHash: `range-${offset}`, present: false }],
});

const openStrict = (persistence: MemoryPersistence, limits?: object) =>
	RebalanceWorkStore.open({
		persistence,
		durability: "strict",
		limits,
	});

describe("rebalance work store", () => {
	it("materializes a cleared baseline before the first active generation", async () => {
		const persistence = new MemoryPersistence();
		const store = await openStrict(persistence);
		expect(store.snapshot()).to.deep.equal({
			revision: 0n,
			active: undefined,
		});
		expect(store.currentDurableCommit()).to.equal(undefined);

		const installed = await store.install(0n, {
			resolution: "u32",
			viewId: viewId("a"),
			plan: makePlanU32(),
		});

		expect(persistence.writes).to.deep.equal([
			REBALANCE_WORK_FILES[1],
			REBALANCE_WORK_FILES[0],
		]);
		expect(persistence.barriers).to.deep.equal(persistence.writes);
		expect([...persistence.files.keys()].sort()).to.deep.equal(
			[...REBALANCE_WORK_FILES].sort(),
		);
		expect(installed.snapshot.revision).to.equal(2n);
		expect(installed.snapshot.active?.installSequence).to.equal(2n);
		expect(installed.snapshot.active?.cursor).to.deep.equal({
			taskOrdinal: 0,
			afterHashNumber: undefined,
			bucket: undefined,
		});
		expect(installed.durableCommit?.revision).to.equal(2n);
		expect(installed.durableCommit?.fence).to.deep.equal({
			viewId: viewId("a"),
			planDigest: installed.snapshot.active?.planDigest,
			installSequence: 2n,
		});

		for (const bytes of persistence.files.values()) {
			const outer = JSON.parse(new TextDecoder().decode(bytes));
			expect(outer.payload).to.equal(
				deterministicStringify(JSON.parse(outer.payload)),
			);
		}

		const beforeClose = installed.snapshot;
		await store.close();
		const barriersBeforeReopen = persistence.barriers.length;
		const reopened = await openStrict(persistence);
		expect(reopened.snapshot()).to.deep.equal(beforeClose);
		expect(reopened.currentDurableCommit()).to.deep.include({ revision: 2n });
		expect(persistence.barriers).to.have.length(barriersBeforeReopen + 1);
		expect(persistence.barriers.at(-1)).to.equal(REBALANCE_WORK_FILES[0]);
		await reopened.close();
	});

	it("round-trips checkpoints and a durable cleared tombstone", async () => {
		const persistence = new MemoryPersistence();
		const store = await openStrict(persistence);
		const installed = await store.install(0n, {
			resolution: "u64",
			viewId: viewId("b"),
			plan: makePlanU64(),
		});
		const fence = installed.durableCommit!.fence!;
		const frozen = await store.checkpoint(fence, 2n, {
			taskOrdinal: 0,
			bucket: {
				hashNumber: 16n,
				hashes: ["hash-a", "hash-b"],
				nextIndex: 0,
			},
		});
		const checkpoint = await store.checkpoint(fence, 3n, {
			taskOrdinal: 0,
			bucket: {
				hashNumber: 16n,
				hashes: ["hash-a", "hash-b"],
				nextIndex: 1,
			},
		});
		expect(frozen.snapshot.revision).to.equal(3n);
		expect(checkpoint.snapshot.revision).to.equal(4n);
		expect(checkpoint.snapshot.active?.cursor).to.deep.equal({
			taskOrdinal: 0,
			afterHashNumber: undefined,
			bucket: {
				hashNumber: 16n,
				hashes: ["hash-a", "hash-b"],
				nextIndex: 1,
			},
		});

		await store.close();
		const reopened = await openStrict(persistence);
		expect(reopened.snapshot()).to.deep.equal(checkpoint.snapshot);
		await expect(reopened.clear(fence, 4n)).to.be.rejectedWith(
			"Cannot clear incomplete rebalance work",
		);
		const completedBucket = await reopened.checkpoint(fence, 4n, {
			taskOrdinal: 0,
			bucket: {
				hashNumber: 16n,
				hashes: ["hash-a", "hash-b"],
				nextIndex: 2,
			},
		});
		const advanced = await reopened.checkpoint(fence, 5n, {
			taskOrdinal: 0,
			afterHashNumber: 16n,
		});
		const nextTask = await reopened.checkpoint(fence, 6n, {
			taskOrdinal: 1,
		});
		const completed = await reopened.checkpoint(fence, 7n, {
			taskOrdinal: 2,
		});
		expect(completedBucket.snapshot.revision).to.equal(5n);
		expect(advanced.snapshot.active?.cursor.afterHashNumber).to.equal(16n);
		expect(nextTask.snapshot.active?.cursor.taskOrdinal).to.equal(1);
		expect(completed.snapshot.active?.cursor.taskOrdinal).to.equal(2);
		const cleared = await reopened.clear(fence, 8n);
		expect(cleared.snapshot).to.deep.equal({
			revision: 9n,
			active: undefined,
		});
		expect(cleared.durableCommit?.fence).to.equal(undefined);

		await reopened.close();
		const afterClear = await openStrict(persistence);
		expect(afterClear.snapshot()).to.deep.equal(cleared.snapshot);
		await afterClear.close();
	});

	it("falls back from a torn newest slot but fails with no valid generation", async () => {
		const persistence = new MemoryPersistence();
		const store = await openStrict(persistence);
		const installed = await store.install(0n, {
			resolution: "u32",
			viewId: viewId("c"),
			plan: makePlanU32(),
		});
		const fence = installed.durableCommit!.fence!;
		await store.checkpoint(fence, 2n, {
			taskOrdinal: 0,
			bucket: {
				hashNumber: 12,
				hashes: ["hash-12"],
				nextIndex: 0,
			},
		});
		await store.close();
		persistence.files.set(
			REBALANCE_WORK_FILES[1],
			new TextEncoder().encode("{torn"),
		);

		const recovered = await openStrict(persistence);
		expect(recovered.snapshot()).to.deep.equal(installed.snapshot);
		await recovered.close();

		persistence.files.delete(REBALANCE_WORK_FILES[1]);
		persistence.files.set(
			REBALANCE_WORK_FILES[0],
			new TextEncoder().encode("{also-torn"),
		);
		await expect(openStrict(persistence)).to.be.rejectedWith(
			"No valid rebalance work generation remains",
		);
	});

	it("rejects conflicting valid frames at the same sequence", async () => {
		const first = new MemoryPersistence();
		const firstStore = await openStrict(first);
		await firstStore.install(0n, {
			resolution: "u32",
			viewId: viewId("d"),
			plan: makePlanU32(10),
		});
		const second = new MemoryPersistence();
		const secondStore = await openStrict(second);
		await secondStore.install(0n, {
			resolution: "u32",
			viewId: viewId("e"),
			plan: makePlanU32(20),
		});
		await firstStore.close();
		await secondStore.close();
		first.files.set(
			REBALANCE_WORK_FILES[1],
			new Uint8Array(second.files.get(REBALANCE_WORK_FILES[0])!),
		);

		await expect(openStrict(first)).to.be.rejectedWith(
			"Conflicting rebalance work generations",
		);
	});

	it("rejects null aliases for absent stored cursor fields", async () => {
		for (const field of ["afterHashNumber", "bucket"] as const) {
			const persistence = new MemoryPersistence();
			const store = await openStrict(persistence);
			await store.install(0n, {
				resolution: "u32",
				viewId: viewId(field === "bucket" ? "b" : "a"),
				plan: makePlanU32(),
			});
			await store.close();
			const { payload } = readFrame(
				persistence.files.get(REBALANCE_WORK_FILES[0])!,
			);
			payload.value.cursor[field] = null;
			persistence.files.set(
				REBALANCE_WORK_FILES[0],
				encodeFramePayload(payload),
			);
			persistence.files.delete(REBALANCE_WORK_FILES[1]);
			await expect(openStrict(persistence)).to.be.rejectedWith(
				"No valid rebalance work generation remains",
			);
		}
	});

	it("requires persistence to bound a raw frame before returning it", async () => {
		const persistence = new MemoryPersistence();
		persistence.files.set(REBALANCE_WORK_FILES[0], new Uint8Array(257));
		let failure: unknown;
		try {
			await openStrict(persistence, { maxFrameBytes: 256 });
		} catch (error) {
			failure = error;
		}
		expect(failure).to.be.instanceOf(AggregateError);
		expect((failure as AggregateError).message).to.equal(
			"No valid rebalance work generation remains",
		);
		expect(
			(failure as AggregateError).errors.some(
				(error) =>
					error instanceof Error &&
					error.message.includes("read exceeds byte bound"),
			),
		).to.be.true;
		expect(persistence.closeCalls).to.equal(1);
	});

	it("recovers a valid slot when the other bounded read rejects", async () => {
		const persistence = new MemoryPersistence();
		const store = await openStrict(persistence, { maxFrameBytes: 8192 });
		const installed = await store.install(0n, {
			resolution: "u32",
			viewId: viewId("e"),
			plan: makePlanU32(),
		});
		await store.close();
		persistence.files.set(REBALANCE_WORK_FILES[1], new Uint8Array(8193));

		const recovered = await openStrict(persistence, { maxFrameBytes: 8192 });
		expect(recovered.snapshot()).to.deep.equal(installed.snapshot);
		await recovered.close();
	});

	it("waits for both slot reads before failed-open cleanup", async () => {
		const persistence = new MemoryPersistence();
		const delayed = persistence.blockNextRead(REBALANCE_WORK_FILES[0]);
		persistence.failNextReadWith(
			REBALANCE_WORK_FILES[1],
			new Error("planned read failure"),
		);
		const opening = openStrict(persistence);
		await delayed.entered;
		await Promise.resolve();
		expect(persistence.closeCalls).to.equal(0);
		delayed.release();
		await expect(opening).to.be.rejectedWith(
			"No valid rebalance work generation remains",
		);
		expect(persistence.closeCalls).to.equal(1);
	});

	it("poisons after a barrier failure without publishing the revision", async () => {
		const persistence = new MemoryPersistence();
		const store = await openStrict(persistence);
		const installed = await store.install(0n, {
			resolution: "u32",
			viewId: viewId("f"),
			plan: makePlanU32(),
		});
		const fence = installed.durableCommit!.fence!;
		await store.checkpoint(fence, 2n, {
			taskOrdinal: 0,
			bucket: {
				hashNumber: 11,
				hashes: ["hash-11"],
				nextIndex: 0,
			},
		});
		persistence.failNextBarrierWith(new Error("barrier failed"));
		await expect(
			store.checkpoint(fence, 3n, {
				taskOrdinal: 0,
				bucket: {
					hashNumber: 11,
					hashes: ["hash-11"],
					nextIndex: 1,
				},
			}),
		).to.be.rejectedWith("Failed to persist rebalance work frame");
		expect(() => store.snapshot()).to.throw("poisoned");
		const writes = persistence.writes.length;
		await expect(
			store.checkpoint(fence, 3n, {
				taskOrdinal: 0,
			}),
		).to.be.rejectedWith("poisoned");
		expect(persistence.writes).to.have.length(writes);

		await expect(store.close()).to.be.rejectedWith("poisoned");
		const reopened = await openStrict(persistence.fork());
		expect(reopened.snapshot().revision).to.equal(4n);
		expect(reopened.snapshot().active?.cursor.bucket?.nextIndex).to.equal(1);
		expect(reopened.currentDurableCommit()?.revision).to.equal(4n);
		await reopened.close();
	});

	it("requires a fresh recovery barrier and poisons on falsey failures", async () => {
		const persistence = new MemoryPersistence();
		const store = await openStrict(persistence);
		await store.install(0n, {
			resolution: "u32",
			viewId: viewId("0"),
			plan: makePlanU32(),
		});
		await store.close();

		const failedRecovery = persistence.fork();
		failedRecovery.failNextBarrierWith(undefined);
		await expect(openStrict(failedRecovery)).to.be.rejectedWith(
			"Failed to confirm recovered rebalance work frame",
		);
		expect(failedRecovery.closeCalls).to.equal(1);
		const recoveredPersistence = persistence.fork();
		const recovered = await openStrict(recoveredPersistence);
		expect(recovered.currentDurableCommit()?.revision).to.equal(2n);

		const fence = recovered.currentDurableCommit()!.fence!;
		recoveredPersistence.failNextBarrierWith(null);
		await expect(
			recovered.checkpoint(fence, 2n, {
				taskOrdinal: 0,
				bucket: {
					hashNumber: 11,
					hashes: ["hash-11"],
					nextIndex: 0,
				},
			}),
		).to.be.rejectedWith("Failed to persist rebalance work frame");
		expect(() => recovered.snapshot()).to.throw("poisoned");
		await expect(recovered.close()).to.be.rejectedWith("poisoned");
	});

	it("rejects stale revisions and fences before backend writes", async () => {
		const persistence = new MemoryPersistence();
		const store = await openStrict(persistence);
		const installed = await store.install(0n, {
			resolution: "u32",
			viewId: viewId("1"),
			plan: makePlanU32(),
		});
		const writes = persistence.writes.length;
		await expect(
			store.checkpoint(installed.durableCommit!.fence!, 1n, {
				taskOrdinal: 0,
			}),
		).to.be.rejectedWith("Stale rebalance work revision");
		await expect(
			store.checkpoint(
				{ ...installed.durableCommit!.fence!, viewId: viewId("2") },
				2n,
				{ taskOrdinal: 0 },
			),
		).to.be.rejectedWith("Stale rebalance work fence");
		expect(persistence.writes).to.have.length(writes);
	});

	it("keeps physical storage bounded across 10,000 checkpoints", async () => {
		const persistence = new MemoryPersistence();
		const store = await openStrict(persistence);
		let result = await store.install(0n, {
			resolution: "u32",
			viewId: viewId("3"),
			plan: makePlanU32(),
		});
		const fence = result.durableCommit!.fence!;
		for (let index = 0; index < 10_000; index++) {
			result = await store.checkpoint(fence, result.snapshot.revision, {
				taskOrdinal: 0,
			});
		}

		expect([...persistence.files.keys()].sort()).to.deep.equal(
			[...REBALANCE_WORK_FILES].sort(),
		);
		expect(result.snapshot.revision).to.equal(10_002n);
		expect(persistence.barriers).to.have.length(10_002);
		expect(
			[...persistence.files.values()].reduce(
				(total, value) => total + value.byteLength,
				0,
			),
		).to.be.at.most(2 * DEFAULT_REBALANCE_WORK_LIMITS.maxFrameBytes);
	});

	it("validates collision buckets and both numeric domains", async () => {
		for (const resolution of ["u32", "u64"] as const) {
			const persistence = new MemoryPersistence();
			const store = await openStrict(persistence, { maxCollisionBucket: 2 });
			const installed = await store.install(0n, {
				resolution,
				viewId: viewId(resolution === "u32" ? "4" : "5"),
				plan: (resolution === "u32" ? makePlanU32() : makePlanU64()) as any,
			});
			const fence = installed.durableCommit!.fence!;
			const bucketNumber = resolution === "u32" ? 11 : 11n;
			const writesBeforeOverflow = persistence.writes.length;
			const sparseHashes = new Array<string>(1);
			await expect(
				store.checkpoint(fence, 2n, {
					taskOrdinal: 0,
					bucket: {
						hashNumber: bucketNumber as any,
						hashes: sparseHashes,
						nextIndex: 0,
					},
				}),
			).to.be.rejectedWith("Invalid rebalance collision bucket hash 0");
			await expect(
				store.checkpoint(fence, 2n, {
					taskOrdinal: 0,
					bucket: {
						hashNumber: bucketNumber as any,
						hashes: ["a", "b", "c"],
						nextIndex: 0,
					},
				}),
			).to.be.rejectedWith("exceeds configured bounds");
			expect(persistence.writes).to.have.length(writesBeforeOverflow);
			expect(store.snapshot().active?.cursor.bucket).to.equal(undefined);
			const valid = await store.checkpoint(fence, 2n, {
				taskOrdinal: 0,
				bucket: {
					hashNumber: bucketNumber as any,
					hashes: ["a", "b"],
					nextIndex: 0,
				},
			});
			expect(valid.snapshot.active?.cursor.bucket?.hashes).to.deep.equal([
				"a",
				"b",
			]);

			for (const hashes of [
				["a", "a"],
				["b", "a"],
				["a", "b", "c"],
			]) {
				await expect(
					store.checkpoint(fence, 3n, {
						taskOrdinal: 0,
						bucket: {
							hashNumber: bucketNumber as any,
							hashes,
							nextIndex: 0,
						},
					}),
				).to.be.rejected;
			}
			await expect(
				store.checkpoint(fence, 3n, {
					taskOrdinal: 0,
					afterHashNumber: (resolution === "u32" ? -1 : MAX_U64 + 1n) as any,
				}),
			).to.be.rejectedWith("Invalid rebalance cursor hash number");
		}
	});

	it("permits only monotone collision-bucket cursor transitions", async () => {
		const persistence = new MemoryPersistence();
		const store = await openStrict(persistence);
		const installed = await store.install(0n, {
			resolution: "u64",
			viewId: viewId("9"),
			plan: makePlanU64(),
		});
		const fence = installed.durableCommit!.fence!;

		await expect(
			store.checkpoint(fence, 2n, {
				taskOrdinal: 0,
				afterHashNumber: 10n,
			}),
		).to.be.rejectedWith("only through a frozen bucket");
		await expect(
			store.checkpoint(fence, 2n, {
				taskOrdinal: 0,
				bucket: {
					hashNumber: 11n,
					hashes: ["a", "b"],
					nextIndex: 1,
				},
			}),
		).to.be.rejectedWith("must start at zero");

		await store.checkpoint(fence, 2n, {
			taskOrdinal: 0,
			bucket: {
				hashNumber: 11n,
				hashes: ["a", "b"],
				nextIndex: 0,
			},
		});
		await expect(
			store.checkpoint(fence, 3n, {
				taskOrdinal: 0,
				bucket: {
					hashNumber: 12n,
					hashes: ["a", "b"],
					nextIndex: 0,
				},
			}),
		).to.be.rejectedWith("cannot replace or rewind");
		await store.checkpoint(fence, 3n, {
			taskOrdinal: 0,
			bucket: {
				hashNumber: 11n,
				hashes: ["a", "b"],
				nextIndex: 1,
			},
		});
		await expect(
			store.checkpoint(fence, 4n, {
				taskOrdinal: 0,
				bucket: {
					hashNumber: 11n,
					hashes: ["a", "b"],
					nextIndex: 0,
				},
			}),
		).to.be.rejectedWith("cannot replace or rewind");
		await expect(
			store.checkpoint(fence, 4n, {
				taskOrdinal: 0,
				afterHashNumber: 11n,
			}),
		).to.be.rejectedWith("only after its bucket is complete");
		await expect(
			store.checkpoint(fence, 4n, { taskOrdinal: 1 }),
		).to.be.rejectedWith("task transition is not monotone");

		await store.checkpoint(fence, 4n, {
			taskOrdinal: 0,
			bucket: {
				hashNumber: 11n,
				hashes: ["a", "b"],
				nextIndex: 2,
			},
		});
		await store.checkpoint(fence, 5n, {
			taskOrdinal: 0,
			afterHashNumber: 11n,
		});
		await expect(
			store.checkpoint(fence, 6n, { taskOrdinal: 2 }),
		).to.be.rejectedWith("task transition is not monotone");
		await store.checkpoint(fence, 6n, { taskOrdinal: 1 });
		await expect(
			store.checkpoint(fence, 7n, { taskOrdinal: 0 }),
		).to.be.rejectedWith("task transition is not monotone");
		const completed = await store.checkpoint(fence, 7n, { taskOrdinal: 2 });
		expect(completed.snapshot.active?.cursor).to.deep.equal({
			taskOrdinal: 2,
			afterHashNumber: undefined,
			bucket: undefined,
		});
	});

	it("advances past an empty exact collision bucket", async () => {
		const persistence = new MemoryPersistence();
		const store = await openStrict(persistence);
		const installed = await store.install(0n, {
			resolution: "u32",
			viewId: viewId("a"),
			plan: makePlanU32(),
		});
		const fence = installed.durableCommit!.fence!;
		const frozen = await store.checkpoint(fence, 2n, {
			taskOrdinal: 0,
			bucket: {
				hashNumber: 11,
				hashes: [],
				nextIndex: 0,
			},
		});
		expect(frozen.snapshot.active?.cursor.bucket?.hashes).to.deep.equal([]);
		const advanced = await store.checkpoint(fence, 3n, {
			taskOrdinal: 0,
			afterHashNumber: 11,
		});
		expect(advanced.snapshot.active?.cursor).to.deep.equal({
			taskOrdinal: 0,
			afterHashNumber: 11,
			bucket: undefined,
		});
	});

	it("requires a physical barrier in strict mode and withholds durable tokens in memory mode", async () => {
		const persistence = new MemoryPersistence();
		const withoutBarrier = {
			read: persistence.read.bind(persistence),
			write: persistence.write.bind(persistence),
			flush: persistence.flush.bind(persistence),
		};
		await expect(
			RebalanceWorkStore.open({
				persistence: withoutBarrier,
				durability: "strict",
			}),
		).to.be.rejectedWith("requires a physical durability barrier");

		const memory = await RebalanceWorkStore.open({
			persistence: withoutBarrier,
			durability: "memory",
		});
		const installed = await memory.install(0n, {
			resolution: "u32",
			viewId: viewId("6"),
			plan: makePlanU32(),
		});
		expect(installed.durableCommit).to.equal(undefined);
		expect(memory.currentDurableCommit()).to.equal(undefined);
		expect(persistence.flushes).to.deep.equal([
			REBALANCE_WORK_FILES[1],
			REBALANCE_WORK_FILES[0],
		]);
		await memory.close();

		const sharedFiles = new Map<string, Uint8Array>();
		const strictPersistence = new MemoryPersistence(sharedFiles);
		const strict = await openStrict(strictPersistence);
		await strict.install(0n, {
			resolution: "u32",
			viewId: viewId("a"),
			plan: makePlanU32(),
		});
		await strict.close();
		const degradedPersistence = new MemoryPersistence(sharedFiles);
		const degraded = await RebalanceWorkStore.open({
			persistence: degradedPersistence,
			durability: "memory",
		});
		await degraded.install(2n, {
			resolution: "u32",
			viewId: viewId("b"),
			plan: makePlanU32(20),
		});
		await degraded.close();
		await expect(
			openStrict(new MemoryPersistence(sharedFiles)),
		).to.be.rejectedWith("cannot adopt a memory generation");
	});

	it("preflights bounded plans before writing the initial baseline", async () => {
		const persistence = new MemoryPersistence();
		const store = await openStrict(persistence, { maxFrameBytes: 256 });
		await expect(
			store.install(0n, {
				resolution: "u32",
				viewId: viewId("c"),
				plan: makePlanU32(),
			}),
		).to.be.rejectedWith("exceeds configured byte bound");
		expect(persistence.writes).to.have.length(0);
		expect(store.snapshot().revision).to.equal(0n);

		const invalidPlan = makePlanU32() as any;
		invalidPlan.ownedIntervals[0].end = 999n;
		await expect(
			store.install(0n, {
				resolution: "u32",
				viewId: viewId("d"),
				plan: invalidPlan,
			}),
		).to.be.rejectedWith("do not match query geometry");
		expect(persistence.writes).to.have.length(0);
	});

	it("rejects sparse caller arrays before acknowledging a generation", async () => {
		const persistence = new MemoryPersistence();
		const store = await openStrict(persistence);

		const sparseGeometry = makePlanU32() as any;
		sparseGeometry.geometryRanges = new Array(1);
		sparseGeometry.ownedIntervals = [];
		await expect(
			store.install(0n, {
				resolution: "u32",
				viewId: viewId("a"),
				plan: sparseGeometry,
			}),
		).to.be.rejectedWith("Invalid rebalance geometry range 0");

		const sparseIntervals = makePlanU32() as any;
		sparseIntervals.ownedIntervals = new Array(1);
		await expect(
			store.install(0n, {
				resolution: "u32",
				viewId: viewId("b"),
				plan: sparseIntervals,
			}),
		).to.be.rejectedWith("Invalid rebalance owned interval 0");

		const sparseHistory = makePlanU32() as any;
		sparseHistory.historyMutations = new Array(1);
		await expect(
			store.install(0n, {
				resolution: "u32",
				viewId: viewId("c"),
				plan: sparseHistory,
			}),
		).to.be.rejectedWith("Invalid rebalance history mutation 0 state");

		expect(persistence.writes).to.have.length(0);
		expect(store.snapshot().revision).to.equal(0n);
	});

	it("binds the digest to the canonical nested plan and rejects tampering", async () => {
		const persistence = new MemoryPersistence();
		const store = await openStrict(persistence);
		await store.install(0n, {
			resolution: "u32",
			viewId: viewId("d"),
			plan: makePlanU32(),
		});
		const activeBytes = persistence.files.get(REBALANCE_WORK_FILES[0])!;
		const { payload } = readFrame(activeBytes);
		const storedPlan = payload.value.plan;
		const digestBody = {
			resolution: storedPlan.resolution,
			viewId: storedPlan.viewId,
			plan: {
				boundary: storedPlan.boundary,
				geometryRanges: storedPlan.geometryRanges,
				ownedIntervals: storedPlan.ownedIntervals,
				taskCount: storedPlan.taskCount,
				historyMutations: storedPlan.historyMutations,
			},
		};
		expect(storedPlan.planDigest).to.equal(
			toHexString(
				sha256Sync(textEncoder.encode(deterministicStringify(digestBody))),
			),
		);
		await store.close();

		payload.value.plan.historyMutations[0].rangeHash = "tampered";
		persistence.files.set(REBALANCE_WORK_FILES[0], encodeFramePayload(payload));
		persistence.files.delete(REBALANCE_WORK_FILES[1]);
		await expect(openStrict(persistence)).to.be.rejectedWith(
			"No valid rebalance work generation remains",
		);
	});

	it("captures plan, cursor, and fence inputs when calls are admitted", async () => {
		const persistence = new MemoryPersistence();
		const store = await openStrict(persistence);
		const plan = makePlanU32() as any;
		const installing = store.install(0n, {
			resolution: "u32",
			viewId: viewId("e"),
			plan,
		});
		plan.geometryRanges[0].start1 = 999;
		plan.historyMutations.push({ rangeHash: "late", present: true });
		const installed = await installing;
		expect(installed.snapshot.active?.plan.geometryRanges[0].start1).to.equal(
			10,
		);
		expect(installed.snapshot.active?.plan.historyMutations).to.have.length(1);

		const fence = { ...installed.durableCommit!.fence! };
		const cursor = {
			taskOrdinal: 0,
			bucket: {
				hashNumber: 11,
				hashes: ["a", "b"],
				nextIndex: 0,
			},
		};
		const checkpointing = store.checkpoint(fence, 2n, cursor);
		fence.viewId = viewId("f");
		cursor.bucket.hashes[0] = "changed";
		cursor.bucket.hashes.push("late");
		const checkpoint = await checkpointing;
		expect(checkpoint.snapshot.active?.cursor.bucket?.hashes).to.deep.equal([
			"a",
			"b",
		]);
	});

	it("rejects a second open of the same persistence adapter", async () => {
		const persistence = new MemoryPersistence();
		const store = await openStrict(persistence);
		await expect(openStrict(persistence)).to.be.rejectedWith("already open");
		await store.close();
		const reopened = await openStrict(persistence);
		await reopened.close();
	});

	it("terminally drops incomplete work and keeps ownership until close", async () => {
		const persistence = new MemoryPersistence();
		const store = await openStrict(persistence);
		const installed = await store.install(0n, {
			resolution: "u32",
			viewId: viewId("0"),
			plan: makePlanU32(),
		});
		const fence = installed.durableCommit!.fence!;
		const writesBeforeDrop = persistence.writes.length;

		await expect(store.clear(fence, 2n)).to.be.rejectedWith(
			"Cannot clear incomplete rebalance work",
		);
		const dropping = store.beginDrop();
		expect(store.beginDrop()).to.equal(dropping);
		expect(() => store.snapshot()).to.throw("dropping");
		expect(() => store.currentDurableCommit()).to.throw("dropping");
		await expect(
			store.checkpoint(fence, 2n, { taskOrdinal: 0 }),
		).to.be.rejectedWith("dropping");
		await expect(store.clear(fence, 2n)).to.be.rejectedWith("dropping");

		const dropped = await dropping;
		expect(dropped.snapshot).to.deep.equal({
			revision: 3n,
			active: undefined,
		});
		expect(dropped.durableCommit).to.deep.include({
			revision: 3n,
			fence: undefined,
		});
		expect(persistence.writes).to.have.length(writesBeforeDrop + 1);
		await expect(openStrict(persistence)).to.be.rejectedWith("already open");

		await store.close();
		const reopened = await openStrict(persistence);
		const writesBeforeRetry = persistence.writes.length;
		const retry = reopened.beginDrop();
		expect(reopened.beginDrop()).to.equal(retry);
		expect((await retry).snapshot).to.deep.equal(dropped.snapshot);
		expect(persistence.writes).to.have.length(writesBeforeRetry);
		await reopened.close();
	});

	it("rejects a cached drop result after the persistence lease is released", async () => {
		const persistence = new MemoryPersistence();
		const original = await openStrict(persistence);
		await original.install(0n, {
			resolution: "u32",
			viewId: viewId("4"),
			plan: makePlanU32(),
		});
		const dropped = await original.beginDrop();
		await original.close();

		const replacement = await openStrict(persistence);
		const installed = await replacement.install(dropped.snapshot.revision, {
			resolution: "u32",
			viewId: viewId("5"),
			plan: makePlanU32(30),
		});
		await expect(original.beginDrop()).to.be.rejectedWith("closing");
		expect(replacement.snapshot()).to.deep.equal(installed.snapshot);
		expect(replacement.snapshot().active).not.to.equal(undefined);
		await replacement.close();
	});

	it("treats an implicit empty namespace as authoritative cleared state", async () => {
		const persistence = new MemoryPersistence();
		const store = await openStrict(persistence);
		const dropped = await store.beginDrop();

		expect(dropped.snapshot).to.deep.equal({
			revision: 0n,
			active: undefined,
		});
		expect(dropped.durableCommit).to.equal(undefined);
		expect(persistence.writes).to.have.length(0);
		expect(persistence.barriers).to.have.length(0);
		await store.close();
		const reopened = await openStrict(persistence);
		expect(reopened.snapshot()).to.deep.equal(dropped.snapshot);
		expect(persistence.writes).to.have.length(0);
		await reopened.close();
	});

	it("drains admitted success and stale failure before drop and close", async () => {
		const persistence = new MemoryPersistence();
		const store = await openStrict(persistence);
		const installed = await store.install(0n, {
			resolution: "u32",
			viewId: viewId("1"),
			plan: makePlanU32(),
		});
		const fence = installed.durableCommit!.fence!;
		const admittedGate = persistence.blockNextBarrier();
		const admitted = store.checkpoint(fence, 2n, {
			taskOrdinal: 0,
			bucket: {
				hashNumber: 11,
				hashes: ["hash-11"],
				nextIndex: 0,
			},
		});
		await admittedGate.entered;
		const stale = store.checkpoint(fence, 2n, { taskOrdinal: 0 });
		const staleRejected = expect(stale).to.be.rejectedWith(
			"Stale rebalance work revision",
		);
		const dropping = store.beginDrop();
		expect(store.beginDrop()).to.equal(dropping);
		const dropGate = persistence.blockNextBarrier();
		const closing = store.close();
		expect(store.close()).to.equal(closing);
		expect(store.beginDrop()).to.equal(dropping);

		await expect(
			store.checkpoint(fence, 3n, { taskOrdinal: 0 }),
		).to.be.rejectedWith("dropping");
		await expect(openStrict(persistence)).to.be.rejectedWith("already open");
		admittedGate.release();
		await admitted;
		await staleRejected;
		await dropGate.entered;
		expect(persistence.closeCalls).to.equal(0);
		await expect(openStrict(persistence)).to.be.rejectedWith("already open");
		dropGate.release();

		const dropped = await dropping;
		expect(dropped.snapshot).to.deep.equal({
			revision: 4n,
			active: undefined,
		});
		await closing;
		expect(persistence.closeCalls).to.equal(1);
	});

	it("lets close win admission before a terminal drop starts", async () => {
		const persistence = new MemoryPersistence();
		const store = await openStrict(persistence);
		const closing = store.close();
		await expect(store.beginDrop()).to.be.rejectedWith("closing");
		await closing;
		expect(persistence.writes).to.have.length(0);
	});

	it("retries terminal drop after a falsey pre-write failure and reopen", async () => {
		const persistence = new MemoryPersistence();
		const store = await openStrict(persistence);
		await store.install(0n, {
			resolution: "u32",
			viewId: viewId("2"),
			plan: makePlanU32(),
		});
		const writesBeforeDrop = persistence.writes.length;
		persistence.failNextWriteWith(null);
		await expect(store.beginDrop()).to.be.rejectedWith(
			"Failed to persist rebalance work frame",
		);
		expect(persistence.writes).to.have.length(writesBeforeDrop);
		await expect(store.close()).to.be.rejectedWith("poisoned");

		const recoveredPersistence = persistence.fork();
		const recovered = await openStrict(recoveredPersistence);
		expect(recovered.snapshot().active).not.to.equal(undefined);
		expect(recovered.snapshot().revision).to.equal(2n);
		const dropped = await recovered.beginDrop();
		expect(dropped.snapshot).to.deep.equal({
			revision: 3n,
			active: undefined,
		});
		expect(recoveredPersistence.writes).to.have.length(1);
		await recovered.close();
	});

	it("recovers an ambiguous falsey-barrier drop without sequence churn", async () => {
		const persistence = new MemoryPersistence();
		const store = await openStrict(persistence);
		const installed = await store.install(0n, {
			resolution: "u32",
			viewId: viewId("2"),
			plan: makePlanU32(),
		});
		persistence.failNextBarrierWith(undefined);
		const dropping = store.beginDrop();
		expect(store.beginDrop()).to.equal(dropping);
		persistence.failNextCloseWith(0);
		const closing = store.close();
		const closeOutcome = closing.then(
			() => undefined,
			(error: unknown) => error,
		);
		await expect(dropping).to.be.rejectedWith(
			"Failed to persist rebalance work frame",
		);
		expect(() => store.snapshot()).to.throw("poisoned");
		await expect(
			store.checkpoint(installed.durableCommit!.fence!, 2n, {
				taskOrdinal: 0,
			}),
		).to.be.rejectedWith("dropping");
		const closeFailure = await closeOutcome;
		expect(closeFailure).to.be.instanceOf(AggregateError);
		expect((closeFailure as AggregateError).message).to.include("poisoned");
		expect((closeFailure as AggregateError).errors[1]).to.equal(0);
		expect(persistence.closeCalls).to.equal(1);

		const recoveredPersistence = persistence.fork();
		const recovered = await openStrict(recoveredPersistence);
		expect(recovered.snapshot()).to.deep.equal({
			revision: 3n,
			active: undefined,
		});
		const writesBeforeRetry = recoveredPersistence.writes.length;
		const retried = await recovered.beginDrop();
		expect(retried.snapshot.revision).to.equal(3n);
		expect(retried.durableCommit?.revision).to.equal(3n);
		expect(recoveredPersistence.writes).to.have.length(writesBeforeRetry);
		await recovered.close();
	});

	it("enforces sequence bounds for active and cleared terminal state", async () => {
		const persistence = new MemoryPersistence();
		const store = await openStrict(persistence);
		await store.install(0n, {
			resolution: "u32",
			viewId: viewId("3"),
			plan: makePlanU32(),
		});
		await store.close();

		const activeFile = REBALANCE_WORK_FILES[0];
		const { payload } = readFrame(persistence.files.get(activeFile)!);
		payload.sequence = (MAX_U64 - 1n).toString();
		persistence.files.set(activeFile, encodeFramePayload(payload));
		const lastWritable = await openStrict(persistence);
		const writesBeforeLast = persistence.writes.length;
		const finalTombstone = await lastWritable.beginDrop();
		expect(finalTombstone.snapshot.revision).to.equal(MAX_U64);
		expect(persistence.writes).to.have.length(writesBeforeLast + 1);
		await lastWritable.close();

		payload.sequence = MAX_U64.toString();
		persistence.files.set(activeFile, encodeFramePayload(payload));
		persistence.files.delete(REBALANCE_WORK_FILES[1]);
		const atMaximum = await openStrict(persistence);
		const writesAtMaximum = persistence.writes.length;
		const failedDrop = atMaximum.beginDrop();
		await expect(failedDrop).to.be.rejectedWith(
			"Rebalance work sequence exhausted",
		);
		expect(atMaximum.beginDrop()).to.equal(failedDrop);
		expect(persistence.writes).to.have.length(writesAtMaximum);
		await expect(atMaximum.close()).to.be.rejectedWith(
			"Rebalance work sequence exhausted",
		);

		payload.state = "cleared";
		payload.value = null;
		persistence.files.set(activeFile, encodeFramePayload(payload));
		const alreadyCleared = await openStrict(persistence);
		const writesBeforeClearedDrop = persistence.writes.length;
		const dropped = await alreadyCleared.beginDrop();
		expect(dropped.snapshot).to.deep.equal({
			revision: MAX_U64,
			active: undefined,
		});
		expect(persistence.writes).to.have.length(writesBeforeClearedDrop);
		await alreadyCleared.close();
	});

	it("drains admitted work on close and rejects late mutations", async () => {
		const persistence = new MemoryPersistence();
		const store = await openStrict(persistence);
		const installed = await store.install(0n, {
			resolution: "u32",
			viewId: viewId("7"),
			plan: makePlanU32(),
		});
		const gate = persistence.blockNextBarrier();
		const admitted = store.checkpoint(installed.durableCommit!.fence!, 2n, {
			taskOrdinal: 0,
			bucket: {
				hashNumber: 11,
				hashes: ["hash-11"],
				nextIndex: 0,
			},
		});
		await gate.entered;
		const closing = store.close();
		const repeatedClose = store.close();
		expect(repeatedClose).to.equal(closing);
		await expect(
			store.checkpoint(installed.durableCommit!.fence!, 2n, {
				taskOrdinal: 0,
			}),
		).to.be.rejectedWith("closing");
		let closed = false;
		closing.then(() => {
			closed = true;
		});
		await Promise.resolve();
		expect(closed).to.be.false;
		gate.release();
		await admitted;
		await closing;
		expect(closed).to.be.true;
		expect(persistence.closeCalls).to.equal(1);
	});

	it("closes the backend but rejects when admitted work becomes ambiguous", async () => {
		const persistence = new MemoryPersistence();
		const store = await openStrict(persistence);
		const installed = await store.install(0n, {
			resolution: "u32",
			viewId: viewId("d"),
			plan: makePlanU32(),
		});
		const gate = persistence.blockNextBarrier();
		const admitted = store.checkpoint(installed.durableCommit!.fence!, 2n, {
			taskOrdinal: 0,
			bucket: {
				hashNumber: 11,
				hashes: ["hash-11"],
				nextIndex: 0,
			},
		});
		await gate.entered;
		const closing = store.close();
		persistence.failNextBarrierWith(0);
		gate.release();
		await expect(admitted).to.be.rejectedWith(
			"Failed to persist rebalance work frame",
		);
		await expect(closing).to.be.rejectedWith("poisoned");
		expect(persistence.closeCalls).to.equal(1);
	});

	it("does not hide a falsey backend close rejection", async () => {
		const persistence = new MemoryPersistence();
		const store = await openStrict(persistence);
		persistence.failNextCloseWith(undefined);
		let rejected = false;
		try {
			await store.close();
		} catch {
			rejected = true;
		}
		expect(rejected).to.be.true;
		expect(persistence.closeCalls).to.equal(1);
		const reopened = await openStrict(persistence);
		await reopened.close();
	});

	it("isolates snapshots from caller mutation", async () => {
		const persistence = new MemoryPersistence();
		const store = await openStrict(persistence);
		const installed = await store.install(0n, {
			resolution: "u32",
			viewId: viewId("8"),
			plan: makePlanU32(),
		});
		(installed.snapshot.active!.plan.geometryRanges as any[])[0].start1 = 999;
		(installed.snapshot.active!.plan.historyMutations as any[]).push({
			rangeHash: "mutated",
			present: true,
		});
		expect(store.snapshot().active?.plan.geometryRanges[0].start1).to.equal(10);
		expect(store.snapshot().active?.plan.historyMutations).to.have.length(1);
	});
});
