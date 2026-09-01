import { type AnyStore } from "@peerbit/any-store-interface";
import { SHA256 } from "@stablelib/sha256";
import { expect } from "chai";
import {
	CheckpointAmbiguousCommitError,
	CheckpointCommitInFlightError,
	CheckpointCorruptionError,
	CheckpointUnsupportedStoreError,
	CrashSafeTwoSlotCheckpoint,
	type CrashSafeTwoSlotCheckpointOptions,
	MAX_CHECKPOINT_PAYLOAD_BYTES,
} from "../src/checkpoint.js";
import { MemoryStore } from "../src/memory.js";

const scope = (value = "checkpoint-tests") => new TextEncoder().encode(value);
const recordDomain = new TextEncoder().encode(
	"peerbit:any-store:two-slot-checkpoint:record:v1\0",
);
const checksumOffset = 88;
const headerLength = 120;
const maxU64 = (1n << 64n) - 1n;

const resignRecord = (record: Uint8Array): Uint8Array => {
	const hasher = new SHA256();
	try {
		hasher.update(recordDomain);
		hasher.update(record.subarray(0, checksumOffset));
		hasher.update(record.subarray(headerLength));
		record.set(hasher.digest(), checksumOffset);
		return record;
	} finally {
		hasher.clean();
	}
};

class AtomicTestStore implements AnyStore {
	readonly values = new Map<string, Uint8Array>();
	readonly history: Array<{ key: string; value: Uint8Array }> = [];
	barrierCalls = 0;
	atomicReplaceCalls = 0;
	activeGets = 0;
	peakConcurrentGets = 0;
	readonly getKeys: string[] = [];
	failNext: "before" | "after" | "torn" | undefined;
	holdNext: Promise<void> | undefined;
	private opened = true;

	readonly crashSafeDurability = {
		crashSafe: true as const,
		barrier: async () => {
			this.barrierCalls++;
		},
		atomicReplace: async (key: string, value: Uint8Array) => {
			this.atomicReplaceCalls++;
			if (this.holdNext) {
				const hold = this.holdNext;
				this.holdNext = undefined;
				await hold;
			}
			const failure = this.failNext;
			this.failNext = undefined;
			if (failure === "before") throw new Error("injected-before");
			const copy = new Uint8Array(value);
			if (failure === "torn") {
				this.values.set(key, copy.subarray(0, Math.floor(copy.byteLength / 2)));
				throw new Error("injected-torn");
			}
			this.values.set(key, copy);
			this.history.push({ key, value: new Uint8Array(copy) });
			if (failure === "after") throw new Error("injected-after");
		},
	};

	status() {
		return this.opened ? ("open" as const) : ("closed" as const);
	}
	open() {
		this.opened = true;
	}
	close() {
		this.opened = false;
	}
	async get(key: string) {
		this.getKeys.push(key);
		this.activeGets++;
		this.peakConcurrentGets = Math.max(
			this.peakConcurrentGets,
			this.activeGets,
		);
		try {
			await Promise.resolve();
			return this.values.get(key);
		} finally {
			this.activeGets--;
		}
	}
	put(key: string, value: Uint8Array) {
		this.values.set(key, new Uint8Array(value));
	}
	del(key: string) {
		this.values.delete(key);
	}
	sublevel() {
		return this;
	}
	async *iterator(): AsyncGenerator<[string, Uint8Array], void, void> {
		for (const entry of this.values) yield entry;
	}
	clear() {
		this.values.clear();
	}
	size() {
		let size = 0;
		for (const value of this.values.values()) size += value.byteLength;
		return size;
	}
	persisted() {
		return true;
	}
}

const open = (
	store: AnyStore,
	options: { scope?: Uint8Array; maxPayloadBytes?: number } = {},
) =>
	CrashSafeTwoSlotCheckpoint.open({
		store,
		scope: options.scope ?? scope(),
		maxPayloadBytes: options.maxPayloadBytes ?? 64,
	});

const expectRejected = async (
	promise: Promise<unknown>,
	errorType: new (...args: never[]) => Error,
	message?: string,
): Promise<void> => {
	let thrown: unknown;
	try {
		await promise;
	} catch (error) {
		thrown = error;
	}
	expect(thrown).to.be.instanceOf(errorType);
	if (message) expect((thrown as Error).message).to.contain(message);
};

describe("CrashSafeTwoSlotCheckpoint", () => {
	it("fails closed when the store does not prove atomic replacement", async () => {
		const store = new MemoryStore();
		store.open();
		await expectRejected(open(store), CheckpointUnsupportedStoreError);
	});

	it("alternates two fixed slots and reopens the newest linked generation", async () => {
		const store = new AtomicTestStore();
		let checkpoint = await open(store);
		expect(checkpoint.current).to.equal(undefined);
		expect(store.barrierCalls).to.equal(1);

		for (let generation = 1; generation <= 100; generation++) {
			const committed = await checkpoint.commit(
				new Uint8Array([generation & 0xff]),
			);
			expect(committed.generation).to.equal(BigInt(generation));
		}
		expect(store.values.size).to.equal(2);
		expect([...store.values.keys()].sort()).to.deep.equal([
			"\0peerbit:two-slot-checkpoint:v1:a",
			"\0peerbit:two-slot-checkpoint:v1:b",
		]);

		checkpoint = await open(store);
		expect(checkpoint.current).to.deep.equal({
			generation: 100n,
			payload: new Uint8Array([100]),
		});
		expect(store.barrierCalls).to.equal(2);
		for (const value of store.values.values()) value.fill(0);
		expect(checkpoint.current).to.deep.equal({
			generation: 100n,
			payload: new Uint8Array([100]),
		});
	});

	it("captures accessor-backed options exactly once without crossing stores or scopes", async () => {
		const sourceStore = new AtomicTestStore();
		const sourceScope = scope("accessor-source");
		const source = await open(sourceStore, { scope: sourceScope });
		await source.commit(new Uint8Array([7]));
		const unrelatedStore = new AtomicTestStore();

		let storeReads = 0;
		let scopeReads = 0;
		let boundReads = 0;
		const options: CrashSafeTwoSlotCheckpointOptions = {
			get store() {
				storeReads++;
				return storeReads === 1 ? sourceStore : unrelatedStore;
			},
			get scope() {
				scopeReads++;
				return scopeReads === 1 ? sourceScope : scope("accessor-mismatch");
			},
			get maxPayloadBytes() {
				boundReads++;
				return boundReads === 1 ? 64 : 0;
			},
		};

		const checkpoint = await CrashSafeTwoSlotCheckpoint.open(options);
		expect(checkpoint.current).to.deep.equal({
			generation: 1n,
			payload: new Uint8Array([7]),
		});
		expect(storeReads).to.equal(1);
		expect(scopeReads).to.equal(1);
		expect(boundReads).to.equal(1);
		expect(unrelatedStore.getKeys).to.have.length(0);
	});

	it("reads and decodes the two slots sequentially", async () => {
		const store = new AtomicTestStore();
		const seed = await open(store);
		await seed.commit(new Uint8Array([1]));
		await seed.commit(new Uint8Array([2]));
		store.getKeys.length = 0;
		store.peakConcurrentGets = 0;

		const reopened = await open(store);
		expect(reopened.current).to.deep.equal({
			generation: 2n,
			payload: new Uint8Array([2]),
		});
		expect(store.peakConcurrentGets).to.equal(1);
		expect(store.getKeys).to.deep.equal([
			"\0peerbit:two-slot-checkpoint:v1:a",
			"\0peerbit:two-slot-checkpoint:v1:b",
		]);
	});

	it("captures scope and payload bytes without iterator, species, or shadowed-length hooks", async () => {
		class HostileBytes extends Uint8Array {
			static get [Symbol.species]() {
				throw new Error("species must not run");
			}
			[Symbol.iterator](): ArrayIterator<number> {
				throw new Error("iterator must not run");
			}
		}

		const hostileScope = new HostileBytes([1, 2, 3]);
		Object.defineProperty(hostileScope, "byteLength", {
			get: () => {
				throw new Error("shadowed byteLength must not run");
			},
		});
		const originalScope = new Uint8Array([1, 2, 3]);
		const store = new AtomicTestStore();
		const checkpoint = await open(store, { scope: hostileScope });
		hostileScope.fill(9);

		const payload = new HostileBytes([4, 5, 6]);
		Object.defineProperty(payload, "byteLength", {
			get: () => {
				throw new Error("shadowed byteLength must not run");
			},
		});
		const committed = await checkpoint.commit(payload);
		payload.fill(9);
		committed.payload.fill(8);
		const firstRead = checkpoint.current!;
		expect(firstRead.payload).to.deep.equal(new Uint8Array([4, 5, 6]));
		firstRead.payload.fill(7);
		expect(checkpoint.current!.payload).to.deep.equal(
			new Uint8Array([4, 5, 6]),
		);

		const reopened = await open(store, { scope: originalScope });
		expect(reopened.current!.payload).to.deep.equal(new Uint8Array([4, 5, 6]));
	});

	it("rejects proxies, other typed arrays, and detached inputs", async () => {
		const store = new AtomicTestStore();
		await expectRejected(
			open(store, {
				scope: new Proxy(new Uint8Array([1]), {}) as Uint8Array,
			}),
			TypeError,
			"genuine Uint8Array",
		);
		await expectRejected(
			open(store, { scope: new Uint16Array([1]) as unknown as Uint8Array }),
			TypeError,
			"must be a Uint8Array",
		);

		const checkpoint = await open(store);
		const detached = new Uint8Array([1]);
		structuredClone(detached.buffer, { transfer: [detached.buffer] });
		await expectRejected(checkpoint.commit(detached), TypeError);
	});

	it("enforces scope, configuration, and payload bounds before writing", async () => {
		const store = new AtomicTestStore();
		expect(MAX_CHECKPOINT_PAYLOAD_BYTES).to.equal(64 * 1024 * 1024);
		await expectRejected(open(store, { scope: new Uint8Array() }), RangeError);
		await expectRejected(
			open(store, { maxPayloadBytes: MAX_CHECKPOINT_PAYLOAD_BYTES + 1 }),
			RangeError,
		);

		const checkpoint = await open(store, { maxPayloadBytes: 2 });
		await expectRejected(checkpoint.commit(new Uint8Array(3)), RangeError);
		expect(store.values.size).to.equal(0);
		await checkpoint.commit(new Uint8Array([1, 2]));
		await expectRejected(
			open(store, { maxPayloadBytes: 1 }),
			CheckpointCorruptionError,
			"configured bound",
		);
	});

	it("uses intrinsic lengths to reject oversized values before copying or replacing", async () => {
		const shadowLength = (bytes: Uint8Array): Uint8Array => {
			Object.defineProperty(bytes, "byteLength", { get: () => 0 });
			return bytes;
		};

		const scopeStore = new AtomicTestStore();
		await expectRejected(
			open(scopeStore, {
				scope: shadowLength(new Uint8Array(1_025)),
			}),
			RangeError,
		);
		expect(scopeStore.barrierCalls).to.equal(0);
		expect(scopeStore.atomicReplaceCalls).to.equal(0);

		const payloadStore = new AtomicTestStore();
		const checkpoint = await open(payloadStore, { maxPayloadBytes: 2 });
		await expectRejected(
			checkpoint.commit(shadowLength(new Uint8Array(3))),
			RangeError,
		);
		expect(payloadStore.atomicReplaceCalls).to.equal(0);

		const slotStore = new AtomicTestStore();
		slotStore.values.set(
			"\0peerbit:two-slot-checkpoint:v1:a",
			shadowLength(new Uint8Array(headerLength + 2)),
		);
		await expectRejected(
			open(slotStore, { maxPayloadBytes: 1 }),
			CheckpointCorruptionError,
			"configured bound",
		);
		expect(slotStore.atomicReplaceCalls).to.equal(0);
	});

	it("rejects a malformed slot even when another complete slot is valid", async () => {
		const store = new AtomicTestStore();
		const checkpoint = await open(store);
		await checkpoint.commit(new Uint8Array([1]));
		await checkpoint.commit(new Uint8Array([2]));
		const olderKey = [...store.values.keys()].find((key) =>
			key.endsWith(":a"),
		)!;
		const corrupt = new Uint8Array(store.values.get(olderKey)!);
		corrupt[corrupt.byteLength - 1] ^= 0xff;
		store.values.set(olderKey, corrupt);

		await expectRejected(open(store), CheckpointCorruptionError, "checksum");
	});

	it("strictly rejects truncated, trailing, unsupported, reserved, and wrong-slot records", async () => {
		const sourceStore = new AtomicTestStore();
		const checkpoint = await open(sourceStore);
		await checkpoint.commit(new Uint8Array([1]));
		const { key, value } = sourceStore.history[0];
		const variants: Array<{
			message: string;
			mutate: (value: Uint8Array) => Uint8Array;
		}> = [
			{
				message: "truncated",
				mutate: (bytes) => bytes.subarray(0, headerLength - 1),
			},
			{
				message: "encoded length",
				mutate: (bytes) => {
					const trailing = new Uint8Array(bytes.byteLength + 1);
					trailing.set(bytes);
					return trailing;
				},
			},
			{
				message: "unsupported format version",
				mutate: (bytes) => {
					bytes[8] = 2;
					return bytes;
				},
			},
			{
				message: "reserved bytes",
				mutate: (bytes) => {
					bytes[10] = 1;
					return bytes;
				},
			},
			{
				message: "wrong slot",
				mutate: (bytes) => {
					bytes[9] = 1;
					return bytes;
				},
			},
		];

		for (const variant of variants) {
			const store = new AtomicTestStore();
			store.values.set(key, variant.mutate(new Uint8Array(value)));
			await expectRejected(
				open(store),
				CheckpointCorruptionError,
				variant.message,
			);
		}
	});

	it("rejects missing, non-consecutive, and independently valid unlinked slots", async () => {
		const missingStore = new AtomicTestStore();
		const missingCheckpoint = await open(missingStore);
		await missingCheckpoint.commit(new Uint8Array([1]));
		await missingCheckpoint.commit(new Uint8Array([2]));
		const firstKey = missingStore.history[0].key;
		missingStore.values.delete(firstKey);
		await expectRejected(
			open(missingStore),
			CheckpointCorruptionError,
			"lone checkpoint",
		);

		const gapStore = new AtomicTestStore();
		const gapCheckpoint = await open(gapStore);
		for (let i = 1; i <= 4; i++) {
			await gapCheckpoint.commit(new Uint8Array([i]));
		}
		gapStore.values.set(
			gapStore.history[0].key,
			new Uint8Array(gapStore.history[0].value),
		);
		await expectRejected(
			open(gapStore),
			CheckpointCorruptionError,
			"consecutive",
		);

		const leftStore = new AtomicTestStore();
		const left = await open(leftStore);
		await left.commit(new Uint8Array([1]));
		await left.commit(new Uint8Array([2]));
		const rightStore = new AtomicTestStore();
		const right = await open(rightStore);
		await right.commit(new Uint8Array([9]));
		await right.commit(new Uint8Array([8]));
		leftStore.values.set(
			rightStore.history[1].key,
			new Uint8Array(rightStore.history[1].value),
		);
		await expectRejected(
			open(leftStore),
			CheckpointCorruptionError,
			"predecessor chain",
		);
	});

	it("rejects a duplicate generation and refuses to overflow u64", async () => {
		const duplicateStore = new AtomicTestStore();
		const duplicate = await open(duplicateStore);
		await duplicate.commit(new Uint8Array([1]));
		await duplicate.commit(new Uint8Array([2]));
		const duplicateSecond = new Uint8Array(duplicateStore.history[1].value);
		new DataView(
			duplicateSecond.buffer,
			duplicateSecond.byteOffset,
			duplicateSecond.byteLength,
		).setBigUint64(12, 1n, true);
		resignRecord(duplicateSecond);
		duplicateStore.values.set(duplicateStore.history[1].key, duplicateSecond);
		await expectRejected(
			open(duplicateStore),
			CheckpointCorruptionError,
			"wrong parity",
		);

		const overflowStore = new AtomicTestStore();
		const seed = await open(overflowStore);
		await seed.commit(new Uint8Array([1]));
		await seed.commit(new Uint8Array([2]));
		const newerKey = overflowStore.history[0].key;
		const olderKey = overflowStore.history[1].key;
		const older = new Uint8Array(overflowStore.history[1].value);
		const olderView = new DataView(
			older.buffer,
			older.byteOffset,
			older.byteLength,
		);
		olderView.setBigUint64(12, maxU64 - 1n, true);
		resignRecord(older);
		const olderChecksum = older.subarray(checksumOffset, checksumOffset + 32);

		const newer = new Uint8Array(overflowStore.history[0].value);
		const newerView = new DataView(
			newer.buffer,
			newer.byteOffset,
			newer.byteLength,
		);
		newerView.setBigUint64(12, maxU64, true);
		newer.set(olderChecksum, 56);
		resignRecord(newer);
		overflowStore.values.set(olderKey, older);
		overflowStore.values.set(newerKey, newer);

		const exhausted = await open(overflowStore);
		expect(exhausted.current!.generation).to.equal(maxU64);
		await expectRejected(
			exhausted.commit(new Uint8Array([3])),
			RangeError,
			"exceeds u64",
		);
		expect(overflowStore.history).to.have.length(2);
	});

	it("rejects a valid record copied under a different scope", async () => {
		const sourceStore = new AtomicTestStore();
		const source = await open(sourceStore, { scope: scope("source") });
		await source.commit(new Uint8Array([1]));

		const targetStore = new AtomicTestStore();
		const target = await open(targetStore, { scope: scope("target") });
		await target.commit(new Uint8Array([2]));
		const targetKey = targetStore.history[0].key;
		targetStore.values.set(
			targetKey,
			new Uint8Array(sourceStore.history[0].value),
		);
		await expectRejected(
			open(targetStore, { scope: scope("target") }),
			CheckpointCorruptionError,
			"different scope",
		);
	});

	it("becomes terminal after both before-write and after-write failures", async () => {
		for (const failure of ["before", "after"] as const) {
			const store = new AtomicTestStore();
			const checkpoint = await open(store);
			await checkpoint.commit(new Uint8Array([1]));
			store.failNext = failure;
			let ambiguous: unknown;
			try {
				await checkpoint.commit(new Uint8Array([2]));
			} catch (error) {
				ambiguous = error;
			}
			expect(ambiguous).to.be.instanceOf(CheckpointAmbiguousCommitError);
			let readError: unknown;
			try {
				checkpoint.current;
			} catch (error) {
				readError = error;
			}
			expect(readError).to.equal(ambiguous);
			let laterCommitError: unknown;
			try {
				await checkpoint.commit(new Uint8Array([3]));
			} catch (error) {
				laterCommitError = error;
			}
			expect(laterCommitError).to.equal(ambiguous);

			const reopened = await open(store);
			expect(reopened.current).to.deep.equal({
				generation: failure === "after" ? 2n : 1n,
				payload: new Uint8Array([failure === "after" ? 2 : 1]),
			});
		}
	});

	it("rejects a torn ambiguous replacement instead of falling back", async () => {
		const store = new AtomicTestStore();
		const checkpoint = await open(store);
		await checkpoint.commit(new Uint8Array([1]));
		store.failNext = "torn";
		await expectRejected(
			checkpoint.commit(new Uint8Array([2])),
			CheckpointAmbiguousCommitError,
		);
		await expectRejected(open(store), CheckpointCorruptionError, "truncated");
	});

	it("allows only one in-flight commit", async () => {
		const store = new AtomicTestStore();
		const checkpoint = await open(store);
		let release!: () => void;
		store.holdNext = new Promise<void>((resolve) => {
			release = resolve;
		});
		const payload = new Uint8Array([1]);
		const first = checkpoint.commit(payload);
		payload[0] = 9;
		await expectRejected(
			checkpoint.commit(new Uint8Array([2])),
			CheckpointCommitInFlightError,
		);
		release();
		expect(await first).to.deep.equal({
			generation: 1n,
			payload: new Uint8Array([1]),
		});
		expect((await open(store)).current!.payload).to.deep.equal(
			new Uint8Array([1]),
		);
	});
});
