import { createStore } from "@peerbit/any-store";
import type {
	AnyStore,
	CrashSafeAtomicReplaceDurability,
} from "@peerbit/any-store-interface";
import {
	calculateRawCid,
	cidifyString,
	stringifyCid,
} from "@peerbit/blocks-interface";
import { expect } from "chai";
import { CID } from "multiformats";
import * as raw from "multiformats/codecs/raw";
import { create as createDigest } from "multiformats/hashes/digest";
import { spawn } from "node:child_process";
import { once } from "node:events";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { AnyBlockStore } from "../src/any-blockstore.js";
import { createBuiltInScopedReclamationBlockStore } from "../src/scoped-reclamation.js";

type Mutation = Readonly<{
	path: string;
	operation: "put" | "del" | "atomicReplace" | "barrier";
	key?: string;
}>;

type Failure = Readonly<{
	match: (mutation: Mutation) => boolean;
	when: "before" | "after";
}>;

type Pause = Readonly<{
	match: (mutation: Mutation) => boolean;
	reached: () => void;
	wait: Promise<void>;
}>;

type StoreRead = Readonly<{
	path: string;
	operation: "get" | "iterator";
	key: string;
}>;

type ReadPause = Readonly<{
	match: (read: StoreRead) => boolean;
	reached: () => void;
	wait: Promise<void>;
	failAfter?: Error;
}>;

class TestStoreBackend {
	readonly levels = new Map<string, Map<string, Uint8Array>>();
	failure?: Failure;
	pause?: Pause;
	readPause?: ReadPause;

	level(path: string): Map<string, Uint8Array> {
		let level = this.levels.get(path);
		if (!level) {
			level = new Map();
			this.levels.set(path, level);
		}
		return level;
	}

	async mutate(mutation: Mutation, apply: () => void): Promise<void> {
		const failure = this.failure;
		if (failure?.match(mutation) && failure.when === "before") {
			this.failure = undefined;
			throw new Error("simulated crash before mutation");
		}
		apply();
		if (this.pause?.match(mutation)) {
			const pause = this.pause;
			pause.reached();
			await pause.wait;
		}
		if (failure?.match(mutation) && failure.when === "after") {
			this.failure = undefined;
			throw new Error("simulated crash after mutation");
		}
	}

	async read<T>(read: StoreRead, apply: () => T): Promise<T> {
		const pause = this.readPause;
		if (pause?.match(read)) {
			pause.reached();
			await pause.wait;
			if (pause.failAfter) throw pause.failAfter;
		}
		return apply();
	}
}

class TestCrashSafeStore implements AnyStore {
	private openState: "open" | "closed" = "closed";
	readonly crashSafeDurability: CrashSafeAtomicReplaceDurability;

	constructor(
		readonly backend = new TestStoreBackend(),
		readonly path = "root",
	) {
		this.crashSafeDurability = {
			crashSafe: true,
			barrier: () =>
				this.backend.mutate(
					{ path: this.path, operation: "barrier" },
					() => {},
				),
			atomicReplace: (key, value) =>
				this.backend.mutate(
					{ path: this.path, operation: "atomicReplace", key },
					() => this.level().set(key, new Uint8Array(value)),
				),
		};
	}

	private level(): Map<string, Uint8Array> {
		return this.backend.level(this.path);
	}

	status(): "open" | "closed" {
		return this.openState;
	}

	async open(): Promise<void> {
		this.openState = "open";
	}

	async close(): Promise<void> {
		this.openState = "closed";
	}

	async get(key: string): Promise<Uint8Array | undefined> {
		return this.backend.read({ path: this.path, operation: "get", key }, () => {
			const value = this.level().get(key);
			return value && new Uint8Array(value);
		});
	}

	put(key: string, value: Uint8Array): Promise<void> {
		return this.backend.mutate({ path: this.path, operation: "put", key }, () =>
			this.level().set(key, new Uint8Array(value)),
		);
	}

	del(key: string): Promise<void> {
		return this.backend.mutate(
			{ path: this.path, operation: "del", key },
			() => void this.level().delete(key),
		);
	}

	sublevel(name: string): AnyStore {
		return new TestCrashSafeStore(this.backend, `${this.path}/${name}`);
	}

	async *iterator(): AsyncGenerator<[string, Uint8Array], void, void> {
		for (const [key, value] of this.level()) {
			yield await this.backend.read(
				{ path: this.path, operation: "iterator", key },
				() => [key, new Uint8Array(value)],
			);
		}
	}

	async clear(): Promise<void> {
		this.level().clear();
	}

	size(): number {
		let size = 0;
		for (const value of this.level().values()) size += value.byteLength;
		return size;
	}

	persisted(): boolean {
		return true;
	}
}

class OpenFailingStore extends TestCrashSafeStore {
	override async open(): Promise<void> {
		throw new Error("simulated open failure");
	}
}

const createManagedStore = async (backing = new TestCrashSafeStore()) => {
	const store = createBuiltInScopedReclamationBlockStore(backing);
	await store.start();
	return { backing, store, reclamation: store.localReclamation! };
};

const scopeKey = (value: number): Uint8Array => {
	const key = new Uint8Array(32);
	new DataView(key.buffer).setUint32(key.byteLength - 4, value);
	return key;
};

const within = async <T>(
	promise: Promise<T>,
	timeoutMs = 15_000,
): Promise<T> => {
	let timer: ReturnType<typeof setTimeout> | undefined;
	try {
		return await Promise.race([
			promise,
			new Promise<never>((_, reject) => {
				timer = setTimeout(
					() => reject(new Error("Timed out waiting for reclamation worker")),
					timeoutMs,
				);
			}),
		]);
	} finally {
		if (timer) clearTimeout(timer);
	}
};

const runAcknowledgedThenKill = async (
	directory: string,
	mode: "retain" | "release",
	cid?: string,
): Promise<string> => {
	const workerPath = path.join(
		process.cwd(),
		"test/scoped-reclamation-hard-kill-worker.mjs",
	);
	const worker = spawn(
		process.execPath,
		[workerPath, directory, mode, ...(cid ? [cid] : [])],
		{ stdio: ["ignore", "pipe", "pipe"] },
	);
	const exited = once(worker, "exit");
	let stdout = "";
	let stderr = "";
	worker.stderr?.on("data", (chunk) => {
		stderr += chunk.toString();
	});
	try {
		const acknowledgedCid = await within(
			new Promise<string>((resolve, reject) => {
				worker.stdout?.on("data", (chunk) => {
					stdout += chunk.toString();
					for (const line of stdout.split("\n")) {
						if (!line.includes('"event":"ack"')) continue;
						const acknowledged = JSON.parse(line) as {
							mode: string;
							cid: string;
						};
						if (acknowledged.mode === mode) resolve(acknowledged.cid);
					}
				});
				worker.once("exit", (code, signal) => {
					reject(
						new Error(
							`Reclamation worker exited before acknowledgement (code=${String(code)}, signal=${String(signal)}): ${stderr}`,
						),
					);
				});
			}),
		);
		expect(worker.kill("SIGKILL")).to.equal(true);
		await within(exited.then((): void => undefined));
		return acknowledgedCid;
	} finally {
		if (worker.exitCode === null && worker.signalCode === null) {
			worker.kill("SIGKILL");
		}
	}
};

describe("scoped block reclamation", () => {
	it("keeps one CID until both independent scopes release it", async () => {
		const { store, reclamation } = await createManagedStore();
		const left = reclamation.openScope(scopeKey(1));
		const right = reclamation.openScope(scopeKey(2));
		const bytes = new Uint8Array([1, 2, 3]);
		const cid = (await calculateRawCid(bytes)).cid;

		await left.retain(cid, bytes);
		await right.retain(cid, bytes);
		expect(await left.release(cid)).to.equal("retained");
		expect(await store.get(cid)).to.deep.equal(bytes);
		expect(await right.release(cid)).to.equal("reclaimed");
		expect(await store.get(cid)).to.equal(undefined);
		await store.stop();
	});

	it("keeps raw and managed aliases isolated in both deletion directions", async () => {
		const { store, reclamation } = await createManagedStore();
		const scope = reclamation.openScope(scopeKey(1));
		const bytes = new Uint8Array([4, 5, 6]);
		const cid = await store.put(bytes);

		await scope.retain(cid, bytes);
		await store.rm(cid);
		expect(await store.get(cid)).to.deep.equal(bytes);

		await store.put(bytes);
		expect(await scope.release(cid)).to.equal("reclaimed");
		expect(await store.get(cid)).to.deep.equal(bytes);
		await store.rm(cid);
		expect(await store.get(cid)).to.equal(undefined);
		await store.stop();
	});

	it("keeps real Level sublevel rows outside every raw block API", async () => {
		const directory = await fs.mkdtemp(
			path.join(os.tmpdir(), "peerbit-scoped-reclamation-boundary-"),
		);
		const backing = createStore(directory);
		const store = createBuiltInScopedReclamationBlockStore(backing);
		await store.start();
		try {
			const scope = store.localReclamation!.openScope(scopeKey(1));
			const managedBytes = new Uint8Array([41, 42, 43]);
			const managedCid = await scope.put(managedBytes);
			const physicalEntries: Array<[string, Uint8Array]> = [];
			for await (const [key, value] of backing.iterator()) {
				physicalEntries.push([key, new Uint8Array(value)]);
			}
			const internalEntries = physicalEntries.filter(([key]) =>
				key.startsWith("!peerbit-scoped-reclamation-v1!"),
			);
			expect(internalEntries).to.have.length(2);

			const rawBytes = new Uint8Array([44, 45, 46]);
			const rawCid = await store.put(rawBytes);
			const untouchedBytes = new Uint8Array([47, 48, 49]);
			const untouched = await calculateRawCid(untouchedBytes);
			const invalidKey = internalEntries[0]![0];
			const oversizedRawBytes = new Uint8Array(600);
			const oversizedValidCid = stringifyCid(
				CID.createV1(raw.code, createDigest(0, oversizedRawBytes)),
			);
			expect(cidifyString(oversizedValidCid).multihash.size).to.equal(600);
			expect(
				new TextEncoder().encode(oversizedValidCid).byteLength,
			).to.be.greaterThan(store.localReclamation!.limits.maxCidBytes);
			await backing.put(oversizedValidCid, oversizedRawBytes);

			expect(await store.get(invalidKey)).to.equal(undefined);
			expect(await store.get(oversizedValidCid)).to.deep.equal(
				oversizedRawBytes,
			);
			expect(
				await store.getMany([managedCid, invalidKey, oversizedValidCid]),
			).to.deep.equal([managedBytes, undefined, oversizedRawBytes]);
			expect(await store.has(invalidKey)).to.equal(false);
			expect(await store.has(oversizedValidCid)).to.equal(true);
			expect(
				await store.hasMany([managedCid, invalidKey, oversizedValidCid]),
			).to.deep.equal([true, false, true]);

			await expect(
				store.put({ cid: invalidKey, block: untouched.block } as any),
			).to.be.rejectedWith("valid CID string");
			await expect(
				store.putMany([
					{ cid: untouched.cid, block: untouched.block },
					{ cid: invalidKey, block: untouched.block } as any,
				]),
			).to.be.rejectedWith("valid CID string");
			expect(await store.has(untouched.cid)).to.equal(false);
			expect(() => store.putKnown(invalidKey, untouchedBytes)).to.throw(
				"valid CID string",
			);
			expect(
				await store.putKnown(oversizedValidCid, oversizedRawBytes),
			).to.equal(oversizedValidCid);
			expect(() =>
				store.putKnown("z".repeat(64 * 1024 + 1), untouchedBytes),
			).to.throw("exceeds 65536 bytes");
			expect(() =>
				store.putKnownMany([
					[untouched.cid, untouchedBytes],
					[invalidKey, untouchedBytes],
				]),
			).to.throw("valid CID string");
			expect(await store.has(untouched.cid)).to.equal(false);
			await expect(store.rm(invalidKey)).to.be.rejectedWith("valid CID string");
			await store.rm(oversizedValidCid);
			expect(await store.has(oversizedValidCid)).to.equal(false);
			expect(
				await store.putKnown(oversizedValidCid, oversizedRawBytes),
			).to.equal(oversizedValidCid);
			await expect(store.rmMany([rawCid, invalidKey])).to.be.rejectedWith(
				"valid CID string",
			);
			expect(await store.has(rawCid)).to.equal(true);

			const switchingPut = await calculateRawCid(new Uint8Array([50]));
			let switchingPutReads = 0;
			const switchingPutInput = {
				block: switchingPut.block,
				get cid() {
					switchingPutReads += 1;
					return switchingPutReads === 1 ? switchingPut.cid : invalidKey;
				},
			};
			expect(await store.put(switchingPutInput)).to.equal(switchingPut.cid);
			expect(switchingPutReads).to.equal(1);

			const switchingBatch = await calculateRawCid(new Uint8Array([51]));
			let switchingBatchReads = 0;
			const switchingBatchInput = {
				block: switchingBatch.block,
				get cid() {
					switchingBatchReads += 1;
					return switchingBatchReads === 1 ? switchingBatch.cid : invalidKey;
				},
			};
			expect(
				await store.putMany([
					switchingBatchInput,
					{ cid: untouched.cid, block: untouched.block },
				]),
			).to.deep.equal([switchingBatch.cid, untouched.cid]);
			expect(switchingBatchReads).to.equal(1);

			const switchingKnown = await calculateRawCid(new Uint8Array([52]));
			let switchingKnownReads = 0;
			const switchingKnownTuple = new Proxy(
				[switchingKnown.cid, switchingKnown.block.bytes] as const,
				{
					get(target, property, receiver) {
						if (property === "0") {
							switchingKnownReads += 1;
							return switchingKnownReads === 1
								? switchingKnown.cid
								: invalidKey;
						}
						return Reflect.get(target, property, receiver);
					},
				},
			);
			expect(
				await store.putKnownMany([switchingKnownTuple, [rawCid, rawBytes]]),
			).to.deep.equal([switchingKnown.cid, rawCid]);
			expect(switchingKnownReads).to.equal(1);

			let switchingRemoveReads = 0;
			const switchingRemove = new Proxy([switchingKnown.cid], {
				get(target, property, receiver) {
					if (property === "0") {
						switchingRemoveReads += 1;
						return switchingRemoveReads === 1 ? switchingKnown.cid : invalidKey;
					}
					return Reflect.get(target, property, receiver);
				},
			});
			expect(await store.rmMany(switchingRemove)).to.equal(1);
			expect(switchingRemoveReads).to.equal(1);
			expect(await store.has(switchingKnown.cid)).to.equal(false);

			for (const [key, value] of internalEntries) {
				expect(await backing.get(key)).to.deep.equal(value);
			}
			const visible: string[] = [];
			for await (const [cid] of store.iterator()) visible.push(cid);
			expect(visible.sort()).to.deep.equal(
				[
					managedCid,
					rawCid,
					oversizedValidCid,
					switchingPut.cid,
					switchingBatch.cid,
					untouched.cid,
				].sort(),
			);
			expect(await scope.release(managedCid)).to.equal("reclaimed");
			expect(await store.get(managedCid)).to.equal(undefined);
		} finally {
			await store.stop();
			await fs.rm(directory, { recursive: true, force: true });
		}
	});

	it("reopens acknowledged retain and release state after SIGKILL", async function () {
		this.timeout(30_000);
		if (process.platform === "win32") this.skip();
		const directory = await fs.mkdtemp(
			path.join(os.tmpdir(), "peerbit-scoped-reclamation-hard-kill-"),
		);
		let reopened: AnyBlockStore | undefined;
		try {
			const cid = await runAcknowledgedThenKill(directory, "retain");
			reopened = createBuiltInScopedReclamationBlockStore(
				createStore(directory),
			);
			await reopened.start();
			expect(await reopened.get(cid)).to.deep.equal(
				new Uint8Array([71, 72, 73, 74]),
			);
			await reopened.stop();
			reopened = undefined;

			expect(await runAcknowledgedThenKill(directory, "release", cid)).to.equal(
				cid,
			);
			reopened = createBuiltInScopedReclamationBlockStore(
				createStore(directory),
			);
			await reopened.start();
			expect(await reopened.get(cid)).to.equal(undefined);
			expect(
				await reopened.localReclamation!.openScope(scopeKey(1)).release(cid),
			).to.equal("not-retained");
		} finally {
			await reopened?.stop();
			await fs.rm(directory, { recursive: true, force: true });
		}
	});

	it("makes retain and release idempotent", async () => {
		const { store, reclamation } = await createManagedStore();
		const scope = reclamation.openScope(scopeKey(1));
		const bytes = new Uint8Array([7, 8, 9]);
		const cid = (await calculateRawCid(bytes)).cid;

		expect(await scope.retain(cid, bytes)).to.equal(cid);
		expect(await scope.retain(cid, bytes)).to.equal(cid);
		expect(await scope.release(cid)).to.equal("reclaimed");
		expect(await scope.release(cid)).to.equal("not-retained");
		expect(await store.get(cid)).to.equal(undefined);
		await store.stop();
	});

	it("requires, copies, and lifecycle-binds each exact 32-byte scope key", async () => {
		const { store, reclamation } = await createManagedStore();
		expect(() => reclamation.openScope(new Uint8Array(31))).to.throw(
			"exactly 32 bytes",
		);
		expect(() => reclamation.openScope(new Uint8Array(33))).to.throw(
			"exactly 32 bytes",
		);

		const originalKey = scopeKey(1);
		const originalHandle = reclamation.openScope(originalKey);
		originalKey[31] = 2;
		const bytes = new Uint8Array([1, 3, 5]);
		const cid = await originalHandle.put(bytes);
		expect(await reclamation.openScope(scopeKey(1)).release(cid)).to.equal(
			"reclaimed",
		);

		await store.stop();
		expect(reclamation.health()).to.deep.equal({ status: "closed" });
		await store.start();
		expect(reclamation.health()).to.deep.equal({ status: "ready" });
		await expect(originalHandle.put(bytes)).to.be.rejectedWith(
			"not valid for this service lifecycle",
		);
		const currentHandle = store.localReclamation!.openScope(scopeKey(1));
		expect(await currentHandle.put(bytes)).to.equal(cid);
		await store.stop();
	});

	it("drains an admitted managed read and keeps close authoritative", async () => {
		const backend = new TestStoreBackend();
		const opened = await createManagedStore(new TestCrashSafeStore(backend));
		const bytes = new Uint8Array([81, 82, 83]);
		const cid = await opened.reclamation.openScope(scopeKey(1)).put(bytes);
		let resumeRead!: () => void;
		let reachedRead!: () => void;
		const paused = new Promise<void>((resolve) => {
			resumeRead = resolve;
		});
		const reached = new Promise<void>((resolve) => {
			reachedRead = resolve;
		});
		backend.readPause = {
			match: (read) =>
				read.path.endsWith("/data") &&
				read.operation === "get" &&
				read.key === cid,
			reached: reachedRead,
			wait: paused,
			failAfter: new Error("simulated admitted read failure"),
		};

		const read = opened.store.get(cid);
		const rejected = expect(read).to.be.rejectedWith("storage-failure");
		await reached;
		let stopped = false;
		const stopping = opened.store.stop().then(() => {
			stopped = true;
		});
		await Promise.resolve();
		await Promise.resolve();
		expect(stopped).to.equal(false);
		expect(opened.reclamation.health()).to.deep.equal({ status: "closed" });
		resumeRead();
		await rejected;
		await stopping;
		expect(stopped).to.equal(true);
		expect(opened.reclamation.health()).to.deep.equal({ status: "closed" });
	});

	it("drains an active managed iterator before closing its backing store", async () => {
		const backend = new TestStoreBackend();
		const opened = await createManagedStore(new TestCrashSafeStore(backend));
		const bytes = new Uint8Array([84, 85, 86]);
		const cid = await opened.reclamation.openScope(scopeKey(1)).put(bytes);
		let resumeRead!: () => void;
		let reachedRead!: () => void;
		const paused = new Promise<void>((resolve) => {
			resumeRead = resolve;
		});
		const reached = new Promise<void>((resolve) => {
			reachedRead = resolve;
		});
		backend.readPause = {
			match: (read) =>
				read.path.endsWith("/data") &&
				read.operation === "iterator" &&
				read.key === cid,
			reached: reachedRead,
			wait: paused,
		};

		const iterator = opened.store.iterator();
		const next = iterator.next();
		await reached;
		let stopped = false;
		const stopping = opened.store.stop().then(() => {
			stopped = true;
		});
		await Promise.resolve();
		await Promise.resolve();
		expect(stopped).to.equal(false);
		expect(opened.reclamation.health()).to.deep.equal({ status: "closed" });
		resumeRead();
		expect(await next).to.deep.equal({ done: false, value: [cid, bytes] });
		await stopping;
		expect(stopped).to.equal(true);
		expect(opened.reclamation.health()).to.deep.equal({ status: "closed" });
		expect(await iterator.next()).to.deep.equal({
			done: true,
			value: undefined,
		});
	});

	it("bounds block, CID, and queued work before it executes", async () => {
		const backend = new TestStoreBackend();
		const opened = await createManagedStore(new TestCrashSafeStore(backend));
		expect(Object.isFrozen(opened.reclamation.limits)).to.equal(true);
		expect(
			Reflect.set(opened.reclamation, "kind", "caller-forged-kind"),
		).to.equal(false);
		expect(
			Reflect.set(opened.reclamation, "limits", {
				...opened.reclamation.limits,
				maxCidBytes: Number.MAX_SAFE_INTEGER,
			}),
		).to.equal(false);
		expect(
			Reflect.set(
				opened.reclamation.limits,
				"maxCidBytes",
				Number.MAX_SAFE_INTEGER,
			),
		).to.equal(false);
		expect(opened.reclamation.kind).to.equal("scoped-references-v1");
		expect(opened.reclamation.limits.maxCidBytes).to.equal(512);
		const scope = opened.reclamation.openScope(scopeKey(1));
		await expect(
			scope.put(new Uint8Array(opened.reclamation.limits.maxBlockBytes + 1)),
		).to.be.rejectedWith("at most");
		class HostileBytes extends Uint8Array {
			static get [Symbol.species]() {
				throw new Error("species must not run");
			}
			[Symbol.iterator](): ArrayIterator<number> {
				throw new Error("iterator must not run");
			}
		}
		const shadowedOversized = new HostileBytes(
			opened.reclamation.limits.maxBlockBytes + 1,
		);
		Object.defineProperty(shadowedOversized, "byteLength", { value: 0 });
		await expect(scope.put(shadowedOversized)).to.be.rejectedWith("at most");
		const exactHostile = new HostileBytes([23, 24, 25]);
		Object.defineProperty(exactHostile, "byteLength", { value: 0 });
		expect(await scope.put(exactHostile)).to.equal(
			(await calculateRawCid(new Uint8Array([23, 24, 25]))).cid,
		);
		if (typeof SharedArrayBuffer !== "undefined") {
			const shared = new Uint8Array(new SharedArrayBuffer(1));
			Object.defineProperty(shared, "buffer", { value: new ArrayBuffer(1) });
			await expect(scope.put(shared)).to.be.rejectedWith("shared memory");
		}
		await expect(scope.release("x".repeat(513))).to.be.rejectedWith(
			"exceeds 512 bytes",
		);

		let releasePause!: () => void;
		let reachedPause!: () => void;
		const paused = new Promise<void>((resolve) => {
			releasePause = resolve;
		});
		const reached = new Promise<void>((resolve) => {
			reachedPause = resolve;
		});
		backend.pause = {
			match: (mutation) =>
				mutation.path.endsWith("/data") && mutation.operation === "put",
			reached: reachedPause,
			wait: paused,
		};
		const bytes = new Uint8Array([21]);
		const cid = (await calculateRawCid(bytes)).cid;
		const first = scope.retain(cid, bytes);
		await reached;
		const queued: Promise<unknown>[] = [];
		for (let i = 1; i < opened.reclamation.limits.maxPendingOperations; i++) {
			queued.push(Promise.resolve(scope.release(cid)));
		}
		await expect(scope.release(cid)).to.be.rejectedWith(
			"pending-work limit exceeded",
		);
		backend.pause = undefined;
		releasePause();
		await first;
		await Promise.all(queued);

		let releaseBytePause!: () => void;
		let reachedBytePause!: () => void;
		const bytePaused = new Promise<void>((resolve) => {
			releaseBytePause = resolve;
		});
		const byteReached = new Promise<void>((resolve) => {
			reachedBytePause = resolve;
		});
		backend.pause = {
			match: (mutation) =>
				mutation.path.endsWith("/data") && mutation.operation === "put",
			reached: reachedBytePause,
			wait: bytePaused,
		};
		const maximumBlock = new Uint8Array(
			opened.reclamation.limits.maxBlockBytes,
		);
		const firstMaximum = scope.put(maximumBlock);
		await byteReached;
		const byteQueued = Array.from({ length: 3 }, () => scope.put(maximumBlock));
		await expect(scope.put(maximumBlock)).to.be.rejectedWith(
			"pending-work limit exceeded",
		);
		backend.pause = undefined;
		releaseBytePause();
		await Promise.all([firstMaximum, ...byteQueued]);
		await opened.store.stop();
	});

	it("bounds the durable reference set for each block", async () => {
		const { store, reclamation } = await createManagedStore();
		const bytes = new Uint8Array([22]);
		const cid = (await calculateRawCid(bytes)).cid;
		for (let i = 0; i < reclamation.limits.maxReferencesPerBlock; i++) {
			await reclamation.openScope(scopeKey(i + 1)).retain(cid, bytes);
		}
		await expect(
			reclamation
				.openScope(scopeKey(reclamation.limits.maxReferencesPerBlock + 1))
				.retain(cid, bytes),
		).to.be.rejectedWith("reference count exceeds 1024");
		expect(await store.get(cid)).to.deep.equal(bytes);
		await store.stop();
	});

	it("serializes a concurrent retain against the last release", async () => {
		const { store, reclamation } = await createManagedStore();
		const oldScope = reclamation.openScope(scopeKey(1));
		const newScope = reclamation.openScope(scopeKey(2));
		const bytes = new Uint8Array([10, 11, 12]);
		const cid = (await calculateRawCid(bytes)).cid;
		await oldScope.retain(cid, bytes);

		const [released, retained] = await Promise.all([
			oldScope.release(cid),
			newScope.retain(cid, bytes),
		]);
		expect(released).to.equal("reclaimed");
		expect(retained).to.equal(cid);
		expect(await store.get(cid)).to.deep.equal(bytes);
		expect(await newScope.release(cid)).to.equal("reclaimed");

		await oldScope.retain(cid, bytes);
		const [retainedFirst, releasedSecond] = await Promise.all([
			newScope.retain(cid, bytes),
			oldScope.release(cid),
		]);
		expect(retainedFirst).to.equal(cid);
		expect(releasedSecond).to.equal("retained");
		expect(await store.get(cid)).to.deep.equal(bytes);
		expect(await newScope.release(cid)).to.equal("reclaimed");
		await store.stop();
	});

	it("faults closed on corrupt reference state without deleting bytes", async () => {
		const backing = new TestCrashSafeStore();
		let opened = await createManagedStore(backing);
		const scope = opened.reclamation.openScope(scopeKey(1));
		const bytes = new Uint8Array([13, 14, 15]);
		const cid = (await calculateRawCid(bytes)).cid;
		await scope.retain(cid, bytes);
		await opened.store.stop();

		backing.backend
			.level("root/peerbit-scoped-reclamation-v1/references")
			.set(cid, new Uint8Array([0xde, 0xad]));
		opened = await createManagedStore(new TestCrashSafeStore(backing.backend));
		const reopenedScope = opened.reclamation.openScope(scopeKey(2));
		await expect(reopenedScope.release(cid)).to.be.rejectedWith(
			"corrupt-state",
		);
		expect(opened.reclamation.health().status).to.equal("faulted");
		expect(await opened.store.get(cid)).to.deep.equal(bytes);
		await opened.store.stop();
	});

	it("faults when any scope touches references whose managed bytes are missing", async () => {
		const backing = new TestCrashSafeStore();
		let opened = await createManagedStore(backing);
		const bytes = new Uint8Array([31, 32, 33]);
		const cid = await opened.reclamation.openScope(scopeKey(1)).put(bytes);
		await opened.store.stop();
		backing.backend
			.level("root/peerbit-scoped-reclamation-v1/data")
			.delete(cid);

		opened = await createManagedStore(new TestCrashSafeStore(backing.backend));
		await expect(
			opened.reclamation.openScope(scopeKey(2)).release(cid),
		).to.be.rejectedWith("corrupt-state");
		expect(opened.reclamation.health()).to.deep.equal({
			status: "faulted",
			reason: "corrupt-state",
		});
		expect(
			backing.backend
				.level("root/peerbit-scoped-reclamation-v1/references")
				.has(cid),
		).to.equal(true);
		await opened.store.stop();
	});

	it("never sweeps or deletes on oversized reference metadata", async () => {
		const backing = new TestCrashSafeStore();
		let opened = await createManagedStore(backing);
		const scope = opened.reclamation.openScope(scopeKey(1));
		const bytes = new Uint8Array([16, 17, 18]);
		const cid = await scope.put(bytes);
		await opened.store.stop();

		class HostileReferenceState extends Uint8Array {
			static get [Symbol.species]() {
				throw new Error("reference-state species must not run");
			}
		}
		const hostileReferenceState = new HostileReferenceState(40_000);
		Object.defineProperty(hostileReferenceState, "byteLength", { value: 1 });
		backing.backend
			.level("root/peerbit-scoped-reclamation-v1/references")
			.set(cid, hostileReferenceState);
		opened = await createManagedStore(new TestCrashSafeStore(backing.backend));
		const unrelatedBytes = new Uint8Array([19, 20]);
		const unrelatedCid = await opened.reclamation
			.openScope(scopeKey(2))
			.put(unrelatedBytes);
		expect(await opened.store.get(unrelatedCid)).to.deep.equal(unrelatedBytes);
		await expect(
			opened.reclamation.openScope(scopeKey(1)).release(cid),
		).to.be.rejectedWith("corrupt-state");
		expect(await opened.store.get(cid)).to.deep.equal(bytes);
		await opened.store.stop();
	});

	it("faults startup and closes the backing store when durability proof fails", async () => {
		const backing = new TestCrashSafeStore();
		const store = createBuiltInScopedReclamationBlockStore(backing);
		const reclamation = store.localReclamation!;
		expect(reclamation.health()).to.deep.equal({ status: "opening" });
		backing.backend.failure = {
			match: (mutation) => mutation.operation === "barrier",
			when: "before",
		};
		await expect(store.start()).to.be.rejectedWith("storage-failure");
		expect(reclamation.health()).to.deep.equal({
			status: "faulted",
			reason: "storage-failure",
		});
		expect(backing.status()).to.equal("closed");
	});

	it("faults instead of remaining opening when the backing store cannot open", async () => {
		const backing = new OpenFailingStore();
		const store = createBuiltInScopedReclamationBlockStore(backing);
		const reclamation = store.localReclamation!;
		await expect(store.start()).to.be.rejectedWith("storage-failure");
		expect(reclamation.health()).to.deep.equal({
			status: "faulted",
			reason: "storage-failure",
		});
		expect(backing.status()).to.equal("closed");
	});

	it("reopens safely across ambiguous crash cuts", async () => {
		const cutPoints: Array<{
			name: string;
			operation: "retain" | "release";
			match: Failure["match"];
		}> = [
			{
				name: "managed-byte write",
				operation: "retain",
				match: (m) => m.path.endsWith("/data") && m.operation === "put",
			},
			{
				name: "reference publication",
				operation: "retain",
				match: (m) =>
					m.path.endsWith("/references") && m.operation === "atomicReplace",
			},
			{
				name: "managed-byte durability barrier",
				operation: "retain",
				match: (m) => m.path.endsWith("/data") && m.operation === "barrier",
			},
			{
				name: "last-reference removal",
				operation: "release",
				match: (m) =>
					m.path.endsWith("/references") && m.operation === "atomicReplace",
			},
			{
				name: "managed-byte deletion",
				operation: "release",
				match: (m) => m.path.endsWith("/data") && m.operation === "del",
			},
			{
				name: "managed-byte deletion barrier",
				operation: "release",
				match: (m) => m.path.endsWith("/data") && m.operation === "barrier",
			},
			{
				name: "zero-reference metadata cleanup",
				operation: "release",
				match: (m) => m.path.endsWith("/references") && m.operation === "del",
			},
			{
				name: "metadata cleanup barrier",
				operation: "release",
				match: (m) =>
					m.path.endsWith("/references") && m.operation === "barrier",
			},
		];

		for (const cut of cutPoints) {
			for (const when of ["before", "after"] as const) {
				const name = `${cut.name} (${when})`;
				const backend = new TestStoreBackend();
				let opened = await createManagedStore(new TestCrashSafeStore(backend));
				const scope = opened.reclamation.openScope(scopeKey(1));
				const bytes = new TextEncoder().encode(name);
				const cid = (await calculateRawCid(bytes)).cid;
				if (cut.operation === "release") await scope.retain(cid, bytes);

				backend.failure = { match: cut.match, when };
				await expect(
					cut.operation === "retain"
						? scope.retain(cid, bytes)
						: scope.release(cid),
					name,
				).to.be.rejected;
				expect(opened.reclamation.health().status, name).to.equal("faulted");
				await opened.store.stop();

				opened = await createManagedStore(new TestCrashSafeStore(backend));
				const reopenedScope = opened.reclamation.openScope(scopeKey(1));
				const bytesSurviveCut =
					cut.operation === "retain"
						? cut.name !== "managed-byte write" || when === "after"
						: cut.name === "last-reference removal" ||
							(cut.name === "managed-byte deletion" && when === "before");
				expect(await opened.store.get(cid), name).to.deep.equal(
					bytesSurviveCut ? bytes : undefined,
				);
				// Recovery is requested-CID-only. It either consumes a surviving
				// reference or cleans an unpublished/zero-reference orphan.
				const result = await reopenedScope.release(cid);
				expect(result === "reclaimed" || result === "not-retained").to.equal(
					true,
					name,
				);
				expect(await opened.store.get(cid), name).to.equal(undefined);
				await opened.store.stop();
			}
		}
	});

	it("does not advertise the capability on a plain or unsupported store", async () => {
		const plain = new AnyBlockStore();
		const customAtomic = new AnyBlockStore(new TestCrashSafeStore());
		expect(plain.localReclamation).to.equal(undefined);
		expect(customAtomic.localReclamation).to.equal(undefined);
	});
});
