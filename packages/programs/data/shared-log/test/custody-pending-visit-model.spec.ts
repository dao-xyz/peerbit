import { serialize } from "@dao-xyz/borsh";
import { calculateRawCid } from "@peerbit/blocks-interface";
import { Ed25519Keypair, PreHash, fromBase64, toBase64 } from "@peerbit/crypto";
import { expect } from "chai";
import {
	type CanonicalCustodyHandoffManifest,
	createCustodyHandoffManifestV1,
	createCustodyHandoffReceiptV1,
	decodeCustodyHandoffManifestV1,
} from "../src/custody-handoff-codec.js";
import {
	type CustodyRecordPersistence,
	type CustodyRecordSlot,
	CustodyRecordStore,
	type CustodySourceReceiptAuthority,
	type DurableCustodySourceReceiptCommit,
	MemoryCustodyRecordPersistence,
} from "../src/custody-store.js";
import { type RebalanceScanPlan } from "../src/ranges.js";
import {
	type BoundedRebalanceScanSource,
	type RebalanceCustodyVisitBridge,
	type RebalanceScanCandidate,
	RebalanceScanExecutor,
	type RebalanceScanSourceRequest,
	type RebalanceVisitKey,
} from "../src/rebalance-scan-executor.js";
import {
	REBALANCE_WORK_FILES,
	type RebalanceWorkFence,
	type RebalanceWorkPersistence,
	RebalanceWorkStore,
} from "../src/rebalance-work-store.js";

const MODEL_SEED = 0x5eedc0de;
const MODEL_ROWS = 4;
const MODEL_MAX_STEPS = 200;
const HASH_NUMBER = 73;

type Fault = { remaining: number; error: Error };
type CrashGate = Readonly<{ entered: Promise<void>; release: () => void }>;

class StrictWorkPersistence implements RebalanceWorkPersistence {
	readonly writes: string[] = [];
	readonly barriers: string[] = [];
	closeCalls = 0;
	private writeFault?: Fault;
	private barrierFault?: Fault;
	private barrierGate?: {
		remaining: number;
		entered: () => void;
		wait: Promise<void>;
	};
	private readGate?: {
		entered: () => void;
		wait: Promise<void>;
	};
	private lastWrite?: {
		name: string;
		previous: Uint8Array | undefined;
	};

	constructor(readonly files = new Map<string, Uint8Array>()) {}

	async read(name: string, maxBytes: number) {
		const gate = this.readGate;
		if (gate) {
			this.readGate = undefined;
			gate.entered();
			await gate.wait;
		}
		const value = this.files.get(name);
		if (value && value.byteLength > maxBytes) throw new Error("oversized work");
		return value ? new Uint8Array(value) : undefined;
	}

	async write(name: string, bytes: Uint8Array) {
		this.trip("write");
		this.writes.push(name);
		const previous = this.files.get(name);
		this.lastWrite = {
			name,
			previous: previous ? new Uint8Array(previous) : undefined,
		};
		this.files.set(name, new Uint8Array(bytes));
	}

	async durableBarrier(name?: string) {
		this.barriers.push(name!);
		const gate = this.barrierGate;
		if (gate && gate.remaining > 0) {
			gate.remaining--;
		} else if (gate) {
			this.barrierGate = undefined;
			gate.entered();
			await gate.wait;
			const last = this.lastWrite;
			if (last && last.name === name) {
				if (last.previous) this.files.set(last.name, last.previous);
				else this.files.delete(last.name);
			}
			throw new Error("seeded crash at second work mirror barrier");
		}
		this.trip("barrier");
	}

	async close() {
		this.closeCalls++;
	}

	failWrite(afterSuccessfulWrites = 0) {
		this.writeFault = {
			remaining: afterSuccessfulWrites,
			error: new Error("seeded work write crash"),
		};
	}

	failBarrier(afterSuccessfulBarriers = 0) {
		this.barrierFault = {
			remaining: afterSuccessfulBarriers,
			error: new Error("seeded work barrier crash"),
		};
	}

	blockBarrierAndLoseWrite(afterSuccessfulBarriers = 0): CrashGate {
		let entered!: () => void;
		let release!: () => void;
		const enteredPromise = new Promise<void>((resolve) => {
			entered = resolve;
		});
		const wait = new Promise<void>((resolve) => {
			release = resolve;
		});
		this.barrierGate = {
			remaining: afterSuccessfulBarriers,
			entered,
			wait,
		};
		return { entered: enteredPromise, release };
	}

	blockNextRead(): CrashGate {
		let entered!: () => void;
		let release!: () => void;
		const enteredPromise = new Promise<void>((resolve) => {
			entered = resolve;
		});
		const wait = new Promise<void>((resolve) => {
			release = resolve;
		});
		this.readGate = { entered, wait };
		return { entered: enteredPromise, release };
	}

	fork() {
		return new StrictWorkPersistence(this.files);
	}

	private trip(kind: "write" | "barrier") {
		const fault = kind === "write" ? this.writeFault : this.barrierFault;
		if (!fault) return;
		if (fault.remaining > 0) {
			fault.remaining--;
			return;
		}
		if (kind === "write") this.writeFault = undefined;
		else this.barrierFault = undefined;
		throw fault.error;
	}
}

class StrictCustodyPersistence implements CustodyRecordPersistence {
	readonly writes: string[] = [];
	readonly barriers: string[] = [];
	closeCalls = 0;
	private writeFault?: Fault;
	private barrierFault?: Fault;

	constructor(readonly files = new Map<string, Uint8Array>()) {}

	async read(moveKey: string, slot: CustodyRecordSlot, maxBytes: number) {
		const value = this.files.get(`${moveKey}:${slot}`);
		if (value && value.byteLength > maxBytes) {
			throw new Error("oversized custody record");
		}
		return value ? new Uint8Array(value) : undefined;
	}

	async write(moveKey: string, slot: CustodyRecordSlot, bytes: Uint8Array) {
		this.trip("write");
		const key = `${moveKey}:${slot}`;
		this.writes.push(key);
		this.files.set(key, new Uint8Array(bytes));
	}

	async durableBarrier(moveKey: string, slot: CustodyRecordSlot) {
		this.barriers.push(`${moveKey}:${slot}`);
		this.trip("barrier");
	}

	async close() {
		this.closeCalls++;
	}

	failWrite(afterSuccessfulWrites = 0) {
		this.writeFault = {
			remaining: afterSuccessfulWrites,
			error: new Error("seeded custody write crash"),
		};
	}

	failBarrier(afterSuccessfulBarriers = 0) {
		this.barrierFault = {
			remaining: afterSuccessfulBarriers,
			error: new Error("seeded custody barrier crash"),
		};
	}

	fork() {
		return new StrictCustodyPersistence(this.files);
	}

	private trip(kind: "write" | "barrier") {
		const fault = kind === "write" ? this.writeFault : this.barrierFault;
		if (!fault) return;
		if (fault.remaining > 0) {
			fault.remaining--;
			return;
		}
		if (kind === "write") this.writeFault = undefined;
		else this.barrierFault = undefined;
		throw fault.error;
	}
}

const seededRandom = (seed: number) => {
	let value = seed >>> 0;
	return () => {
		value ^= value << 13;
		value ^= value >>> 17;
		value ^= value << 5;
		return value >>> 0;
	};
};

const digest = (byte: number) =>
	(byte & 0xff).toString(16).padStart(2, "0").repeat(32);

const fenceFor = (store: RebalanceWorkStore): RebalanceWorkFence => {
	const active = store.snapshot().active;
	if (!active) throw new Error("model work is not active");
	return {
		viewId: active.viewId,
		planDigest: active.planDigest,
		installSequence: active.installSequence,
	};
};

const boundaryPlan = (): RebalanceScanPlan<"u32"> => ({
	boundary: true,
	geometryRanges: [],
	ownedIntervals: [],
	taskCount: 1,
	historyMutations: [{ rangeHash: "seeded-four-row-model", present: true }],
});

const candidateBytes = (candidates: readonly RebalanceScanCandidate[]) =>
	candidates.reduce(
		(total, candidate) =>
			total + candidate.hash.length + 1 + candidate.coordinates.length * 4,
		0,
	);

class OneBucketSource implements BoundedRebalanceScanSource {
	readonly calls: RebalanceScanSourceRequest[] = [];
	private returned = false;

	constructor(
		private readonly candidates: readonly RebalanceScanCandidate<"u32">[],
	) {}

	readNextCollisionBucket(request: RebalanceScanSourceRequest) {
		this.calls.push(request);
		if (this.returned) throw new Error("model source read past its one bucket");
		this.returned = true;
		return {
			resolution: "u32" as const,
			eof: false as const,
			hashNumber: HASH_NUMBER,
			candidates: this.candidates,
			visited: this.candidates.length,
			results: this.candidates.length,
			bytes: candidateBytes(this.candidates),
		};
	}
}

describe("seeded pending-visit custody reducer model", () => {
	it("retains one exact attempt across four rows and forced crash/reopen points", async () => {
		const random = seededRandom(MODEL_SEED);
		const sourceKey = await Ed25519Keypair.create();
		const destinationKey = await Ed25519Keypair.create();
		const logId = new Uint8Array(16);
		for (let index = 0; index < logId.length; index++) {
			logId[index] = random() & 0xff;
		}

		const rows = await Promise.all(
			Array.from({ length: MODEL_ROWS }, async (_, ordinal) => {
				const block = new Uint8Array(8 + ordinal);
				for (let index = 0; index < block.length; index++) {
					block[index] = random() & 0xff;
				}
				const hash = (await calculateRawCid(block)).cid;
				const attempt = new Uint8Array(32);
				for (let index = 0; index < attempt.length; index++) {
					attempt[index] = random() & 0xff;
				}
				attempt[0] ||= ordinal + 1;
				return { hash, block, attempt };
			}),
		);
		rows.sort((left, right) => left.hash.localeCompare(right.hash));
		expect(rows).to.have.length(MODEL_ROWS);

		const skipIndex = 1;
		const source = new OneBucketSource(
			rows.map((row) => ({
				hash: row.hash,
				coordinates: [HASH_NUMBER],
				assignedToRangeBoundary: true,
			})),
		);
		const manifestByHash = new Map<string, CanonicalCustodyHandoffManifest>();
		const firstPendingBytes = new Map<string, string>();
		const receipts = new Map<
			string,
			Awaited<ReturnType<typeof createCustodyHandoffReceiptV1>>
		>();
		const prepareCalls = new Map<string, number>();
		const sourcePointReads = new Map<string, number>();
		const networkEffects = new Map<string, number>();
		const custodyMutationAttempts = new Map<string, number>();
		let custodyMutations = 0;
		let viewGuardCalls = 0;
		let recoveryGuardCalls = 0;
		let viewCurrent = true;
		let recoveryCurrent = true;
		let crashAfterNetworkReceipt = false;

		let workPersistence = new StrictWorkPersistence();
		let custodyPersistence = new StrictCustodyPersistence();
		let custody!: CustodyRecordStore;
		let authority!: CustodySourceReceiptAuthority;
		let work!: RebalanceWorkStore;
		let executor!: RebalanceScanExecutor;

		const createManifest = async (
			key: RebalanceVisitKey,
			attempt: Uint8Array,
		) =>
			createCustodyHandoffManifestV1(
				{
					logId,
					entryHash: key.hash,
					entryByteLength: BigInt(
						rows.find((row) => row.hash === key.hash)!.block.byteLength,
					),
					source: sourceKey.publicKey,
					destination: destinationKey.publicKey,
					visit: {
						viewId: key.viewId,
						planDigest: key.planDigest,
						installSequence: key.installSequence,
						taskOrdinal: key.taskOrdinal,
						resolution: "u32",
						hashNumber: key.hashNumber as number,
					},
					ownerPlanId: digest(0x41),
					attemptGeneration: attempt,
				},
				sourceKey.signer(PreHash.SHA_256),
			);

		const assertPendingDurableBeforeEffect = (
			key: RebalanceVisitKey,
			manifest: CanonicalCustodyHandoffManifest,
		) => {
			const snapshot = work.snapshot();
			const pending = snapshot.active?.cursor.pendingVisit;
			expect(pending, "effect requires a named pending row").to.not.equal(
				undefined,
			);
			expect(pending).to.deep.include({
				index: snapshot.active?.cursor.bucket?.nextIndex,
				moveKey: manifest.moveKey,
				handoffId: manifest.handoffId,
			});
			expect(snapshot.active?.cursor.bucket?.hashes[pending!.index]).to.equal(
				key.hash,
			);
			expect(work.currentDurableCommit()).to.not.equal(undefined);
		};

		const bridge = (): RebalanceCustodyVisitBridge => ({
			sourceReceiptAuthority: authority,
			prepare: async ({ key }) => {
				prepareCalls.set(key.hash, (prepareCalls.get(key.hash) ?? 0) + 1);
				// This exact four-key lookup models the future bridge's bounded current
				// point-read. It is the only operation allowed to return an explicit skip.
				sourcePointReads.set(
					key.hash,
					(sourcePointReads.get(key.hash) ?? 0) + 1,
				);
				const rowIndex = rows.findIndex((row) => row.hash === key.hash);
				if (rowIndex === skipIndex) {
					return { status: "skip", bytes: 0 };
				}
				let manifest = manifestByHash.get(key.hash);
				if (!manifest) {
					manifest = await createManifest(
						key,
						rows.find((row) => row.hash === key.hash)!.attempt,
					);
					manifestByHash.set(key.hash, manifest);
				}
				return {
					status: "manifest",
					bytes: manifest.bytes.byteLength,
					manifest: manifest.bytes,
				};
			},
			recoveryGuard: () => {
				recoveryGuardCalls++;
				return recoveryCurrent;
			},
			visit: async ({ key, manifest }) => {
				// Raw custody reducer calls are deliberately confined to this modeled
				// coordinator callback; no production source/network consumer is implied.
				assertPendingDurableBeforeEffect(key, manifest);
				const current = await custody.read(manifest.moveKey);
				if (current.snapshot.state === "absent") {
					custodyMutations++;
					custodyMutationAttempts.set(
						key.hash,
						(custodyMutationAttempts.get(key.hash) ?? 0) + 1,
					);
					await custody.prepareSource(0n, manifest.bytes);
					return { status: "progress", bytes: manifest.bytes.byteLength };
				}
				if (current.snapshot.state === "source-prepared") {
					let receipt = receipts.get(manifest.handoffId);
					if (!receipt) {
						networkEffects.set(
							key.hash,
							(networkEffects.get(key.hash) ?? 0) + 1,
						);
						receipt = await createCustodyHandoffReceiptV1(
							{
								manifest,
								custodyEpoch: new Uint8Array(32).fill(
									rows.findIndex((row) => row.hash === key.hash) + 17,
								),
								pinSequence: BigInt(
									rows.findIndex((row) => row.hash === key.hash) + 1,
								),
							},
							destinationKey.signer(PreHash.SHA_256),
						);
						receipts.set(manifest.handoffId, receipt);
					}
					if (crashAfterNetworkReceipt) {
						crashAfterNetworkReceipt = false;
						throw new Error("seeded crash after network receipt");
					}
					custodyMutations++;
					custodyMutationAttempts.set(
						key.hash,
						(custodyMutationAttempts.get(key.hash) ?? 0) + 1,
					);
					await custody.markSourceReceiptDurable(
						2n,
						manifest.bytes,
						receipt.bytes,
					);
					return { status: "progress", bytes: receipt.bytes.byteLength };
				}
				if (current.snapshot.state !== "source-receipt-durable") {
					throw new Error(`unexpected source state ${current.snapshot.state}`);
				}
				const durableCommit = current.durableCommit;
				if (!durableCommit) throw new Error("terminal row lost strict proof");
				return {
					status: "complete",
					bytes: 0,
					durableCommit: durableCommit as DurableCustodySourceReceiptCommit,
				};
			},
		});

		const makeExecutor = () =>
			new RebalanceScanExecutor({
				store: work,
				source,
				viewGuard: () => {
					viewGuardCalls++;
					return viewCurrent;
				},
				custody: bridge(),
				limits: { maxTickMs: 10_000 },
			});

		const openCustody = async () => {
			custody = await CustodyRecordStore.open({
				persistence: custodyPersistence,
				durability: "strict",
				binding: {
					logId,
					localPublicKey: serialize(sourceKey.publicKey),
					role: "source",
				},
			});
			authority = custody.sourceReceiptAuthority();
		};

		const openWork = async () => {
			work = await RebalanceWorkStore.open({
				persistence: workPersistence,
				durability: "strict",
				custody: { sourceReceiptAuthority: authority, logId },
			});
			executor = makeExecutor();
		};

		let restarts = 0;
		const crashAndReopen = async () => {
			await Promise.allSettled([work.close(), custody.close()]);
			workPersistence = workPersistence.fork();
			custodyPersistence = custodyPersistence.fork();
			await openCustody();
			await openWork();
			restarts++;
		};

		let lastAuditedNextIndex = 0;
		const audit = async () => {
			const snapshot = work.snapshot();
			const active = snapshot.active;
			expect(active, "model must retain installed work").to.not.equal(
				undefined,
			);
			const bucket = active!.cursor.bucket;
			expect(bucket?.hashes).to.deep.equal(rows.map((row) => row.hash));
			const nextIndex = bucket!.nextIndex;
			expect(nextIndex).to.be.at.least(lastAuditedNextIndex);
			expect(nextIndex - lastAuditedNextIndex).to.be.at.most(1);
			const pending = active!.cursor.pendingVisit;
			if (pending) {
				expect(pending.index).to.equal(nextIndex);
				expect(pending.index).to.be.lessThan(MODEL_ROWS);
				const expected = manifestByHash.get(rows[pending.index].hash);
				expect(expected).to.not.equal(undefined);
				expect(pending).to.deep.include({
					moveKey: expected!.moveKey,
					handoffId: expected!.handoffId,
				});
				const retained = firstPendingBytes.get(pending.moveKey);
				if (retained === undefined) {
					firstPendingBytes.set(pending.moveKey, pending.manifestBase64);
				} else {
					expect(pending.manifestBase64).to.equal(retained);
				}
				expect(pending.manifestBase64).to.equal(toBase64(expected!.bytes));
				const decoded = await decodeCustodyHandoffManifestV1(
					fromBase64(pending.manifestBase64),
				);
				expect(decoded).to.deep.include({
					moveKey: pending.moveKey,
					handoffId: pending.handoffId,
					entryHash: rows[pending.index].hash,
				});
				expect(decoded.logId).to.equal(
					Array.from(logId, (byte) => byte.toString(16).padStart(2, "0")).join(
						"",
					),
				);
				expect(decoded.visit).to.deep.equal({
					viewId: active!.viewId,
					planDigest: active!.planDigest,
					installSequence: active!.installSequence,
					taskOrdinal: 0,
					resolution: "u32",
					hashNumber: HASH_NUMBER,
				});
			}

			for (let index = 0; index < MODEL_ROWS; index++) {
				const expected = manifestByHash.get(rows[index].hash)!;
				const record = await custody.read(expected.moveKey);
				const state = record.snapshot.state;
				if (index < nextIndex) {
					if (index === skipIndex) {
						expect(state).to.equal("absent");
					} else {
						expect(state).to.equal("source-receipt-durable");
					}
				} else if (index > nextIndex || !pending) {
					expect(state).to.equal("absent");
				} else {
					expect([
						"absent",
						"source-prepared",
						"source-receipt-durable",
					]).to.include(state);
				}
				if (state === "source-prepared") {
					expect(index).to.equal(nextIndex);
					expect(pending?.moveKey).to.equal(expected.moveKey);
				}
				if (state !== "absent") {
					expect(record.snapshot.manifest).to.equal(toBase64(expected.bytes));
				}
			}
			lastAuditedNextIndex = nextIndex;
			return { nextIndex, pending };
		};

		await openCustody();
		await openWork();
		await work.install(0n, {
			resolution: "u32",
			viewId: digest(0x33),
			plan: boundaryPlan(),
		});
		const frozen = await executor.tick();
		expect(frozen.status).to.equal("bucket-frozen");
		expect(source.calls).to.have.length(1);
		const frozenActive = work.snapshot().active!;
		for (const row of rows) {
			manifestByHash.set(
				row.hash,
				await createManifest(
					{
						...fenceFor(work),
						taskOrdinal: frozenActive.cursor.taskOrdinal,
						hashNumber: frozenActive.cursor.bucket!.hashNumber,
						hash: row.hash,
					},
					row.attempt,
				),
			);
		}
		await audit();

		const pendingAttempts = new Map<number, number>();
		const prepareAttempts = new Map<number, number>();
		const terminalAttempts = new Map<number, number>();
		const completionAttempts = new Map<number, number>();
		const skipAttempts = new Map<number, number>();
		let pendingNegativesDone = false;
		let staleViewRecoveryDone = false;
		let oldReceiptAfterCloseDone = false;
		let secondMirrorBlockedAndHealed = false;
		let steps = 0;

		while (steps++ < MODEL_MAX_STEPS) {
			const stable = await audit();
			if (stable.nextIndex === MODEL_ROWS) break;
			const index = stable.nextIndex;
			const isSkip = index === skipIndex;
			let currentState: string = "absent";
			let expectCrash = false;
			let secondMirrorGate: CrashGate | undefined;
			if (stable.pending) {
				const expected = manifestByHash.get(rows[index].hash)!;
				currentState = (await custody.read(expected.moveKey)).snapshot.state;
			}

			if (!stable.pending && isSkip) {
				const attempt = skipAttempts.get(index) ?? 0;
				skipAttempts.set(index, attempt + 1);
				if (attempt === 0) {
					workPersistence.failWrite();
					expectCrash = true;
				} else if (attempt === 1) {
					workPersistence.failBarrier();
					expectCrash = true;
				}
			} else if (!stable.pending) {
				const attempt = pendingAttempts.get(index) ?? 0;
				pendingAttempts.set(index, attempt + 1);
				if (index === 0 && attempt === 0) {
					workPersistence.failWrite();
					expectCrash = true;
				} else if (index === 0 && attempt === 1) {
					// The second mirror is written before this forced barrier failure.
					secondMirrorGate = workPersistence.blockBarrierAndLoseWrite(1);
					expectCrash = true;
				}
			} else if (currentState === "absent") {
				const attempt = prepareAttempts.get(index) ?? 0;
				prepareAttempts.set(index, attempt + 1);
				if (index === 0 && attempt === 0) {
					custodyPersistence.failWrite();
					expectCrash = true;
				} else if (index === 0 && attempt === 1) {
					// Baseline barrier succeeds; the prepared-frame barrier is ambiguous.
					custodyPersistence.failBarrier(1);
					expectCrash = true;
				}
			} else if (currentState === "source-prepared") {
				const attempt = terminalAttempts.get(index) ?? 0;
				terminalAttempts.set(index, attempt + 1);
				if (index === 0 && attempt === 0) {
					crashAfterNetworkReceipt = true;
					expectCrash = true;
				} else if (index === 0 && attempt === 1) {
					custodyPersistence.failWrite();
					expectCrash = true;
				} else if (index === 0 && attempt === 2) {
					// Callback read + reducer read confirm prepared before terminal write.
					custodyPersistence.failBarrier(2);
					expectCrash = true;
				}
			} else if (currentState === "source-receipt-durable") {
				const attempt = completionAttempts.get(index) ?? 0;
				completionAttempts.set(index, attempt + 1);
				if (index === 0 && attempt === 0) {
					workPersistence.failWrite();
					expectCrash = true;
				} else if (index === 0 && attempt === 1) {
					workPersistence.failBarrier();
					expectCrash = true;
				}
			}

			if (stable.pending && index === 2 && !pendingNegativesDone) {
				pendingNegativesDone = true;
				const snapshot = work.snapshot();
				const active = snapshot.active!;
				const pending = active.cursor.pendingVisit!;
				const manifest = manifestByHash.get(rows[index].hash)!;
				const changedAttempt = new Uint8Array(rows[index].attempt);
				changedAttempt[0] ^= 0xff;
				const changed = await createManifest(
					{
						...fenceFor(work),
						taskOrdinal: active.cursor.taskOrdinal,
						hashNumber: active.cursor.bucket!.hashNumber,
						hash: rows[index].hash,
					},
					changedAttempt,
				);
				expect(changed.moveKey).to.equal(manifest.moveKey);
				expect(changed.handoffId).to.not.equal(manifest.handoffId);
				await expect(
					work.preparePendingVisit(
						fenceFor(work),
						snapshot.revision,
						index,
						changed.bytes,
					),
				).to.be.rejectedWith("different pending visit");
				expect(work.snapshot().active?.cursor.pendingVisit).to.deep.equal(
					pending,
				);
				await expect(
					work.checkpoint(fenceFor(work), snapshot.revision, {
						taskOrdinal: active.cursor.taskOrdinal,
						bucket: {
							...active.cursor.bucket!,
							nextIndex: index + 1,
						},
					}),
				).to.be.rejectedWith("dedicated visit transition");
				await expect(
					work.install(snapshot.revision, {
						resolution: "u32",
						viewId: digest(0x44),
						plan: boundaryPlan(),
					}),
				).to.be.rejectedWith("pending visit");
				await expect(
					work.clear(fenceFor(work), snapshot.revision),
				).to.be.rejectedWith("pending visit");
				const priorManifest = manifestByHash.get(rows[0].hash)!;
				const priorToken = (
					await custody.readSourceReceipt(priorManifest.moveKey)
				).durableCommit!;
				await expect(
					work.completePendingVisit(
						fenceFor(work),
						snapshot.revision,
						priorToken,
					),
				).to.be.rejectedWith("does not match the pending visit");

				const unpairedPersistence = workPersistence.fork();
				await expect(
					RebalanceWorkStore.open({
						persistence: unpairedPersistence,
						durability: "strict",
					}),
				).to.be.rejectedWith("paired source custody store");
				expect(unpairedPersistence.closeCalls).to.equal(1);

				const filesBeforeDrop = [...workPersistence.files.entries()].map(
					([name, bytes]) => [name, toBase64(bytes)] as const,
				);
				await expect(work.beginDrop()).to.be.rejectedWith("pending visit");
				expect(() => work.snapshot()).to.throw("dropping");
				expect(
					[...workPersistence.files.entries()].map(
						([name, bytes]) => [name, toBase64(bytes)] as const,
					),
				).to.deep.equal(filesBeforeDrop);
				await expect(work.close()).to.be.rejectedWith("pending visit");
				workPersistence = workPersistence.fork();
				await openWork();
				expect(work.snapshot().active?.cursor.pendingVisit).to.deep.equal(
					pending,
				);

				recoveryCurrent = false;
				const mutationsBeforeGuard = custodyMutations;
				const viewCallsBeforeGuard = viewGuardCalls;
				await expect(executor.tick()).to.be.rejectedWith(
					"recovery namespace is no longer current",
				);
				expect(custodyMutations).to.equal(mutationsBeforeGuard);
				expect(viewGuardCalls).to.equal(viewCallsBeforeGuard);
				recoveryCurrent = true;
				viewCurrent = false;
				const viewCallsBeforeResume = viewGuardCalls;
				const resumed = await executor.tick();
				expect(resumed.status).to.equal("visit-progress");
				expect(viewGuardCalls).to.equal(viewCallsBeforeResume);
				viewCurrent = true;
				staleViewRecoveryDone = true;
				await audit();
				continue;
			}

			if (
				stable.pending &&
				index === 3 &&
				currentState === "source-receipt-durable" &&
				!oldReceiptAfterCloseDone
			) {
				oldReceiptAfterCloseDone = true;
				const token = (await custody.readSourceReceipt(stable.pending.moveKey))
					.durableCommit;
				if (!token) throw new Error("missing old strict receipt token");
				const oldAuthority = authority;
				await custody.close();
				await work.completePendingVisit(
					fenceFor(work),
					work.snapshot().revision,
					token,
				);
				await expect(
					RebalanceWorkStore.open({
						persistence: workPersistence.fork(),
						durability: "strict",
						custody: { sourceReceiptAuthority: oldAuthority, logId },
					}),
				).to.be.rejectedWith("Invalid custody source receipt authority");
				await work.close();
				workPersistence = workPersistence.fork();
				custodyPersistence = custodyPersistence.fork();
				await openCustody();
				await openWork();
				await audit();
				continue;
			}

			if (secondMirrorGate) {
				const before = work.snapshot();
				const beforeCommit = work.currentDurableCommit();
				const mutationsBefore = custodyMutations;
				const ticking = executor.tick();
				await secondMirrorGate.entered;
				// Neither the effect-admitting snapshot nor its durable commit is
				// published while mirror #2 is still outside its named barrier.
				expect(work.snapshot()).to.deep.equal(before);
				expect(work.currentDurableCommit()).to.deep.equal(beforeCommit);
				expect(work.snapshot().active?.cursor.pendingVisit).to.equal(undefined);
				secondMirrorGate.release();
				await expect(ticking).to.be.rejectedWith(
					"Failed to persist rebalance work frame",
				);
				await crashAndReopen();
				const healed = work.snapshot().active?.cursor.pendingVisit;
				expect(healed).to.not.equal(undefined);
				expect(custodyMutations).to.equal(mutationsBefore);
				expect(workPersistence.writes).to.have.length(1);
				const workFrames = [...workPersistence.files.values()].map(toBase64);
				expect(workFrames).to.have.length(2);
				expect(new Set(workFrames).size).to.equal(1);
				secondMirrorBlockedAndHealed = true;
				continue;
			}

			try {
				await executor.tick();
			} catch (error) {
				if (!expectCrash) throw error;
				await crashAndReopen();
				continue;
			}
			if (expectCrash) expect.fail("seeded crash point did not reject");

			// Seeded extra restarts explore stable recovery boundaries as well as the
			// forced old/new outcomes above.
			if (random() % 7 === 0 && work.snapshot().active?.cursor.bucket) {
				await crashAndReopen();
			}
		}

		const final = await audit();
		expect(final.nextIndex).to.equal(MODEL_ROWS);
		expect(final.pending).to.equal(undefined);
		expect(steps).to.be.at.most(MODEL_MAX_STEPS);
		expect(restarts).to.be.greaterThan(0);
		expect(staleViewRecoveryDone).to.equal(true);
		expect(pendingNegativesDone).to.equal(true);
		expect(oldReceiptAfterCloseDone).to.equal(true);
		expect(secondMirrorBlockedAndHealed).to.equal(true);
		expect(recoveryGuardCalls).to.be.greaterThan(0);
		expect(skipAttempts.get(skipIndex)).to.equal(2);
		expect(networkEffects.get(rows[skipIndex].hash) ?? 0).to.equal(0);
		expect(custodyMutationAttempts.get(rows[skipIndex].hash) ?? 0).to.equal(0);
		for (let index = 0; index < MODEL_ROWS; index++) {
			if (index === skipIndex) continue;
			expect(networkEffects.get(rows[index].hash)).to.equal(1);
			expect(
				firstPendingBytes.get(manifestByHash.get(rows[index].hash)!.moveKey),
			).to.equal(toBase64(manifestByHash.get(rows[index].hash)!.bytes));
		}
		expect(prepareCalls.get(rows[0].hash)).to.equal(2);
		expect(prepareCalls.get(rows[2].hash)).to.equal(1);
		expect(prepareCalls.get(rows[3].hash)).to.equal(1);
		for (const row of rows) {
			expect(sourcePointReads.get(row.hash)).to.equal(
				prepareCalls.get(row.hash),
			);
		}
		const replacement = await work.install(work.snapshot().revision, {
			resolution: "u32",
			viewId: digest(0x45),
			plan: boundaryPlan(),
		});
		expect(replacement.snapshot.active?.viewId).to.equal(digest(0x45));
		expect(replacement.snapshot.active?.cursor.pendingVisit).to.equal(
			undefined,
		);

		await Promise.all([work.close(), custody.close()]);
	});

	it("rejects forged, wrong-generation, and ineligible source capabilities", async () => {
		const source = await Ed25519Keypair.create();
		const destination = await Ed25519Keypair.create();
		const logId = new Uint8Array([9, 8, 7]);
		const hash = (await calculateRawCid(new Uint8Array([6, 5, 4]))).cid;
		const strict = await CustodyRecordStore.open({
			persistence: new StrictCustodyPersistence(),
			durability: "strict",
			binding: {
				logId,
				localPublicKey: serialize(source.publicKey),
				role: "source",
			},
		});
		const authority = strict.sourceReceiptAuthority();
		const racingCustody = await CustodyRecordStore.open({
			persistence: new StrictCustodyPersistence(),
			durability: "strict",
			binding: {
				logId,
				localPublicKey: serialize(source.publicKey),
				role: "source",
			},
		});
		const racingAuthority = racingCustody.sourceReceiptAuthority();
		const racingWorkPersistence = new StrictWorkPersistence();
		const readGate = racingWorkPersistence.blockNextRead();
		const racingOpen = RebalanceWorkStore.open({
			persistence: racingWorkPersistence,
			durability: "strict",
			custody: { sourceReceiptAuthority: racingAuthority, logId },
		});
		await readGate.entered;
		await racingCustody.close();
		readGate.release();
		await expect(racingOpen).to.be.rejectedWith(
			"Invalid custody source receipt authority",
		);
		expect(racingWorkPersistence.closeCalls).to.equal(1);
		await expect(
			RebalanceWorkStore.open({
				persistence: new StrictWorkPersistence(),
				durability: "strict",
				custody: {
					sourceReceiptAuthority: authority,
					logId: new Uint8Array([9, 8, 6]),
				},
			}),
		).to.be.rejectedWith("authority log mismatch");
		const workPersistence = new StrictWorkPersistence();
		const work = await RebalanceWorkStore.open({
			persistence: workPersistence,
			durability: "strict",
			custody: { sourceReceiptAuthority: authority, logId },
		});
		await work.install(0n, {
			resolution: "u32",
			viewId: digest(0x51),
			plan: boundaryPlan(),
		});
		let manifest!: CanonicalCustodyHandoffManifest;
		let genuine!: DurableCustodySourceReceiptCommit;
		const sourceBucket = new OneBucketSource([
			{
				hash,
				coordinates: [HASH_NUMBER],
				assignedToRangeBoundary: true,
			},
		]);
		expect(
			() =>
				new RebalanceScanExecutor({
					store: work,
					source: sourceBucket,
					viewGuard: () => true,
					visit: () => ({ bytes: 0 }),
				}),
		).to.throw("custody-paired work store cannot use the legacy visitor");
		const unpairedWork = await RebalanceWorkStore.open({
			persistence: new StrictWorkPersistence(),
			durability: "strict",
		});
		expect(
			() =>
				new RebalanceScanExecutor({
					store: unpairedWork,
					source: sourceBucket,
					viewGuard: () => true,
					custody: {
						sourceReceiptAuthority: authority,
						prepare: () => ({ status: "skip", bytes: 0 }),
						recoveryGuard: () => true,
						visit: () => ({ status: "progress", bytes: 0 }),
					},
				}),
		).to.throw("does not match its paired work store");
		await unpairedWork.close();
		const executor = new RebalanceScanExecutor({
			store: work,
			source: sourceBucket,
			viewGuard: () => true,
			custody: {
				sourceReceiptAuthority: authority,
				prepare: () => ({
					status: "manifest",
					bytes: manifest.bytes.byteLength,
					manifest: manifest.bytes,
				}),
				recoveryGuard: () => true,
				visit: () => ({ status: "complete", bytes: 0, durableCommit: genuine }),
			},
			limits: { maxTickMs: 10_000 },
		});
		await executor.tick();
		const active = work.snapshot().active!;
		manifest = await createCustodyHandoffManifestV1(
			{
				logId,
				entryHash: hash,
				entryByteLength: 3n,
				source: source.publicKey,
				destination: destination.publicKey,
				visit: {
					viewId: active.viewId,
					planDigest: active.planDigest,
					installSequence: active.installSequence,
					taskOrdinal: active.cursor.taskOrdinal,
					resolution: "u32",
					hashNumber: active.cursor.bucket!.hashNumber as number,
				},
				ownerPlanId: digest(0x53),
				attemptGeneration: new Uint8Array(32).fill(0x54),
			},
			source.signer(PreHash.SHA_256),
		);
		const receipt = await createCustodyHandoffReceiptV1(
			{
				manifest,
				custodyEpoch: new Uint8Array(32).fill(0x55),
				pinSequence: 1n,
			},
			destination.signer(PreHash.SHA_256),
		);
		await strict.prepareSource(0n, manifest.bytes);
		genuine = (
			await strict.markSourceReceiptDurable(2n, manifest.bytes, receipt.bytes)
		).durableCommit!;
		await executor.tick();
		let invalidGuardVisits = 0;
		const invalidGuardExecutor = new RebalanceScanExecutor({
			store: work,
			source: sourceBucket,
			viewGuard: () => true,
			custody: {
				sourceReceiptAuthority: authority,
				prepare: () => ({ status: "skip", bytes: 0 }),
				recoveryGuard: () => "truthy" as unknown as boolean,
				visit: () => {
					invalidGuardVisits++;
					return { status: "progress", bytes: 0 };
				},
			},
		});
		await expect(invalidGuardExecutor.tick()).to.be.rejectedWith(
			"Invalid rebalance custody recovery guard result",
		);
		expect(invalidGuardVisits).to.equal(0);
		const snapshot = work.snapshot();
		const forged = { ...genuine } as DurableCustodySourceReceiptCommit;
		await expect(
			work.completePendingVisit(fenceFor(work), snapshot.revision, forged),
		).to.be.rejectedWith("Invalid durable custody source receipt commit");

		const otherStrict = await CustodyRecordStore.open({
			persistence: new StrictCustodyPersistence(),
			durability: "strict",
			binding: {
				logId,
				localPublicKey: serialize(source.publicKey),
				role: "source",
			},
		});
		const otherAuthority = otherStrict.sourceReceiptAuthority();
		expect(
			() =>
				new RebalanceScanExecutor({
					store: work,
					source: sourceBucket,
					viewGuard: () => true,
					custody: {
						sourceReceiptAuthority: otherAuthority,
						prepare: () => ({ status: "skip", bytes: 0 }),
						recoveryGuard: () => true,
						visit: () => ({ status: "progress", bytes: 0 }),
					},
				}),
		).to.throw("does not match its paired work store");
		await otherStrict.prepareSource(0n, manifest.bytes);
		const wrongAuthorityCommit = (
			await otherStrict.markSourceReceiptDurable(
				2n,
				manifest.bytes,
				receipt.bytes,
			)
		).durableCommit!;
		await expect(
			work.completePendingVisit(
				fenceFor(work),
				snapshot.revision,
				wrongAuthorityCommit,
			),
		).to.be.rejectedWith("commit authority");

		await work.completePendingVisit(
			fenceFor(work),
			work.snapshot().revision,
			genuine,
		);
		const completionFile = workPersistence.writes.at(-1)!;
		const pendingFile = REBALANCE_WORK_FILES.find(
			(name) => name !== completionFile,
		)!;
		await work.close();
		const cloneFiles = () =>
			new Map(
				[...workPersistence.files.entries()].map(
					([name, bytes]) => [name, new Uint8Array(bytes)] as const,
				),
			);
		const completionCorruptFiles = cloneFiles();
		completionCorruptFiles.set(completionFile, new Uint8Array([1, 2, 3]));
		const fallback = await RebalanceWorkStore.open({
			persistence: new StrictWorkPersistence(completionCorruptFiles),
			durability: "strict",
			custody: { sourceReceiptAuthority: authority, logId },
		});
		expect(fallback.snapshot().active?.cursor.pendingVisit).to.not.equal(
			undefined,
		);
		expect(fallback.snapshot().active?.cursor.bucket?.nextIndex).to.equal(0);
		await fallback.close();

		const pendingCorruptFiles = cloneFiles();
		pendingCorruptFiles.set(pendingFile, new Uint8Array([4, 5, 6]));
		const fallForward = await RebalanceWorkStore.open({
			persistence: new StrictWorkPersistence(pendingCorruptFiles),
			durability: "strict",
			custody: { sourceReceiptAuthority: authority, logId },
		});
		expect(fallForward.snapshot().active?.cursor.pendingVisit).to.equal(
			undefined,
		);
		expect(fallForward.snapshot().active?.cursor.bucket?.nextIndex).to.equal(1);
		await fallForward.close();

		const memory = await CustodyRecordStore.open({
			persistence: new MemoryCustodyRecordPersistence(),
			durability: "memory",
			binding: {
				logId,
				localPublicKey: serialize(source.publicKey),
				role: "source",
			},
		});
		await expect(
			Promise.resolve().then(() => memory.sourceReceiptAuthority()),
		).to.be.rejectedWith("strict durability");
		const unbound = await CustodyRecordStore.open({
			persistence: new StrictCustodyPersistence(),
			durability: "strict",
		});
		await expect(
			Promise.resolve().then(() => unbound.sourceReceiptAuthority()),
		).to.be.rejectedWith("source binding");
		const destinationBound = await CustodyRecordStore.open({
			persistence: new StrictCustodyPersistence(),
			durability: "strict",
			binding: {
				logId,
				localPublicKey: serialize(destination.publicKey),
				role: "destination",
			},
		});
		await expect(
			Promise.resolve().then(() => destinationBound.sourceReceiptAuthority()),
		).to.be.rejectedWith("source binding");

		const runtimeMemory = memory as unknown as Record<string, unknown>;
		const stolen = Object.values(runtimeMemory).find(
			(value) => value && typeof value === "object",
		) as CustodySourceReceiptAuthority | undefined;
		expect(stolen).to.not.equal(undefined);
		await expect(
			RebalanceWorkStore.open({
				persistence: new StrictWorkPersistence(),
				durability: "strict",
				custody: { sourceReceiptAuthority: stolen!, logId },
			}),
		).to.be.rejectedWith("Invalid custody source receipt authority");

		await Promise.all([
			strict.close(),
			otherStrict.close(),
			memory.close(),
			unbound.close(),
			destinationBound.close(),
		]);
	});
});
