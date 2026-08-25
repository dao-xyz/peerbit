import { calculateRawCid } from "@peerbit/blocks-interface";
import { Ed25519Keypair, PreHash, toBase64 } from "@peerbit/crypto";
import { expect } from "chai";
import {
	CUSTODY_HANDOFF_PROFILE_ID,
	CUSTODY_HANDOFF_PROFILE_MASK,
	type CanonicalCustodyHandoffManifest,
	type CanonicalCustodyHandoffReceipt,
	createCustodyHandoffManifestV1,
	createCustodyHandoffReceiptV1,
} from "../src/custody-handoff-codec.js";
import {
	type CustodyRecordPersistence,
	type CustodyRecordSlot,
	CustodyRecordStore,
	MemoryCustodyRecordPersistence,
	issueCustodyDestinationPinEvidenceForTest,
} from "../src/custody-store.js";

type Gate = Readonly<{
	entered: Promise<void>;
	release: () => void;
}>;

class StrictMemoryPersistence implements CustodyRecordPersistence {
	readonly reads: string[] = [];
	readonly writes: string[] = [];
	readonly barriers: string[] = [];
	closeCalls = 0;
	private writeFailure: unknown;
	private barrierFailure: unknown;
	private barrierFailureAfter = 0;
	private closeShouldFail = false;
	private closeFailure: unknown;
	private readonly readFailures = new Map<string, unknown>();
	private readGate?: {
		key: string;
		entered: () => void;
		wait: Promise<void>;
	};
	private barrierGate?: {
		remaining: number;
		entered: () => void;
		wait: Promise<void>;
	};

	constructor(readonly files = new Map<string, Uint8Array>()) {}

	async read(moveKey: string, slot: CustodyRecordSlot, maxBytes: number) {
		const key = this.key(moveKey, slot);
		this.reads.push(key);
		const gate = this.readGate;
		if (gate?.key === key) {
			this.readGate = undefined;
			gate.entered();
			await gate.wait;
		}
		if (this.readFailures.has(key)) {
			const failure = this.readFailures.get(key);
			this.readFailures.delete(key);
			throw failure;
		}
		const value = this.files.get(key);
		if (value && value.byteLength > maxBytes) {
			throw new Error("read bound exceeded");
		}
		return value ? new Uint8Array(value) : undefined;
	}

	async write(moveKey: string, slot: CustodyRecordSlot, bytes: Uint8Array) {
		if (this.writeFailure !== undefined) {
			const failure = this.writeFailure;
			this.writeFailure = undefined;
			throw failure;
		}
		const key = this.key(moveKey, slot);
		this.writes.push(key);
		this.files.set(key, new Uint8Array(bytes));
	}

	async durableBarrier(moveKey: string, slot: CustodyRecordSlot) {
		this.barriers.push(this.key(moveKey, slot));
		const gate = this.barrierGate;
		if (gate && gate.remaining > 0) {
			gate.remaining--;
		} else if (gate) {
			this.barrierGate = undefined;
			gate.entered();
			await gate.wait;
		}
		if (this.barrierFailure !== undefined) {
			if (this.barrierFailureAfter > 0) {
				this.barrierFailureAfter--;
				return;
			}
			const failure = this.barrierFailure;
			this.barrierFailure = undefined;
			throw failure;
		}
	}

	async close() {
		this.closeCalls++;
		if (this.closeShouldFail) {
			this.closeShouldFail = false;
			const failure = this.closeFailure;
			this.closeFailure = undefined;
			throw failure;
		}
	}

	failNextWrite(error: unknown) {
		this.writeFailure = error;
	}

	failNextRead(moveKey: string, slot: CustodyRecordSlot, error: unknown) {
		this.readFailures.set(this.key(moveKey, slot), error);
	}

	failNextBarrier(error: unknown, afterSuccessfulBarriers = 0) {
		this.barrierFailure = error;
		this.barrierFailureAfter = afterSuccessfulBarriers;
	}

	failClose(error: unknown) {
		this.closeShouldFail = true;
		this.closeFailure = error;
	}

	blockNextRead(moveKey: string, slot: CustodyRecordSlot): Gate {
		let entered!: () => void;
		let release!: () => void;
		const enteredPromise = new Promise<void>((resolve) => {
			entered = resolve;
		});
		const wait = new Promise<void>((resolve) => {
			release = resolve;
		});
		this.readGate = { key: this.key(moveKey, slot), entered, wait };
		return { entered: enteredPromise, release };
	}

	blockNextBarrier(afterSuccessfulBarriers = 0): Gate {
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

	corrupt(moveKey: string, slot: CustodyRecordSlot) {
		this.files.set(this.key(moveKey, slot), new Uint8Array([1, 2, 3]));
	}

	fork() {
		return new StrictMemoryPersistence(this.files);
	}

	private key(moveKey: string, slot: CustodyRecordSlot) {
		return `${moveKey}:${slot}`;
	}
}

const digest = (byte: number) => byte.toString(16).padStart(2, "0").repeat(32);

describe("custody record store", () => {
	let source: Ed25519Keypair;
	let destination: Ed25519Keypair;
	let entryHash: string;

	before(async () => {
		[source, destination] = await Promise.all([
			Ed25519Keypair.create(),
			Ed25519Keypair.create(),
		]);
		entryHash = (await calculateRawCid(new Uint8Array([1, 2, 3]))).cid;
	});

	const manifest = async (
		attempt = 1,
	): Promise<CanonicalCustodyHandoffManifest> =>
		createCustodyHandoffManifestV1(
			{
				logId: new Uint8Array([4, 5, 6]),
				entryHash,
				entryByteLength: 3n,
				source: source.publicKey,
				destination: destination.publicKey,
				visit: {
					viewId: digest(10),
					planDigest: digest(11),
					installSequence: 7n,
					taskOrdinal: 2,
					resolution: "u32",
					hashNumber: 99,
				},
				ownerPlanId: digest(12),
				attemptGeneration: new Uint8Array(32).fill(attempt),
			},
			source.signer(PreHash.SHA_256),
		);

	const receipt = async (
		value: CanonicalCustodyHandoffManifest,
		pinSequence = 5n,
	): Promise<CanonicalCustodyHandoffReceipt> =>
		createCustodyHandoffReceiptV1(
			{
				manifest: value,
				custodyEpoch: new Uint8Array(32).fill(21),
				pinSequence,
			},
			destination.signer(PreHash.SHA_256),
		);

	const pinEvidence = (
		value: CanonicalCustodyHandoffManifest,
		valueReceipt: CanonicalCustodyHandoffReceipt,
	) =>
		issueCustodyDestinationPinEvidenceForTest({
			moveKey: value.moveKey,
			handoffId: value.handoffId,
			custodyProfileId: CUSTODY_HANDOFF_PROFILE_ID,
			custodyProfileMask: CUSTODY_HANDOFF_PROFILE_MASK,
			custodyEpoch: valueReceipt.custodyEpoch,
			pinSequence: valueReceipt.pinSequence,
			compositeCommitId: digest(31),
		});

	it("persists the exact source lifecycle with an explicit baseline and idempotent retries", async () => {
		const persistence = new StrictMemoryPersistence();
		const store = await CustodyRecordStore.open({
			persistence,
			durability: "strict",
		});
		const value = await manifest();
		const valueReceipt = await receipt(value);

		expect((await store.read(value.moveKey)).snapshot).to.deep.equal({
			moveKey: value.moveKey,
			revision: 0n,
			durability: "strict",
			state: "absent",
		});
		const prepared = await store.prepareSource(0n, value.bytes);
		expect(prepared.snapshot).to.deep.include({
			moveKey: value.moveKey,
			revision: 2n,
			state: "source-prepared",
			manifest: toBase64(value.bytes),
		});
		expect(prepared.durableCommit).to.deep.include({
			revision: 2n,
			state: "source-prepared",
		});
		expect(persistence.writes).to.deep.equal([
			`${value.moveKey}:b`,
			`${value.moveKey}:a`,
		]);
		expect(persistence.barriers.slice(-2)).to.deep.equal(persistence.writes);

		const writesBeforeRetry = persistence.writes.length;
		const retried = await store.prepareSource(0n, value.bytes);
		expect(retried.snapshot.revision).to.equal(2n);
		expect(persistence.writes).to.have.length(writesBeforeRetry);

		const durable = await store.markSourceReceiptDurable(
			2n,
			value.bytes,
			valueReceipt.bytes,
		);
		expect(durable.snapshot).to.deep.include({
			revision: 3n,
			state: "source-receipt-durable",
			receipt: toBase64(valueReceipt.bytes),
		});
		const terminalWrites = persistence.writes.length;
		expect(
			(
				await store.markSourceReceiptDurable(
					2n,
					value.bytes,
					valueReceipt.bytes,
				)
			).snapshot.revision,
		).to.equal(3n);
		expect(persistence.writes).to.have.length(terminalWrites);

		await store.close();
		const reopenedPersistence = persistence.fork();
		const reopened = await CustodyRecordStore.open({
			persistence: reopenedPersistence,
			durability: "strict",
		});
		const recovered = await reopened.read(value.moveKey);
		expect(recovered.snapshot).to.deep.equal(durable.snapshot);
		expect(reopenedPersistence.barriers.at(-1)).to.equal(`${value.moveKey}:b`);
		await reopened.close();
	});

	it("requires opaque composite pin evidence before a destination receipt", async () => {
		const persistence = new StrictMemoryPersistence();
		const store = await CustodyRecordStore.open({
			persistence,
			durability: "strict",
		});
		const value = await manifest();
		const valueReceipt = await receipt(value);
		const collecting = await store.beginDestination(0n, value.bytes);
		expect(collecting.snapshot.state).to.equal("destination-collecting");

		await expect(
			store.markDestinationReceipted(2n, value.bytes, valueReceipt.bytes),
		).to.be.rejectedWith(
			"Invalid custody record transition destination-collecting -> destination-receipted",
		);
		await expect(
			store.markDestinationPinned(2n, value.bytes, {} as never),
		).to.be.rejectedWith("Invalid custody destination pin evidence");

		const evidence = pinEvidence(value, valueReceipt);
		const pinned = await store.markDestinationPinned(2n, value.bytes, evidence);
		expect(pinned.snapshot).to.deep.include({
			revision: 3n,
			state: "destination-pinned",
		});
		expect(pinned.snapshot.pin).to.deep.equal({
			moveKey: value.moveKey,
			handoffId: value.handoffId,
			custodyProfileId: CUSTODY_HANDOFF_PROFILE_ID,
			custodyProfileMask: CUSTODY_HANDOFF_PROFILE_MASK,
			custodyEpoch: valueReceipt.custodyEpoch,
			pinSequence: valueReceipt.pinSequence,
			compositeCommitId: digest(31),
		});

		const receipted = await store.markDestinationReceipted(
			3n,
			value.bytes,
			valueReceipt.bytes,
		);
		expect(receipted.snapshot).to.deep.include({
			revision: 4n,
			state: "destination-receipted",
			receipt: toBase64(valueReceipt.bytes),
		});
		const writes = persistence.writes.length;
		expect(
			(
				await store.markDestinationReceipted(
					3n,
					value.bytes,
					valueReceipt.bytes,
				)
			).snapshot.revision,
		).to.equal(4n);
		expect(persistence.writes).to.have.length(writes);
		await store.close();
	});

	it("rejects stale, skipped, cross-side, changed-attempt, and mismatched-pin transitions without writes", async () => {
		const persistence = new StrictMemoryPersistence();
		const store = await CustodyRecordStore.open({
			persistence,
			durability: "strict",
		});
		const first = await manifest(1);
		const secondAttempt = await manifest(2);
		expect(secondAttempt.moveKey).to.equal(first.moveKey);
		expect(secondAttempt.handoffId).not.to.equal(first.handoffId);
		await store.prepareSource(0n, first.bytes);
		const writes = persistence.writes.length;

		await expect(store.prepareSource(1n, secondAttempt.bytes)).to.be.rejected;
		await expect(store.beginDestination(2n, first.bytes)).to.be.rejectedWith(
			"Invalid custody record transition source-prepared -> destination-collecting",
		);
		const valueReceipt = await receipt(first);
		const wrongEvidence = issueCustodyDestinationPinEvidenceForTest({
			moveKey: first.moveKey,
			handoffId: first.handoffId,
			custodyProfileId: CUSTODY_HANDOFF_PROFILE_ID,
			custodyProfileMask: CUSTODY_HANDOFF_PROFILE_MASK,
			custodyEpoch: valueReceipt.custodyEpoch,
			pinSequence: valueReceipt.pinSequence + 1n,
			compositeCommitId: digest(31),
		});
		await expect(store.markDestinationPinned(2n, first.bytes, wrongEvidence)).to
			.be.rejected;
		expect(persistence.writes).to.have.length(writes);
		await store.close();
	});

	it("bounds admission synchronously and close drains only already admitted work", async () => {
		const persistence = new StrictMemoryPersistence();
		const store = await CustodyRecordStore.open({
			persistence,
			durability: "strict",
			limits: { maxPendingOperations: 2 },
		});
		const value = await manifest();
		const gate = persistence.blockNextRead(value.moveKey, "a");
		const first = store.read(value.moveKey);
		await gate.entered;
		const second = store.read(value.moveKey);
		await expect(store.read(value.moveKey)).to.be.rejectedWith(
			"Custody pending-operation bound exceeded",
		);
		const closing = store.close();
		await expect(store.read(value.moveKey)).to.be.rejectedWith(
			"Custody record store is closing",
		);
		let closed = false;
		void closing.then(() => {
			closed = true;
		});
		await Promise.resolve();
		expect(closed).to.equal(false);
		gate.release();
		await Promise.all([first, second, closing]);
		expect(persistence.closeCalls).to.equal(1);
	});

	it("poisons an ambiguous barrier failure and recovers the durable baseline on reopen", async () => {
		const persistence = new StrictMemoryPersistence();
		const store = await CustodyRecordStore.open({
			persistence,
			durability: "strict",
		});
		const value = await manifest();
		persistence.failNextBarrier(new Error("barrier failed"));
		await expect(store.prepareSource(0n, value.bytes)).to.be.rejectedWith(
			"Failed to persist custody record frame",
		);
		await expect(store.read(value.moveKey)).to.be.rejectedWith(
			"Custody record store is poisoned",
		);
		await expect(store.close()).to.be.rejectedWith(
			"Custody record store is poisoned",
		);

		const reopenedPersistence = persistence.fork();
		const reopened = await CustodyRecordStore.open({
			persistence: reopenedPersistence,
			durability: "strict",
		});
		const recovered = await reopened.read(value.moveKey);
		expect(recovered.snapshot).to.deep.equal({
			moveKey: value.moveKey,
			revision: 1n,
			durability: "strict",
			state: "absent",
		});
		expect(recovered.durableCommit?.revision).to.equal(1n);
		expect(
			(await reopened.prepareSource(0n, value.bytes)).snapshot.revision,
		).to.equal(2n);
		await reopened.close();
	});

	it("never returns a target before its barrier and resolves target-barrier ambiguity on reopen", async () => {
		const value = await manifest();
		const blockedPersistence = new StrictMemoryPersistence();
		const blockedStore = await CustodyRecordStore.open({
			persistence: blockedPersistence,
			durability: "strict",
		});
		// The first barrier confirms seq1 absent; block the following seq2 target.
		const gate = blockedPersistence.blockNextBarrier(1);
		let settled = false;
		const preparing = blockedStore.prepareSource(0n, value.bytes).then(
			(result) => {
				settled = true;
				return result;
			},
			(error) => {
				settled = true;
				throw error;
			},
		);
		await gate.entered;
		expect(blockedPersistence.writes).to.have.length(2);
		expect(settled).to.equal(false);
		gate.release();
		expect((await preparing).snapshot.state).to.equal("source-prepared");
		await blockedStore.close();
		const confirmed = await CustodyRecordStore.open({
			persistence: blockedPersistence.fork(),
			durability: "strict",
		});
		expect((await confirmed.read(value.moveKey)).snapshot.state).to.equal(
			"source-prepared",
		);
		await confirmed.close();

		const failedPersistence = new StrictMemoryPersistence();
		const failedStore = await CustodyRecordStore.open({
			persistence: failedPersistence,
			durability: "strict",
		});
		failedPersistence.failNextBarrier(new Error("target barrier failed"), 1);
		await expect(failedStore.prepareSource(0n, value.bytes)).to.be.rejectedWith(
			"Failed to persist custody record frame",
		);
		await expect(failedStore.close()).to.be.rejectedWith(
			"Custody record store is poisoned",
		);
		const recovered = await CustodyRecordStore.open({
			persistence: failedPersistence.fork(),
			durability: "strict",
		});
		const recoveredTarget = await recovered.read(value.moveKey);
		expect(recoveredTarget.snapshot).to.deep.include({
			revision: 2n,
			state: "source-prepared",
		});
		// Original-token retry observes the same authenticated target, no rewrite.
		expect(
			(await recovered.prepareSource(0n, value.bytes)).snapshot.revision,
		).to.equal(2n);
		await recovered.close();
	});

	it("uses the older valid generation after a torn latest slot and fails when neither remains", async () => {
		const persistence = new StrictMemoryPersistence();
		const store = await CustodyRecordStore.open({
			persistence,
			durability: "strict",
		});
		const value = await manifest();
		await store.beginDestination(0n, value.bytes);
		await store.close();

		persistence.corrupt(value.moveKey, "a");
		const fallbackPersistence = persistence.fork();
		const fallback = await CustodyRecordStore.open({
			persistence: fallbackPersistence,
			durability: "strict",
		});
		expect((await fallback.read(value.moveKey)).snapshot).to.deep.equal({
			moveKey: value.moveKey,
			revision: 1n,
			durability: "strict",
			state: "absent",
		});
		await fallback.close();

		persistence.corrupt(value.moveKey, "b");
		const broken = await CustodyRecordStore.open({
			persistence: persistence.fork(),
			durability: "strict",
		});
		await expect(broken.read(value.moveKey)).to.be.rejectedWith(
			"No valid custody record generation remains",
		);
		await broken.close();
	});

	it("fails closed on any rejected slot read instead of overwriting a hidden newer generation", async () => {
		const persistence = new StrictMemoryPersistence();
		const store = await CustodyRecordStore.open({
			persistence,
			durability: "strict",
		});
		const value = await manifest();
		const valueReceipt = await receipt(value);
		await store.beginDestination(0n, value.bytes);
		await store.markDestinationPinned(
			2n,
			value.bytes,
			pinEvidence(value, valueReceipt),
		);
		await store.close();

		// Slot b holds the newer pinned seq3 while slot a holds collecting seq2.
		const retryPersistence = persistence.fork();
		retryPersistence.failNextRead(
			value.moveKey,
			"b",
			new Error("transient read failure"),
		);
		const retry = await CustodyRecordStore.open({
			persistence: retryPersistence,
			durability: "strict",
		});
		await expect(retry.read(value.moveKey)).to.be.rejectedWith(
			"Failed to read every custody record generation",
		);
		expect(retryPersistence.writes).to.have.length(0);
		expect((await retry.read(value.moveKey)).snapshot.state).to.equal(
			"destination-pinned",
		);
		await retry.close();
	});

	it("rejects impossible A/B histories, wrong slot parity, and changed manifests", async () => {
		const first = await manifest(1);
		const second = await manifest(2);
		const firstReceipt = await receipt(first);
		const secondReceipt = await receipt(second);

		const sourcePersistence = new StrictMemoryPersistence();
		const sourceStore = await CustodyRecordStore.open({
			persistence: sourcePersistence,
			durability: "strict",
		});
		await sourceStore.prepareSource(0n, first.bytes);
		await sourceStore.close();

		const secondSourcePersistence = new StrictMemoryPersistence();
		const secondSourceStore = await CustodyRecordStore.open({
			persistence: secondSourcePersistence,
			durability: "strict",
		});
		await secondSourceStore.prepareSource(0n, second.bytes);
		await secondSourceStore.markSourceReceiptDurable(
			2n,
			second.bytes,
			secondReceipt.bytes,
		);
		await secondSourceStore.close();

		const destinationPersistence = new StrictMemoryPersistence();
		const destinationStore = await CustodyRecordStore.open({
			persistence: destinationPersistence,
			durability: "strict",
		});
		await destinationStore.beginDestination(0n, first.bytes);
		await destinationStore.markDestinationPinned(
			2n,
			first.bytes,
			pinEvidence(first, firstReceipt),
		);
		await destinationStore.markDestinationReceipted(
			3n,
			first.bytes,
			firstReceipt.bytes,
		);
		await destinationStore.close();

		const key = first.moveKey;
		const sourceA = sourcePersistence.files.get(`${key}:a`)!; // seq2 prepared
		const sourceB = sourcePersistence.files.get(`${key}:b`)!; // seq1 absent
		const secondSourceB = secondSourcePersistence.files.get(`${key}:b`)!; // seq3 receipt
		const destinationA = destinationPersistence.files.get(`${key}:a`)!; // seq4 receipt
		const destinationB = destinationPersistence.files.get(`${key}:b`)!; // seq3 pinned

		const expectInvalid = async (
			files: Map<string, Uint8Array>,
			message: string,
		) => {
			const persistence = new StrictMemoryPersistence(files);
			const store = await CustodyRecordStore.open({
				persistence,
				durability: "strict",
			});
			await expect(store.read(key)).to.be.rejectedWith(message);
			expect(persistence.writes).to.have.length(0);
			await store.close();
		};

		await expectInvalid(
			new Map([[`${key}:a`, new Uint8Array(sourceB)]]),
			"No valid custody record generation remains",
		);
		await expectInvalid(
			new Map([
				[`${key}:a`, new Uint8Array(destinationA)],
				[`${key}:b`, new Uint8Array(sourceB)],
			]),
			"Non-adjacent custody record generations",
		);
		await expectInvalid(
			new Map([
				[`${key}:a`, new Uint8Array(sourceA)],
				[`${key}:b`, new Uint8Array(destinationB)],
			]),
			"Invalid custody record generation history",
		);
		await expectInvalid(
			new Map([
				[`${key}:a`, new Uint8Array(sourceA)],
				[`${key}:b`, new Uint8Array(secondSourceB)],
			]),
			"Custody record generation changed its manifest",
		);
	});

	it("rejects invalid pin generations before writing a permanently stuck pinned row", async () => {
		const persistence = new StrictMemoryPersistence();
		const store = await CustodyRecordStore.open({
			persistence,
			durability: "strict",
		});
		const value = await manifest();
		await store.beginDestination(0n, value.bytes);
		const writes = persistence.writes.length;
		for (const facts of [
			{ custodyEpoch: digest(0), compositeCommitId: digest(31) },
			{ custodyEpoch: digest(21), compositeCommitId: digest(0) },
		]) {
			expect(() =>
				issueCustodyDestinationPinEvidenceForTest({
					moveKey: value.moveKey,
					handoffId: value.handoffId,
					custodyProfileId: CUSTODY_HANDOFF_PROFILE_ID,
					custodyProfileMask: CUSTODY_HANDOFF_PROFILE_MASK,
					custodyEpoch: facts.custodyEpoch,
					pinSequence: 1n,
					compositeCommitId: facts.compositeCommitId,
				}),
			).to.throw();
		}
		expect(persistence.writes).to.have.length(writes);
		await store.close();
	});

	it("fails closed on invalid adapter values before copying or writing", async () => {
		for (const invalid of [null, new Uint8Array(16 * 1024 + 1)]) {
			let writes = 0;
			const persistence: CustodyRecordPersistence = {
				async read() {
					return invalid as never;
				},
				async write() {
					writes++;
				},
				async durableBarrier() {},
			};
			const store = await CustodyRecordStore.open({
				persistence,
				durability: "strict",
			});
			await expect(store.read(digest(55))).to.be.rejectedWith(
				"Invalid custody persistence read value",
			);
			expect(writes).to.equal(0);
			await store.close();
		}
	});

	it("keeps the memory adapter defensive, lazy by key, and explicitly nondurable", async () => {
		const persistence = new MemoryCustodyRecordPersistence();
		const key = digest(44);
		const input = new Uint8Array([4, 5, 6]);
		await persistence.write(key, "a", input);
		input.fill(0);
		const firstRead = await persistence.read(key, "a", 3);
		expect([...firstRead!]).to.deep.equal([4, 5, 6]);
		firstRead!.fill(9);
		expect([...(await persistence.read(key, "a", 3))!]).to.deep.equal([
			4, 5, 6,
		]);
		await expect(persistence.read(key, "a", 2)).to.be.rejectedWith(
			"Custody persistence read exceeds byte bound",
		);

		const store = await CustodyRecordStore.open({
			persistence,
			durability: "memory",
		});
		const value = await manifest();
		const mutable = new Uint8Array(value.bytes);
		const preparing = store.prepareSource(0n, mutable);
		mutable.fill(0);
		const prepared = await preparing;
		expect(prepared.snapshot.manifest).to.equal(toBase64(value.bytes));
		expect(prepared.durableCommit).to.equal(undefined);
		expect(prepared.snapshot.durability).to.equal("memory");
		const valueReceipt = await receipt(value);
		const simulatedTerminal = await store.markSourceReceiptDurable(
			2n,
			value.bytes,
			valueReceipt.bytes,
		);
		expect(simulatedTerminal.snapshot.state).to.equal("source-receipt-durable");
		expect(simulatedTerminal.snapshot.receipt).to.equal(undefined);
		expect(simulatedTerminal.durableCommit).to.equal(undefined);
		expect((await store.read(digest(45))).snapshot.state).to.equal("absent");
		await expect(
			CustodyRecordStore.open({ persistence, durability: "memory" }),
		).to.be.rejectedWith("Custody persistence is already open");
		await store.close();

		const reopened = await CustodyRecordStore.open({
			persistence: persistence.fork(),
			durability: "memory",
		});
		expect((await reopened.read(value.moveKey)).snapshot.state).to.equal(
			"source-receipt-durable",
		);
		await reopened.close();
	});

	it("preflights hard byte limits before materializing the absent baseline", async () => {
		const persistence = new StrictMemoryPersistence();
		const store = await CustodyRecordStore.open({
			persistence,
			durability: "strict",
			limits: { maxFrameBytes: 256 },
		});
		const value = await manifest();
		await expect(store.prepareSource(0n, value.bytes)).to.be.rejectedWith(
			"Custody record frame exceeds configured byte bound",
		);
		expect(persistence.writes).to.have.length(0);
		await store.close();

		await expect(
			CustodyRecordStore.open({
				persistence: new MemoryCustodyRecordPersistence(),
				durability: "memory",
				limits: { maxPendingOperations: 65 },
			}),
		).to.be.rejectedWith("Invalid custody pending-operation bound");
	});

	it("captures namespace binding properties exactly once", async () => {
		const reads = { logId: 0, localPublicKey: 0, role: 0 };
		const binding = {
			get logId() {
				if (++reads.logId > 1) throw new Error("logId read twice");
				return new Uint8Array([4, 5, 6]);
			},
			get localPublicKey() {
				if (++reads.localPublicKey > 1) {
					throw new Error("localPublicKey read twice");
				}
				return new Uint8Array([7, 8, 9]);
			},
			get role() {
				if (++reads.role > 1) throw new Error("role read twice");
				return "source" as const;
			},
		};
		const store = await CustodyRecordStore.open({
			persistence: new MemoryCustodyRecordPersistence(),
			durability: "memory",
			binding,
		});
		expect(reads).to.deep.equal({ logId: 1, localPublicKey: 1, role: 1 });
		await store.close();
	});

	it("aggregates close failure with an existing poison cause", async () => {
		const persistence = new StrictMemoryPersistence();
		const store = await CustodyRecordStore.open({
			persistence,
			durability: "strict",
		});
		const value = await manifest();
		persistence.failNextWrite(new Error("write failed"));
		await expect(store.prepareSource(0n, value.bytes)).to.be.rejected;
		persistence.failClose(new Error("close failed"));
		try {
			await store.close();
			expect.fail("close should fail");
		} catch (error) {
			expect(error).to.be.instanceOf(AggregateError);
			expect((error as AggregateError).errors).to.have.length(2);
		}
	});

	it("fails closed when persistence close rejects undefined", async () => {
		const persistence = new StrictMemoryPersistence();
		const store = await CustodyRecordStore.open({
			persistence,
			durability: "strict",
		});
		persistence.failClose(undefined);
		const [outcome] = await Promise.allSettled([store.close()]);
		expect(outcome!.status).to.equal("rejected");
		if (outcome!.status === "rejected") {
			expect(outcome.reason).to.equal(undefined);
		}
		expect(persistence.closeCalls).to.equal(1);

		const reopened = await CustodyRecordStore.open({
			persistence: persistence.fork(),
			durability: "strict",
		});
		await reopened.close();
	});

	it("aggregates an undefined close rejection with a poison cause", async () => {
		const persistence = new StrictMemoryPersistence();
		const store = await CustodyRecordStore.open({
			persistence,
			durability: "strict",
		});
		const value = await manifest();
		persistence.failNextWrite(new Error("write failed"));
		await expect(store.prepareSource(0n, value.bytes)).to.be.rejected;
		persistence.failClose(undefined);
		const [outcome] = await Promise.allSettled([store.close()]);
		expect(outcome!.status).to.equal("rejected");
		if (outcome!.status === "rejected") {
			expect(outcome.reason).to.be.instanceOf(AggregateError);
			const errors = (outcome.reason as AggregateError).errors;
			expect(errors).to.have.length(2);
			expect(errors[0]).to.be.instanceOf(Error);
			expect(errors[1]).to.equal(undefined);
		}
	});
});
