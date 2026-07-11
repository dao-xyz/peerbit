import { expect } from "chai";
import {
	NATIVE_DURABILITY_JOURNAL_CODEC_BRAND,
	NATIVE_DURABILITY_JOURNAL_MAX_TRANSACTION_ID_LENGTH,
	NATIVE_DURABILITY_JOURNAL_MAX_U64,
	NativeDurabilityJournalCorruptionError,
	NativeDurabilityJournalInputError,
	NativeDurabilityOperationKind,
	NativeDurabilityPhase,
	createNativeDurabilityJournalCodec,
	isNativeDurabilityJournalCodec,
	type NativeDurabilityJournalRecord,
	type NativeDurabilityJournalValidationContext,
} from "../src/index.js";

const concatBytes = (chunks: readonly Uint8Array[]): Uint8Array => {
	const out = new Uint8Array(
		chunks.reduce((length, chunk) => length + chunk.byteLength, 0),
	);
	let offset = 0;
	for (const chunk of chunks) {
		out.set(chunk, offset);
		offset += chunk.byteLength;
	}
	return out;
};

const programId = new Uint8Array([7, 8, 9]);
const digest = (value: number): Uint8Array => new Uint8Array(32).fill(value);

const context = (
	overrides: Partial<NativeDurabilityJournalValidationContext> = {},
): NativeDurabilityJournalValidationContext => ({
	checkpointLsn: 0n,
	checkpointTxSequenceHighwater: 0n,
	expectedProgramId: programId,
	expectedWriterDomainId: "program-directory",
	checkpointWriterEpoch: 0n,
	currentWriterEpoch: 1n,
	currentWriterOwnerId: "writer-1",
	...overrides,
});

const record = (
	recordLsn: bigint,
	txSequence: bigint,
	transactionId: string,
	phase: NativeDurabilityPhase,
): NativeDurabilityJournalRecord => ({
	recordLsn,
	txSequence,
	writerEpoch: 1n,
	writerOwnerId: "writer-1",
	writerDomainId: "program-directory",
	phase,
	operationKind: NativeDurabilityOperationKind.Append,
	programId,
	transactionId,
	planDigest: digest(Number(txSequence & 0xffn)),
	payload: new Uint8Array([Number(phase), 42]),
});

describe("native durability transaction journal codec", function () {
	this.timeout(120_000);

	it("round-trips structured metadata without converting u64 values to numbers", async () => {
		const codec = await createNativeDurabilityJournalCodec(context());
		expect(isNativeDurabilityJournalCodec(codec)).to.equal(true);
		const records = [
			record(1n, 1n, "tx-1", NativeDurabilityPhase.DurablePrepared),
			record(2n, 1n, "tx-1", NativeDurabilityPhase.NativeApplied),
			record(3n, 1n, "tx-1", NativeDurabilityPhase.Published),
			record(4n, 1n, "tx-1", NativeDurabilityPhase.Committed),
			record(5n, 1n, "tx-1", NativeDurabilityPhase.Clean),
		];
		const bytes = concatBytes(records.map((value) => codec.encode(value)));
		const scan = codec.scan(bytes);

		expect(scan.validLength).to.equal(bytes.byteLength);
		expect(scan.incompleteTailOffset).to.equal(undefined);
		expect(scan.lastRecordLsn).to.equal(5n);
		expect(typeof scan.records[0]!.recordLsn).to.equal("bigint");
		expect(typeof scan.records[0]!.txSequence).to.equal("bigint");
		expect(typeof scan.records[0]!.writerEpoch).to.equal("bigint");
		expect(scan.records).to.deep.equal(records);
	});

	it("does not treat the public informational brand as codec authority", () => {
		const forged = {
			[NATIVE_DURABILITY_JOURNAL_CODEC_BRAND]: true,
			classify: () => ({
				kind: "incomplete-tail" as const,
				validLength: 0,
				lastRecordLsn: 0n,
				reason: "short-header" as const,
			}),
		};
		expect(isNativeDurabilityJournalCodec(forged)).to.equal(false);
	});

	it("snapshots validation context before asynchronous construction", async () => {
		const mutableProgramId = new Uint8Array(programId);
		const mutableRetainedDigest = digest(17);
		const mutableContext = context({
			checkpointTxSequenceHighwater: 1n,
			expectedProgramId: mutableProgramId,
			retainedTransactions: [
				{
					txSequence: 1n,
					transactionId: "retained-tx",
					phase: NativeDurabilityPhase.DurablePrepared,
					operationKind: NativeDurabilityOperationKind.Append,
					planDigest: mutableRetainedDigest,
				},
			],
		});
		const codecPromise = createNativeDurabilityJournalCodec(mutableContext);
		mutableProgramId.fill(99);
		mutableRetainedDigest.fill(88);
		mutableContext.currentWriterOwnerId = "mutated-writer";
		const codec = await codecPromise;

		expect(codec.context.expectedProgramId).to.deep.equal(programId);
		expect(codec.context.currentWriterOwnerId).to.equal("writer-1");
		expect(codec.context.retainedTransactions?.[0]?.planDigest).to.deep.equal(
			digest(17),
		);
		const exposedContext = codec.context;
		exposedContext.expectedProgramId.fill(55);
		exposedContext.retainedTransactions?.[0]?.planDigest.fill(44);
		expect(codec.context.expectedProgramId).to.deep.equal(programId);
		expect(codec.context.retainedTransactions?.[0]?.planDigest).to.deep.equal(
			digest(17),
		);

		const frame = codec.encode(
			record(1n, 2n, "tx-2", NativeDurabilityPhase.DurablePrepared),
		);
		expect(codec.classify(frame).kind).to.equal("complete");
	});

	it("is the storage classifier for short header, body, and trailer tails", async () => {
		const codec = await createNativeDurabilityJournalCodec(context());
		const frame = codec.encode(
			record(1n, 1n, "tx-1", NativeDurabilityPhase.DurablePrepared),
		);

		expect(codec.classify(frame.subarray(0, 10))).to.deep.equal({
			kind: "incomplete-tail",
			validLength: 0,
			lastRecordLsn: 0n,
			reason: "short-header",
		});
		// The fixed trailer is 12 bytes; removing thirteen also removes one body byte.
		expect(codec.classify(frame.subarray(0, frame.byteLength - 13))).to.deep.equal({
			kind: "incomplete-tail",
			validLength: 0,
			lastRecordLsn: 0n,
			reason: "short-body",
		});
		expect(codec.classify(frame.subarray(0, frame.byteLength - 1))).to.deep.equal({
			kind: "incomplete-tail",
			validLength: 0,
			lastRecordLsn: 0n,
			reason: "short-trailer",
		});
	});

	it("throws a typed corruption error for a complete bad frame and non-tail corruption", async () => {
		const codec = await createNativeDurabilityJournalCodec(context());
		const first = codec.encode(
			record(1n, 1n, "tx-1", NativeDurabilityPhase.DurablePrepared),
		);
		const second = codec.encode(
			record(2n, 1n, "tx-1", NativeDurabilityPhase.NativeApplied),
		);
		const corrupt = new Uint8Array(first);
		corrupt[corrupt.byteLength - 20] ^= 1;

		for (const bytes of [corrupt, concatBytes([corrupt, second])]) {
			let thrown: unknown;
			try {
				codec.classify(bytes);
			} catch (error) {
				thrown = error;
			}
			expect(thrown).to.be.instanceOf(NativeDurabilityJournalCorruptionError);
			expect(
				(thrown as NativeDurabilityJournalCorruptionError).code,
			).to.equal("ERR_NATIVE_DURABILITY_JOURNAL_INVALID_FRAME_CHECKSUM");
			expect(
				(thrown as NativeDurabilityJournalCorruptionError).byteOffset,
			).to.equal(0);
		}
	});

	it("returns typed transition errors with decoded append metadata", async () => {
		const codec = await createNativeDurabilityJournalCodec(context());
		const bytes = concatBytes([
			codec.encode(record(1n, 1n, "tx-1", NativeDurabilityPhase.DurablePrepared)),
			codec.encode(record(2n, 1n, "tx-1", NativeDurabilityPhase.Published)),
		]);
		let thrown: unknown;
		try {
			codec.scan(bytes);
		} catch (error) {
			thrown = error;
		}
		expect(thrown).to.be.instanceOf(NativeDurabilityJournalCorruptionError);
		expect((thrown as NativeDurabilityJournalCorruptionError).code).to.equal(
			"ERR_NATIVE_DURABILITY_JOURNAL_INVALID_PHASE_TRANSITION",
		);
		expect((thrown as NativeDurabilityJournalCorruptionError).byteOffset).to.be.greaterThan(
			0,
		);
	});

	it("shares Rust u64 and UTF-8 bounds at the TypeScript boundary", async () => {
		const codec = await createNativeDurabilityJournalCodec(context());
		const tooLarge = record(
			NATIVE_DURABILITY_JOURNAL_MAX_U64 + 1n,
			1n,
			"tx-1",
			NativeDurabilityPhase.DurablePrepared,
		);
		expect(() => codec.encode(tooLarge)).to.throw(NativeDurabilityJournalInputError);

		const tooLongId = record(
			1n,
			1n,
			"é".repeat(NATIVE_DURABILITY_JOURNAL_MAX_TRANSACTION_ID_LENGTH),
			NativeDurabilityPhase.DurablePrepared,
		);
		expect(() => codec.encode(tooLongId)).to.throw(NativeDurabilityJournalInputError);
	});

	it("round-trips the maximum LSN exactly", async () => {
		const codec = await createNativeDurabilityJournalCodec(
			context({ checkpointLsn: NATIVE_DURABILITY_JOURNAL_MAX_U64 - 1n }),
		);
		const maximum = record(
			NATIVE_DURABILITY_JOURNAL_MAX_U64,
			1n,
			"tx-max",
			NativeDurabilityPhase.DurablePrepared,
		);
		const scan = codec.scan(codec.encode(maximum));
		expect(scan.lastRecordLsn).to.equal(NATIVE_DURABILITY_JOURNAL_MAX_U64);
		expect(scan.records[0]!.recordLsn).to.equal(NATIVE_DURABILITY_JOURNAL_MAX_U64);
	});
});
