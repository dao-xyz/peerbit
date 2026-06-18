import type { NativePeerbitBackbone } from "./index.js";

type NativeBackboneBenchmarkHandle = {
	benchmark_plain_committed_no_next_storage_append_transaction_loop: (
		iterations: number,
		wallTimeStart: bigint,
		payloadData: Uint8Array,
		replicas: number,
		selfHash: string,
		useDocumentIndex: boolean,
		documentByteElementIndexLimit: number,
		trimLengthTo: number | undefined,
	) => number[];
};

export type NativeBackboneLoopBenchmark = {
	totalMs: number;
	logLength: number;
	blockLength: number;
	coordinateLength: number;
	documentLength: number;
};

export const benchmarkPlainCommittedNoNextStorageAppendTransactionLoop = (
	backbone: NativePeerbitBackbone,
	input: {
		iterations: number;
		wallTimeStart: bigint | number | string;
		payloadData: Uint8Array;
		replicas: number;
		selfHash: string;
		useDocumentIndex?: boolean;
		documentByteElementIndexLimit?: number;
		trimLengthTo?: number;
	},
): NativeBackboneLoopBenchmark => {
	const native = (
		backbone as unknown as { native: NativeBackboneBenchmarkHandle }
	).native;
	const row =
		native.benchmark_plain_committed_no_next_storage_append_transaction_loop(
			input.iterations,
			BigInt(input.wallTimeStart),
			input.payloadData,
			input.replicas,
			input.selfHash,
			input.useDocumentIndex === true,
			input.documentByteElementIndexLimit ?? 0,
			input.trimLengthTo,
		);
	return {
		totalMs: Number(row[0] ?? 0),
		logLength: Number(row[1] ?? 0),
		blockLength: Number(row[2] ?? 0),
		coordinateLength: Number(row[3] ?? 0),
		documentLength: Number(row[4] ?? 0),
	};
};
