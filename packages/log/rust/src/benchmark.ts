import { loadWasm } from "./wasm.js";

type BenchmarkWasmModule = {
	default: (input?: unknown) => Promise<unknown>;
	initSync: (input?: unknown) => unknown;
	benchmark_plain_entry_v0_core: (
		clockId: Uint8Array,
		privateKey: Uint8Array,
		publicKey: Uint8Array,
		iterations: number,
		payloadData: Uint8Array,
	) => number[];
	benchmark_plain_entry_v0_digest_key_core: (
		clockId: Uint8Array,
		privateKey: Uint8Array,
		publicKey: Uint8Array,
		iterations: number,
		payloadData: Uint8Array,
	) => number[];
	benchmark_plain_entry_v0_crypto: (
		clockId: Uint8Array,
		privateKey: Uint8Array,
		publicKey: Uint8Array,
		iterations: number,
		payloadData: Uint8Array,
	) => number[];
	benchmark_entry_v0_storage_verify_modes: (
		clockId: Uint8Array,
		privateKey: Uint8Array,
		publicKey: Uint8Array,
		iterations: number,
		payloadData: Uint8Array,
	) => [
		parseMs: number,
		batchVerifyMs: number,
		serialVerifyMs: number,
		storageVerifyMs: number,
		iterations: number,
		batchOk: boolean,
		serialOk: boolean,
		storageOk: boolean,
		checksum: number,
		storageBytesTotal: number,
	];
};

type PlainEntryV0CoreBenchmark = {
	totalMs: number;
	inputCopyMs: number;
	entryCoreMs: number;
	encodeMetaMs: number;
	encodePayloadMs: number;
	encodeSignableMs: number;
	signMs: number;
	encodeSignatureMs: number;
	encodeStorageMs: number;
	cidMs: number;
	cidHashMs: number;
	cidStringMs: number;
	indexEntryMs: number;
	storageBytesTotal: number;
	hashBytesTotal: number;
};

type PlainEntryV0CryptoBenchmark = {
	totalMs: number;
	signableBytes: number;
	storageBytes: number;
	signMs: number;
	verifyMs: number;
	sha256Ms: number;
	cidStringMs: number;
	checksum: number;
	cidLenTotal: number;
};

type EntryV0StorageVerifyBenchmark = {
	parseMs: number;
	batchVerifyMs: number;
	serialVerifyMs: number;
	storageVerifyMs: number;
	iterations: number;
	batchOk: boolean;
	serialOk: boolean;
	storageOk: boolean;
	checksum: number;
	storageBytesTotal: number;
};

const plainEntryV0CoreBenchmarkFromRow = (
	row: number[],
): PlainEntryV0CoreBenchmark => ({
	totalMs: row[0] ?? 0,
	inputCopyMs: row[1] ?? 0,
	entryCoreMs: row[2] ?? 0,
	encodeMetaMs: row[3] ?? 0,
	encodePayloadMs: row[4] ?? 0,
	encodeSignableMs: row[5] ?? 0,
	signMs: row[6] ?? 0,
	encodeSignatureMs: row[7] ?? 0,
	encodeStorageMs: row[8] ?? 0,
	cidMs: row[9] ?? 0,
	cidHashMs: row[10] ?? 0,
	cidStringMs: row[11] ?? 0,
	indexEntryMs: row[12] ?? 0,
	storageBytesTotal: row[13] ?? 0,
	hashBytesTotal: row[14] ?? 0,
});

export const benchmarkPlainEntryV0Core = async (input: {
	clockId: Uint8Array;
	privateKey: Uint8Array;
	publicKey: Uint8Array;
	iterations: number;
	payloadData: Uint8Array;
}): Promise<PlainEntryV0CoreBenchmark> => {
	const wasm = await loadWasm<BenchmarkWasmModule>();
	const row = wasm.benchmark_plain_entry_v0_core(
		input.clockId,
		input.privateKey,
		input.publicKey,
		input.iterations,
		input.payloadData,
	);
	return plainEntryV0CoreBenchmarkFromRow(row);
};

export const benchmarkPlainEntryV0DigestKeyCore = async (input: {
	clockId: Uint8Array;
	privateKey: Uint8Array;
	publicKey: Uint8Array;
	iterations: number;
	payloadData: Uint8Array;
}): Promise<PlainEntryV0CoreBenchmark> => {
	const wasm = await loadWasm<BenchmarkWasmModule>();
	const row = wasm.benchmark_plain_entry_v0_digest_key_core(
		input.clockId,
		input.privateKey,
		input.publicKey,
		input.iterations,
		input.payloadData,
	);
	return plainEntryV0CoreBenchmarkFromRow(row);
};

export const benchmarkPlainEntryV0Crypto = async (input: {
	clockId: Uint8Array;
	privateKey: Uint8Array;
	publicKey: Uint8Array;
	iterations: number;
	payloadData: Uint8Array;
}): Promise<PlainEntryV0CryptoBenchmark> => {
	const wasm = await loadWasm<BenchmarkWasmModule>();
	const row = wasm.benchmark_plain_entry_v0_crypto(
		input.clockId,
		input.privateKey,
		input.publicKey,
		input.iterations,
		input.payloadData,
	);
	return {
		totalMs: row[0] ?? 0,
		signableBytes: row[1] ?? 0,
		storageBytes: row[2] ?? 0,
		signMs: row[3] ?? 0,
		verifyMs: row[4] ?? 0,
		sha256Ms: row[5] ?? 0,
		cidStringMs: row[6] ?? 0,
		checksum: row[7] ?? 0,
		cidLenTotal: row[8] ?? 0,
	};
};

export const benchmarkEntryV0StorageVerifyModes = async (input: {
	clockId: Uint8Array;
	privateKey: Uint8Array;
	publicKey: Uint8Array;
	iterations: number;
	payloadData: Uint8Array;
}): Promise<EntryV0StorageVerifyBenchmark> => {
	const wasm = await loadWasm<BenchmarkWasmModule>();
	const row = wasm.benchmark_entry_v0_storage_verify_modes(
		input.clockId,
		input.privateKey,
		input.publicKey,
		input.iterations,
		input.payloadData,
	);
	return {
		parseMs: row[0],
		batchVerifyMs: row[1],
		serialVerifyMs: row[2],
		storageVerifyMs: row[3],
		iterations: row[4],
		batchOk: row[5],
		serialOk: row[6],
		storageOk: row[7],
		checksum: row[8],
		storageBytesTotal: row[9],
	};
};
