import { loadWasm } from "./wasm.js";

type TestingWasmModule = {
	default: (input?: unknown) => Promise<unknown>;
	initSync: (input?: unknown) => unknown;
	sign_ed25519: (
		privateKey: Uint8Array,
		publicKey: Uint8Array,
		data: Uint8Array,
	) => Uint8Array;
	encode_entry_v0_signable_batch: (
		clockIds: Uint8Array[],
		wallTimes: BigUint64Array,
		logicals: Uint32Array,
		gids: string[],
		nexts: string[][],
		types: Uint8Array,
		metaDatas: Array<Uint8Array | undefined>,
		payloadDatas: Uint8Array[],
	) => Uint8Array[];
	encode_entry_v0_storage_batch_with_cids: (
		clockIds: Uint8Array[],
		wallTimes: BigUint64Array,
		logicals: Uint32Array,
		gids: string[],
		nexts: string[][],
		types: Uint8Array,
		metaDatas: Array<Uint8Array | undefined>,
		payloadDatas: Uint8Array[],
		signatures: Uint8Array[],
		signaturePublicKeys: Uint8Array[],
		prehashes: Uint8Array,
	) => Array<[Uint8Array, string]>;
};

type EntryV0EncodeInput = {
	clockId: Uint8Array;
	wallTime: bigint | number | string;
	logical?: number;
	gid: string;
	next?: string[];
	type?: number;
	metaData?: Uint8Array;
	payloadData: Uint8Array;
};

type EntryV0StorageEncodeInput = EntryV0EncodeInput & {
	signature: Uint8Array;
	signaturePublicKey: Uint8Array;
	prehash?: number;
};

type EntryV0EncodedStorage = {
	bytes: Uint8Array;
	cid: string;
};

const entryColumns = (inputs: EntryV0EncodeInput[]) => {
	const clockIds = new Array<Uint8Array>(inputs.length);
	const wallTimes = new BigUint64Array(inputs.length);
	const logicals = new Uint32Array(inputs.length);
	const gids = new Array<string>(inputs.length);
	const nexts = new Array<string[]>(inputs.length);
	const types = new Uint8Array(inputs.length);
	const metaDatas = new Array<Uint8Array | undefined>(inputs.length);
	const payloadDatas = new Array<Uint8Array>(inputs.length);
	for (let i = 0; i < inputs.length; i++) {
		const input = inputs[i]!;
		clockIds[i] = input.clockId;
		wallTimes[i] = BigInt(input.wallTime);
		logicals[i] = input.logical ?? 0;
		gids[i] = input.gid;
		nexts[i] = input.next ?? [];
		types[i] = input.type ?? 0;
		metaDatas[i] = input.metaData;
		payloadDatas[i] = input.payloadData;
	}
	return {
		clockIds,
		wallTimes,
		logicals,
		gids,
		nexts,
		types,
		metaDatas,
		payloadDatas,
	};
};

export const signEd25519ForTest = async (input: {
	privateKey: Uint8Array;
	publicKey: Uint8Array;
	data: Uint8Array;
}): Promise<Uint8Array> => {
	const wasm = await loadWasm<TestingWasmModule>();
	return wasm.sign_ed25519(input.privateKey, input.publicKey, input.data);
};

export const encodeEntryV0SignableBatchForTest = async (
	inputs: EntryV0EncodeInput[],
): Promise<Uint8Array[]> => {
	if (inputs.length === 0) {
		return [];
	}
	const wasm = await loadWasm<TestingWasmModule>();
	const columns = entryColumns(inputs);
	return wasm.encode_entry_v0_signable_batch(
		columns.clockIds,
		columns.wallTimes,
		columns.logicals,
		columns.gids,
		columns.nexts,
		columns.types,
		columns.metaDatas,
		columns.payloadDatas,
	);
};

export const encodeEntryV0StorageBatchWithCidsForTest = async (
	inputs: EntryV0StorageEncodeInput[],
): Promise<EntryV0EncodedStorage[]> => {
	if (inputs.length === 0) {
		return [];
	}
	const wasm = await loadWasm<TestingWasmModule>();
	const columns = entryColumns(inputs);
	const signatures = new Array<Uint8Array>(inputs.length);
	const signaturePublicKeys = new Array<Uint8Array>(inputs.length);
	const prehashes = new Uint8Array(inputs.length);
	for (let i = 0; i < inputs.length; i++) {
		const input = inputs[i]!;
		signatures[i] = input.signature;
		signaturePublicKeys[i] = input.signaturePublicKey;
		prehashes[i] = input.prehash ?? 0;
	}
	return wasm
		.encode_entry_v0_storage_batch_with_cids(
			columns.clockIds,
			columns.wallTimes,
			columns.logicals,
			columns.gids,
			columns.nexts,
			columns.types,
			columns.metaDatas,
			columns.payloadDatas,
			signatures,
			signaturePublicKeys,
			prehashes,
		)
		.map(([bytes, cid]) => ({ bytes, cid }));
};
