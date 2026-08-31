import { expect } from "chai";
import { benchmarkPlainCommittedNoNextStorageAppendTransactionLoop } from "../src/benchmark.js";
import {
	NativeBackboneBufferedCoordinatePersistenceStore,
	NativeBackboneCoordinatePersistence,
	NativeBackboneCoordinatePersistenceReadLimitError,
	type NativeBackboneCoordinatePersistenceStore,
	NativeBackboneMemoryCoordinatePersistenceStore,
	NativeBackboneNodeCoordinatePersistence,
	NativeBackboneNodeCoordinatePersistenceStore,
	NativeBackboneOPFSCoordinatePersistenceStore,
	type NativeBackboneOPFSDirectoryHandle,
	createBufferedNativeBackboneCoordinatePersistence,
	createBufferedNativeBackboneNodeCoordinatePersistence,
	createNativeBackboneCoordinatePersistence,
	createNativePeerbitBackbone,
	defaultNativeBackboneCoordinateFlushMaxPendingBytes,
} from "../src/index.js";

const fromHex = (hex: string) =>
	Uint8Array.from(
		hex.match(/.{2}/g)?.map((byte) => Number.parseInt(byte, 16)) ?? [],
	);

const privateKey = fromHex(
	"9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60",
);
const publicKey = fromHex(
	"d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a",
);

type NativeBackboneTestInstance = Awaited<
	ReturnType<typeof createNativePeerbitBackbone>
>;
type NativeGraphCommitInput = Parameters<
	NativeBackboneTestInstance["graph"]["prepareEntryV0PlainEntryCommit"]
>[0];

const prepareGraphCommit = (
	backbone: NativeBackboneTestInstance,
	input: Omit<
		NativeGraphCommitInput,
		| "clockId"
		| "privateKey"
		| "publicKey"
		| "includeMaterializationBytes"
		| "includeAppendFactsBytes"
	>,
) => {
	const prepared = backbone.graph.prepareEntryV0PlainEntryCommit(
		{
			clockId: publicKey,
			privateKey,
			publicKey,
			includeMaterializationBytes: false,
			includeAppendFactsBytes: true,
			...input,
		},
		backbone.blocks,
	);
	if (!prepared) {
		throw new Error("Native graph commit was unavailable");
	}
	return prepared;
};

const hideGraphCapability = (
	backbone: NativeBackboneTestInstance,
	name: string,
): void => {
	const native = (
		backbone.graph as unknown as { native: Record<string, unknown> }
	).native;
	Object.defineProperty(native, name, { value: undefined });
};

const concatBytes = (chunks: Uint8Array[]) => {
	const total = chunks.reduce((sum, chunk) => sum + chunk.byteLength, 0);
	const out = new Uint8Array(total);
	let offset = 0;
	for (const chunk of chunks) {
		out.set(chunk, offset);
		offset += chunk.byteLength;
	}
	return out;
};

const writeU32 = (out: number[], value: number) => {
	out.push(
		value & 0xff,
		(value >> 8) & 0xff,
		(value >> 16) & 0xff,
		value >>> 24,
	);
};

const writeString = (out: number[], value: string) => {
	const bytes = new TextEncoder().encode(value);
	writeU32(out, bytes.byteLength);
	out.push(...bytes);
};

const fnv1a = (bytes: Uint8Array) => {
	let hash = 0x811c9dc5;
	for (const byte of bytes) {
		hash ^= byte;
		hash = Math.imul(hash, 0x01000193) >>> 0;
	}
	return hash >>> 0;
};

const keyValueSnapshotEnvelope = (payload: Uint8Array) => {
	const out: number[] = [...new TextEncoder().encode("PBRIDXK1")];
	writeU32(out, payload.byteLength);
	writeU32(out, fnv1a(payload));
	out.push(...payload);
	return Uint8Array.from(out);
};

const schemaWithIdScoreAndBytes = () => {
	const out: number[] = [1, 14];
	writeU32(out, 0);
	writeU32(out, 3);
	writeString(out, "id");
	writeU32(out, 1);
	writeU32(out, 101);
	out.push(12);
	writeString(out, "score");
	writeU32(out, 2);
	writeU32(out, 102);
	out.push(3);
	writeString(out, "bytes");
	writeU32(out, 3);
	writeU32(out, 103);
	out.push(13);
	return Uint8Array.from(out);
};

const encodedDocumentWithIdScoreAndBytes = (
	id = "abc",
	score = 7,
	bytes = new Uint8Array([9, 10]),
) => {
	const out: number[] = [];
	writeString(out, id);
	writeU32(out, score);
	writeU32(out, bytes.byteLength);
	out.push(...bytes);
	return Uint8Array.from(out);
};

const plainPutPayload = (document: Uint8Array) => {
	const out = new Uint8Array(6 + document.byteLength);
	out[0] = 0;
	out[1] = 3;
	new DataView(out.buffer, out.byteOffset, out.byteLength).setUint32(
		2,
		document.byteLength,
		true,
	);
	out.set(document, 6);
	return out;
};

const contextOnlySchema = () => {
	const out: number[] = [1, 14];
	writeU32(out, 1);
	out.push(0);
	writeU32(out, 5);
	writeString(out, "created");
	writeU32(out, 1);
	writeU32(out, 101);
	out.push(4);
	writeString(out, "modified");
	writeU32(out, 2);
	writeU32(out, 102);
	out.push(4);
	writeString(out, "head");
	writeU32(out, 3);
	writeU32(out, 103);
	out.push(12);
	writeString(out, "gid");
	writeU32(out, 4);
	writeU32(out, 104);
	out.push(12);
	writeString(out, "size");
	writeU32(out, 5);
	writeU32(out, 105);
	out.push(3);
	return Uint8Array.from(out);
};

const configureCheckpointDocumentSchema = (
	backbone: NativeBackboneTestInstance,
): void => {
	backbone.configureDocumentSchemaIr(contextOnlySchema());
	backbone.setDocumentContextHeadField(3);
	backbone.setDocumentContextFields({
		created: 1,
		modified: 2,
		head: 3,
		gid: 4,
		size: 5,
	});
};

const appendCheckpointFixture = (
	backbone: NativeBackboneTestInstance,
	label: string,
	ordinal: number,
): void => {
	backbone.putEntryCoordinates(
		`hash-checkpoint-${label}`,
		`gid-checkpoint-coordinate-${label}`,
		[BigInt(ordinal)],
		false,
		1,
		BigInt(ordinal),
	);
	backbone.preparePlainCommittedNoNextStorageAppendDocumentIndexCompactTransaction(
		{
			wallTime: BigInt(ordinal),
			logical: ordinal,
			gid: `gid-checkpoint-document-${label}`,
			payloadData: new Uint8Array([ordinal & 0xff]),
			replicas: 1,
			selfHash: "peer",
			documentIndex: {
				key: `doc-checkpoint-${label}`,
				valuePrefixBytes: new Uint8Array(0),
			},
		},
	);
	const documentValue = backbone.documentValueBytes(`doc-checkpoint-${label}`);
	if (!documentValue) {
		throw new Error(`Missing checkpoint fixture document ${label}`);
	}
	backbone.putDocumentEncodedPartsStored(
		`doc-checkpoint-${label}`,
		documentValue,
		new Uint8Array(0),
	);
};

const expectCheckpointFixtures = (
	backbone: NativeBackboneTestInstance,
	labels: readonly string[],
): void => {
	for (const label of labels) {
		expect(backbone.hasCoordinateIndexHash(`hash-checkpoint-${label}`)).equal(
			true,
		);
	}
	expect(
		Array.from(
			backbone.documentKeysExist(
				labels.map((label) => `doc-checkpoint-${label}`),
			),
		),
	).to.deep.equal(labels.map(() => 1));
	const documentValues = labels.map((label) => {
		const value = backbone.documentValueBytes(`doc-checkpoint-${label}`);
		expect(value, `document ${label}`).to.exist;
		return value!;
	});
	backbone.clearDocumentIndex();
	for (const [index, label] of labels.entries()) {
		backbone.putDocumentEncodedPartsStored(
			`doc-checkpoint-${label}`,
			documentValues[index]!,
			new Uint8Array(0),
		);
		expect(
			Array.from(
				backbone.documentPreviousSignaturePublicKey(`doc-checkpoint-${label}`)
					?.publicKey ?? [],
			),
			`signer ${label}`,
		).to.deep.equal(Array.from(publicKey));
	}
};

const configureContextDocumentIndex = (
	backbone: NativeBackboneTestInstance,
): void => {
	backbone.configureDocumentSchemaIr(contextOnlySchema());
	backbone.setDocumentContextHeadField(3);
	backbone.setDocumentContextFields({
		created: 1,
		modified: 2,
		head: 3,
		gid: 4,
		size: 5,
	});
};

const contextOnlyProjectionPlan = {
	documentFieldNames: ["id", "score", "bytes"],
	documentFieldTypes: ["string", "u32", "bytes"],
	outputFieldTypes: [],
	sourceKinds: [],
	sourceValues: [],
};

class FakeOPFSWritable {
	private position = 0;

	constructor(
		private readonly handle: FakeOPFSFileHandle,
		keepExistingData: boolean,
	) {
		if (!keepExistingData) {
			this.handle.replace(new Uint8Array());
		}
	}

	async seek(position: number): Promise<void> {
		this.position = position;
	}

	async write(data: Uint8Array): Promise<void> {
		this.handle.writeAt(this.position, data);
		this.position += data.byteLength;
	}

	async close(): Promise<void> {}
}

class FakeOPFSFileHandle {
	constructor(
		private readonly directory: FakeOPFSDirectoryHandle,
		private readonly name: string,
		private readonly syncAccess: boolean,
	) {}

	async getFile(): Promise<{
		arrayBuffer(): Promise<ArrayBuffer>;
		size: number;
	}> {
		const bytes = this.directory.fileBytes(this.name);
		return {
			size: bytes.byteLength,
			arrayBuffer: async () => {
				this.directory.arrayBufferCount++;
				const copy = bytes.slice();
				return copy.buffer.slice(
					copy.byteOffset,
					copy.byteOffset + copy.byteLength,
				);
			},
		};
	}

	async createWritable(options?: {
		keepExistingData?: boolean;
	}): Promise<FakeOPFSWritable> {
		this.directory.asyncWritableCount++;
		this.directory.keepExistingDataOptions.push(
			options?.keepExistingData === true,
		);
		return new FakeOPFSWritable(this, options?.keepExistingData === true);
	}

	async createSyncAccessHandle(): Promise<{
		getSize(): number;
		write(buffer: Uint8Array, options?: { at?: number }): number;
		truncate(size: number): void;
		flush(): void;
		close(): void;
	}> {
		if (!this.syncAccess) {
			const error = new Error("sync handles unavailable") as Error & {
				name: string;
			};
			error.name = "InvalidStateError";
			throw error;
		}
		this.directory.syncAccessCount++;
		return {
			getSize: () => this.directory.fileBytes(this.name).byteLength,
			write: (buffer, options) => {
				const written = this.directory.nextSyncWriteCount(buffer.byteLength);
				if (written > 0 && written <= buffer.byteLength) {
					this.writeAt(options?.at ?? 0, buffer.subarray(0, written));
				}
				this.directory.syncWriteCount++;
				return written;
			},
			truncate: (size) => {
				this.replace(this.directory.fileBytes(this.name).subarray(0, size));
			},
			flush: () => {
				this.directory.syncFlushCount++;
			},
			close: () => {
				this.directory.syncCloseCount++;
			},
		};
	}

	replace(bytes: Uint8Array): void {
		this.directory.files.set(this.name, bytes.slice());
	}

	writeAt(position: number, bytes: Uint8Array): void {
		const existing = this.directory.fileBytes(this.name);
		const nextLength = Math.max(
			existing.byteLength,
			position + bytes.byteLength,
		);
		const next = new Uint8Array(nextLength);
		next.set(existing);
		next.set(bytes, position);
		this.directory.files.set(this.name, next);
	}
}

class FakeOPFSDirectoryHandle implements NativeBackboneOPFSDirectoryHandle {
	readonly files = new Map<string, Uint8Array>();
	readonly keepExistingDataOptions: boolean[] = [];
	asyncWritableCount = 0;
	syncAccessCount = 0;
	syncWriteCount = 0;
	syncFlushCount = 0;
	syncCloseCount = 0;
	arrayBufferCount = 0;

	constructor(
		private readonly syncAccess = false,
		private readonly syncWriteCounts: number[] = [],
	) {}

	nextSyncWriteCount(requested: number): number {
		return this.syncWriteCounts.shift() ?? requested;
	}

	fileBytes(name: string): Uint8Array {
		return this.files.get(name)?.slice() ?? new Uint8Array();
	}

	async getDirectoryHandle(): Promise<NativeBackboneOPFSDirectoryHandle> {
		return this;
	}

	async getFileHandle(
		name: string,
		options?: { create?: boolean },
	): Promise<FakeOPFSFileHandle> {
		if (!this.files.has(name)) {
			if (!options?.create) {
				const error = new Error("not found") as Error & { name: string };
				error.name = "NotFoundError";
				throw error;
			}
			this.files.set(name, new Uint8Array());
		}
		return new FakeOPFSFileHandle(this, name, this.syncAccess);
	}

	async removeEntry(name: string): Promise<void> {
		if (!this.files.delete(name)) {
			const error = new Error("not found") as Error & { name: string };
			error.name = "NotFoundError";
			throw error;
		}
	}
}

describe("native peerbit backbone", () => {
	it("samples intersecting strict ranges excluded from full replica fallback", async () => {
		const maxU64 = (1n << 64n) - 1n;
		const liveRangeEnd = 86_400_000_000_000_000n;
		const backbone = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
			resolution: "u64",
		});
		backbone.putRange({
			id: "writer",
			hash: "peer-writer",
			timestamp: 0,
			start1: 0n,
			end1: maxU64,
			start2: 0n,
			end2: maxU64,
			width: maxU64,
			mode: 0,
		});
		backbone.putRange({
			id: "viewer",
			hash: "peer-viewer",
			timestamp: 0,
			start1: 0n,
			end1: liveRangeEnd,
			start2: 0n,
			end2: liveRangeEnd,
			width: liveRangeEnd,
			mode: 1,
		});

		expect(
			backbone.findLeaders([0n, maxU64 / 2n], 2, {
				fullReplicaFallback: true,
				includeStrictFullReplica: false,
				now: 1_000,
				selfHash: "peer-writer",
				selfReplicating: true,
			}),
		).to.deep.equal(
			new Map([
				["peer-writer", { intersecting: true }],
				["peer-viewer", { intersecting: true }],
			]),
		);
	});

	it("bridges routing-safe full-replica leaders", async () => {
		const backbone = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
			resolution: "u32",
		});
		for (const [id, hash, start, end, mode] of [
			["a", "peer-a", 0, 10, 0],
			["b", "peer-b", 90, 100, 1],
		] as const) {
			backbone.putRange({
				id,
				hash,
				timestamp: 0,
				start1: start,
				end1: end,
				start2: start,
				end2: end,
				width: end - start,
				mode,
			});
		}
		const options = {
			now: 1_000,
			peerFilter: ["peer-a"],
			expandPeerFilter: true,
			selfHash: "peer-a",
			selfReplicating: true,
			fullReplicaFallback: true,
		};

		expect(backbone.getRoutingFullReplicaLeaders(2, options)).to.deep.equal(
			new Map([
				["peer-a", { intersecting: true }],
				["peer-b", { intersecting: true }],
			]),
		);
		expect(
			backbone.getRoutingFullReplicaLeaders(2, {
				...options,
				includeStrictFullReplica: false,
			}),
		).to.equal(undefined);
	});

	it("bridges exact full-replica candidate discovery", async () => {
		const backbone = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
			resolution: "u32",
		});
		for (const [id, hash, start, end, mode] of [
			["a-1", "peer-a", 0, 10, 0],
			["a-2", "peer-a", 10, 20, 0],
			["b", "peer-b", 20, 20, 1],
		] as const) {
			backbone.putRange({
				id,
				hash,
				timestamp: 0,
				start1: start,
				end1: end,
				start2: start,
				end2: end,
				width: end - start,
				mode,
			});
		}

		expect(backbone.fullReplicaCandidatesFor(2, "peer-self")).to.deep.equal([]);
		expect(backbone.fullReplicaCandidatesFor(3, "peer-self")).to.deep.equal([
			"peer-self",
			"peer-a",
			"peer-b",
		]);
	});

	it("defaults buffered coordinate WAL to bounded pending bytes", () => {
		const persistence = new NativeBackboneCoordinatePersistence(
			new NativeBackboneMemoryCoordinatePersistenceStore(),
			{ flushOnAppend: false },
		);

		expect(persistence.flushMaxPendingBytes).equal(
			defaultNativeBackboneCoordinateFlushMaxPendingBytes,
		);
	});

	it("creates buffered coordinate persistence from a store config", async () => {
		const backbone = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});
		const store = new NativeBackboneMemoryCoordinatePersistenceStore();
		const persistence = createNativeBackboneCoordinatePersistence({
			store,
			buffered: true,
			flushOnAppend: false,
		});

		expect(persistence.compactMaxJournalBytes).equal(undefined);
		await persistence.hydrate(backbone);
		backbone.putEntryCoordinates(
			"hash-config",
			"gid-config",
			[1n],
			false,
			1,
			1n,
		);
		expect(await persistence.flushJournalOnAppend?.(backbone)).equal(0);
		expect(store.files.has("coordinates.wal")).equal(false);
		expect(await persistence.flushJournal(backbone)).to.be.greaterThan(0);
		expect(store.files.has("coordinates.wal")).equal(true);
		await persistence.close?.();
		expect(store.files.get("coordinates.wal")?.byteLength).to.be.greaterThan(
			backbone.coordinateJournalHeader().byteLength,
		);
	});

	it("rejects buffered store config coordinate WAL compaction thresholds", () => {
		const store = new NativeBackboneMemoryCoordinatePersistenceStore();
		expect(() =>
			createNativeBackboneCoordinatePersistence({
				store,
				buffered: true,
				flushOnAppend: false,
				flushMaxPendingBytes: 1,
				compactMaxJournalRecords: 1,
			}),
		).to.throw("compaction is disabled");
	});

	it("creates high-throughput buffered coordinate persistence with bounded flush policy", async () => {
		const backbone = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});
		const store = new NativeBackboneMemoryCoordinatePersistenceStore();
		const persistence = createBufferedNativeBackboneCoordinatePersistence(
			store,
			{ flushMaxPendingBytes: 1024, maxBufferedBytes: 2048 },
		);

		expect(persistence.flushOnAppend).equal(false);
		expect(persistence.flushMaxPendingBytes).equal(1024);
		expect(persistence.compactMaxJournalBytes).equal(undefined);
		await persistence.hydrate(backbone);
		backbone.putEntryCoordinates(
			"hash-buffered",
			"gid-buffered",
			[1n],
			false,
			1,
			1n,
		);

		expect(await persistence.flushJournalOnAppend?.(backbone)).equal(0);
		expect(store.files.has("coordinates.wal")).equal(false);
		expect(await persistence.flushJournal(backbone)).to.be.greaterThan(0);
		expect(store.files.has("coordinates.wal")).equal(true);
		await persistence.close?.();
		expect(store.files.get("coordinates.wal")?.byteLength).to.be.greaterThan(
			backbone.coordinateJournalHeader().byteLength,
		);
	});

	it("honors custom buffered document WAL file names", async () => {
		const source = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});
		const restored = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});
		source.configureDocumentSchemaIr(contextOnlySchema());
		source.setDocumentContextHeadField(3);
		source.setDocumentContextFields({
			created: 1,
			modified: 2,
			head: 3,
			gid: 4,
			size: 5,
		});
		const store = new NativeBackboneMemoryCoordinatePersistenceStore();
		const persistence = createBufferedNativeBackboneCoordinatePersistence(
			store,
			{
				flushMaxPendingBytes: 1024,
				documentSnapshot: "custom-document-values.bin",
				documentJournal: "custom-document-values.wal",
			},
		);

		await persistence.hydrate(source);
		source.preparePlainCommittedNoNextStorageAppendDocumentIndexCompactTransaction(
			{
				wallTime: 11n,
				logical: 1,
				gid: "gid-buffered-document-custom",
				payloadData: new Uint8Array([1, 2, 3]),
				replicas: 1,
				selfHash: "peer",
				documentIndex: {
					key: "doc-buffered-custom",
					valuePrefixBytes: new Uint8Array(0),
				},
			},
		);
		expect(source.documentPendingJournalLength).equal(1);

		expect(await persistence.flushJournal(source)).to.be.greaterThan(0);
		expect(store.files.has("custom-document-values.wal")).equal(true);
		expect(store.files.has("document-values.wal")).equal(false);
		await persistence.close?.();
		expect(store.files.has("custom-document-values.wal")).equal(true);
		expect(store.files.has("document-values.wal")).equal(false);

		await new NativeBackboneCoordinatePersistence(store, {
			documentSnapshot: "custom-document-values.bin",
			documentJournal: "custom-document-values.wal",
		}).hydrate(restored);
		expect(restored.documentValueLength).equal(1);
		restored.configureDocumentSchemaIr(contextOnlySchema());
		restored.setDocumentContextHeadField(3);
		restored.setDocumentContextFields({
			created: 1,
			modified: 2,
			head: 3,
			gid: 4,
			size: 5,
		});
		expect(restored.documentIndexLength).equal(1);
		expect(
			Array.from(restored.documentKeysExist(["doc-buffered-custom"])),
		).to.deep.equal([1]);
	});

	it("honors custom buffered document signer WAL file names", async () => {
		const source = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});
		const restored = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});
		for (const backbone of [source, restored]) {
			backbone.configureDocumentSchemaIr(contextOnlySchema());
			backbone.setDocumentContextHeadField(3);
			backbone.setDocumentContextFields({
				created: 1,
				modified: 2,
				head: 3,
				gid: 4,
				size: 5,
			});
		}
		const store = new NativeBackboneMemoryCoordinatePersistenceStore();
		const persistence = createBufferedNativeBackboneCoordinatePersistence(
			store,
			{
				flushMaxPendingBytes: 1024,
				documentSignerSnapshot: "custom-document-signers.bin",
				documentSignerJournal: "custom-document-signers.wal",
			},
		);

		await persistence.hydrate(source);
		source.preparePlainCommittedNoNextStorageAppendDocumentIndexCompactTransaction(
			{
				wallTime: 11n,
				logical: 1,
				gid: "gid-buffered-document-signer-custom",
				payloadData: new Uint8Array([1, 2, 3]),
				replicas: 1,
				selfHash: "peer",
				documentIndex: {
					key: "doc-buffered-signer-custom",
					valuePrefixBytes: new Uint8Array(0),
				},
			},
		);
		const documentValue = source.documentValueBytes(
			"doc-buffered-signer-custom",
		);
		expect(documentValue).to.exist;
		source.putDocumentEncodedPartsStored(
			"doc-buffered-signer-custom",
			documentValue!,
			new Uint8Array(0),
		);
		expect(source.documentSignerPendingJournalLength).equal(1);

		expect(await persistence.flushJournal(source)).to.be.greaterThan(0);
		expect(store.files.has("custom-document-signers.wal")).equal(true);
		expect(store.files.has("document-signers.wal")).equal(false);
		await persistence.close?.();
		expect(store.files.has("custom-document-signers.wal")).equal(true);
		expect(store.files.has("document-signers.wal")).equal(false);

		await new NativeBackboneCoordinatePersistence(store, {
			documentSignerSnapshot: "custom-document-signers.bin",
			documentSignerJournal: "custom-document-signers.wal",
		}).hydrate(restored);
		restored.clearDocumentIndex();
		restored.putDocumentEncodedPartsStored(
			"doc-buffered-signer-custom",
			documentValue!,
			new Uint8Array(0),
		);
		expect(
			Array.from(
				restored.documentPreviousSignaturePublicKey(
					"doc-buffered-signer-custom",
				)?.publicKey ?? [],
			),
		).to.deep.equal(Array.from(publicKey));
	});

	it("rejects generic coordinate WAL compaction thresholds", () => {
		const store = new NativeBackboneMemoryCoordinatePersistenceStore();
		expect(
			() =>
				new NativeBackboneCoordinatePersistence(store, {
					compactMaxJournalBytes: 1,
				}),
		).to.throw("compaction is disabled");
	});

	it("rejects buffered coordinate WAL compaction thresholds", () => {
		const store = new NativeBackboneMemoryCoordinatePersistenceStore();
		expect(() =>
			createBufferedNativeBackboneCoordinatePersistence(store, {
				compactMaxJournalRecords: 1,
				flushMaxPendingBytes: 1,
			}),
		).to.throw("compaction is disabled");
	});

	it("owns coordinate WAL append flush decisions", async () => {
		const delayedBackbone = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});
		const delayedStore = new NativeBackboneMemoryCoordinatePersistenceStore();
		const delayedPersistence = new NativeBackboneCoordinatePersistence(
			delayedStore,
			{
				flushOnAppend: false,
				flushMaxPendingBytes:
					defaultNativeBackboneCoordinateFlushMaxPendingBytes,
			},
		);

		await delayedPersistence.hydrate(delayedBackbone);
		delayedBackbone.putEntryCoordinates("hash-a", "gid-a", [1n], false, 1, 1n);
		expect(
			delayedPersistence.shouldFlushJournalOnAppend(delayedBackbone),
		).equal(false);
		expect(
			await delayedPersistence.flushJournalOnAppend(delayedBackbone),
		).equal(0);
		expect(delayedStore.files.has("coordinates.wal")).equal(false);
		expect(delayedBackbone.coordinatePendingJournalLength).to.be.greaterThan(0);
		expect(
			await delayedPersistence.flushJournal(delayedBackbone),
		).to.be.greaterThan(0);

		const thresholdBackbone = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});
		const thresholdStore = new NativeBackboneMemoryCoordinatePersistenceStore();
		const thresholdPersistence = new NativeBackboneCoordinatePersistence(
			thresholdStore,
			{ flushOnAppend: false, flushMaxPendingBytes: 1 },
		);

		await thresholdPersistence.hydrate(thresholdBackbone);
		thresholdBackbone.putEntryCoordinates(
			"hash-b",
			"gid-b",
			[2n],
			false,
			1,
			2n,
		);
		expect(
			thresholdPersistence.shouldFlushJournalOnAppend(thresholdBackbone),
		).equal(true);
		expect(
			await thresholdPersistence.flushJournalOnAppend(thresholdBackbone),
		).to.be.greaterThan(0);
		expect(thresholdBackbone.coordinatePendingJournalLength).to.equal(0);
		expect(thresholdStore.files.has("coordinates.wal")).equal(true);
	});

	it("writes the initial generic coordinate WAL flush as one append", async () => {
		const backbone = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});
		class CountingStore extends NativeBackboneMemoryCoordinatePersistenceStore {
			appendCount = 0;
			async append(name: string, bytes: Uint8Array): Promise<void> {
				this.appendCount++;
				await super.append(name, bytes);
			}
		}
		const store = new CountingStore();
		const persistence = new NativeBackboneCoordinatePersistence(store);

		await persistence.hydrate(backbone);
		backbone.putEntryCoordinates("hash-a", "gid-a", [1n], false, 1, 1n);
		const recordBytes = await persistence.flushJournal(backbone);

		expect(recordBytes).to.be.greaterThan(0);
		expect(store.appendCount).equal(1);
		expect(store.files.get("coordinates.wal")?.byteLength).equal(
			backbone.coordinateJournalHeader().byteLength + recordBytes,
		);
	});

	it("commits lower-log blocks and shared-log coordinates in one native call", async () => {
		const backbone = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});

		const result = backbone.appendPlainNoNextTransaction({
			wallTime: 1n,
			gid: "gid-a",
			payloadData: new Uint8Array([1, 2, 3]),
			replicas: 1,
			selfHash: "peer-a",
		});

		expect(result.entry.hash).to.be.a("string").and.not.empty;
		expect(result.entry.byteLength).to.be.greaterThan(0);
		expect(result.entry.hashDigestBytes).to.have.length.greaterThan(0);
		expect(result.coordinate.hash).to.equal(result.entry.hash);
		expect(result.coordinate.gid).to.equal("gid-a");
		expect(result.coordinate.requestedReplicas).to.equal(1);
		expect(backbone.logLength).to.equal(1);
		expect(backbone.blockLength).to.equal(1);
		expect(backbone.hasLogEntry(result.entry.hash)).to.equal(true);
		expect(backbone.hasBlock(result.entry.hash)).to.equal(true);
		expect(backbone.getEntryCoordinateHashes()).to.deep.equal([
			result.entry.hash,
		]);
		expect(backbone.coordinateIndexLength).to.equal(1);
		expect(backbone.coordinateValueLength).to.equal(1);
		expect(backbone.hasCoordinateIndexHash(result.entry.hash)).to.equal(true);
	});

	it("can hold a resident document index from encoded Borsh parts", async () => {
		const backbone = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});
		const schemaStats = backbone.configureDocumentSchemaIr(
			schemaWithIdScoreAndBytes(),
		);
		const encoded = encodedDocumentWithIdScoreAndBytes();

		backbone.putDocumentEncodedPartsStored(
			"doc-1",
			encoded.slice(0, 6),
			encoded.slice(6),
			8,
		);

		expect(schemaStats).to.deep.equal({
			rootFields: 3,
			nodeCount: 4,
			genericNodes: 0,
		});
		expect(backbone.documentIndexLength).to.equal(1);
		expect(backbone.documentValueLength).to.equal(1);
		expect(backbone.documentExactStringFirstKey(1, "abc")).to.equal("doc-1");
		expect(
			Array.from(backbone.documentValueBytes("doc-1") ?? []),
		).to.deep.equal(Array.from(encoded));
		expect(backbone.deleteDocument("doc-1")).to.equal(true);
		expect(backbone.documentIndexLength).to.equal(0);
		expect(backbone.documentValueLength).to.equal(0);

		backbone.putDocumentEncodedPartsStoredBatch(
			[
				{
					key: "doc-1",
					valuePrefixBytes: encoded.slice(0, 6),
					valueSuffixBytes: encoded.slice(6),
				},
				{
					key: "doc-2",
					valuePrefixBytes: encoded.slice(0, 6),
					valueSuffixBytes: encoded.slice(6),
				},
			],
			8,
		);
		expect(backbone.documentIndexLength).to.equal(2);
		expect(backbone.documentValueLength).to.equal(2);
		expect(
			Array.from(backbone.documentValueBytes("doc-2") ?? []),
		).to.deep.equal(Array.from(encoded));
		expect(backbone.deleteDocuments(["doc-1", "doc-2", "missing"])).to.equal(2);
		expect(backbone.documentIndexLength).to.equal(0);
		expect(backbone.documentValueLength).to.equal(0);

		backbone.putDocumentEncodedPartsStoredBatch(
			[
				{
					key: "doc-1",
					valuePrefixBytes: encoded.slice(0, 6),
					valueSuffixBytes: encoded.slice(6),
				},
				{
					key: "doc-2",
					valuePrefixBytes: encoded.slice(0, 6),
					valueSuffixBytes: encoded.slice(6),
				},
			],
			8,
		);
		expect(
			Array.from(backbone.documentKeysExist(["doc-2", "missing", "doc-1"])),
		).to.deep.equal([1, 0, 1]);
		expect(
			Array.from(backbone.deleteDocumentsResult(["doc-2", "missing", "doc-1"])),
		).to.deep.equal([1, 0, 1]);
		expect(backbone.documentIndexLength).to.equal(0);
		expect(backbone.documentValueLength).to.equal(0);
	});

	it("coalesces no-next appends with document index commits", async () => {
		const backbone = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});
		backbone.configureDocumentSchemaIr(contextOnlySchema());

		const result = backbone.appendPlainNoNextTransaction({
			wallTime: 10n,
			logical: 1,
			gid: "gid-doc-index",
			payloadData: new Uint8Array([1, 2, 3]),
			replicas: 1,
			selfHash: "peer",
			documentIndex: {
				key: "doc-1",
				valuePrefixBytes: new Uint8Array(0),
			},
		});

		expect(backbone.documentValueLength).to.equal(1);
		expect(backbone.documentExactStringFirstKey(3, result.entry.hash)).to.equal(
			"doc-1",
		);
		expect(backbone.documentExactStringFirstKey(4, "gid-doc-index")).to.equal(
			"doc-1",
		);
		expect(backbone.documentValueBytes("doc-1")).to.exist;
	});

	it("returns compact committed no-next document index facts", async () => {
		const backbone = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});
		backbone.configureDocumentSchemaIr(contextOnlySchema());
		backbone.setDocumentContextHeadField(3);
		backbone.setDocumentContextFields({
			created: 1,
			modified: 2,
			head: 3,
			gid: 4,
			size: 5,
		});

		const first =
			backbone.preparePlainCommittedNoNextStorageAppendDocumentIndexCompactTransaction(
				{
					wallTime: 10n,
					logical: 1,
					gid: "gid-doc-index-compact",
					payloadData: new Uint8Array([1, 2, 3]),
					replicas: 1,
					selfHash: "peer",
					documentIndex: {
						key: "doc-compact-1",
						valuePrefixBytes: new Uint8Array(0),
					},
				},
			);
		const second =
			backbone.preparePlainCommittedNoNextStorageAppendDocumentIndexCompactTransaction(
				{
					wallTime: 11n,
					logical: 2,
					gid: "gid-doc-index-compact",
					payloadData: new Uint8Array([4, 5, 6]),
					replicas: 1,
					selfHash: "peer",
					trimLengthTo: 1,
					documentIndex: {
						key: "doc-compact-2",
						valuePrefixBytes: new Uint8Array(0),
						deleteTrimmedHeads: true,
					},
				},
			);

		expect(first.entry.bytes).equal(undefined);
		expect(first.entry.next).to.deep.equal([]);
		expect(first.entry.hashDigestBytes).equal(undefined);
		expect(first.trimmedGids).equal(undefined);
		expect(first.documentTrimmedHeadsProcessed).equal(undefined);
		expect(second.entry.bytes).equal(undefined);
		expect(second.entry.next).to.deep.equal([]);
		expect(second.trimmed).to.deep.equal([]);
		expect(second.trimmedHashes).to.deep.equal([first.entry.hash]);
		expect(second.trimmedGids).to.deep.equal(["gid-doc-index-compact"]);
		expect(second.documentTrimmedHeadsProcessed).equal(true);
		expect(backbone.hasLogEntry(first.entry.hash)).equal(false);
		expect(backbone.hasBlock(first.entry.hash)).equal(false);
		expect(backbone.hasLogEntry(second.entry.hash)).equal(true);
		expect(backbone.hasBlock(second.entry.hash)).equal(true);
		expect(backbone.documentValueLength).to.equal(1);
		expect(backbone.documentExactStringFirstKey(3, second.entry.hash)).to.equal(
			"doc-compact-2",
		);
		expect(
			backbone.documentExactStringFirstKey(4, "gid-doc-index-compact"),
		).to.equal("doc-compact-2");
	});

	it("keeps multi-victim inline trim hashes and gids in parallel order", async () => {
		const backbone = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});
		configureContextDocumentIndex(backbone);

		const firstVictim = prepareGraphCommit(backbone, {
			wallTime: 10n,
			gid: "gid-inline-victim-a",
			payloadData: new Uint8Array([1]),
			documentIndex: {
				key: "doc-inline-victim-a",
				valuePrefixBytes: new Uint8Array(0),
			},
		});
		const secondVictim = prepareGraphCommit(backbone, {
			wallTime: 11n,
			gid: "gid-inline-victim-b",
			payloadData: new Uint8Array([2]),
			documentIndex: {
				key: "doc-inline-victim-b",
				valuePrefixBytes: new Uint8Array(0),
			},
		});
		const updated = prepareGraphCommit(backbone, {
			wallTime: 12n,
			gid: "gid-inline-appended",
			payloadData: new Uint8Array([3]),
			trimLengthTo: 1,
			resolveTrimmedEntries: false,
			documentIndex: {
				key: "doc-inline-appended",
				valuePrefixBytes: new Uint8Array(0),
				deleteTrimmedHeads: true,
			},
		});

		expect(updated.trimmedEntryHashes).to.deep.equal([
			firstVictim.hash,
			secondVictim.hash,
		]);
		expect(updated.trimmedEntryGids).to.deep.equal([
			"gid-inline-victim-a",
			"gid-inline-victim-b",
		]);
		expect(updated.documentTrimmedHeadsProcessed).to.equal(true);
		expect(backbone.documentValueBytes("doc-inline-victim-a")).to.equal(
			undefined,
		);
		expect(backbone.documentValueBytes("doc-inline-victim-b")).to.equal(
			undefined,
		);
		expect(backbone.documentValueBytes("doc-inline-appended")).to.exist;
	});

	it("returns the victim gid from cached plain-payload document trim refs", async () => {
		const backbone = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});
		configureContextDocumentIndex(backbone);

		const victim = prepareGraphCommit(backbone, {
			wallTime: 20n,
			gid: "gid-cached-victim",
			payloadData: new Uint8Array([1]),
			documentIndex: {
				key: "doc-cached-victim",
				valuePrefixBytes: new Uint8Array(0),
			},
		});
		const updated = prepareGraphCommit(backbone, {
			wallTime: 21n,
			gid: "gid-cached-appended",
			payloadData: plainPutPayload(
				encodedDocumentWithIdScoreAndBytes("cached", 21),
			),
			trimLengthTo: 1,
			resolveTrimmedEntries: false,
			documentIndex: {
				key: "doc-cached-appended",
				deleteTrimmedHeads: true,
				projection: {
					encodedDocument: new Uint8Array(0),
					plan: contextOnlyProjectionPlan,
				},
			},
		});

		expect(updated.trimmedEntryHashes).to.deep.equal([victim.hash]);
		expect(updated.trimmedEntryGids).to.deep.equal(["gid-cached-victim"]);
		expect(updated.documentTrimmedHeadsProcessed).to.equal(true);
		expect(backbone.documentValueBytes("doc-cached-victim")).to.equal(
			undefined,
		);
		expect(backbone.documentValueBytes("doc-cached-appended")).to.exist;
	});

	it("returns the victim gid from cached document trim refs without plain-payload support", async () => {
		const backbone = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});
		configureContextDocumentIndex(backbone);
		hideGraphCapability(
			backbone,
			"prepare_plain_entry_commit_no_next_facts_document_index_cached_plan_compact_trim_refs_plain_put_payload",
		);
		hideGraphCapability(
			backbone,
			"prepare_plain_entry_commit_no_next_facts_document_index_cached_plan_compact_trim_hashes_plain_put_payload",
		);

		const victim = prepareGraphCommit(backbone, {
			wallTime: 25n,
			gid: "gid-cached-standard-victim",
			payloadData: new Uint8Array([1]),
			documentIndex: {
				key: "doc-cached-standard-victim",
				valuePrefixBytes: new Uint8Array(0),
			},
		});
		const updated = prepareGraphCommit(backbone, {
			wallTime: 26n,
			gid: "gid-cached-standard-appended",
			payloadData: new Uint8Array([2]),
			trimLengthTo: 1,
			resolveTrimmedEntries: false,
			documentIndex: {
				key: "doc-cached-standard-appended",
				deleteTrimmedHeads: true,
				projection: {
					encodedDocument: encodedDocumentWithIdScoreAndBytes(
						"cached-standard",
						26,
					),
					plan: contextOnlyProjectionPlan,
				},
			},
		});

		expect(updated.trimmedEntryHashes).to.deep.equal([victim.hash]);
		expect(updated.trimmedEntryGids).to.deep.equal([
			"gid-cached-standard-victim",
		]);
		expect(updated.documentTrimmedHeadsProcessed).to.equal(true);
		expect(backbone.documentValueBytes("doc-cached-standard-victim")).to.equal(
			undefined,
		);
		expect(backbone.documentValueBytes("doc-cached-standard-appended")).to
			.exist;
	});

	it("returns an unrelated victim gid while preserving latest document context", async () => {
		const backbone = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});
		configureContextDocumentIndex(backbone);

		const victim = prepareGraphCommit(backbone, {
			wallTime: 30n,
			gid: "gid-latest-victim",
			payloadData: new Uint8Array([1]),
			documentIndex: {
				key: "doc-latest-victim",
				valuePrefixBytes: new Uint8Array(0),
			},
		});
		const previous = prepareGraphCommit(backbone, {
			wallTime: 31n,
			gid: "gid-latest-target",
			payloadData: new Uint8Array([2]),
			documentIndex: {
				key: "doc-latest-target",
				valuePrefixBytes: new Uint8Array(0),
			},
		});
		const updated = prepareGraphCommit(backbone, {
			wallTime: 32n,
			gid: "gid-latest-fallback",
			payloadData: new Uint8Array([3]),
			trimLengthTo: 2,
			resolveTrimmedEntries: false,
			documentIndex: {
				key: "doc-latest-target",
				valuePrefixBytes: new Uint8Array(0),
				deleteTrimmedHeads: true,
				useLatestContext: true,
			},
		});

		expect(updated.next).to.deep.equal([previous.hash]);
		expect(updated.trimmedEntryHashes).to.deep.equal([victim.hash]);
		expect(updated.trimmedEntryGids).to.deep.equal(["gid-latest-victim"]);
		expect(updated.documentPreviousContext?.head).to.equal(previous.hash);
		expect(updated.documentTrimmedHeadsProcessed).to.equal(true);
		expect(backbone.documentValueBytes("doc-latest-victim")).to.equal(
			undefined,
		);
		expect(backbone.documentValueBytes("doc-latest-target")).to.exist;
	});

	it("returns an unrelated victim gid from cached latest-context trim refs", async () => {
		const backbone = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});
		configureContextDocumentIndex(backbone);

		const victim = prepareGraphCommit(backbone, {
			wallTime: 35n,
			gid: "gid-latest-cached-victim",
			payloadData: new Uint8Array([1]),
			documentIndex: {
				key: "doc-latest-cached-victim",
				valuePrefixBytes: new Uint8Array(0),
			},
		});
		const previous = prepareGraphCommit(backbone, {
			wallTime: 36n,
			gid: "gid-latest-cached-target",
			payloadData: new Uint8Array([2]),
			documentIndex: {
				key: "doc-latest-cached-target",
				valuePrefixBytes: new Uint8Array(0),
			},
		});
		const updated = prepareGraphCommit(backbone, {
			wallTime: 37n,
			gid: "gid-latest-cached-fallback",
			payloadData: new Uint8Array([3]),
			trimLengthTo: 2,
			resolveTrimmedEntries: false,
			documentIndex: {
				key: "doc-latest-cached-target",
				deleteTrimmedHeads: true,
				useLatestContext: true,
				projection: {
					encodedDocument: encodedDocumentWithIdScoreAndBytes(
						"latest-cached",
						37,
					),
					plan: contextOnlyProjectionPlan,
				},
			},
		});

		expect(updated.next).to.deep.equal([previous.hash]);
		expect(updated.trimmedEntryHashes).to.deep.equal([victim.hash]);
		expect(updated.trimmedEntryGids).to.deep.equal([
			"gid-latest-cached-victim",
		]);
		expect(updated.documentPreviousContext?.head).to.equal(previous.hash);
		expect(updated.documentTrimmedHeadsProcessed).to.equal(true);
		expect(backbone.documentValueBytes("doc-latest-cached-victim")).to.equal(
			undefined,
		);
		expect(backbone.documentValueBytes("doc-latest-cached-target")).to.exist;
	});

	it("preserves all five legacy hash-only document trim fallbacks", async () => {
		const cases = [
			{
				name: "inline",
				cached: false,
				latest: false,
				plainPutPayload: false,
				hide: [
					"prepare_plain_entry_commit_no_next_facts_document_index_compact_trim_refs",
				],
			},
			{
				name: "cached-plain-payload",
				cached: true,
				latest: false,
				plainPutPayload: true,
				hide: [
					"prepare_plain_entry_commit_no_next_facts_document_index_cached_plan_compact_trim_refs_plain_put_payload",
				],
			},
			{
				name: "cached-standard",
				cached: true,
				latest: false,
				plainPutPayload: false,
				hide: [
					"prepare_plain_entry_commit_no_next_facts_document_index_cached_plan_compact_trim_refs_plain_put_payload",
					"prepare_plain_entry_commit_no_next_facts_document_index_cached_plan_compact_trim_hashes_plain_put_payload",
					"prepare_plain_entry_commit_no_next_facts_document_index_cached_plan_compact_trim_refs",
				],
			},
			{
				name: "latest-inline",
				cached: false,
				latest: true,
				plainPutPayload: false,
				hide: [
					"prepare_plain_entry_commit_latest_facts_document_index_trim_refs",
				],
			},
			{
				name: "latest-cached",
				cached: true,
				latest: true,
				plainPutPayload: false,
				hide: [
					"prepare_plain_entry_commit_latest_facts_document_index_cached_plan_trim_refs",
				],
			},
		] as const;

		for (const [index, testCase] of cases.entries()) {
			const backbone = await createNativePeerbitBackbone({
				clockId: publicKey,
				privateKey,
				publicKey,
			});
			configureContextDocumentIndex(backbone);
			for (const capability of testCase.hide) {
				hideGraphCapability(backbone, capability);
			}

			const prefix = `legacy-${testCase.name}`;
			const wallTime = BigInt(100 + index * 10);
			const victim = prepareGraphCommit(backbone, {
				wallTime,
				gid: `gid-${prefix}-victim`,
				payloadData: new Uint8Array([1]),
				documentIndex: {
					key: `doc-${prefix}-victim`,
					valuePrefixBytes: new Uint8Array(0),
				},
			});
			const targetKey = `doc-${prefix}-${
				testCase.latest ? "target" : "appended"
			}`;
			const previous = testCase.latest
				? prepareGraphCommit(backbone, {
						wallTime: wallTime + 1n,
						gid: `gid-${prefix}-target`,
						payloadData: new Uint8Array([2]),
						documentIndex: {
							key: targetKey,
							valuePrefixBytes: new Uint8Array(0),
						},
					})
				: undefined;
			const updated = prepareGraphCommit(backbone, {
				wallTime: wallTime + (testCase.latest ? 2n : 1n),
				gid: `gid-${prefix}-fallback`,
				payloadData: testCase.plainPutPayload
					? plainPutPayload(encodedDocumentWithIdScoreAndBytes(prefix, index))
					: new Uint8Array([3]),
				trimLengthTo: testCase.latest ? 2 : 1,
				resolveTrimmedEntries: false,
				documentIndex: {
					key: targetKey,
					valuePrefixBytes: testCase.cached ? undefined : new Uint8Array(0),
					deleteTrimmedHeads: true,
					useLatestContext: testCase.latest,
					projection: testCase.cached
						? {
								encodedDocument: testCase.plainPutPayload
									? new Uint8Array(0)
									: encodedDocumentWithIdScoreAndBytes(prefix, index),
								plan: contextOnlyProjectionPlan,
							}
						: undefined,
				},
			});

			expect(updated.trimmedEntryHashes, testCase.name).to.deep.equal([
				victim.hash,
			]);
			expect(updated.trimmedEntryGids, testCase.name).to.equal(undefined);
			expect(updated.documentTrimmedHeadsProcessed, testCase.name).to.equal(
				true,
			);
			expect(
				backbone.documentValueBytes(`doc-${prefix}-victim`),
				testCase.name,
			).to.equal(undefined);
			expect(backbone.documentValueBytes(targetKey), testCase.name).to.exist;
			if (previous) {
				expect(updated.next, testCase.name).to.deep.equal([previous.hash]);
				expect(updated.documentPreviousContext?.head, testCase.name).to.equal(
					previous.hash,
				);
			}
		}
	});

	it("batches compact committed no-next document index transactions", async () => {
		const backbone = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});
		backbone.configureDocumentSchemaIr(contextOnlySchema());
		backbone.setDocumentContextHeadField(3);

		const results =
			backbone.preparePlainCommittedNoNextStorageAppendDocumentIndexCompactBatchTransaction(
				{
					entries: [
						{
							wallTime: 20n,
							logical: 1,
							gid: "gid-doc-index-batch",
							payloadData: new Uint8Array([1, 2, 3]),
							documentIndex: {
								key: "doc-batch-1",
								valuePrefixBytes: new Uint8Array(0),
							},
						},
						{
							wallTime: 21n,
							logical: 2,
							gid: "gid-doc-index-batch",
							payloadData: new Uint8Array([4, 5, 6]),
							documentIndex: {
								key: "doc-batch-2",
								valuePrefixBytes: new Uint8Array(0),
							},
						},
					],
					replicas: 1,
					selfHash: "peer",
				},
			);

		expect(results).to.have.length(2);
		expect(results?.map((result) => result.entry.bytes)).to.deep.equal([
			undefined,
			undefined,
		]);
		expect(results?.map((result) => result.entry.next)).to.deep.equal([[], []]);
		expect(results?.map((result) => result.trimmedGids)).to.deep.equal([
			undefined,
			undefined,
		]);
		expect(
			results?.map((result) => result.documentTrimmedHeadsProcessed),
		).to.deep.equal([undefined, undefined]);
		expect(backbone.documentValueLength).to.equal(2);
		expect(backbone.getEntryCoordinateHashes()).to.deep.equal(
			results?.map((result) => result.entry.hash),
		);
		expect(
			backbone.documentExactStringFirstKey(3, results![0]!.entry.hash),
		).to.equal("doc-batch-1");
		expect(
			backbone.documentExactStringFirstKey(3, results![1]!.entry.hash),
		).to.equal("doc-batch-2");
	});

	it("batches compact committed no-next cached-plan document index transactions", async () => {
		const backbone = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});
		backbone.configureDocumentSchemaIr(contextOnlySchema());
		backbone.setDocumentContextHeadField(3);

		const projection = {
			documentFieldNames: ["id", "score", "bytes"],
			documentFieldTypes: ["string", "u32", "bytes"],
			outputFieldTypes: [],
			sourceKinds: [],
			sourceValues: [],
		};
		const results =
			backbone.preparePlainCommittedNoNextStorageAppendDocumentIndexCompactBatchTransaction(
				{
					entries: [
						{
							wallTime: 30n,
							logical: 1,
							gid: "gid-doc-index-projected-batch",
							payloadData: new Uint8Array([1, 2, 3]),
							documentIndex: {
								key: "doc-projected-batch-1",
								projection: {
									encodedDocument: encodedDocumentWithIdScoreAndBytes("abc", 7),
									plan: projection,
								},
							},
						},
						{
							wallTime: 31n,
							logical: 2,
							gid: "gid-doc-index-projected-batch",
							payloadData: new Uint8Array([4, 5, 6]),
							documentIndex: {
								key: "doc-projected-batch-2",
								projection: {
									encodedDocument: encodedDocumentWithIdScoreAndBytes("def", 8),
									plan: projection,
								},
							},
						},
					],
					replicas: 1,
					selfHash: "peer",
				},
			);

		expect(results).to.have.length(2);
		expect(results?.map((result) => result.entry.bytes)).to.deep.equal([
			undefined,
			undefined,
		]);
		expect(backbone.documentValueLength).to.equal(2);
		expect(
			backbone.documentExactStringFirstKey(3, results![0]!.entry.hash),
		).to.equal("doc-projected-batch-1");
		expect(
			backbone.documentExactStringFirstKey(3, results![1]!.entry.hash),
		).to.equal("doc-projected-batch-2");
	});

	it("batches compact committed no-next cached-plan plain-put-payload document index transactions", async () => {
		const backbone = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});
		backbone.configureDocumentSchemaIr(contextOnlySchema());
		backbone.setDocumentContextHeadField(3);

		const projection = {
			documentFieldNames: ["id", "score", "bytes"],
			documentFieldTypes: ["string", "u32", "bytes"],
			outputFieldTypes: [],
			sourceKinds: [],
			sourceValues: [],
		};
		const documentA = encodedDocumentWithIdScoreAndBytes("payload-a", 11);
		const documentB = encodedDocumentWithIdScoreAndBytes("payload-b", 12);
		const results =
			backbone.preparePlainCommittedNoNextStorageAppendDocumentIndexCompactBatchTransaction(
				{
					entries: [
						{
							wallTime: 32n,
							logical: 1,
							gid: "gid-doc-index-projected-payload-batch",
							payloadData: plainPutPayload(documentA),
							documentIndex: {
								key: "doc-projected-payload-batch-1",
								usePlainPutPayload: true,
								projection: {
									encodedDocument: new Uint8Array(0),
									plan: projection,
								},
							},
						},
						{
							wallTime: 33n,
							logical: 2,
							gid: "gid-doc-index-projected-payload-batch",
							payloadData: plainPutPayload(documentB),
							documentIndex: {
								key: "doc-projected-payload-batch-2",
								usePlainPutPayload: true,
								projection: {
									encodedDocument: new Uint8Array(0),
									plan: projection,
								},
							},
						},
					],
					replicas: 1,
					selfHash: "peer",
				},
			);

		expect(results).to.have.length(2);
		expect(results?.map((result) => result.entry.bytes)).to.deep.equal([
			undefined,
			undefined,
		]);
		expect(backbone.documentValueLength).to.equal(2);
		expect(
			backbone.documentExactStringFirstKey(3, results![0]!.entry.hash),
		).to.equal("doc-projected-payload-batch-1");
		expect(
			backbone.documentExactStringFirstKey(3, results![1]!.entry.hash),
		).to.equal("doc-projected-payload-batch-2");
	});

	it("commits compact no-next identity document indexes from plain put payloads", async () => {
		const backbone = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});
		backbone.configureDocumentSchemaIr(contextOnlySchema());
		backbone.setDocumentContextHeadField(3);
		backbone.setDocumentContextFields({
			created: 1,
			modified: 2,
			head: 3,
			gid: 4,
			size: 5,
		});

		const result =
			backbone.preparePlainCommittedNoNextStorageAppendDocumentIndexCompactTransaction(
				{
					wallTime: 34n,
					logical: 1,
					gid: "gid-doc-index-identity-payload",
					payloadData: plainPutPayload(new Uint8Array(0)),
					documentIndex: {
						key: "doc-identity-payload",
						valuePrefixBytes: new Uint8Array(0),
						usePlainPutPayload: true,
					},
					replicas: 1,
					selfHash: "peer",
				},
			);

		expect(result.entry.bytes).equal(undefined);
		expect(backbone.documentValueLength).to.equal(1);
		expect(backbone.documentExactStringFirstKey(3, result.entry.hash)).to.equal(
			"doc-identity-payload",
		);
	});

	it("batches committed latest-context document index transactions", async () => {
		const backbone = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});
		backbone.configureDocumentSchemaIr(contextOnlySchema());
		backbone.setDocumentContextHeadField(3);
		backbone.setDocumentContextFields({
			created: 1,
			modified: 2,
			head: 3,
			gid: 4,
			size: 5,
		});

		const first =
			backbone.preparePlainCommittedNoNextStorageAppendDocumentIndexCompactBatchTransaction(
				{
					entries: [
						{
							wallTime: 40n,
							logical: 1,
							gid: "gid-doc-latest-batch",
							payloadData: new Uint8Array([1, 2, 3]),
							documentIndex: {
								key: "doc-latest-batch-1",
								valuePrefixBytes: new Uint8Array(0),
							},
						},
					],
					replicas: 1,
					selfHash: "peer",
				},
			)!;

		const updated =
			backbone.preparePlainCommittedStorageAppendDocumentIndexLatestBatchTransaction(
				{
					entries: [
						{
							wallTime: 41n,
							logical: 2,
							gid: "fallback-doc-latest-batch",
							payloadData: new Uint8Array([4, 5, 6]),
							documentIndex: {
								key: "doc-latest-batch-1",
								valuePrefixBytes: new Uint8Array(0),
							},
						},
					],
					replicas: 1,
					selfHash: "peer",
					resolveTrimmedEntries: false,
				},
			)!;

		expect(updated).to.have.length(1);
		expect(updated[0]!.entry.next).to.deep.equal([first[0]!.entry.hash]);
		expect(updated[0]!.coordinate.gid).to.equal("gid-doc-latest-batch");
		expect(updated[0]!.documentPreviousContext?.head).to.equal(
			first[0]!.entry.hash,
		);
		expect(backbone.documentValueLength).to.equal(1);
		expect(
			backbone.documentExactStringFirstKey(3, updated[0]!.entry.hash),
		).to.equal("doc-latest-batch-1");
	});

	it("commits latest-context identity document indexes from plain put payloads", async () => {
		const backbone = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});
		backbone.configureDocumentSchemaIr(contextOnlySchema());
		backbone.setDocumentContextHeadField(3);
		backbone.setDocumentContextFields({
			created: 1,
			modified: 2,
			head: 3,
			gid: 4,
			size: 5,
		});

		const first =
			backbone.preparePlainCommittedNoNextStorageAppendDocumentIndexCompactTransaction(
				{
					wallTime: 44n,
					logical: 1,
					gid: "gid-doc-latest-identity",
					payloadData: plainPutPayload(new Uint8Array(0)),
					documentIndex: {
						key: "doc-latest-identity",
						valuePrefixBytes: new Uint8Array(0),
						usePlainPutPayload: true,
					},
					replicas: 1,
					selfHash: "peer",
				},
			);

		const updated = backbone.preparePlainCommittedStorageAppendTransaction({
			wallTime: 45n,
			logical: 2,
			gid: "fallback-doc-latest-identity",
			payloadData: plainPutPayload(new Uint8Array(0)),
			documentIndex: {
				key: "doc-latest-identity",
				valuePrefixBytes: new Uint8Array(0),
				useLatestContext: true,
				usePlainPutPayload: true,
			},
			replicas: 1,
			selfHash: "peer",
			resolveTrimmedEntries: false,
		});

		expect(updated.entry.next).to.deep.equal([first.entry.hash]);
		expect(updated.coordinate.gid).to.equal("gid-doc-latest-identity");
		expect(backbone.documentValueLength).to.equal(1);
		expect(
			backbone.documentExactStringFirstKey(3, updated.entry.hash),
		).to.equal("doc-latest-identity");
	});

	it("batches committed latest-context cached-plan plain-put-payload document index transactions", async () => {
		const backbone = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});
		backbone.configureDocumentSchemaIr(contextOnlySchema());
		backbone.setDocumentContextHeadField(3);
		backbone.setDocumentContextFields({
			created: 1,
			modified: 2,
			head: 3,
			gid: 4,
			size: 5,
		});

		const first =
			backbone.preparePlainCommittedNoNextStorageAppendDocumentIndexCompactBatchTransaction(
				{
					entries: [
						{
							wallTime: 42n,
							logical: 1,
							gid: "gid-doc-latest-payload-batch",
							payloadData: new Uint8Array([1, 2, 3]),
							documentIndex: {
								key: "doc-latest-payload-batch-1",
								valuePrefixBytes: new Uint8Array(0),
							},
						},
					],
					replicas: 1,
					selfHash: "peer",
				},
			)!;

		const projection = {
			documentFieldNames: ["id", "score", "bytes"],
			documentFieldTypes: ["string", "u32", "bytes"],
			outputFieldTypes: [],
			sourceKinds: [],
			sourceValues: [],
		};
		const document = encodedDocumentWithIdScoreAndBytes("latest-payload", 13);
		const updated =
			backbone.preparePlainCommittedStorageAppendDocumentIndexLatestBatchTransaction(
				{
					entries: [
						{
							wallTime: 43n,
							logical: 2,
							gid: "fallback-doc-latest-payload-batch",
							payloadData: plainPutPayload(document),
							documentIndex: {
								key: "doc-latest-payload-batch-1",
								usePlainPutPayload: true,
								projection: {
									encodedDocument: new Uint8Array(0),
									plan: projection,
								},
							},
						},
					],
					replicas: 1,
					selfHash: "peer",
					resolveTrimmedEntries: false,
				},
			)!;

		expect(updated).to.have.length(1);
		expect(updated[0]!.entry.next).to.deep.equal([first[0]!.entry.hash]);
		expect(updated[0]!.coordinate.gid).to.equal("gid-doc-latest-payload-batch");
		expect(updated[0]!.documentPreviousContext?.head).to.equal(
			first[0]!.entry.hash,
		);
		expect(backbone.documentValueLength).to.equal(1);
		expect(
			backbone.documentExactStringFirstKey(3, updated[0]!.entry.hash),
		).to.equal("doc-latest-payload-batch-1");
	});

	it("coalesces trim deletes with shared-log coordinate state", async () => {
		const backbone = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});

		const first = backbone.appendPlainNoNextTransaction({
			wallTime: 1n,
			gid: "gid-a",
			payloadData: new Uint8Array([1]),
			replicas: 1,
			selfHash: "peer-a",
		});
		const second = backbone.appendPlainNoNextTransaction({
			wallTime: 2n,
			gid: "gid-a",
			payloadData: new Uint8Array([2]),
			replicas: 1,
			selfHash: "peer-a",
			trimLengthTo: 1,
		});

		expect(second.trimmed.map((entry) => entry.hash)).to.deep.equal([
			first.entry.hash,
		]);
		expect(backbone.logLength).to.equal(1);
		expect(backbone.blockLength).to.equal(1);
		expect(backbone.hasLogEntry(first.entry.hash)).to.equal(false);
		expect(backbone.hasBlock(first.entry.hash)).to.equal(false);
		expect(backbone.hasLogEntry(second.entry.hash)).to.equal(true);
		expect(backbone.hasBlock(second.entry.hash)).to.equal(true);
		expect(backbone.getEntryCoordinateHashes()).to.deep.equal([
			second.entry.hash,
		]);
		expect(backbone.coordinateIndexLength).to.equal(1);
		expect(backbone.coordinateValueLength).to.equal(1);
		expect(backbone.hasCoordinateIndexHash(first.entry.hash)).to.equal(false);
		expect(backbone.hasCoordinateIndexHash(second.entry.hash)).to.equal(true);
	});

	it("can update graph while returning block bytes for external storage", async () => {
		const backbone = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});

		const prepared = backbone.storageBackedGraph.prepareEntryV0PlainEntryAndPut(
			{
				clockId: publicKey,
				privateKey,
				publicKey,
				wallTime: 1n,
				gid: "gid-external",
				payloadData: new Uint8Array([7, 8, 9]),
				includeMaterializationBytes: false,
				includeAppendFactsBytes: true,
			},
		);

		expect(prepared?.hash).to.be.a("string").and.not.empty;
		expect(prepared?.bytes).to.be.instanceOf(Uint8Array);
		expect(prepared?.bytes?.byteLength).to.be.greaterThan(0);
		expect(prepared?.hashDigestBytes).to.have.length.greaterThan(0);
		expect(backbone.hasLogEntry(prepared!.hash)).equal(true);
		expect(backbone.hasBlock(prepared!.hash)).equal(false);
		expect(backbone.blockLength).equal(0);
	});

	it("tracks heads and next adjacency through append-chain graph batches", async () => {
		const backbone = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});
		const entry = (
			hash: string,
			next: string[],
			wallTime: bigint,
			head?: boolean,
		) => ({
			hash,
			gid: "gid-chain",
			next,
			type: 0,
			head,
			payloadSize: 1,
			clock: { timestamp: { wallTime, logical: 0 } },
		});

		backbone.graph.put(entry("root", [], 1n));
		backbone.graph.putAppendChain([
			entry("a", ["root"], 2n, false),
			entry("b", ["a"], 3n, false),
			entry("c", ["b"], 4n, true),
		]);

		expect(backbone.graph.heads()).to.deep.equal(["c"]);
		expect(backbone.graph.countHasNext("root")).to.equal(1);
		expect(backbone.graph.countHasNext("a")).to.equal(1);
		expect(backbone.graph.countHasNext("b")).to.equal(1);
		expect(backbone.graph.payloadSizeSum()).to.equal(4);
	});

	it("reads entry metadata hints through the WASM receiver", async () => {
		const backbone = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});
		const prepared = backbone.storageBackedGraph.prepareEntryV0PlainEntryAndPut(
			{
				clockId: publicKey,
				privateKey,
				publicKey,
				wallTime: 1n,
				gid: "gid-metadata-hints",
				metaData: new Uint8Array([7]),
				payloadData: new Uint8Array([1]),
				includeMaterializationBytes: false,
				includeAppendFactsBytes: true,
			},
		);

		expect(
			backbone.graph.entryMetadataHintsBatch([prepared.hash, "missing"]),
		).to.deep.equal([
			{
				hash: prepared.hash,
				gid: "gid-metadata-hints",
				data: new Uint8Array([7]),
			},
			undefined,
		]);
	});

	it("commits blocks graph and coordinates in one native batch", async () => {
		const source = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});
		const target = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});
		const prepared = source.storageBackedGraph.prepareEntryV0PlainEntryAndPut({
			clockId: publicKey,
			privateKey,
			publicKey,
			wallTime: 1n,
			gid: "gid-combined",
			payloadData: new Uint8Array([1, 2, 3]),
			includeMaterializationBytes: false,
			includeAppendFactsBytes: true,
		});

		target.graph.commitBlocksGraphAndCoordinatesBatch(
			[
				{
					hash: prepared.hash,
					gid: "gid-combined",
					next: [],
					type: 0,
					payloadSize: prepared.byteLength,
					clock: { timestamp: { wallTime: 1n, logical: 0 } },
					bytes: prepared.bytes,
				},
			],
			{
				hashes: [prepared.hash],
				gids: ["gid-combined"],
				hashNumbers: ["7"],
				coordinateBatches: [["42"]],
				nextHashBatches: [[]],
				assignedToRangeBoundaries: new Uint8Array([1]),
				requestedReplicas: [1],
			},
		);

		expect(target.hasBlock(prepared.hash)).to.equal(true);
		expect(target.hasLogEntry(prepared.hash)).to.equal(true);
		expect(target.getEntryCoordinateHashes()).to.deep.equal([prepared.hash]);
		expect(target.hasCoordinateIndexHash(prepared.hash)).to.equal(true);
	});

	it("prepares raw receive entries and commits them by hash", async () => {
		const source = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});
		const target = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});
		const metaData = new Uint8Array([0, 3, 0, 0, 0]);
		const prepared = source.storageBackedGraph.prepareEntryV0PlainEntryAndPut({
			clockId: publicKey,
			privateKey,
			publicKey,
			wallTime: 1n,
			gid: "gid-raw-receive",
			metaData,
			payloadData: new Uint8Array([4, 5, 6]),
			includeMaterializationBytes: false,
			includeAppendFactsBytes: true,
		});

		const [facts] = target.prepareRawReceiveBatch([prepared.bytes]);
		expect(facts.cid).to.equal(prepared.hash);
		expect(facts.gid).to.equal("gid-raw-receive");
		expect(facts.requestedReplicas).to.equal(3);
		const expectedHashNumber = new DataView(
			prepared.hashDigestBytes!.buffer,
			prepared.hashDigestBytes!.byteOffset,
			prepared.hashDigestBytes!.byteLength,
		).getBigUint64(0, true);
		expect(facts.hashNumber).to.equal(String(expectedHashNumber));
		const expectedColumns = target.prepareRawReceiveColumnsBatch(
			[prepared.bytes],
			[prepared.hash],
		);
		expect(expectedColumns?.[0]).to.deep.equal([prepared.hash]);
		expect(Array.from(expectedColumns?.[12] ?? [])).to.deep.equal([1]);
		expect(Array.from(expectedColumns?.[13] ?? [])).to.deep.equal([3]);
		expect(Array.from(expectedColumns?.[14] ?? [], String)).to.deep.equal([
			String(expectedHashNumber),
		]);
		const compactExpectedColumns = target.prepareRawReceiveExpectedColumnsBatch(
			[prepared.bytes],
			[prepared.hash],
		);
		expect(compactExpectedColumns?.[0]).to.deep.equal([]);
		expect(compactExpectedColumns?.[1][0]).to.equal(undefined);
		expect(Array.from(compactExpectedColumns?.[12] ?? [])).to.deep.equal([1]);
		expect(
			Array.from(compactExpectedColumns?.[14] ?? [], String),
		).to.deep.equal([String(expectedHashNumber)]);
		const unverifiedColumns = target.prepareRawReceiveColumnsBatch(
			[prepared.bytes],
			[prepared.hash],
			{ verifySignatures: false },
		);
		expect(unverifiedColumns?.[0]).to.deep.equal([prepared.hash]);
		expect(Array.from(unverifiedColumns?.[12] ?? [])).to.deep.equal([0]);
		expect(Array.from(unverifiedColumns?.[13] ?? [])).to.deep.equal([3]);
		expect(Array.from(unverifiedColumns?.[14] ?? [], String)).to.deep.equal([
			String(expectedHashNumber),
		]);
		const compactUnverifiedColumns =
			target.prepareRawReceiveExpectedColumnsBatch(
				[prepared.bytes],
				[prepared.hash],
				{ verifySignatures: false },
			);
		expect(compactUnverifiedColumns?.[0]).to.deep.equal([]);
		expect(compactUnverifiedColumns?.[1][0]).to.equal(undefined);
		expect(Array.from(compactUnverifiedColumns?.[12] ?? [])).to.deep.equal([0]);
		expect(
			target.graph.verifyPreparedRawReceiveEntries([prepared.hash]),
		).to.deep.equal([true]);
		expect(target.graph.verifyPreparedRawReceiveEntries(["missing"])).to.equal(
			undefined,
		);
		expect(() =>
			target.prepareRawReceiveColumnsBatch([prepared.bytes], ["not-a-cid"]),
		).to.throw("Expected base58btc CID");
		expect(
			target.graph.commitPreparedRawReceiveBatch([prepared.hash], [true], {
				hashes: [prepared.hash],
				gids: ["gid-raw-receive"],
				hashNumbers: ["9"],
				coordinateBatches: [["11"]],
				nextHashBatches: [[]],
				assignedToRangeBoundaries: new Uint8Array([0]),
				requestedReplicas: [1],
			}),
		).to.equal(true);

		expect(target.hasBlock(prepared.hash)).to.equal(true);
		expect(target.hasLogEntry(prepared.hash)).to.equal(true);
		expect(target.graph.heads()).to.deep.equal([prepared.hash]);
		expect(target.getEntryCoordinateHashes()).to.deep.equal([prepared.hash]);
		expect(
			target.graph.commitPreparedRawReceiveBatch([prepared.hash], [true]),
		).to.equal(false);

		const verifiedCommit =
			source.storageBackedGraph.prepareEntryV0PlainEntryAndPut({
				clockId: publicKey,
				privateKey,
				publicKey,
				wallTime: 2n,
				gid: "gid-raw-receive-verified-commit",
				metaData,
				payloadData: new Uint8Array([7, 8, 9]),
				includeMaterializationBytes: false,
				includeAppendFactsBytes: true,
			});
		target.prepareRawReceiveColumnsBatch(
			[verifiedCommit.bytes],
			[verifiedCommit.hash],
			{ verifySignatures: false },
		);
		expect(
			target.graph.commitVerifiedPreparedRawReceiveJoinBatch(
				[verifiedCommit.hash],
				[true],
				[],
			),
		).to.equal(false);
		expect(
			target.graph.commitVerifiedPreparedRawReceiveJoinBatch(
				[verifiedCommit.hash],
				[true],
				[verifiedCommit.hash],
			),
		).to.equal(true);
		expect(target.hasBlock(verifiedCommit.hash)).to.equal(true);
		expect(target.hasLogEntry(verifiedCommit.hash)).to.equal(true);
	});

	it("plans prepared raw receive groups natively", async () => {
		const source = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});
		const target = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});
		const replicaData = (replicas: number) =>
			new Uint8Array([0, replicas, 0, 0, 0]);
		const first = source.storageBackedGraph.prepareEntryV0PlainEntryAndPut({
			clockId: publicKey,
			privateKey,
			publicKey,
			wallTime: 1n,
			gid: "gid-raw-group-a",
			metaData: replicaData(2),
			payloadData: new Uint8Array([1]),
			includeMaterializationBytes: false,
			includeAppendFactsBytes: true,
		});
		const second = source.storageBackedGraph.prepareEntryV0PlainEntryAndPut({
			clockId: publicKey,
			privateKey,
			publicKey,
			wallTime: 3n,
			gid: "gid-raw-group-a",
			metaData: replicaData(4),
			payloadData: new Uint8Array([2]),
			includeMaterializationBytes: false,
			includeAppendFactsBytes: true,
		});
		const third = source.storageBackedGraph.prepareEntryV0PlainEntryAndPut({
			clockId: publicKey,
			privateKey,
			publicKey,
			wallTime: 2n,
			gid: "gid-raw-group-b",
			metaData: replicaData(1),
			payloadData: new Uint8Array([3]),
			includeMaterializationBytes: false,
			includeAppendFactsBytes: true,
		});

		target.prepareRawReceiveColumnsBatch(
			[first.bytes, second.bytes, third.bytes],
			[first.hash, second.hash, third.hash],
		);

		const plans = target.planPreparedRawReceiveGroups(
			[first.hash, second.hash, third.hash],
			{ minReplicas: 1, maxReplicas: 3 },
		);
		expect(plans).to.have.length(2);
		expect(plans?.[0]).to.deep.include({
			gid: "gid-raw-group-a",
			latestHash: second.hash,
			maxReplicasFromHead: 1,
			maxReplicasFromNewEntries: 3,
			maxMaxReplicas: 3,
		});
		expect(plans?.[0].hashes).to.deep.equal([first.hash, second.hash]);
		expect(plans?.[0].requestedReplicas).to.deep.equal([2, 4]);
		expect(plans?.[1]).to.deep.include({
			gid: "gid-raw-group-b",
			latestHash: third.hash,
			maxReplicasFromHead: 1,
			maxReplicasFromNewEntries: 1,
			maxMaxReplicas: 1,
		});
		const indexPlans = target.planPreparedRawReceiveGroupIndexes(
			[first.hash, second.hash, third.hash],
			{ minReplicas: 1, maxReplicas: 3 },
		);
		expect(indexPlans).to.have.length(2);
		expect(indexPlans?.[0]).to.deep.include({
			gid: "gid-raw-group-a",
			latestIndex: 1,
			maxReplicasFromHead: 1,
			maxReplicasFromNewEntries: 3,
			maxMaxReplicas: 3,
		});
		expect(Array.from(indexPlans?.[0].indexes ?? [])).to.deep.equal([0, 1]);
		expect(indexPlans?.[0].requestedReplicas).to.deep.equal([2, 4]);
		expect(indexPlans?.[1]).to.deep.include({
			gid: "gid-raw-group-b",
			latestIndex: 2,
			maxReplicasFromHead: 1,
			maxReplicasFromNewEntries: 1,
			maxMaxReplicas: 1,
		});
		const groupACoordinate = Number(
			target.getGidCoordinates("gid-raw-group-a", 3)[0],
		);
		target.putRange({
			id: "peer-keep-range",
			hash: "peer-keep",
			timestamp: 0,
			start1: groupACoordinate,
			end1: groupACoordinate + 1,
			start2: groupACoordinate,
			end2: groupACoordinate + 1,
			width: 1,
			mode: 0,
		});
		const leaderPlans = target.planPreparedRawReceiveGroupLeaders(
			[first.hash, second.hash, third.hash],
			{ minReplicas: 1, maxReplicas: 3 },
			{
				selfHash: "peer-a",
				selfReplicating: false,
				fullReplicaFallback: true,
			},
		);
		expect(leaderPlans).to.have.length(2);
		expect(leaderPlans?.[0]).to.deep.include({
			gid: "gid-raw-group-a",
			latestIndex: 1,
			maxReplicasFromHead: 1,
			maxReplicasFromNewEntries: 3,
			maxMaxReplicas: 3,
		});
		expect(Array.from(leaderPlans?.[0].indexes ?? [])).to.deep.equal([0, 1]);
		expect(leaderPlans?.[0].coordinates).to.have.length(3);
		expect(leaderPlans?.[0].coordinateStrings).to.have.length(3);
		expect(leaderPlans?.[0].leaders).to.be.instanceOf(Map);
		expect(leaderPlans?.[0].leaders.has("peer-keep")).to.equal(true);
		const assignmentPlans = target.planPreparedRawReceiveGroupAssignments(
			[first.hash, second.hash, third.hash],
			{ minReplicas: 1, maxReplicas: 3 },
			{
				selfHash: "peer-keep",
				selfReplicating: false,
				fullReplicaFallback: false,
			},
			"peer-b",
		);
		expect(assignmentPlans).to.have.length(2);
		expect(assignmentPlans?.[0]).to.deep.include({
			gid: "gid-raw-group-a",
			latestIndex: 1,
			isLeader: true,
			fromIsLeader: false,
		});
		expect(assignmentPlans?.[0].coordinates).to.have.length(3);
		expect(assignmentPlans?.[0].coordinateStrings).to.have.length(3);
		expect(assignmentPlans?.[0].assignedToRangeBoundary).to.be.a("boolean");
		expect(assignmentPlans?.[1]).to.deep.include({
			gid: "gid-raw-group-b",
			latestIndex: 2,
			fromIsLeader: false,
		});
		expect(assignmentPlans?.[1].isLeader).to.be.a("boolean");
		expect(
			target.planPreparedRawReceiveFastDrop(
				[first.hash, second.hash, third.hash],
				{ minReplicas: 1, maxReplicas: 3 },
				{
					selfHash: "peer-a",
					selfReplicating: false,
					fullReplicaFallback: true,
				},
				"peer-b",
			),
		).to.deep.include({
			canDrop: true,
			groupCount: 2,
			plannedHashCount: 3,
		});
		expect(
			target.selectPreparedRawReceiveHashes(
				[first.hash, second.hash, third.hash],
				{ minReplicas: 1, maxReplicas: 3 },
				{
					selfHash: "peer-a",
					selfReplicating: false,
					fullReplicaFallback: true,
				},
				"peer-b",
			),
		).to.deep.include({
			retainedHashes: [],
			droppedHashes: [first.hash, second.hash, third.hash],
			groupCount: 2,
			plannedHashCount: 3,
			usedNativeFastDropPlan: true,
			usedLeaderSamplePlans: false,
		});
		expect(
			target.planPreparedRawReceiveFastDrop(
				[first.hash, second.hash, third.hash],
				{ minReplicas: 1, maxReplicas: 3 },
				{
					selfHash: "peer-a",
					selfReplicating: false,
					fullReplicaFallback: true,
				},
				"peer-keep",
			),
		).to.deep.include({
			canDrop: true,
			groupCount: 2,
			plannedHashCount: 3,
		});
		expect(
			target.selectPreparedRawReceiveHashes(
				[first.hash, second.hash, third.hash],
				{ minReplicas: 1, maxReplicas: 3 },
				{
					selfHash: "peer-a",
					selfReplicating: false,
					fullReplicaFallback: true,
				},
				"peer-keep",
			),
		).to.deep.include({
			retainedHashes: [],
			droppedHashes: [first.hash, second.hash, third.hash],
			groupCount: 2,
			plannedHashCount: 3,
			usedNativeFastDropPlan: true,
			usedLeaderSamplePlans: false,
		});
		expect(
			target.planPreparedRawReceiveSelection(
				[first.hash, second.hash, third.hash],
				{ minReplicas: 1, maxReplicas: 3 },
				{
					selfHash: "peer-a",
					selfReplicating: false,
					fullReplicaFallback: true,
				},
				"peer-b",
			),
		).to.deep.include({
			retainedHashes: [],
			droppedHashes: [first.hash, second.hash, third.hash],
			groupCount: 2,
			plannedHashCount: 3,
			usedNativeFastDropPlan: true,
			usedLeaderSamplePlans: true,
		});
		const groupBCoordinate = Number(
			target.getGidCoordinates("gid-raw-group-b", 1)[0],
		);
		target.putRange({
			id: "peer-drop-range",
			hash: "peer-drop",
			timestamp: 0,
			start1: groupBCoordinate,
			end1: groupBCoordinate + 1,
			start2: groupBCoordinate,
			end2: groupBCoordinate + 1,
			width: 1,
			mode: 0,
		});
		const mixedSelection = target.selectPreparedRawReceiveHashes(
			[first.hash, second.hash, third.hash],
			{ minReplicas: 1, maxReplicas: 3 },
			{
				selfHash: "peer-keep",
				selfReplicating: false,
				fullReplicaFallback: false,
			},
			"peer-b",
		);
		expect(mixedSelection?.retainedHashes).to.deep.equal([
			first.hash,
			second.hash,
		]);
		expect(mixedSelection?.droppedHashes).to.deep.equal([third.hash]);
		expect(Array.from(mixedSelection?.retainedIndexes ?? [])).to.deep.equal([
			0, 1,
		]);
		expect(Array.from(mixedSelection?.droppedIndexes ?? [])).to.deep.equal([2]);
		expect(mixedSelection).to.deep.include({
			groupCount: 2,
			plannedHashCount: 3,
			usedNativeFastDropPlan: false,
			usedLeaderSamplePlans: false,
		});
		expect(mixedSelection?.retainedGroupLeaderPlans).to.have.length(1);
		expect(mixedSelection?.retainedGroupLeaderPlans?.[0]).to.deep.include({
			gid: "gid-raw-group-a",
			latestIndex: 1,
			maxReplicasFromHead: 1,
			maxReplicasFromNewEntries: 3,
			maxMaxReplicas: 3,
		});
		expect(
			Array.from(mixedSelection?.retainedGroupLeaderPlans?.[0]?.indexes ?? []),
		).to.deep.equal([0, 1]);
		expect(
			mixedSelection?.retainedGroupLeaderPlans?.[0]?.leaders.has("peer-keep"),
		).to.equal(true);
		const mixedFusedSelection = target.planPreparedRawReceiveSelection(
			[first.hash, second.hash, third.hash],
			{ minReplicas: 1, maxReplicas: 3 },
			{
				selfHash: "peer-keep",
				selfReplicating: false,
				fullReplicaFallback: false,
			},
			"peer-b",
		);
		expect(mixedFusedSelection?.retainedHashes).to.deep.equal([
			first.hash,
			second.hash,
		]);
		expect(mixedFusedSelection?.droppedHashes).to.deep.equal([third.hash]);
		expect(
			Array.from(mixedFusedSelection?.retainedIndexes ?? []),
		).to.deep.equal([0, 1]);
		expect(Array.from(mixedFusedSelection?.droppedIndexes ?? [])).to.deep.equal(
			[2],
		);
		expect(mixedFusedSelection).to.deep.include({
			groupCount: 2,
			plannedHashCount: 3,
			usedNativeFastDropPlan: false,
			usedLeaderSamplePlans: false,
		});
		expect(mixedFusedSelection?.retainedGroupLeaderPlans).to.have.length(1);
		const mixedPreparedSelection =
			target.prepareRawReceiveExpectedColumnsAndSelectionBatch(
				[first.bytes, second.bytes, third.bytes],
				[first.hash, second.hash, third.hash],
				{
					verifySignatures: false,
					minReplicas: 1,
					maxReplicas: 3,
					leaderOptions: {
						selfHash: "peer-keep",
						selfReplicating: false,
						fullReplicaFallback: false,
					},
					fromHash: "peer-b",
				},
			);
		expect(mixedPreparedSelection?.columns[0]).to.deep.equal([]);
		expect(Array.from(mixedPreparedSelection?.columns[12] ?? [])).to.deep.equal(
			[0, 0, 0],
		);
		expect(mixedPreparedSelection?.selection?.retainedHashes).to.deep.equal([
			first.hash,
			second.hash,
		]);
		expect(mixedPreparedSelection?.selection?.droppedHashes).to.deep.equal([
			third.hash,
		]);
		expect(
			Array.from(mixedPreparedSelection?.selection?.retainedIndexes ?? []),
		).to.deep.equal([0, 1]);
		expect(
			Array.from(mixedPreparedSelection?.selection?.droppedIndexes ?? []),
		).to.deep.equal([2]);

		target.storageBackedGraph.prepareEntryV0PlainEntryAndPut({
			clockId: publicKey,
			privateKey,
			publicKey,
			wallTime: 1n,
			gid: "gid-raw-existing-head",
			metaData: replicaData(5),
			payloadData: new Uint8Array([4]),
			includeMaterializationBytes: false,
			includeAppendFactsBytes: true,
		});
		const existingGidIncoming =
			source.storageBackedGraph.prepareEntryV0PlainEntryAndPut({
				clockId: publicKey,
				privateKey,
				publicKey,
				wallTime: 2n,
				gid: "gid-raw-existing-head",
				metaData: replicaData(2),
				payloadData: new Uint8Array([5]),
				includeMaterializationBytes: false,
				includeAppendFactsBytes: true,
			});
		target.prepareRawReceiveColumnsBatch(
			[existingGidIncoming.bytes],
			[existingGidIncoming.hash],
		);
		const [existingHeadPlan] =
			target.planPreparedRawReceiveGroups([existingGidIncoming.hash], {
				minReplicas: 1,
				maxReplicas: 4,
			}) ?? [];
		expect(existingHeadPlan).to.deep.include({
			gid: "gid-raw-existing-head",
			latestHash: existingGidIncoming.hash,
			maxReplicasFromHead: 4,
			maxReplicasFromNewEntries: 2,
			maxMaxReplicas: 4,
		});
		expect(
			target.planPreparedRawReceiveGroups(["missing"], {
				minReplicas: 1,
				maxReplicas: 3,
			}),
		).to.equal(undefined);
		expect(
			target.planPreparedRawReceiveFastDrop(
				["missing"],
				{ minReplicas: 1, maxReplicas: 3 },
				{
					selfHash: "peer-a",
					selfReplicating: false,
					fullReplicaFallback: true,
				},
				"peer-b",
			),
		).to.equal(undefined);
		expect(
			target.selectPreparedRawReceiveHashes(
				["missing"],
				{ minReplicas: 1, maxReplicas: 3 },
				{
					selfHash: "peer-a",
					selfReplicating: false,
					fullReplicaFallback: true,
				},
				"peer-b",
			),
		).to.equal(undefined);
	});

	it("validates and commits prepared raw receive joins natively", async () => {
		const source = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});
		const target = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});
		const parent = source.storageBackedGraph.prepareEntryV0PlainEntryAndPut({
			clockId: publicKey,
			privateKey,
			publicKey,
			wallTime: 1n,
			gid: "gid-raw-join",
			payloadData: new Uint8Array([1]),
			includeMaterializationBytes: false,
			includeAppendFactsBytes: true,
		});
		const child = source.storageBackedGraph.prepareEntryV0PlainEntryAndPut({
			clockId: publicKey,
			privateKey,
			publicKey,
			wallTime: 2n,
			gid: "gid-raw-join",
			next: [parent.hash],
			payloadData: new Uint8Array([2]),
			includeMaterializationBytes: false,
			includeAppendFactsBytes: true,
		});

		target.prepareRawReceiveColumnsBatch(
			[child.bytes, parent.bytes],
			[child.hash, parent.hash],
		);
		expect(
			target.graph.commitPreparedRawReceiveJoinBatch(
				[child.hash, parent.hash],
				[true, false],
			),
		).to.equal(true);
		expect(target.hasBlock(child.hash)).to.equal(true);
		expect(target.hasBlock(parent.hash)).to.equal(true);
		expect(target.hasLogEntry(child.hash)).to.equal(true);
		expect(target.hasLogEntry(parent.hash)).to.equal(true);
		expect(target.graph.heads("gid-raw-join")).to.deep.equal([child.hash]);
	});

	it("verifies all committed prepared raw receive joins without duplicate verify hashes", async () => {
		const source = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});
		const target = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});
		const parent = source.storageBackedGraph.prepareEntryV0PlainEntryAndPut({
			clockId: publicKey,
			privateKey,
			publicKey,
			wallTime: 1n,
			gid: "gid-raw-join-verified-all",
			payloadData: new Uint8Array([1]),
			includeMaterializationBytes: false,
			includeAppendFactsBytes: true,
		});
		const child = source.storageBackedGraph.prepareEntryV0PlainEntryAndPut({
			clockId: publicKey,
			privateKey,
			publicKey,
			wallTime: 2n,
			gid: "gid-raw-join-verified-all",
			next: [parent.hash],
			payloadData: new Uint8Array([2]),
			includeMaterializationBytes: false,
			includeAppendFactsBytes: true,
		});

		target.prepareRawReceiveColumnsBatch(
			[child.bytes, parent.bytes],
			[child.hash, parent.hash],
			{ verifySignatures: false },
		);
		expect(
			target.graph.commitVerifiedAllPreparedRawReceiveJoinBatch(
				[child.hash, parent.hash],
				[true, false],
			),
		).to.equal(true);
		expect(target.hasBlock(child.hash)).to.equal(true);
		expect(target.hasBlock(parent.hash)).to.equal(true);
		expect(target.hasLogEntry(child.hash)).to.equal(true);
		expect(target.hasLogEntry(parent.hash)).to.equal(true);
		expect(target.graph.heads("gid-raw-join-verified-all")).to.deep.equal([
			child.hash,
		]);
	});

	it("returns flat unique reference rows for native exchange-head planning", async () => {
		const backbone = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});

		const root = backbone.storageBackedGraph.prepareEntryV0PlainEntryAndPut({
			clockId: publicKey,
			privateKey,
			publicKey,
			wallTime: 1n,
			gid: "root-gid",
			payloadData: new Uint8Array([1]),
		});
		const side = backbone.storageBackedGraph.prepareEntryV0PlainEntryAndPut({
			clockId: publicKey,
			privateKey,
			publicKey,
			wallTime: 2n,
			gid: "side-gid",
			payloadData: new Uint8Array([2]),
		});
		const head = backbone.storageBackedGraph.prepareEntryV0PlainEntryAndPut({
			clockId: publicKey,
			privateKey,
			publicKey,
			wallTime: 3n,
			gid: "head-gid",
			next: [root.hash, side.hash],
			payloadData: new Uint8Array([3]),
		});

		expect(
			backbone.graph.uniqueReferenceGidRowsFlatBatch([head.hash]),
		).to.deep.equal([
			[0, root.hash, "root-gid"],
			[0, side.hash, "side-gid"],
		]);
		expect(
			backbone.graph.uniqueReferenceGidRowsFlatBatch([head.hash, root.hash]),
		).to.deep.equal([
			[0, root.hash, "root-gid"],
			[0, side.hash, "side-gid"],
		]);
		expect(
			backbone.graph.uniqueReferenceGidRowsFlatBatch([head.hash, "missing"]),
		).to.equal(undefined);
	});

	it("exposes shared-log coordinate planning for storage-backed paths", async () => {
		const backbone = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});

		backbone.putEntryCoordinates("hash-a", "gid-a", [1n], false, 1, 1n);
		expect(backbone.getEntryCoordinateHashes()).to.deep.equal(["hash-a"]);
		expect(backbone.coordinateIndexLength).to.equal(1);
		expect(backbone.hasCoordinateIndexHash("hash-a")).equal(true);
		expect(backbone.deleteEntryCoordinates("hash-a")).equal(true);
		expect(backbone.getEntryCoordinateHashes()).to.deep.equal([]);
		expect(backbone.coordinateIndexLength).to.equal(0);
		expect(backbone.coordinateValueLength).to.equal(0);

		const plan = backbone.planAppendForGid({
			entryHash: "hash-b",
			gid: "gid-b",
			hashNumber: 2n,
			replicas: 1,
			selfHash: "peer-a",
			deliveryEnabled: false,
			reliabilityAck: false,
			requireRecipients: false,
		});

		expect(plan.coordinate.hash).equal("hash-b");
		expect(plan.coordinate.gid).equal("gid-b");
		expect(plan.coordinate.requestedReplicas).equal(1);
		expect(plan.delivery?.hasRemoteRecipients).equal(false);
		expect(backbone.getEntryCoordinateHashes()).to.deep.equal(["hash-b"]);
		expect(backbone.coordinateIndexLength).to.equal(1);
		expect(backbone.hasCoordinateIndexHash("hash-b")).equal(true);

		backbone.commitEntryCoordinates(
			"hash-c",
			"gid-c",
			[3n],
			["hash-b"],
			false,
			1,
			3n,
		);
		expect(backbone.getEntryCoordinateHashes()).to.deep.equal(["hash-c"]);
		expect(backbone.getEntryCoordinates("hash-c")).to.deep.equal([3n]);
		expect(backbone.getEntryHashesForHashNumbers([3n]).get(3n)).to.deep.equal([
			"hash-c",
		]);
		const typedHashes = backbone.getEntryHashesForHashNumbersU64(
			new BigUint64Array([3n]),
		);
		expect(typedHashes).to.exist;
		expect(typedHashes!.get(3n)).to.deep.equal(["hash-c"]);
		expect(
			backbone.getEntryHashListForHashNumbersU64(new BigUint64Array([3n])),
		).to.deep.equal(["hash-c"]);
		expect(
			backbone.getEntryHashListForHashNumbersU64(new BigUint64Array([3n, 3n])),
		).to.deep.equal(["hash-c"]);
		expect(
			backbone.getEntryHashNumbersInRange({
				start1: 0n,
				end1: 10n,
				start2: 0n,
				end2: 0n,
			}),
		).to.deep.equal([3n]);
		const typedHashNumbers = backbone.getEntryHashNumbersInRangeU64({
			start1: 0n,
			end1: 10n,
			start2: 0n,
			end2: 0n,
		});
		expect(typedHashNumbers).to.be.instanceOf(BigUint64Array);
		expect(Array.from(typedHashNumbers!)).to.deep.equal([3n]);
		expect(
			backbone.countEntryCoordinatesInRanges([
				{
					start1: 0n,
					end1: 10n,
					start2: 0n,
					end2: 0n,
				},
			]),
		).to.equal(1);
		expect(backbone.coordinateIndexLength).to.equal(1);
		expect(backbone.hasCoordinateIndexHash("hash-b")).equal(false);
		expect(backbone.hasCoordinateIndexHash("hash-c")).equal(true);

		const batchPlans = backbone.planAppendForGidsBatch({
			entries: [
				{
					entryHash: "hash-d",
					gid: "gid-d",
					hashNumber: 4n,
					replicas: 1,
				},
				{
					entryHash: "hash-e",
					gid: "gid-e",
					hashNumber: 5n,
					nextHashes: ["hash-c"],
					replicas: 1,
				},
			],
			selfHash: "peer-a",
			deliveryEnabled: false,
			reliabilityAck: false,
			requireRecipients: false,
		});
		expect(batchPlans.map((plan) => plan.coordinate.hash)).to.deep.equal([
			"hash-d",
			"hash-e",
		]);
		expect(backbone.getEntryCoordinateHashes()).to.deep.equal([
			"hash-d",
			"hash-e",
		]);
		expect(backbone.coordinateIndexLength).to.equal(2);
		expect(backbone.hasCoordinateIndexHash("hash-c")).equal(false);
		expect(backbone.hasCoordinateIndexHash("hash-d")).equal(true);
		expect(backbone.hasCoordinateIndexHash("hash-e")).equal(true);
	});

	it("bounds wrapped native hash-number range materialization", async () => {
		const backbone = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});
		backbone.putEntryCoordinates("low-a", "gid-low-a", [1n], false, 1, 5n);
		backbone.putEntryCoordinates("low-b", "gid-low-b", [2n], false, 1, 8n);
		backbone.putEntryCoordinates("high-a", "gid-high-a", [3n], false, 1, 90n);
		backbone.putEntryCoordinates("high-b", "gid-high-b", [4n], false, 1, 90n);

		const range = {
			start1: 80n,
			end1: 100n,
			start2: 0n,
			end2: 10n,
		};
		expect(backbone.getEntryHashNumbersInRange(range)).to.deep.equal([
			90n,
			90n,
			5n,
			8n,
		]);
		// Two accepted rows plus one overflow sentinel.
		expect(
			backbone.getEntryHashNumbersInRangeLimited({ ...range, limit: 3 }),
		).to.deep.equal([90n, 90n, 5n]);
		// An exact fit is not truncated.
		expect(
			Array.from(
				backbone.getEntryHashNumbersInRangeU64Limited({
					...range,
					limit: 4,
				})!,
			),
		).to.deep.equal([90n, 90n, 5n, 8n]);
	});

	it("coalesces storage-backed no-next append with shared-log coordinate state", async () => {
		const backbone = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});

		const first = backbone.preparePlainNoNextStorageAppendTransaction({
			wallTime: 1n,
			gid: "gid-storage",
			payloadData: new Uint8Array([1]),
			replicas: 1,
			selfHash: "peer-a",
		});
		const second = backbone.preparePlainNoNextStorageAppendTransaction({
			wallTime: 2n,
			gid: "gid-storage",
			payloadData: new Uint8Array([2]),
			replicas: 1,
			selfHash: "peer-a",
			trimLengthTo: 1,
		});

		expect(first.entry.bytes).to.be.instanceOf(Uint8Array);
		expect(first.entry.bytes.byteLength).to.be.greaterThan(0);
		expect(first.entry.hashDigestBytes).to.have.length.greaterThan(0);
		expect(first.entry.next).to.deep.equal([]);
		expect(backbone.hasLogEntry(first.entry.hash)).equal(false);
		expect(backbone.hasBlock(first.entry.hash)).equal(false);
		expect(second.trimmed.map((entry) => entry.hash)).to.deep.equal([
			first.entry.hash,
		]);
		expect(backbone.hasLogEntry(second.entry.hash)).equal(true);
		expect(backbone.hasBlock(second.entry.hash)).equal(false);
		expect(backbone.getEntryCoordinateHashes()).to.deep.equal([
			second.entry.hash,
		]);
		expect(backbone.coordinateIndexLength).to.equal(1);
		expect(backbone.hasCoordinateIndexHash(first.entry.hash)).equal(false);
		expect(backbone.hasCoordinateIndexHash(second.entry.hash)).equal(true);
		const [coordinate] = backbone.getEntryCoordinateFields();
		expect(coordinate?.hash).equal(second.entry.hash);
		expect(coordinate?.wallTime).equal(2n);
		expect(coordinate?.metaBytes.byteLength).to.be.greaterThan(0);
	});

	it("coalesces committed storage-backed appends without returning block bytes", async () => {
		const backbone = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});

		const first = backbone.preparePlainCommittedStorageAppendTransaction({
			wallTime: 1n,
			gid: "gid-storage-committed",
			payloadData: new Uint8Array([1]),
			replicas: 1,
			selfHash: "peer-a",
		});
		const second = backbone.preparePlainCommittedStorageAppendTransaction({
			wallTime: 2n,
			gid: "gid-storage-committed",
			next: [first.entry.hash],
			payloadData: new Uint8Array([2]),
			replicas: 1,
			selfHash: "peer-a",
			trimLengthTo: 1,
		});

		expect(first.entry.bytes).equal(undefined);
		expect(first.entry.hashDigestBytes).to.have.length.greaterThan(0);
		expect(backbone.hasLogEntry(first.entry.hash)).equal(false);
		expect(backbone.hasBlock(first.entry.hash)).equal(false);
		expect(second.entry.bytes).equal(undefined);
		expect(second.entry.next).to.deep.equal([first.entry.hash]);
		expect(second.trimmed.map((entry) => entry.hash)).to.deep.equal([
			first.entry.hash,
		]);
		expect(second.trimmedGids).to.deep.equal(["gid-storage-committed"]);
		expect(second.trimmed[0]?.gid).equal("gid-storage-committed");
		expect(second.trimmed[0]?.next).to.deep.equal([]);
		expect(second.trimmed[0]?.type).equal(0);
		expect(second.trimmed[0]?.payloadSize).equal(1);
		expect(second.trimmed[0]?.clock.timestamp.wallTime).equal(1n);
		expect(second.trimmed[0]?.clock.timestamp.logical).equal(0);
		expect(backbone.hasLogEntry(second.entry.hash)).equal(true);
		expect(backbone.hasBlock(second.entry.hash)).equal(true);
		expect(backbone.getEntryCoordinateHashes()).to.deep.equal([
			second.entry.hash,
		]);
	});

	it("coalesces committed storage-backed no-next appends without returning block bytes", async () => {
		const backbone = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});

		const first = backbone.preparePlainCommittedNoNextStorageAppendTransaction({
			wallTime: 1n,
			gid: "gid-storage-committed-no-next",
			payloadData: new Uint8Array([1]),
			replicas: 1,
			selfHash: "peer-a",
		});
		const second = backbone.preparePlainCommittedNoNextStorageAppendTransaction(
			{
				wallTime: 2n,
				gid: "gid-storage-committed-no-next",
				payloadData: new Uint8Array([2]),
				replicas: 1,
				selfHash: "peer-a",
				trimLengthTo: 1,
			},
		);

		expect(first.entry.bytes).equal(undefined);
		expect(first.entry.hashDigestBytes).to.have.length.greaterThan(0);
		expect(first.entry.next).to.deep.equal([]);
		expect(second.entry.bytes).equal(undefined);
		expect(second.entry.next).to.deep.equal([]);
		expect(second.trimmed.map((entry) => entry.hash)).to.deep.equal([
			first.entry.hash,
		]);
		expect(second.trimmedGids).to.deep.equal(["gid-storage-committed-no-next"]);
		expect(second.trimmed[0]?.gid).equal("gid-storage-committed-no-next");
		expect(second.trimmed[0]?.next).to.deep.equal([]);
		expect(second.trimmed[0]?.type).equal(0);
		expect(second.trimmed[0]?.payloadSize).equal(1);
		expect(second.trimmed[0]?.clock.timestamp.wallTime).equal(1n);
		expect(second.trimmed[0]?.clock.timestamp.logical).equal(0);
		expect(backbone.hasLogEntry(first.entry.hash)).equal(false);
		expect(backbone.hasBlock(first.entry.hash)).equal(false);
		expect(backbone.hasLogEntry(second.entry.hash)).equal(true);
		expect(backbone.hasBlock(second.entry.hash)).equal(true);
		expect(backbone.getEntryCoordinateHashes()).to.deep.equal([
			second.entry.hash,
		]);

		const leaderPlan = backbone.planLeadersForGid(
			"gid-storage-committed-no-next",
			1,
			{
				selfHash: "peer-a",
				selfReplicating: true,
				fullReplicaFallback: true,
			},
		);
		expect(leaderPlan.coordinates).to.have.length(1);
		const [batchLeaderPlan] = backbone.planLeadersForGidsBatch(
			[
				{
					gid: "gid-storage-committed-no-next",
					replicas: 1,
				},
			],
			{
				selfHash: "peer-a",
				selfReplicating: true,
				fullReplicaFallback: true,
			},
		);
		expect(batchLeaderPlan?.coordinates).to.deep.equal(leaderPlan.coordinates);
		expect(batchLeaderPlan?.leaders).to.deep.equal(leaderPlan.leaders);
		const requestPruneHints = backbone.planRequestPruneLeaderHints(
			[second.entry.hash, "missing"],
			[],
			{
				selfHash: "peer-a",
				selfReplicating: true,
				fullReplicaFallback: true,
			},
		)!;
		expect(requestPruneHints.entries.get(second.entry.hash)?.gid).equal(
			"gid-storage-committed-no-next",
		);
		expect(requestPruneHints.presentBlockHashes.has(second.entry.hash)).equal(
			true,
		);
		expect(requestPruneHints.replicaCounts.get(second.entry.hash)).equal(1);
		expect(requestPruneHints.localLeaderHashes.has(second.entry.hash)).equal(
			leaderPlan.leaders.has("peer-a"),
		);
		expect(requestPruneHints.entries.has("missing")).equal(false);
		const requestPruneHintColumns = backbone.planRequestPruneLeaderHintColumns(
			[second.entry.hash, "missing"],
			[],
			{
				selfHash: "peer-a",
				selfReplicating: true,
				fullReplicaFallback: true,
			},
		)!;
		expect(requestPruneHintColumns.gids[0]).equal(
			"gid-storage-committed-no-next",
		);
		expect(requestPruneHintColumns.gids[1]).equal(undefined);
		expect([...requestPruneHintColumns.presentBlockFlags]).to.deep.equal([
			1, 0,
		]);
		expect([...requestPruneHintColumns.replicaCounts]).to.deep.equal([1, 0]);
		expect([...requestPruneHintColumns.localLeaderFlags]).to.deep.equal([
			leaderPlan.leaders.has("peer-a") ? 1 : 0,
			0,
		]);
		expect([...requestPruneHintColumns.peerHistoryRemovedFlags]).to.deep.equal([
			1, 0,
		]);
		expect(
			backbone.getGidCoordinates("gid-storage-committed-no-next", 1),
		).to.deep.equal(leaderPlan.coordinates);
		expect(backbone.getGrid(leaderPlan.coordinates[0]!, 1)).to.deep.equal(
			leaderPlan.coordinates,
		);
		expect(
			backbone.findLeaders(leaderPlan.coordinates, 1, {
				selfHash: "peer-a",
				selfReplicating: true,
				fullReplicaFallback: true,
			}),
		).to.deep.equal(leaderPlan.leaders);
		const [cursorBatchLeaderPlan] = backbone.findLeadersBatch(
			[
				{
					cursors: leaderPlan.coordinates,
					replicas: 1,
				},
			],
			{
				selfHash: "peer-a",
				selfReplicating: true,
				fullReplicaFallback: true,
			},
		);
		expect(cursorBatchLeaderPlan).to.deep.equal(leaderPlan.leaders);
		const assignmentPlan = backbone.planEntryAssignmentForGid(
			"gid-storage-committed-no-next",
			1,
			{
				selfHash: "peer-a",
				selfReplicating: true,
				fullReplicaFallback: true,
			},
		);
		expect(assignmentPlan.coordinates).to.deep.equal(leaderPlan.coordinates);
		expect(assignmentPlan.assignedToRangeBoundary).to.be.a("boolean");

		backbone.addGidPeers("gid-storage-committed-no-next", ["peer-a"], true);
		backbone.markEntriesKnownByPeer([second.entry.hash], "peer-a");
		const repairPlan = backbone.planRepairDispatchForResidentEntries(
			{
				pendingModes: ["join-authoritative"],
				pendingPeersByMode: new Map([
					["join-authoritative", ["peer-a", "peer-b"]],
				]),
				optimisticPeersByMode: new Map([
					[
						"join-authoritative",
						new Map([["gid-storage-committed-no-next", ["peer-b"]]]),
					],
				]),
				fullReplicaRepairCandidates: ["peer-b"],
				fullReplicaRepairCandidateCount: 1,
				selfHash: "peer-a",
			},
			{
				selfHash: "peer-a",
				selfReplicating: true,
				fullReplicaFallback: true,
			},
		);
		expect(repairPlan.get("join-authoritative")?.get("peer-a")).equal(
			undefined,
		);
		expect(repairPlan.get("join-authoritative")?.get("peer-b")).to.deep.equal([
			second.entry.hash,
		]);
	});

	it("exposes committed no-next storage append loop counters", async () => {
		const backbone = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});

		backbone.configureDocumentSchemaIr(contextOnlySchema());
		backbone.setCoordinateJournalEnabled(true);
		backbone.resetAppendProfile();
		backbone.setAppendProfileEnabled(true);
		const result = benchmarkPlainCommittedNoNextStorageAppendTransactionLoop(
			backbone,
			{
				iterations: 3,
				wallTimeStart: 100n,
				payloadData: new Uint8Array([1, 2, 3]),
				replicas: 1,
				selfHash: "peer-a",
				useDocumentIndex: true,
			},
		);
		backbone.setAppendProfileEnabled(false);

		const profile = backbone.appendProfile();
		expect(result.totalMs).to.be.greaterThanOrEqual(0);
		expect(result.logLength).to.equal(3);
		expect(result.blockLength).to.equal(3);
		expect(result.coordinateLength).to.equal(3);
		expect(result.documentLength).to.equal(3);
		expect(backbone.coordinatePendingJournalLength).to.equal(3);
		expect(profile.nativeBackboneResultRowMs).to.equal(0);
		expect(profile.nativeBackboneLogSignMs).to.be.greaterThanOrEqual(0);
	});

	it("returns trim hashes without materializing trim rows for unresolved storage appends", async () => {
		const backbone = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});

		const first = backbone.preparePlainCommittedNoNextStorageAppendTransaction({
			wallTime: 1n,
			gid: "gid-storage-compact-trim-old",
			payloadData: new Uint8Array([1]),
			replicas: 1,
			selfHash: "peer-a",
		});
		const second = backbone.preparePlainCommittedNoNextStorageAppendTransaction(
			{
				wallTime: 2n,
				gid: "gid-storage-compact-trim-new",
				payloadData: new Uint8Array([2]),
				replicas: 1,
				selfHash: "peer-a",
				trimLengthTo: 1,
				resolveTrimmedEntries: false,
			},
		);

		expect(second.trimmedHashes).to.deep.equal([first.entry.hash]);
		expect(second.trimmedGids).to.deep.equal(["gid-storage-compact-trim-old"]);
		expect(second.trimmed).to.deep.equal([]);
		expect(backbone.hasLogEntry(first.entry.hash)).equal(false);
		expect(backbone.hasBlock(first.entry.hash)).equal(false);
		expect(backbone.getEntryCoordinateHashes()).to.deep.equal([
			second.entry.hash,
		]);
	});

	it("coalesces storage-backed append with next into shared-log coordinate state", async () => {
		const backbone = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});

		const first = backbone.preparePlainStorageAppendTransaction({
			wallTime: 1n,
			gid: "gid-storage-next",
			payloadData: new Uint8Array([1]),
			replicas: 1,
			selfHash: "peer-a",
		});
		const second = backbone.preparePlainStorageAppendTransaction({
			wallTime: 2n,
			gid: "gid-storage-next",
			next: [first.entry.hash],
			payloadData: new Uint8Array([2]),
			replicas: 1,
			selfHash: "peer-a",
		});

		expect(second.entry.next).to.deep.equal([first.entry.hash]);
		expect(backbone.hasLogEntry(first.entry.hash)).equal(true);
		expect(backbone.hasLogEntry(second.entry.hash)).equal(true);
		expect(backbone.getEntryCoordinateHashes()).to.deep.equal([
			second.entry.hash,
		]);
		expect(backbone.coordinateIndexLength).to.equal(1);
		expect(backbone.hasCoordinateIndexHash(first.entry.hash)).equal(false);
		expect(backbone.hasCoordinateIndexHash(second.entry.hash)).equal(true);
	});

	it("replays shared-log coordinate state from native WAL bytes", async () => {
		const source = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});
		const target = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});

		source.setCoordinateJournalEnabled(true);
		source.putEntryCoordinates("hash-a", "gid-a", [1n], false, 1, 1n);
		source.commitEntryCoordinates(
			"hash-b",
			"gid-b",
			[2n],
			["hash-a"],
			true,
			1,
			2n,
		);
		expect(source.coordinatePendingJournalLength).to.equal(3);
		expect(source.coordinatePendingJournalByteLength).to.be.greaterThan(0);
		expect(source.coordinatePendingJournalByteLength).to.equal(
			source.coordinateJournal().byteLength,
		);
		const records = source.coordinateJournal();
		source.clearCoordinateJournal();
		const journal = concatBytes([source.coordinateJournalHeader(), records]);

		expect(source.coordinatePendingJournalLength).to.equal(0);
		expect(source.coordinatePendingJournalByteLength).to.equal(0);
		expect(
			target.loadCoordinateSnapshotAndJournal(undefined, journal),
		).to.equal(3);
		expect(target.getEntryCoordinateHashes()).to.deep.equal(["hash-b"]);
		expect(target.coordinateIndexLength).to.equal(1);
		expect(target.coordinateValueLength).to.equal(1);
		expect(target.hasCoordinateIndexHash("hash-a")).equal(false);
		expect(target.hasCoordinateIndexHash("hash-b")).equal(true);
	});

	it("replays native WAL coordinate metadata for storage-backed appends", async () => {
		const source = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});
		const target = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});

		source.setCoordinateJournalEnabled(true);
		const first = source.preparePlainStorageAppendTransaction({
			wallTime: 11n,
			gid: "gid-storage-wal",
			payloadData: new Uint8Array([1]),
			replicas: 1,
			selfHash: "peer-a",
		});
		const second = source.preparePlainStorageAppendTransaction({
			wallTime: 12n,
			gid: "gid-storage-wal",
			next: [first.entry.hash],
			payloadData: new Uint8Array([2]),
			replicas: 1,
			selfHash: "peer-a",
		});
		const records = source.coordinateJournal();
		source.clearCoordinateJournal();
		const journal = concatBytes([source.coordinateJournalHeader(), records]);

		expect(
			target.loadCoordinateSnapshotAndJournal(undefined, journal),
		).to.equal(3);
		const [coordinate] = target.getEntryCoordinateFields();
		expect(coordinate?.hash).equal(second.entry.hash);
		expect(coordinate?.gid).equal("gid-storage-wal");
		expect(coordinate?.wallTime).equal(12n);
		expect(coordinate?.metaBytes.byteLength).to.be.greaterThan(0);
		expect(target.hasCoordinateIndexHash(first.entry.hash)).equal(false);
		expect(target.hasCoordinateIndexHash(second.entry.hash)).equal(true);
	});

	it("restores shared-log coordinate state from a native snapshot", async () => {
		const source = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});
		const target = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});

		source.putEntryCoordinates("hash-a", "gid-a", [1n], false, 1, 1n);
		source.putEntryCoordinates("hash-b", "gid-b", [2n, 3n], true, 2, 2n);
		source.clearCoordinateJournal();
		const snapshot = source.coordinateSnapshot();

		expect(target.loadCoordinateSnapshotAndJournal(snapshot)).to.equal(0);
		expect(target.getEntryCoordinateHashes()).to.deep.equal([
			"hash-a",
			"hash-b",
		]);
		expect(target.coordinateIndexLength).to.equal(2);
		expect(target.coordinateValueLength).to.equal(2);
		expect(target.hasCoordinateIndexHash("hash-a")).equal(true);
		expect(target.hasCoordinateIndexHash("hash-b")).equal(true);
		expect(target.coordinatePendingJournalLength).to.equal(0);
	});

	it("flushes native coordinate state and rejects unsafe compaction", async () => {
		const source = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});
		const restored = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});
		const reflushed = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});
		const store = new NativeBackboneMemoryCoordinatePersistenceStore();
		const persistence = new NativeBackboneCoordinatePersistence(store);

		expect(source.coordinateJournalEnabled).equal(false);
		await persistence.hydrate(source);
		expect(source.coordinateJournalEnabled).equal(true);
		source.putEntryCoordinates("hash-a", "gid-a", [1n], false, 1, 1n);
		const journalBytes = await persistence.flushJournal(source);
		expect(journalBytes).to.be.greaterThan(0);
		expect(source.coordinatePendingJournalLength).to.equal(0);
		expect(await persistence.hydrate(restored)).to.equal(1);
		expect(restored.getEntryCoordinateHashes()).to.deep.equal(["hash-a"]);

		source.putEntryCoordinates("hash-b", "gid-b", [2n], false, 1, 2n);
		let compactError: unknown;
		await persistence.compact(source).catch((error) => {
			compactError = error;
		});
		expect(String(compactError)).to.contain("compaction is disabled");
		expect(source.coordinatePendingJournalLength).to.equal(1);
		await persistence.flushJournal(source);
		expect(store.files.has("coordinates.wal")).equal(true);
		expect(await persistence.hydrate(reflushed)).to.equal(2);
		expect(reflushed.getEntryCoordinateHashes()).to.deep.equal([
			"hash-a",
			"hash-b",
		]);
		expect(reflushed.coordinateIndexLength).to.equal(2);
	});

	it("drops every configured native file through memory and buffered stores", async () => {
		const files = {
			snapshot: "custom-coordinates.bin",
			journal: "custom-coordinates.wal",
			documentSnapshot: "custom-document-values.bin",
			documentJournal: "custom-document-values.wal",
			documentSignerSnapshot: "custom-document-signers.bin",
			documentSignerJournal: "custom-document-signers.wal",
		};
		for (const buffered of [false, true]) {
			const memory = new NativeBackboneMemoryCoordinatePersistenceStore();
			const store = buffered
				? new NativeBackboneBufferedCoordinatePersistenceStore(memory)
				: memory;
			const persistence = new NativeBackboneCoordinatePersistence(store, files);
			for (const name of Object.values(files)) {
				await store.append(name, new Uint8Array([1, 2, 3]));
			}
			await store.write("strict-intent-a", new Uint8Array([4]));
			await store.write("strict-intent-b", new Uint8Array([5]));

			await persistence.drop(["strict-intent-a", "strict-intent-b"]);
			await persistence.close();
			expect(memory.files.size, `buffered=${String(buffered)}`).equal(0);
		}
	});

	it("fails closed on a corrupt drop tombstone but still permits explicit erase", async () => {
		const target = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});
		const memory = new NativeBackboneMemoryCoordinatePersistenceStore();
		await memory.write("coordinates.wal", new Uint8Array([1, 2, 3]));
		await memory.write(
			"native-backbone-drop.tombstone",
			new TextEncoder().encode(
				'{"format":"peerbit-native-backbone-coordinate-drop"',
			),
		);
		const persistence = new NativeBackboneCoordinatePersistence(memory);

		let hydrateError: unknown;
		await persistence.hydrate(target).catch((error) => {
			hydrateError = error;
		});
		expect(String(hydrateError)).to.contain(
			"Invalid native backbone drop tombstone JSON",
		);
		expect(memory.files.has("coordinates.wal")).equal(true);
		expect(memory.files.has("native-backbone-drop.tombstone")).equal(true);

		await persistence.drop();
		expect(memory.files.size).equal(0);
	});

	it("durably upgrades legacy interrupted-drop tombstones before erase", async () => {
		const legacyFiles = [
			"coordinates.bin",
			"coordinates.wal",
			"document-values.bin",
			"document-values.wal",
			"document-signers.bin",
			"document-signers.wal",
		] as const;
		const checkpointFiles = [
			"coordinates.bin.checkpoint-state",
			"coordinates.bin.checkpoint-a",
			"coordinates.bin.checkpoint-b",
			"coordinates.wal.checkpoint-a",
			"document-values.wal.checkpoint-a",
			"document-signers.wal.checkpoint-a",
			"coordinates.wal.checkpoint-b",
			"document-values.wal.checkpoint-b",
			"document-signers.wal.checkpoint-b",
			"coordinates.bin.checkpoint-state.tmp",
			"coordinates.bin.checkpoint-a.tmp",
			"coordinates.bin.checkpoint-b.tmp",
			"coordinates.wal.tmp",
			"native-backbone-drop.tombstone.tmp",
		] as const;
		const extraFile = "strict-intent-from-old-version";
		const expectedExpandedFiles = [
			...legacyFiles,
			extraFile,
			...checkpointFiles,
		];
		const legacyTombstone = new TextEncoder().encode(
			JSON.stringify({
				format: "peerbit-native-backbone-coordinate-drop",
				version: 1,
				files: [...legacyFiles, extraFile],
				// Golden CRC32 from the V1 body above, as emitted before checkpoint files
				// became part of the configured namespace.
				checksum: "391beb42",
			}),
		);

		for (const atomic of [true, false]) {
			const memory = new NativeBackboneMemoryCoordinatePersistenceStore();
			for (const file of expectedExpandedFiles) {
				await memory.write(file, new Uint8Array([1]));
			}
			await memory.write("native-backbone-drop.tombstone", legacyTombstone);

			let rejectRemovals = true;
			let upgradeWritten = false;
			let upgradeDurable = false;
			let removeBeforeUpgradeWasDurable = false;
			let directTombstoneWrites = 0;
			let atomicTombstoneReplacements = 0;
			const flushes: Array<string | undefined> = [];
			const removals: string[] = [];
			const store: NativeBackboneCoordinatePersistenceStore = {
				read: (name) => memory.read(name),
				write: async (name, bytes) => {
					if (name === "native-backbone-drop.tombstone") {
						directTombstoneWrites++;
						upgradeWritten = true;
						upgradeDurable = false;
					}
					await memory.write(name, bytes);
				},
				append: (name, bytes) => memory.append(name, bytes),
				remove: async (name) => {
					if (!upgradeDurable) {
						removeBeforeUpgradeWasDurable = true;
					}
					removals.push(name);
					if (rejectRemovals) {
						throw new Error("injected post-upgrade erase failure");
					}
					await memory.remove(name);
				},
				flush: async (name) => {
					flushes.push(name);
					await memory.flush(name);
					if (name === "native-backbone-drop.tombstone" && upgradeWritten) {
						upgradeDurable = true;
					}
				},
				...(atomic
					? {
							atomicReplace: async (name: string, bytes: Uint8Array) => {
								if (name === "native-backbone-drop.tombstone") {
									atomicTombstoneReplacements++;
									upgradeWritten = true;
									upgradeDurable = true;
								}
								await memory.write(name, bytes);
							},
						}
					: {}),
			};

			const interrupted = new NativeBackboneCoordinatePersistence(store);
			const firstFailure = await interrupted.resumeDrop().then(
				() => undefined,
				(error: unknown) => error,
			);
			expect(firstFailure, `atomic=${String(atomic)}`).to.be.instanceOf(
				AggregateError,
			);
			expect(upgradeDurable, `atomic=${String(atomic)}`).equal(true);
			expect(removeBeforeUpgradeWasDurable, `atomic=${String(atomic)}`).equal(
				false,
			);
			expect(
				atomicTombstoneReplacements,
				`atomic replacements, atomic=${String(atomic)}`,
			).equal(atomic ? 1 : 0);
			expect(
				directTombstoneWrites,
				`direct writes, atomic=${String(atomic)}`,
			).equal(atomic ? 0 : 1);
			if (!atomic) {
				expect(flushes).to.include("native-backbone-drop.tombstone");
			}
			const upgradedBytes = memory.files.get("native-backbone-drop.tombstone");
			expect(upgradedBytes, `atomic=${String(atomic)}`).to.exist;
			const upgraded = JSON.parse(new TextDecoder().decode(upgradedBytes)) as {
				files: string[];
			};
			expect(upgraded.files, `atomic=${String(atomic)}`).to.deep.equal(
				expectedExpandedFiles,
			);

			// A fresh process must validate the upgraded marker and finish its complete
			// erase without trying to rewrite the already-current intent.
			rejectRemovals = false;
			removals.length = 0;
			const recovering = new NativeBackboneCoordinatePersistence(store);
			expect(await recovering.resumeDrop()).equal(true);
			for (const file of expectedExpandedFiles) {
				expect(removals, `atomic=${String(atomic)}`).to.include(file);
			}
			expect(memory.files.size, `atomic=${String(atomic)}`).equal(0);
			expect(atomicTombstoneReplacements).equal(atomic ? 1 : 0);
			expect(directTombstoneWrites).equal(atomic ? 0 : 1);
		}
	});

	it("makes close terminal before a later drop can touch the store", async () => {
		const memory = new NativeBackboneMemoryCoordinatePersistenceStore();
		let closed = false;
		let closeCalls = 0;
		let postCloseOperations = 0;
		const assertOpen = () => {
			if (closed) {
				postCloseOperations++;
				throw new Error("terminal store is closed");
			}
		};
		const inner: NativeBackboneCoordinatePersistenceStore = {
			read: (name) => {
				assertOpen();
				return memory.read(name);
			},
			write: (name, bytes) => {
				assertOpen();
				return memory.write(name, bytes);
			},
			append: (name, bytes) => {
				assertOpen();
				return memory.append(name, bytes);
			},
			remove: (name) => {
				assertOpen();
				return memory.remove(name);
			},
			flush: (name) => {
				assertOpen();
				return memory.flush(name);
			},
			close: async () => {
				assertOpen();
				closeCalls++;
				closed = true;
			},
		};
		const buffered = new NativeBackboneBufferedCoordinatePersistenceStore(
			inner,
		);
		const persistence = new NativeBackboneCoordinatePersistence(buffered);
		await buffered.append("coordinates.wal", new Uint8Array([7, 8, 9]));

		const closing = persistence.close();
		expect(persistence.close()).equal(closing);
		let dropError: unknown;
		await persistence.drop().catch((error) => {
			dropError = error;
		});
		await closing;

		expect(String(dropError)).to.contain("while closing");
		expect(memory.files.get("coordinates.wal")).to.deep.equal(
			new Uint8Array([7, 8, 9]),
		);
		expect(closeCalls).equal(1);
		expect(postCloseOperations).equal(0);
	});

	it("retries a transient active-close flush before closing the store", async () => {
		const memory = new NativeBackboneMemoryCoordinatePersistenceStore();
		let flushCalls = 0;
		let closeCalls = 0;
		const closeOptions: Array<{ flush?: boolean } | undefined> = [];
		const store: NativeBackboneCoordinatePersistenceStore = {
			read: (name) => memory.read(name),
			write: (name, bytes) => memory.write(name, bytes),
			append: (name, bytes) => memory.append(name, bytes),
			remove: (name) => memory.remove(name),
			flush: async () => {
				flushCalls++;
				if (flushCalls === 1) {
					throw new Error("transient close flush failure");
				}
			},
			close: async (options) => {
				closeCalls++;
				closeOptions.push(options);
			},
		};
		const persistence = new NativeBackboneCoordinatePersistence(store);

		const firstClose = persistence.close();
		expect(persistence.close()).equal(firstClose);
		const firstError = await firstClose.then(
			() => undefined,
			(error: unknown) => error,
		);
		expect(String(firstError)).to.contain("transient close flush failure");
		expect(flushCalls).equal(1);
		expect(closeCalls).equal(0);

		const retry = persistence.close();
		expect(retry).not.equal(firstClose);
		await retry;
		expect(persistence.close()).equal(retry);
		expect(flushCalls).equal(2);
		expect(closeCalls).equal(1);
		expect(closeOptions).to.deep.equal([undefined]);
	});

	it("retries only the incomplete store-close stage", async () => {
		const memory = new NativeBackboneMemoryCoordinatePersistenceStore();
		let flushCalls = 0;
		let closeCalls = 0;
		const closeOptions: Array<{ flush?: boolean } | undefined> = [];
		const store: NativeBackboneCoordinatePersistenceStore = {
			read: (name) => memory.read(name),
			write: (name, bytes) => memory.write(name, bytes),
			append: (name, bytes) => memory.append(name, bytes),
			remove: (name) => memory.remove(name),
			flush: async () => {
				flushCalls++;
			},
			close: async (options) => {
				closeCalls++;
				closeOptions.push(options);
				if (closeCalls === 1) {
					throw new Error("transient store close failure");
				}
			},
		};
		const persistence = new NativeBackboneCoordinatePersistence(store);

		const firstClose = persistence.close();
		const firstError = await firstClose.then(
			() => undefined,
			(error: unknown) => error,
		);
		expect(String(firstError)).to.contain("transient store close failure");
		expect(flushCalls).equal(1);
		expect(closeCalls).equal(1);

		const retry = persistence.close();
		expect(retry).not.equal(firstClose);
		await retry;
		expect(flushCalls).equal(1);
		expect(closeCalls).equal(2);
		expect(closeOptions).to.deep.equal([undefined, undefined]);
	});

	it("fences ordinary operations after a markerless explicit drop failure", async () => {
		const memory = new NativeBackboneMemoryCoordinatePersistenceStore();
		let failTombstoneWrite = true;
		const store: NativeBackboneCoordinatePersistenceStore = {
			read: (name) => memory.read(name),
			write: async (name, bytes) => {
				if (name === "native-backbone-drop.tombstone" && failTombstoneWrite) {
					failTombstoneWrite = false;
					throw new Error("transient tombstone write failure");
				}
				await memory.write(name, bytes);
			},
			append: (name, bytes) => memory.append(name, bytes),
			remove: (name) => memory.remove(name),
			flush: (name) => memory.flush(name),
		};
		const persistence = new NativeBackboneCoordinatePersistence(store);
		const target = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});
		await memory.write("coordinates.wal", new Uint8Array([1, 2, 3]));

		const dropError = await persistence.drop().then(
			() => undefined,
			(error: unknown) => error,
		);
		expect(String(dropError)).to.contain("transient tombstone write failure");
		expect(await persistence.resumeDrop()).equal(false);

		const hydrateError = await persistence.hydrate(target).then(
			() => undefined,
			(error: unknown) => error,
		);
		expect(String(hydrateError)).to.contain(
			"after drop was initiated; retry drop or resume drop first",
		);
		let flushError: unknown;
		try {
			await persistence.flushJournal(target);
		} catch (error) {
			flushError = error;
		}
		expect(String(flushError)).to.contain(
			"after drop was initiated; retry drop or resume drop first",
		);

		// Only terminal recovery remains admitted on this generation.
		await persistence.drop();
		expect(memory.files.size).equal(0);
	});

	it("closes markerless explicit-drop debt without flushing", async () => {
		const memory = new NativeBackboneMemoryCoordinatePersistenceStore();
		let failTombstoneWrite = true;
		let flushCalls = 0;
		const closeOptions: Array<{ flush?: boolean } | undefined> = [];
		const store: NativeBackboneCoordinatePersistenceStore = {
			read: (name) => memory.read(name),
			write: async (name, bytes) => {
				if (name === "native-backbone-drop.tombstone" && failTombstoneWrite) {
					failTombstoneWrite = false;
					throw new Error("transient tombstone write failure");
				}
				await memory.write(name, bytes);
			},
			append: (name, bytes) => memory.append(name, bytes),
			remove: (name) => memory.remove(name),
			flush: async (name) => {
				flushCalls++;
				await memory.flush(name);
			},
			close: async (options) => {
				closeOptions.push(options);
			},
		};
		const persistence = new NativeBackboneCoordinatePersistence(store);
		const target = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});

		await persistence.drop().then(
			() => undefined,
			() => undefined,
		);
		expect(await persistence.resumeDrop()).equal(false);
		await persistence.close();

		expect(flushCalls).equal(0);
		expect(closeOptions).to.deep.equal([{ flush: false }]);
		const hydrateError = await persistence.hydrate(target).then(
			() => undefined,
			(error: unknown) => error,
		);
		expect(String(hydrateError)).to.contain("while closed");
	});

	it("closes after a queued explicit drop rejection before admitting resume", async () => {
		const memory = new NativeBackboneMemoryCoordinatePersistenceStore();
		let writeEntered!: () => void;
		const entered = new Promise<void>((resolve) => {
			writeEntered = resolve;
		});
		let releaseWrite!: () => void;
		const gate = new Promise<void>((resolve) => {
			releaseWrite = resolve;
		});
		let closed = false;
		let postCloseReads = 0;
		const closeOptions: Array<{ flush?: boolean } | undefined> = [];
		const store: NativeBackboneCoordinatePersistenceStore = {
			read: (name) => {
				if (closed) {
					postCloseReads++;
					throw new Error("terminal store is closed");
				}
				return memory.read(name);
			},
			write: async (name, bytes) => {
				if (name === "native-backbone-drop.tombstone") {
					writeEntered();
					await gate;
					throw new Error("queued tombstone write failure");
				}
				await memory.write(name, bytes);
			},
			append: (name, bytes) => memory.append(name, bytes),
			remove: (name) => memory.remove(name),
			flush: (name) => memory.flush(name),
			close: async (options) => {
				closeOptions.push(options);
				closed = true;
			},
		};
		const persistence = new NativeBackboneCoordinatePersistence(store);

		const dropping = persistence.drop();
		await entered;
		const closing = persistence.close();
		const lateResume = persistence.resumeDrop().then(
			() => undefined,
			(error: unknown) => error,
		);
		releaseWrite();
		const dropError = await dropping.then(
			() => undefined,
			(error: unknown) => error,
		);
		expect(String(dropError)).to.contain("queued tombstone write failure");
		await closing;

		const resumeError = await lateResume;
		expect(String(resumeError)).to.contain("after close was initiated");
		const retryDropError = await persistence.drop().then(
			() => undefined,
			(error: unknown) => error,
		);
		expect(String(retryDropError)).to.contain("while closed");
		expect(closeOptions).to.deep.equal([{ flush: false }]);
		expect(postCloseReads).equal(0);
	});

	it("closes behind recovery-origin resume without leaving an active adapter", async () => {
		const memory = new NativeBackboneMemoryCoordinatePersistenceStore();
		let failRemoval = true;
		const seedStore: NativeBackboneCoordinatePersistenceStore = {
			read: (name) => memory.read(name),
			write: (name, bytes) => memory.write(name, bytes),
			append: (name, bytes) => memory.append(name, bytes),
			remove: async (name) => {
				if (name === "coordinates.bin" && failRemoval) {
					failRemoval = false;
					throw new Error("seed interrupted drop");
				}
				await memory.remove(name);
			},
			flush: (name) => memory.flush(name),
		};
		await memory.write("coordinates.bin", new Uint8Array([1]));
		const seed = new NativeBackboneCoordinatePersistence(seedStore);
		await seed.drop().then(
			() => undefined,
			() => undefined,
		);
		expect(memory.files.has("native-backbone-drop.tombstone")).equal(true);

		let readEntered!: () => void;
		const entered = new Promise<void>((resolve) => {
			readEntered = resolve;
		});
		let releaseRead!: () => void;
		const gate = new Promise<void>((resolve) => {
			releaseRead = resolve;
		});
		let gateArmed = true;
		let closed = false;
		let postCloseReads = 0;
		const closeOptions: Array<{ flush?: boolean } | undefined> = [];
		const recoveryStore: NativeBackboneCoordinatePersistenceStore = {
			read: async (name) => {
				if (closed) {
					postCloseReads++;
					throw new Error("terminal store is closed");
				}
				if (name === "native-backbone-drop.tombstone" && gateArmed) {
					gateArmed = false;
					readEntered();
					await gate;
				}
				return memory.read(name);
			},
			write: (name, bytes) => memory.write(name, bytes),
			append: (name, bytes) => memory.append(name, bytes),
			remove: (name) => memory.remove(name),
			flush: (name) => memory.flush(name),
			close: async (options) => {
				closeOptions.push(options);
				closed = true;
			},
		};
		const recovering = new NativeBackboneCoordinatePersistence(recoveryStore);
		const target = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});

		const resuming = recovering.resumeDrop();
		await entered;
		const closing = recovering.close();
		releaseRead();
		expect(await resuming).equal(true);
		await closing;

		expect(closeOptions).to.deep.equal([{ flush: false }]);
		const hydrateError = await recovering.hydrate(target).then(
			() => undefined,
			(error: unknown) => error,
		);
		expect(String(hydrateError)).to.contain("while closed");
		expect(postCloseReads).equal(0);
	});

	it("retries a drop-wins store close without flushing", async () => {
		const memory = new NativeBackboneMemoryCoordinatePersistenceStore();
		let closeCalls = 0;
		const closeOptions: Array<{ flush?: boolean } | undefined> = [];
		const store: NativeBackboneCoordinatePersistenceStore = {
			read: (name) => memory.read(name),
			write: (name, bytes) => memory.write(name, bytes),
			append: (name, bytes) => memory.append(name, bytes),
			remove: (name) => memory.remove(name),
			flush: (name) => memory.flush(name),
			close: async (options) => {
				closeCalls++;
				closeOptions.push(options);
				if (closeCalls === 1) {
					throw new Error("transient drop-wins close failure");
				}
			},
		};
		const persistence = new NativeBackboneCoordinatePersistence(store);
		await memory.write("coordinates.wal", new Uint8Array([1, 2, 3]));

		const dropping = persistence.drop();
		const firstClose = persistence.close();
		await dropping;
		const firstError = await firstClose.then(
			() => undefined,
			(error: unknown) => error,
		);
		expect(String(firstError)).to.contain("transient drop-wins close failure");
		expect(memory.files.size).equal(0);

		const retry = persistence.close();
		expect(retry).not.equal(firstClose);
		await retry;
		expect(closeCalls).equal(2);
		expect(closeOptions).to.deep.equal([{ flush: false }, { flush: false }]);
	});

	it("lets an in-flight drop finish before terminal close", async () => {
		const memory = new NativeBackboneMemoryCoordinatePersistenceStore();
		let closed = false;
		let closeCalls = 0;
		const assertOpen = () => {
			if (closed) {
				throw new Error("terminal store is closed");
			}
		};
		const store: NativeBackboneCoordinatePersistenceStore = {
			read: (name) => {
				assertOpen();
				return memory.read(name);
			},
			write: (name, bytes) => {
				assertOpen();
				return memory.write(name, bytes);
			},
			append: (name, bytes) => {
				assertOpen();
				return memory.append(name, bytes);
			},
			remove: (name) => {
				assertOpen();
				return memory.remove(name);
			},
			flush: (name) => {
				assertOpen();
				return memory.flush(name);
			},
			close: async () => {
				assertOpen();
				closeCalls++;
				closed = true;
			},
		};
		const persistence = new NativeBackboneCoordinatePersistence(store);
		await memory.write("coordinates.wal", new Uint8Array([1, 2, 3]));

		const dropping = persistence.drop();
		const closing = persistence.close();
		expect(persistence.close()).equal(closing);
		await Promise.all([dropping, closing]);

		expect(memory.files.size).equal(0);
		expect(closeCalls).equal(1);
	});

	it("serializes close behind all hydrate data reads and backbone loads", async () => {
		const target = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});
		const memory = new NativeBackboneMemoryCoordinatePersistenceStore();
		let readEntered!: () => void;
		const entered = new Promise<void>((resolve) => {
			readEntered = resolve;
		});
		let releaseRead!: () => void;
		const gate = new Promise<void>((resolve) => {
			releaseRead = resolve;
		});
		let gateArmed = true;
		let closed = false;
		let postCloseReads = 0;
		const store: NativeBackboneCoordinatePersistenceStore = {
			read: async (name) => {
				if (closed) {
					postCloseReads++;
					throw new Error("terminal store is closed");
				}
				if (gateArmed && name === "coordinates.wal") {
					gateArmed = false;
					readEntered();
					await gate;
				}
				return memory.read(name);
			},
			write: (name, bytes) => memory.write(name, bytes),
			append: (name, bytes) => memory.append(name, bytes),
			remove: (name) => memory.remove(name),
			flush: (name) => memory.flush(name),
			close: async () => {
				closed = true;
			},
		};
		const persistence = new NativeBackboneCoordinatePersistence(store);
		const hydrating = persistence.hydrate(target);
		await entered;
		let closeSettled = false;
		const closing = persistence.close().finally(() => {
			closeSettled = true;
		});
		await Promise.resolve();
		expect(closeSettled).equal(false);
		releaseRead();
		expect(await hydrating).equal(0);
		await closing;

		expect(postCloseReads).equal(0);
	});

	it("rejects drop before erasure while a hydrate data read is in flight", async () => {
		const target = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});
		const memory = new NativeBackboneMemoryCoordinatePersistenceStore();
		await memory.write("coordinates.bin", target.coordinateSnapshot());
		let readEntered!: () => void;
		const entered = new Promise<void>((resolve) => {
			readEntered = resolve;
		});
		let releaseRead!: () => void;
		const gate = new Promise<void>((resolve) => {
			releaseRead = resolve;
		});
		let gateArmed = true;
		let removes = 0;
		const store: NativeBackboneCoordinatePersistenceStore = {
			read: async (name) => {
				if (gateArmed && name === "document-values.wal") {
					gateArmed = false;
					readEntered();
					await gate;
				}
				return memory.read(name);
			},
			write: (name, bytes) => memory.write(name, bytes),
			append: (name, bytes) => memory.append(name, bytes),
			remove: async (name) => {
				removes++;
				await memory.remove(name);
			},
		};
		const persistence = new NativeBackboneCoordinatePersistence(store);
		const hydrating = persistence.hydrate(target);
		await entered;
		const dropError = await persistence.drop().then(
			() => undefined,
			(error: unknown) => error,
		);
		expect(String(dropError)).to.contain("while hydrating");
		expect(removes).equal(0);
		releaseRead();
		expect(await hydrating).equal(0);
		expect(memory.files.has("coordinates.bin")).equal(true);
	});

	it("does not reactivate a dropped generation from a queued resume", async () => {
		const memory = new NativeBackboneMemoryCoordinatePersistenceStore();
		let removeEntered!: () => void;
		const entered = new Promise<void>((resolve) => {
			removeEntered = resolve;
		});
		let releaseRemove!: () => void;
		const gate = new Promise<void>((resolve) => {
			releaseRemove = resolve;
		});
		let gateArmed = true;
		const store: NativeBackboneCoordinatePersistenceStore = {
			read: (name) => memory.read(name),
			write: (name, bytes) => memory.write(name, bytes),
			append: (name, bytes) => memory.append(name, bytes),
			remove: async (name) => {
				if (name === "coordinates.bin" && gateArmed) {
					gateArmed = false;
					removeEntered();
					await gate;
				}
				await memory.remove(name);
			},
		};
		const persistence = new NativeBackboneCoordinatePersistence(store);
		await memory.append("coordinates.bin", new Uint8Array([1]));

		const dropping = persistence.drop();
		await entered;
		const resuming = persistence.resumeDrop();
		releaseRemove();
		await dropping;
		expect(await resuming).equal(true);

		let secondDropError: unknown;
		try {
			await persistence.drop();
		} catch (error) {
			secondDropError = error;
		}
		expect(String(secondDropError)).to.contain("while dropped");
		expect(memory.files.size).equal(0);
	});

	it("keeps a retried recovery-origin resume active", async () => {
		const memory = new NativeBackboneMemoryCoordinatePersistenceStore();
		let failRemoval = true;
		let failTombstoneRead = false;
		const store: NativeBackboneCoordinatePersistenceStore = {
			read: async (name) => {
				if (name === "native-backbone-drop.tombstone" && failTombstoneRead) {
					failTombstoneRead = false;
					throw new Error("transient tombstone read failure");
				}
				return memory.read(name);
			},
			write: (name, bytes) => memory.write(name, bytes),
			append: (name, bytes) => memory.append(name, bytes),
			remove: async (name) => {
				if (name === "coordinates.bin" && failRemoval) {
					failRemoval = false;
					throw new Error("seed interrupted drop");
				}
				await memory.remove(name);
			},
		};
		await memory.append("coordinates.bin", new Uint8Array([1]));
		const seed = new NativeBackboneCoordinatePersistence(store);
		const seedError = await seed.drop().then(
			() => undefined,
			(error: unknown) => error,
		);
		expect(String(seedError)).to.contain(
			"Failed to erase native backbone coordinate persistence namespace",
		);
		expect(memory.files.has("native-backbone-drop.tombstone")).equal(true);

		const recovering = new NativeBackboneCoordinatePersistence(store);
		failTombstoneRead = true;
		const resumeError = await recovering.resumeDrop().then(
			() => undefined,
			(error: unknown) => error,
		);
		expect(String(resumeError)).to.contain("transient tombstone read failure");
		expect(await recovering.resumeDrop()).equal(true);
		expect(memory.files.size).equal(0);

		// Recovery did not originate the erased generation, so this adapter remains
		// active and can start a later explicit drop of its own.
		await recovering.drop();
		expect(memory.files.size).equal(0);
	});

	it("resumes a failed drop on the same persistence generation", async () => {
		const memory = new NativeBackboneMemoryCoordinatePersistenceStore();
		let failRemoval = true;
		const store: NativeBackboneCoordinatePersistenceStore = {
			read: (name) => memory.read(name),
			write: (name, bytes) => memory.write(name, bytes),
			append: (name, bytes) => memory.append(name, bytes),
			remove: async (name) => {
				if (name === "coordinates.bin" && failRemoval) {
					failRemoval = false;
					throw new Error("injected removal failure");
				}
				await memory.remove(name);
			},
		};
		const persistence = new NativeBackboneCoordinatePersistence(store);
		await memory.append("coordinates.bin", new Uint8Array([1]));

		const dropError = await persistence.drop().then(
			() => undefined,
			(error: unknown) => error,
		);
		expect(dropError).to.be.instanceOf(AggregateError);
		expect(memory.files.has("native-backbone-drop.tombstone")).equal(true);
		expect(memory.files.has("coordinates.bin")).equal(true);

		expect(await persistence.resumeDrop()).equal(true);
		expect(memory.files.size).equal(0);

		let secondDropError: unknown;
		try {
			await persistence.drop();
		} catch (error) {
			secondDropError = error;
		}
		expect(String(secondDropError)).to.contain("while dropped");
	});

	it("keeps journal records appended during a flush write for the next flush", async () => {
		const source = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});
		const restored = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});
		const memory = new NativeBackboneMemoryCoordinatePersistenceStore();
		let enteredAppend!: () => void;
		const appendEntered = new Promise<void>((resolve) => {
			enteredAppend = resolve;
		});
		let release!: () => void;
		const gate = new Promise<void>((resolve) => {
			release = resolve;
		});
		let gateArmed = false;
		const store: NativeBackboneCoordinatePersistenceStore = {
			read: (name) => memory.read(name),
			write: (name, bytes) => memory.write(name, bytes),
			append: async (name, bytes) => {
				if (gateArmed) {
					gateArmed = false;
					enteredAppend();
					await gate;
				}
				return memory.append(name, bytes);
			},
			remove: (name) => memory.remove(name),
		};
		const persistence = new NativeBackboneCoordinatePersistence(store);
		await persistence.hydrate(source);

		source.putEntryCoordinates("hash-a", "gid-a", [1n], false, 1, 1n);
		gateArmed = true;
		const flushing = persistence.flushJournal(source);
		await appendEntered;
		// The flush is parked inside its disk write; a record appended now must
		// survive the flush's journal clear.
		source.putEntryCoordinates("hash-b", "gid-b", [2n], false, 1, 2n);
		release();
		await flushing;
		expect(source.coordinatePendingJournalLength).to.equal(1);

		await persistence.flushJournal(source);
		expect(source.coordinatePendingJournalLength).to.equal(0);
		expect(await persistence.hydrate(restored)).to.equal(2);
		expect(restored.getEntryCoordinateHashes()).to.deep.equal([
			"hash-a",
			"hash-b",
		]);
	});

	it("keeps the wasm journal pending until the WAL durability barrier completes", async () => {
		const source = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});
		const memory = new NativeBackboneMemoryCoordinatePersistenceStore();
		let flushEntered!: () => void;
		const entered = new Promise<void>((resolve) => {
			flushEntered = resolve;
		});
		let releaseFlush!: () => void;
		const gate = new Promise<void>((resolve) => {
			releaseFlush = resolve;
		});
		let gateArmed = false;
		const store: NativeBackboneCoordinatePersistenceStore = {
			read: (name) => memory.read(name),
			write: (name, bytes) => memory.write(name, bytes),
			append: (name, bytes) => memory.append(name, bytes),
			remove: (name) => memory.remove(name),
			durableBarrier: async (name) => {
				if (gateArmed && name === "coordinates.wal") {
					gateArmed = false;
					flushEntered();
					await gate;
				}
				await memory.flush(name);
			},
		};
		const persistence = new NativeBackboneCoordinatePersistence(store);
		await persistence.hydrate(source);
		source.putEntryCoordinates(
			"hash-barrier",
			"gid-barrier",
			[1n],
			false,
			1,
			1n,
		);

		gateArmed = true;
		let settled = false;
		const flushing = persistence.flushJournal(source).finally(() => {
			settled = true;
		});
		await entered;
		await Promise.resolve();
		expect(settled).equal(false);
		expect(source.coordinatePendingJournalLength).equal(1);

		releaseFlush();
		expect(await flushing).to.be.greaterThan(0);
		expect(source.coordinatePendingJournalLength).equal(0);
	});

	it("poisons the adapter when the WAL durability barrier fails", async () => {
		const source = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});
		const memory = new NativeBackboneMemoryCoordinatePersistenceStore();
		const barrierError = new Error("injected WAL fsync failure");
		let appendCalls = 0;
		const store: NativeBackboneCoordinatePersistenceStore = {
			read: (name) => memory.read(name),
			write: (name, bytes) => memory.write(name, bytes),
			append: async (name, bytes) => {
				appendCalls++;
				await memory.append(name, bytes);
			},
			remove: (name) => memory.remove(name),
			durableBarrier: async (name) => {
				if (name === "coordinates.wal") {
					throw barrierError;
				}
				await memory.flush(name);
			},
		};
		const persistence = new NativeBackboneCoordinatePersistence(store);
		await persistence.hydrate(source);
		source.putEntryCoordinates("hash-fsync", "gid-fsync", [1n], false, 1, 1n);

		let firstError: unknown;
		await persistence.flushJournal(source).catch((error) => {
			firstError = error;
		});
		expect(firstError).equal(barrierError);
		expect(source.coordinatePendingJournalLength).equal(1);
		expect(appendCalls).equal(1);

		let secondError: unknown;
		try {
			await persistence.flushJournal(source);
		} catch (error) {
			secondError = error;
		}
		expect(secondError).equal(firstError);
		expect(appendCalls).equal(1);
		await persistence.drop();
		expect(memory.files.size).equal(0);
	});

	it("fails closed when the journal tail is truncated mid-record", async () => {
		const source = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});
		const target = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});
		source.setCoordinateJournalEnabled(true);
		source.putEntryCoordinates(
			"hash-snapshot",
			"gid-snapshot",
			[1n],
			false,
			1,
			1n,
		);
		const snapshot = source.coordinateSnapshot();
		source.clearCoordinateJournal();
		const recordBytes = (hash: string, coordinate: bigint) => {
			source.putEntryCoordinates(
				hash,
				`gid-${hash}`,
				[coordinate],
				false,
				1,
				coordinate,
			);
			const record = source.coordinateJournal();
			source.clearCoordinateJournal();
			return record;
		};
		const recordA = recordBytes("hash-a", 2n);
		const recordB = recordBytes("hash-b", 3n);
		const store = new NativeBackboneMemoryCoordinatePersistenceStore();
		const persistence = new NativeBackboneCoordinatePersistence(store);

		await store.write("coordinates.bin", snapshot);
		// Keep the length/checksum header of the trailing record but cut its
		// payload short, as a crash mid-append would.
		await store.write(
			"coordinates.wal",
			concatBytes([
				source.coordinateJournalHeader(),
				recordA,
				recordB.subarray(0, recordB.byteLength - 3),
			]),
		);

		let hydrateError: unknown;
		await persistence.hydrate(target).catch((error) => {
			hydrateError = error;
		});
		expect(String(hydrateError)).to.contain("torn record payload");
		expect(target.getEntryCoordinateHashes()).to.deep.equal([]);
		let flushError: unknown;
		try {
			await persistence.flushJournal(target);
		} catch (error) {
			flushError = error;
		}
		expect(flushError).equal(hydrateError);
	});

	it("fails closed on a checksum-corrupted journal record", async () => {
		const source = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});
		const target = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});
		source.setCoordinateJournalEnabled(true);
		const recordBytes = (hash: string, coordinate: bigint) => {
			source.putEntryCoordinates(
				hash,
				`gid-${hash}`,
				[coordinate],
				false,
				1,
				coordinate,
			);
			const record = source.coordinateJournal();
			source.clearCoordinateJournal();
			return record;
		};
		const recordA = recordBytes("hash-a", 1n);
		const recordB = recordBytes("hash-b", 2n);
		const recordC = recordBytes("hash-c", 3n);
		// Flip a payload byte of the middle record so its checksum no longer
		// matches; the length/checksum framing stays intact.
		const corruptedB = recordB.slice();
		corruptedB[8] ^= 0xff;
		const store = new NativeBackboneMemoryCoordinatePersistenceStore();
		const persistence = new NativeBackboneCoordinatePersistence(store);

		await store.write(
			"coordinates.wal",
			concatBytes([
				source.coordinateJournalHeader(),
				recordA,
				corruptedB,
				recordC,
			]),
		);

		let hydrateError: unknown;
		await persistence.hydrate(target).catch((error) => {
			hydrateError = error;
		});
		expect(String(hydrateError)).to.contain("checksum mismatch");
		expect(target.getEntryCoordinateHashes()).to.deep.equal([]);
	});

	it("rejects hydrate on a checksum-corrupted snapshot and stays usable", async () => {
		const source = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});
		const target = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});
		source.putEntryCoordinates(
			"hash-snapshot",
			"gid-snapshot",
			[1n],
			false,
			1,
			1n,
		);
		const snapshot = source.coordinateSnapshot();
		// Flip a payload byte behind the envelope header (magic + length +
		// checksum = 16 bytes) so the envelope checksum no longer matches.
		const corrupted = snapshot.slice();
		corrupted[16] ^= 0xff;
		const store = new NativeBackboneMemoryCoordinatePersistenceStore();
		const persistence = new NativeBackboneCoordinatePersistence(store);
		await store.write("coordinates.bin", corrupted);

		let thrown: unknown;
		try {
			await persistence.hydrate(target);
			expect.fail("hydrate should reject on a corrupted snapshot");
		} catch (error) {
			thrown = error;
		}
		expect(thrown).to.exist;
		expect(thrown).to.not.be.instanceOf(WebAssembly.RuntimeError);
		expect(String(thrown)).to.contain("checksum mismatch");

		// The failed decode happens before any state is cleared; the backbone
		// instance must remain usable.
		target.putEntryCoordinates("hash-after", "gid-after", [9n], false, 1, 9n);
		expect(target.hasCoordinateIndexHash("hash-after")).equal(true);
		await store.write("coordinates.bin", snapshot);
		expect(await persistence.hydrate(target)).to.equal(0);
		expect(target.getEntryCoordinateHashes()).to.deep.equal(["hash-snapshot"]);
	});

	it("rejects hydrate gracefully when a snapshot declares an oversized entry count", async () => {
		const target = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});
		// Valid envelope whose payload claims u32::MAX entries but carries no
		// entry bytes; decoding must clamp the preallocation and fail with a
		// decode error instead of aborting the wasm instance.
		const oversized = keyValueSnapshotEnvelope(
			Uint8Array.from([0xff, 0xff, 0xff, 0xff]),
		);
		const store = new NativeBackboneMemoryCoordinatePersistenceStore();
		const persistence = new NativeBackboneCoordinatePersistence(store);
		await store.write("coordinates.bin", oversized);

		let thrown: unknown;
		try {
			await persistence.hydrate(target);
			expect.fail("hydrate should reject on an oversized snapshot count");
		} catch (error) {
			thrown = error;
		}
		expect(thrown).to.exist;
		expect(thrown).to.not.be.instanceOf(WebAssembly.RuntimeError);
		expect(String(thrown)).to.contain("truncated");

		target.putEntryCoordinates("hash-after", "gid-after", [9n], false, 1, 9n);
		expect(target.hasCoordinateIndexHash("hash-after")).equal(true);
	});

	it("persists native document index values through the native persistence adapter", async () => {
		const source = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});
		const restoredFromWal = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});
		source.configureDocumentSchemaIr(contextOnlySchema());
		source.setDocumentContextHeadField(3);
		source.setDocumentContextFields({
			created: 1,
			modified: 2,
			head: 3,
			gid: 4,
			size: 5,
		});
		const store = new NativeBackboneMemoryCoordinatePersistenceStore();
		const persistence = new NativeBackboneCoordinatePersistence(store);

		await persistence.hydrate(source);
		source.preparePlainCommittedNoNextStorageAppendDocumentIndexCompactTransaction(
			{
				wallTime: 11n,
				logical: 1,
				gid: "gid-document-persist",
				payloadData: new Uint8Array([1, 2, 3]),
				replicas: 1,
				selfHash: "peer",
				documentIndex: {
					key: "doc-value",
					valuePrefixBytes: new Uint8Array(0),
				},
			},
		);
		expect(source.documentPendingJournalLength).to.equal(1);
		await persistence.flushJournal(source);
		expect(store.files.has("document-values.wal")).equal(true);
		expect(
			store.files.get("document-values.wal")?.byteLength,
		).to.be.greaterThan(source.documentJournalHeader().byteLength);

		await persistence.hydrate(restoredFromWal);
		expect(restoredFromWal.documentValueLength).to.equal(1);
		expect(restoredFromWal.documentIndexLength).to.equal(0);
		restoredFromWal.configureDocumentSchemaIr(contextOnlySchema());
		restoredFromWal.setDocumentContextHeadField(3);
		restoredFromWal.setDocumentContextFields({
			created: 1,
			modified: 2,
			head: 3,
			gid: 4,
			size: 5,
		});
		expect(restoredFromWal.documentIndexLength).to.equal(1);
		expect(
			Array.from(restoredFromWal.documentKeysExist(["doc-value"])),
		).to.deep.equal([1]);
	});

	it("persists document previous signer facts through the native persistence adapter", async () => {
		const source = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});
		const restoredFromWal = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});
		for (const backbone of [source, restoredFromWal]) {
			backbone.configureDocumentSchemaIr(contextOnlySchema());
			backbone.setDocumentContextHeadField(3);
			backbone.setDocumentContextFields({
				created: 1,
				modified: 2,
				head: 3,
				gid: 4,
				size: 5,
			});
		}
		const store = new NativeBackboneMemoryCoordinatePersistenceStore();
		const persistence = new NativeBackboneCoordinatePersistence(store);

		await persistence.hydrate(source);
		const append =
			source.preparePlainCommittedNoNextStorageAppendDocumentIndexCompactTransaction(
				{
					wallTime: 11n,
					logical: 1,
					gid: "gid-signer-persist",
					payloadData: new Uint8Array([1, 2, 3]),
					replicas: 1,
					selfHash: "peer",
					documentIndex: {
						key: "doc-signer",
						valuePrefixBytes: new Uint8Array(0),
					},
				},
			);
		const documentValue = source.documentValueBytes("doc-signer");
		expect(documentValue).to.exist;
		source.putDocumentEncodedPartsStored(
			"doc-signer",
			documentValue!,
			new Uint8Array(0),
		);
		expect(source.documentSignerPendingJournalLength).to.equal(1);
		await persistence.flushJournal(source);
		expect(store.files.has("document-signers.wal")).equal(true);
		expect(
			store.files.get("document-signers.wal")?.byteLength,
		).to.be.greaterThan(source.documentSignerJournalHeader().byteLength);

		await persistence.hydrate(restoredFromWal);
		restoredFromWal.clearDocumentIndex();
		restoredFromWal.putDocumentEncodedPartsStored(
			"doc-signer",
			documentValue!,
			new Uint8Array(0),
		);
		expect(restoredFromWal.hasBlock(append.entry.hash)).equal(false);
		expect(
			Array.from(
				restoredFromWal.documentPreviousSignaturePublicKey("doc-signer")
					?.publicKey ?? [],
			),
		).to.deep.equal(Array.from(publicKey));
	});

	it("bounds memory named-file reads without changing unlimited reads", async () => {
		const store = new NativeBackboneMemoryCoordinatePersistenceStore();
		await store.write("bounded.bin", new Uint8Array([1, 2, 3]));

		const unlimited = await store.read("bounded.bin");
		unlimited![0] = 9;
		expect(await store.read("bounded.bin")).to.deep.equal(
			new Uint8Array([1, 2, 3]),
		);
		expect(await store.readLimited("bounded.bin", 3)).to.deep.equal(
			new Uint8Array([1, 2, 3]),
		);
		expect(await store.readLimited("missing.bin", 0)).to.equal(undefined);

		const overflow = await store.readLimited("bounded.bin", 2).then(
			() => undefined,
			(error: unknown) => error,
		);
		expect(overflow).to.be.instanceOf(
			NativeBackboneCoordinatePersistenceReadLimitError,
		);
		expect(
			(overflow as NativeBackboneCoordinatePersistenceReadLimitError)
				.observedBytes,
		).to.equal(3n);
		await expect(store.readLimited("bounded.bin", -1)).to.be.rejectedWith(
			RangeError,
			"non-negative safe integer",
		);
	});

	it("bounds buffered named-file reads before flushing pending bytes", async () => {
		const memory = new NativeBackboneMemoryCoordinatePersistenceStore();
		await memory.write("combined.bin", new Uint8Array([1, 2]));
		const buffered = new NativeBackboneBufferedCoordinatePersistenceStore(
			memory,
		);
		expect(buffered.readLimited).to.be.a("function");
		await buffered.append("combined.bin", new Uint8Array([3]));
		await buffered.append("combined.bin", new Uint8Array([4]));

		const overflow = await buffered.readLimited!("combined.bin", 3).then(
			() => undefined,
			(error: unknown) => error,
		);
		expect(overflow).to.be.instanceOf(
			NativeBackboneCoordinatePersistenceReadLimitError,
		);
		expect(
			(overflow as NativeBackboneCoordinatePersistenceReadLimitError).maxBytes,
		).to.equal(3);
		expect(
			(overflow as NativeBackboneCoordinatePersistenceReadLimitError)
				.observedBytes,
		).to.equal(4n);
		expect(await memory.read("combined.bin")).to.deep.equal(
			new Uint8Array([1, 2]),
		);
		expect(await buffered.readLimited!("combined.bin", 4)).to.deep.equal(
			new Uint8Array([1, 2, 3, 4]),
		);
		expect(await buffered.readLimited!("missing.bin", 0)).to.equal(undefined);
	});

	it("does not advertise bounded reads over a legacy custom store", async () => {
		let unlimitedReads = 0;
		const memory = new NativeBackboneMemoryCoordinatePersistenceStore();
		await memory.write("legacy.bin", new Uint8Array([1, 2, 3]));
		const legacy: NativeBackboneCoordinatePersistenceStore = {
			read: async (name) => {
				unlimitedReads++;
				return memory.read(name);
			},
			write: (name, bytes) => memory.write(name, bytes),
			append: (name, bytes) => memory.append(name, bytes),
		};
		const buffered = new NativeBackboneBufferedCoordinatePersistenceStore(
			legacy,
		);

		expect(buffered.readLimited).to.equal(undefined);
		expect(unlimitedReads).to.equal(0);
		expect(await buffered.read("legacy.bin")).to.deep.equal(
			new Uint8Array([1, 2, 3]),
		);
		expect(unlimitedReads).to.equal(1);
	});

	it("does not infer bounded reads from an injected Node open method", () => {
		const node = new NativeBackboneNodeCoordinatePersistenceStore("legacy", {
			mkdir: async () => {},
			readFile: async () => new Uint8Array(),
			writeFile: async () => {},
			appendFile: async () => {},
			open: async () => ({
				write: async (data) => ({ bytesWritten: data.byteLength }),
				read: async () => ({ bytesRead: 0 }),
				stat: async () => ({ size: 0n }),
				sync: async () => {},
				close: async () => {},
			}),
			rm: async () => {},
		});
		const buffered = new NativeBackboneBufferedCoordinatePersistenceStore(node);

		expect(node.readLimited).to.equal(undefined);
		expect(buffered.readLimited).to.equal(undefined);
	});

	it("fails closed if pending bytes change during a bounded read", async () => {
		const memory = new NativeBackboneMemoryCoordinatePersistenceStore();
		await memory.write("concurrent.bin", new Uint8Array([1]));
		let markReadEntered!: () => void;
		const readEntered = new Promise<void>((resolve) => {
			markReadEntered = resolve;
		});
		let releaseRead!: () => void;
		const readBlocked = new Promise<void>((resolve) => {
			releaseRead = resolve;
		});
		let boundedReads = 0;
		let appendCalls = 0;
		const inner: NativeBackboneCoordinatePersistenceStore = {
			read: (name) => memory.read(name),
			readLimited: async (name, maxBytes) => {
				boundedReads++;
				if (boundedReads === 1) {
					markReadEntered();
					await readBlocked;
				}
				return memory.readLimited(name, maxBytes);
			},
			write: (name, bytes) => memory.write(name, bytes),
			append: async (name, bytes) => {
				appendCalls++;
				await memory.append(name, bytes);
			},
		};
		const buffered = new NativeBackboneBufferedCoordinatePersistenceStore(
			inner,
		);
		await buffered.append("concurrent.bin", new Uint8Array([2]));
		const reading = buffered.readLimited!("concurrent.bin", 3);
		await readEntered;
		await buffered.append("concurrent.bin", new Uint8Array([3]));
		releaseRead();

		await expect(reading).to.be.rejectedWith(
			"pending bytes changed during a bounded read",
		);
		expect(appendCalls).to.equal(0);
		expect(await memory.read("concurrent.bin")).to.deep.equal(
			new Uint8Array([1]),
		);
		expect(await buffered.readLimited!("concurrent.bin", 3)).to.deep.equal(
			new Uint8Array([1, 2, 3]),
		);
		expect(appendCalls).to.equal(1);
	});

	it("uses pre-allocation bounds for Node named-file reads", async () => {
		const bytes = new Uint8Array([1, 2, 3]);
		let readFileCalls = 0;
		let positionalReadCalls = 0;
		let closeCalls = 0;
		let statCalls = 0;
		const store = new NativeBackboneNodeCoordinatePersistenceStore("bounded", {
			mkdir: async () => {},
			readFile: async () => {
				readFileCalls++;
				return bytes.slice();
			},
			writeFile: async () => {},
			appendFile: async () => {},
			openBoundedRead: async (path) => {
				if (path.endsWith("missing.bin")) {
					throw Object.assign(new Error("not found"), { code: "ENOENT" });
				}
				return {
					stat: async () => {
						statCalls++;
						return { size: BigInt(bytes.byteLength) };
					},
					read: async (target, offset, length, position) => {
						positionalReadCalls++;
						const available = Math.max(0, bytes.byteLength - position);
						const count = Math.min(length, available, 2);
						target.set(bytes.subarray(position, position + count), offset);
						return { bytesRead: count };
					},
					close: async () => {
						closeCalls++;
					},
				};
			},
			rm: async () => {},
		});

		expect(await store.read("file.bin")).to.deep.equal(bytes);
		expect(readFileCalls).to.equal(1);
		expect(await store.readLimited!("file.bin", 3)).to.deep.equal(bytes);
		expect(positionalReadCalls).to.equal(3);
		expect(statCalls).to.equal(2);
		expect(closeCalls).to.equal(1);
		expect(await store.readLimited!("missing.bin", 0)).to.equal(undefined);

		const overflow = await store.readLimited!("file.bin", 2).then(
			() => undefined,
			(error: unknown) => error,
		);
		expect(overflow).to.be.instanceOf(
			NativeBackboneCoordinatePersistenceReadLimitError,
		);
		expect(positionalReadCalls).to.equal(3);
		expect(closeCalls).to.equal(2);
		expect(readFileCalls).to.equal(1);
	});

	it("rejects changing Node named files and always closes", async () => {
		let closeCalls = 0;
		let statCalls = 0;
		const store = new NativeBackboneNodeCoordinatePersistenceStore("growth", {
			mkdir: async () => {},
			readFile: async () => new Uint8Array(),
			writeFile: async () => {},
			appendFile: async () => {},
			openBoundedRead: async () => ({
				stat: async () => ({ size: statCalls++ === 0 ? 2n : 3n }),
				read: async (target, offset, length, position) => {
					const grown = new Uint8Array([1, 2, 3]);
					const count = Math.min(length, grown.byteLength - position);
					target.set(grown.subarray(position, position + count), offset);
					return { bytesRead: count };
				},
				close: async () => {
					closeCalls++;
				},
			}),
			rm: async () => {},
		});

		const failure = await store.readLimited!("file.bin", 2).then(
			() => undefined,
			(error: unknown) => error,
		);
		expect(failure).to.be.instanceOf(
			NativeBackboneCoordinatePersistenceReadLimitError,
		);
		expect(closeCalls).to.equal(1);
	});

	it("checks OPFS named-file size before allocating its contents", async () => {
		const directory = new FakeOPFSDirectoryHandle();
		directory.files.set("bounded.bin", new Uint8Array([1, 2, 3]));
		const store = new NativeBackboneOPFSCoordinatePersistenceStore(directory);

		expect(await store.readLimited("bounded.bin", 3)).to.deep.equal(
			new Uint8Array([1, 2, 3]),
		);
		expect(directory.arrayBufferCount).to.equal(1);
		expect(await store.readLimited("missing.bin", 0)).to.equal(undefined);
		const overflow = await store.readLimited("bounded.bin", 2).then(
			() => undefined,
			(error: unknown) => error,
		);
		expect(overflow).to.be.instanceOf(
			NativeBackboneCoordinatePersistenceReadLimitError,
		);
		expect(directory.arrayBufferCount).to.equal(1);
	});

	it("persists native coordinate WAL through the node filesystem store", async () => {
		const [{ mkdtemp, rm }, { tmpdir }, { join }] = await Promise.all([
			import("node:fs/promises"),
			import("node:os"),
			import("node:path"),
		]);
		const directory = await mkdtemp(
			join(tmpdir(), "peerbit-native-backbone-coordinates-"),
		);
		try {
			const source = await createNativePeerbitBackbone({
				clockId: publicKey,
				privateKey,
				publicKey,
			});
			const restored = await createNativePeerbitBackbone({
				clockId: publicKey,
				privateKey,
				publicKey,
			});
			const reflushed = await createNativePeerbitBackbone({
				clockId: publicKey,
				privateKey,
				publicKey,
			});
			const persistence = new NativeBackboneCoordinatePersistence(
				new NativeBackboneNodeCoordinatePersistenceStore(directory),
			);

			await persistence.hydrate(source);
			source.putEntryCoordinates("hash-a", "gid-a", [1n], false, 1, 1n);
			expect(await persistence.flushJournal(source)).to.be.greaterThan(0);
			expect(await persistence.hydrate(restored)).to.equal(1);
			expect(restored.getEntryCoordinateHashes()).to.deep.equal(["hash-a"]);

			source.putEntryCoordinates("hash-b", "gid-b", [2n], false, 1, 2n);
			await persistence.flushJournal(source);
			expect(await persistence.hydrate(reflushed)).to.equal(2);
			expect(reflushed.getEntryCoordinateHashes()).to.deep.equal([
				"hash-a",
				"hash-b",
			]);
			expect(reflushed.coordinateIndexLength).to.equal(2);
		} finally {
			await rm(directory, { recursive: true, force: true });
		}
	});

	it("completes short Node WAL writes and poisons zero-progress writes", async () => {
		const createStore = (writeCounts: number[]) => {
			const files = new Map<string, Uint8Array>();
			let writeCalls = 0;
			const store = new NativeBackboneNodeCoordinatePersistenceStore(
				"coordinate-directory",
				{
					mkdir: async () => {},
					readFile: async (path) => {
						const bytes = files.get(path);
						if (!bytes) {
							throw Object.assign(new Error("not found"), { code: "ENOENT" });
						}
						return bytes.slice();
					},
					writeFile: async (path, data) => {
						files.set(path, data.slice());
					},
					appendFile: async (path, data) => {
						files.set(
							path,
							concatBytes([files.get(path) ?? new Uint8Array(), data]),
						);
					},
					open: async (path) => ({
						write: async (data) => {
							writeCalls++;
							const bytesWritten = writeCounts.shift() ?? data.byteLength;
							if (bytesWritten > 0 && bytesWritten <= data.byteLength) {
								files.set(
									path,
									concatBytes([
										files.get(path) ?? new Uint8Array(),
										data.subarray(0, bytesWritten),
									]),
								);
							}
							return { bytesWritten };
						},
						sync: async () => {},
						close: async () => {},
					}),
					rm: async (path) => {
						files.delete(path);
					},
				},
			);
			return { store, files, writeCalls: () => writeCalls };
		};

		const short = createStore([2]);
		await short.store.append("coordinates.wal", new Uint8Array([1, 2, 3, 4]));
		expect(short.writeCalls()).equal(2);
		expect([...short.files.values()][0]).deep.equal(
			new Uint8Array([1, 2, 3, 4]),
		);

		const zero = createStore([0]);
		let firstError: unknown;
		try {
			await zero.store.append("coordinates.wal", new Uint8Array([5, 6]));
		} catch (error) {
			firstError = error;
		}
		expect(String(firstError)).to.contain(
			"Invalid Node coordinate WAL write progress",
		);
		let secondError: unknown;
		try {
			await zero.store.append("coordinates.wal", new Uint8Array([7]));
		} catch (error) {
			secondError = error;
		}
		expect(secondError).equal(firstError);
		expect(zero.writeCalls()).equal(1);
		expect([...zero.files.values()][0]?.byteLength ?? 0).equal(0);
	});

	it("refuses a Node WAL acknowledgement without FileHandle.sync", async () => {
		const files = new Map<string, Uint8Array>();
		const store = new NativeBackboneNodeCoordinatePersistenceStore(
			"coordinate-directory",
			{
				mkdir: async () => {},
				readFile: async (path) => {
					const bytes = files.get(path);
					if (!bytes) {
						throw Object.assign(new Error("not found"), { code: "ENOENT" });
					}
					return bytes.slice();
				},
				writeFile: async (path, data) => {
					files.set(path, data.slice());
				},
				appendFile: async (path, data) => {
					files.set(
						path,
						concatBytes([files.get(path) ?? new Uint8Array(), data]),
					);
				},
				open: async (path) => ({
					write: async (data) => {
						files.set(
							path,
							concatBytes([files.get(path) ?? new Uint8Array(), data]),
						);
						return { bytesWritten: data.byteLength };
					},
					close: async () => {},
				}),
				rm: async (path) => {
					files.delete(path);
				},
			},
		);
		const persistence = new NativeBackboneCoordinatePersistence(store);
		const source = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});
		await persistence.hydrate(source);
		source.putEntryCoordinates(
			"hash-no-sync",
			"gid-no-sync",
			[1n],
			false,
			1,
			1n,
		);

		const failure = await persistence.flushJournal(source).then(
			() => undefined,
			(error: unknown) => error,
		);
		expect(String(failure)).to.contain("does not expose FileHandle.sync");
		expect(source.coordinatePendingJournalLength).equal(1);
	});

	it("persists native coordinate WAL through the direct node adapter", async () => {
		const [{ mkdtemp, rm }, { tmpdir }, { join }] = await Promise.all([
			import("node:fs/promises"),
			import("node:os"),
			import("node:path"),
		]);
		const directory = await mkdtemp(
			join(tmpdir(), "peerbit-native-backbone-direct-coordinates-"),
		);
		try {
			const source = await createNativePeerbitBackbone({
				clockId: publicKey,
				privateKey,
				publicKey,
			});
			const beforeClose = await createNativePeerbitBackbone({
				clockId: publicKey,
				privateKey,
				publicKey,
			});
			const afterClose = await createNativePeerbitBackbone({
				clockId: publicKey,
				privateKey,
				publicKey,
			});
			const thresholdSource = await createNativePeerbitBackbone({
				clockId: publicKey,
				privateKey,
				publicKey,
			});
			const thresholdRestored = await createNativePeerbitBackbone({
				clockId: publicKey,
				privateKey,
				publicKey,
			});
			const buffered = new NativeBackboneNodeCoordinatePersistence(directory, {
				flushOnAppend: false,
				flushMaxPendingBytes:
					defaultNativeBackboneCoordinateFlushMaxPendingBytes,
				writeBufferMaxBytes:
					defaultNativeBackboneCoordinateFlushMaxPendingBytes,
			});

			await buffered.hydrate(source);
			source.putEntryCoordinates("hash-a", "gid-a", [1n], false, 1, 1n);
			expect(await buffered.flushJournalOnAppend(source)).equal(0);
			expect(await buffered.hydrate(beforeClose)).to.equal(0);
			await buffered.flushJournal(source);
			await buffered.close();

			const writeThrough = new NativeBackboneNodeCoordinatePersistence(
				directory,
			);
			expect(await writeThrough.hydrate(afterClose)).to.equal(1);
			expect(afterClose.getEntryCoordinateHashes()).to.deep.equal(["hash-a"]);

			const threshold = new NativeBackboneNodeCoordinatePersistence(directory, {
				flushOnAppend: false,
				flushMaxPendingBytes: 1,
				writeBufferMaxBytes: 1,
			});
			await threshold.hydrate(thresholdSource);
			thresholdSource.putEntryCoordinates(
				"hash-b",
				"gid-b",
				[2n],
				false,
				1,
				2n,
			);
			expect(
				await threshold.flushJournalOnAppend(thresholdSource),
			).to.be.greaterThan(0);
			await writeThrough.hydrate(thresholdRestored);
			expect(thresholdRestored.getEntryCoordinateHashes()).to.deep.equal([
				"hash-a",
				"hash-b",
			]);
			await writeThrough.close();
			await threshold.close();
		} finally {
			await rm(directory, { recursive: true, force: true });
		}
	});

	it("creates buffered node coordinate persistence with write-buffer defaults", async () => {
		const [{ mkdtemp, rm }, { tmpdir }, { join }] = await Promise.all([
			import("node:fs/promises"),
			import("node:os"),
			import("node:path"),
		]);
		const directory = await mkdtemp(
			join(tmpdir(), "peerbit-native-backbone-node-buffered-helper-"),
		);
		try {
			const source = await createNativePeerbitBackbone({
				clockId: publicKey,
				privateKey,
				publicKey,
			});
			const beforeFlush = await createNativePeerbitBackbone({
				clockId: publicKey,
				privateKey,
				publicKey,
			});
			const afterFlush = await createNativePeerbitBackbone({
				clockId: publicKey,
				privateKey,
				publicKey,
			});
			const persistence = createBufferedNativeBackboneNodeCoordinatePersistence(
				directory,
				{
					flushMaxPendingBytes: 1024,
				},
			);

			expect(persistence.flushOnAppend).equal(false);
			expect(persistence.flushMaxPendingBytes).equal(1024);
			expect(persistence.compactMaxJournalBytes).equal(undefined);
			await persistence.hydrate(source);
			source.putEntryCoordinates(
				"hash-node-buffered",
				"gid-node-buffered",
				[1n],
				false,
				1,
				1n,
			);
			expect(await persistence.flushJournalOnAppend?.(source)).equal(0);
			expect(await persistence.hydrate(beforeFlush)).equal(0);
			await persistence.flushJournal(source);
			await persistence.close?.();

			const writeThrough = new NativeBackboneNodeCoordinatePersistence(
				directory,
			);
			expect(await writeThrough.hydrate(afterFlush)).equal(1);
			expect(afterFlush.getEntryCoordinateHashes()).to.deep.equal([
				"hash-node-buffered",
			]);
			await writeThrough.close();
		} finally {
			await rm(directory, { recursive: true, force: true });
		}
	});

	it("rejects invalid Node checkpoint thresholds", () => {
		for (const options of [
			{ compactMaxJournalBytes: 0 },
			{ compactMaxJournalRecords: Number.NaN },
		]) {
			expect(
				() =>
					new NativeBackboneNodeCoordinatePersistence(
						"unused-node-checkpoint-directory",
						options,
					),
			).to.throw(/positive safe integer/);
		}
	});

	it("checkpoints and reopens through the buffered Node helper", async () => {
		const [{ mkdtemp, readdir, rm }, { tmpdir }, { join }] = await Promise.all([
			import("node:fs/promises"),
			import("node:os"),
			import("node:path"),
		]);
		const directory = await mkdtemp(
			join(tmpdir(), "peerbit-native-backbone-buffered-checkpoint-"),
		);
		try {
			const source = await createNativePeerbitBackbone({
				clockId: publicKey,
				privateKey,
				publicKey,
			});
			const restored = await createNativePeerbitBackbone({
				clockId: publicKey,
				privateKey,
				publicKey,
			});
			for (const backbone of [source, restored]) {
				configureCheckpointDocumentSchema(backbone);
			}

			const persistence = createBufferedNativeBackboneNodeCoordinatePersistence(
				directory,
				{
					compactMaxJournalRecords: 1,
				},
			);
			expect(persistence.crashSafeCompaction).equal(true);
			await persistence.hydrate(source);
			appendCheckpointFixture(source, "buffered-helper", 1);
			await persistence.flushJournal(source);
			await persistence.close?.();
			expect(await readdir(directory)).to.include(
				"coordinates.bin.checkpoint-state",
			);

			const reopening = new NativeBackboneNodeCoordinatePersistence(directory);
			await reopening.hydrate(restored);
			expectCheckpointFixtures(restored, ["buffered-helper"]);
			await reopening.close();
		} finally {
			await rm(directory, { recursive: true, force: true });
		}
	});

	it("migrates all legacy native WAL domains into an opt-in Node checkpoint", async () => {
		const [{ mkdtemp, readFile, readdir, rm }, { tmpdir }, { join }] =
			await Promise.all([
				import("node:fs/promises"),
				import("node:os"),
				import("node:path"),
			]);
		const directory = await mkdtemp(
			join(tmpdir(), "peerbit-native-backbone-node-coordinate-compact-"),
		);
		try {
			const legacySource = await createNativePeerbitBackbone({
				clockId: publicKey,
				privateKey,
				publicKey,
			});
			const checkpointSource = await createNativePeerbitBackbone({
				clockId: publicKey,
				privateKey,
				publicKey,
			});
			const restored = await createNativePeerbitBackbone({
				clockId: publicKey,
				privateKey,
				publicKey,
			});
			for (const backbone of [legacySource, checkpointSource, restored]) {
				configureCheckpointDocumentSchema(backbone);
			}

			const legacy = new NativeBackboneNodeCoordinatePersistence(directory);
			await legacy.hydrate(legacySource);
			appendCheckpointFixture(legacySource, "legacy", 1);
			await legacy.flushJournal(legacySource);
			await legacy.close();

			const checkpointing = new NativeBackboneNodeCoordinatePersistence(
				directory,
				{ compactMaxJournalRecords: 1 },
			);
			expect(checkpointing.crashSafeCompaction).equal(true);
			await checkpointing.hydrate(checkpointSource);
			appendCheckpointFixture(checkpointSource, "migration", 2);
			await checkpointing.flushJournal(checkpointSource);
			appendCheckpointFixture(checkpointSource, "rotation", 3);
			await checkpointing.flushJournal(checkpointSource);
			await checkpointing.close();

			const files = await readdir(directory);
			expect(files).to.include("coordinates.bin.checkpoint-state");
			const downgrade = await createNativePeerbitBackbone({
				clockId: publicKey,
				privateKey,
				publicKey,
			});
			const legacySentinel = await readFile(join(directory, "coordinates.wal"));
			expect(() =>
				downgrade.loadCoordinateSnapshotAndJournal(undefined, legacySentinel),
			).to.throw();
			// Two threshold crossings rotate A -> B. The completed highwater makes B
			// authoritative and permits cleanup of A as retryable retired state.
			expect(files).to.include("coordinates.bin.checkpoint-b");
			expect(files).to.not.include("coordinates.bin.checkpoint-a");
			for (const journal of [
				"coordinates.wal",
				"document-values.wal",
				"document-signers.wal",
			]) {
				expect(files, journal).to.include(`${journal}.checkpoint-b`);
				expect(files, journal).to.not.include(`${journal}.checkpoint-a`);
			}

			const reopening = new NativeBackboneNodeCoordinatePersistence(directory);
			await reopening.hydrate(restored);
			expectCheckpointFixtures(restored, ["legacy", "migration", "rotation"]);
			await reopening.close();
		} finally {
			await rm(directory, { recursive: true, force: true });
		}
	});

	it("recovers exact state across Node checkpoint authority cuts", async () => {
		const [fsPromises, { tmpdir }, { join }] = await Promise.all([
			import("node:fs/promises"),
			import("node:os"),
			import("node:path"),
		]);
		const { mkdtemp, readFile, rm } = fsPromises;
		const journalNames = [
			"coordinates.wal",
			"document-values.wal",
			"document-signers.wal",
		] as const;
		const readU64 = (bytes: Uint8Array, offset: number): bigint => {
			const view = new DataView(
				bytes.buffer,
				bytes.byteOffset,
				bytes.byteLength,
			);
			return (
				BigInt(view.getUint32(offset, true)) |
				(BigInt(view.getUint32(offset + 4, true)) << 32n)
			);
		};
		const cases = [
			{
				name: "before pending authority",
				id: "before-pending",
				failStateRename: 1,
				recoveredHighwater: 0n,
				promotesPending: false,
				completedGeneration: 1n,
			},
			{
				name: "after sentinel and before completed authority",
				id: "before-completed",
				failStateRename: 2,
				recoveredHighwater: 1n,
				promotesPending: true,
				completedGeneration: 2n,
			},
		] as const;

		for (const testCase of cases) {
			const directory = await mkdtemp(
				join(tmpdir(), `peerbit-native-backbone-checkpoint-${testCase.id}-`),
			);
			try {
				let stateRenames = 0;
				const injectedFailure = new Error(
					`injected checkpoint cut ${testCase.name}`,
				);
				const source = await createNativePeerbitBackbone({
					clockId: publicKey,
					privateKey,
					publicKey,
				});
				configureCheckpointDocumentSchema(source);
				const failing = new NativeBackboneNodeCoordinatePersistence(directory, {
					fs: {
						mkdir: async (path, options) => fsPromises.mkdir(path, options),
						readFile: async (path) => fsPromises.readFile(path),
						writeFile: async (path, data) => fsPromises.writeFile(path, data),
						appendFile: async (path, data) => fsPromises.appendFile(path, data),
						openBoundedRead: async (path) => fsPromises.open(path, "r"),
						open: async (path, flags) => fsPromises.open(path, flags),
						rename: async (from, to) => {
							if (
								to.endsWith("coordinates.bin.checkpoint-state") &&
								++stateRenames === testCase.failStateRename
							) {
								throw injectedFailure;
							}
							await fsPromises.rename(from, to);
						},
						rm: async (path, options) => fsPromises.rm(path, options),
					},
				});
				await failing.hydrate(source);
				appendCheckpointFixture(source, `${testCase.id}-base`, 1);
				await failing.flushJournal(source);
				appendCheckpointFixture(source, `${testCase.id}-cut`, 2);
				const failure = await failing.compact(source).then(
					() => undefined,
					(error: unknown) => error,
				);
				expect(failure, testCase.name).equal(injectedFailure);
				await failing.close();

				const authoritativeWal = new Map<string, Uint8Array>();
				for (const journal of journalNames) {
					authoritativeWal.set(
						journal,
						await readFile(join(directory, journal)),
					);
				}
				let cutState: Uint8Array | undefined;
				try {
					cutState = await readFile(
						join(directory, "coordinates.bin.checkpoint-state"),
					);
				} catch (error) {
					if ((error as { code?: string }).code !== "ENOENT") {
						throw error;
					}
				}
				if (testCase.recoveredHighwater === 0n) {
					expect(cutState, testCase.name).equal(undefined);
				} else {
					expect(cutState, testCase.name).to.exist;
					expect(readU64(cutState!, 16), `${testCase.name} highwater`).equal(
						testCase.recoveredHighwater,
					);
					expect(readU64(cutState!, 24), `${testCase.name} pending`).equal(
						testCase.recoveredHighwater,
					);
					expect(readU64(cutState!, 40), `${testCase.name} completed`).equal(
						0n,
					);
				}

				const recovered = await createNativePeerbitBackbone({
					clockId: publicKey,
					privateKey,
					publicKey,
				});
				configureCheckpointDocumentSchema(recovered);
				const recovery = new NativeBackboneNodeCoordinatePersistence(directory);
				await recovery.hydrate(recovered);
				expectCheckpointFixtures(recovered, [
					`${testCase.id}-base`,
					`${testCase.id}-cut`,
				]);
				expect(recovered.coordinateIndexLength, testCase.name).equal(4);
				expect(recovered.coordinateValueLength, testCase.name).equal(4);
				expect(recovered.documentIndexLength, testCase.name).equal(2);
				expect(recovered.documentValueLength, testCase.name).equal(2);
				if (!testCase.promotesPending) {
					for (const journal of journalNames) {
						expect(
							await readFile(join(directory, journal)),
							`${testCase.name} ${journal} authority`,
						).to.deep.equal(authoritativeWal.get(journal));
					}
				}
				const recoveredJournalNames = journalNames.map((journal) =>
					testCase.promotesPending ? `${journal}.checkpoint-a` : journal,
				);
				const recoveredWal = new Map<string, Uint8Array>();
				for (const journal of recoveredJournalNames) {
					recoveredWal.set(journal, await readFile(join(directory, journal)));
				}

				appendCheckpointFixture(recovered, `${testCase.id}-writable`, 3);
				await recovery.flushJournal(recovered);
				for (const journal of recoveredJournalNames) {
					const before = recoveredWal.get(journal)!;
					const after = await readFile(join(directory, journal));
					expect(
						after.subarray(0, before.byteLength),
						`${testCase.name} ${journal} prefix`,
					).to.deep.equal(before);
					expect(
						after.byteLength,
						`${testCase.name} ${journal} writable`,
					).to.be.greaterThan(before.byteLength);
				}

				await recovery.compact(recovered);
				await recovery.close();
				const completedState = await readFile(
					join(directory, "coordinates.bin.checkpoint-state"),
				);
				expect(readU64(completedState, 16), `${testCase.name} highwater`).equal(
					testCase.completedGeneration,
				);
				expect(readU64(completedState, 24), `${testCase.name} pending`).equal(
					0n,
				);
				expect(readU64(completedState, 40), `${testCase.name} completed`).equal(
					testCase.completedGeneration,
				);

				const verified = await createNativePeerbitBackbone({
					clockId: publicKey,
					privateKey,
					publicKey,
				});
				configureCheckpointDocumentSchema(verified);
				const verifier = new NativeBackboneNodeCoordinatePersistence(directory);
				await verifier.hydrate(verified);
				expectCheckpointFixtures(verified, [
					`${testCase.id}-base`,
					`${testCase.id}-cut`,
					`${testCase.id}-writable`,
				]);
				expect(verified.coordinateIndexLength, testCase.name).equal(6);
				expect(verified.coordinateValueLength, testCase.name).equal(6);
				expect(verified.documentIndexLength, testCase.name).equal(3);
				expect(verified.documentValueLength, testCase.name).equal(3);
				await verifier.close();
			} finally {
				await rm(directory, { recursive: true, force: true });
			}
		}
	});

	it("recovers across every checkpoint publication mutation cut", async () => {
		type FaultOperation =
			| "atomicReplace"
			| "remove"
			| "write"
			| "durableBarrier";
		type FaultCase = {
			id: string;
			operation: FaultOperation;
			target: string;
			timing: "before" | "after";
			occurrence?: number;
			rotation?: boolean;
			cleanupFailure?: boolean;
			legacyReadable: boolean;
			expectedCompletedGeneration: bigint;
		};
		const cases: readonly FaultCase[] = [
			{
				id: "pending-before-effect",
				operation: "atomicReplace",
				target: "coordinates.bin.checkpoint-state",
				timing: "before",
				legacyReadable: true,
				expectedCompletedGeneration: 1n,
			},
			{
				id: "pending-after-effect",
				operation: "atomicReplace",
				target: "coordinates.bin.checkpoint-state",
				timing: "after",
				legacyReadable: true,
				expectedCompletedGeneration: 2n,
			},
			{
				id: "wal-reset-after-effect",
				operation: "remove",
				target: "coordinates.wal.checkpoint-a",
				timing: "after",
				legacyReadable: true,
				expectedCompletedGeneration: 2n,
			},
			{
				id: "wal-header-after-effect",
				operation: "write",
				target: "coordinates.wal.checkpoint-a",
				timing: "after",
				legacyReadable: true,
				expectedCompletedGeneration: 2n,
			},
			{
				id: "wal-barrier-after-effect",
				operation: "durableBarrier",
				target: "coordinates.wal.checkpoint-a",
				timing: "after",
				legacyReadable: true,
				expectedCompletedGeneration: 2n,
			},
			{
				id: "checkpoint-before-effect",
				operation: "atomicReplace",
				target: "coordinates.bin.checkpoint-a",
				timing: "before",
				legacyReadable: true,
				expectedCompletedGeneration: 2n,
			},
			{
				id: "checkpoint-after-effect",
				operation: "atomicReplace",
				target: "coordinates.bin.checkpoint-a",
				timing: "after",
				legacyReadable: true,
				expectedCompletedGeneration: 2n,
			},
			{
				id: "completed-before-effect",
				operation: "atomicReplace",
				target: "coordinates.bin.checkpoint-state",
				timing: "before",
				occurrence: 2,
				legacyReadable: false,
				expectedCompletedGeneration: 2n,
			},
			{
				id: "completed-after-effect",
				operation: "atomicReplace",
				target: "coordinates.bin.checkpoint-state",
				timing: "after",
				occurrence: 2,
				legacyReadable: false,
				expectedCompletedGeneration: 2n,
			},
			{
				id: "sentinel-before-effect",
				operation: "atomicReplace",
				target: "coordinates.wal",
				timing: "before",
				legacyReadable: true,
				expectedCompletedGeneration: 2n,
			},
			{
				id: "sentinel-after-effect",
				operation: "atomicReplace",
				target: "coordinates.wal",
				timing: "after",
				legacyReadable: false,
				expectedCompletedGeneration: 2n,
			},
			{
				id: "rotation-pending-after-effect",
				operation: "atomicReplace",
				target: "coordinates.bin.checkpoint-state",
				timing: "after",
				rotation: true,
				legacyReadable: false,
				expectedCompletedGeneration: 4n,
			},
			{
				id: "rotation-checkpoint-after-effect",
				operation: "atomicReplace",
				target: "coordinates.bin.checkpoint-b",
				timing: "after",
				rotation: true,
				legacyReadable: false,
				expectedCompletedGeneration: 4n,
			},
			{
				id: "rotation-completed-after-effect",
				operation: "atomicReplace",
				target: "coordinates.bin.checkpoint-state",
				timing: "after",
				occurrence: 2,
				rotation: true,
				legacyReadable: false,
				expectedCompletedGeneration: 3n,
			},
			{
				id: "rotation-cleanup-before-effect",
				operation: "remove",
				target: "coordinates.bin.checkpoint-a",
				timing: "before",
				rotation: true,
				cleanupFailure: true,
				legacyReadable: false,
				expectedCompletedGeneration: 2n,
			},
		];

		for (const testCase of cases) {
			const memory = new NativeBackboneMemoryCoordinatePersistenceStore();
			const injectedFailure = new Error(
				`injected ${testCase.id} checkpoint failure`,
			);
			let armed = false;
			let matchingCalls = 0;
			let faultConsumed = false;
			const invoke = async <T>(
				operation: FaultOperation,
				target: string,
				effect: () => Promise<T>,
			): Promise<T> => {
				if (
					!armed ||
					faultConsumed ||
					operation !== testCase.operation ||
					target !== testCase.target
				) {
					return effect();
				}
				matchingCalls++;
				if (matchingCalls !== (testCase.occurrence ?? 1)) {
					return effect();
				}
				faultConsumed = true;
				if (testCase.timing === "before") {
					throw injectedFailure;
				}
				await effect();
				throw injectedFailure;
			};
			const store: NativeBackboneCoordinatePersistenceStore = {
				supportsRemoval: true,
				read: (name) => memory.read(name),
				readLimited: (name, maxBytes) => memory.readLimited(name, maxBytes),
				write: (name, bytes) =>
					invoke("write", name, () => memory.write(name, bytes)),
				atomicReplace: (name, bytes) =>
					invoke("atomicReplace", name, () => memory.write(name, bytes)),
				append: (name, bytes) => memory.append(name, bytes),
				remove: (name) => invoke("remove", name, () => memory.remove(name)),
				durableBarrier: (name) =>
					invoke("durableBarrier", name ?? "", () => memory.flush(name)),
				flush: (name) => memory.flush(name),
			};
			const source = await createNativePeerbitBackbone({
				clockId: publicKey,
				privateKey,
				publicKey,
			});
			configureCheckpointDocumentSchema(source);
			const persistence = new NativeBackboneCoordinatePersistence(store);
			await persistence.hydrate(source);
			appendCheckpointFixture(source, `${testCase.id}-base`, 1);
			await persistence.flushJournal(source);
			if (testCase.rotation) {
				await persistence.compact(source);
			}
			appendCheckpointFixture(source, `${testCase.id}-cut`, 2);
			armed = true;
			const failure = await persistence.compact(source).then(
				() => undefined,
				(error: unknown) => error,
			);
			armed = false;
			expect(faultConsumed, testCase.id).equal(true);
			if (testCase.cleanupFailure) {
				expect(failure, testCase.id).equal(undefined);
				expect(
					memory.files.has("coordinates.bin.checkpoint-a"),
					`${testCase.id} retained cleanup debt`,
				).equal(true);
			} else {
				expect(failure, testCase.id).equal(injectedFailure);
			}

			// Model a pre-checkpoint package by reading only the six legacy files and
			// ignoring checkpoint authority. Every stable publication cut must expose
			// either the complete authoritative legacy generation or the semantic
			// rejection sentinel; it must never expose a stale-but-readable downgrade.
			const legacyReader = await createNativePeerbitBackbone({
				clockId: publicKey,
				privateKey,
				publicKey,
			});
			configureCheckpointDocumentSchema(legacyReader);
			let legacyFailure: unknown;
			try {
				legacyReader.loadCoordinateSnapshotAndJournal(
					await memory.read("coordinates.bin"),
					await memory.read("coordinates.wal"),
				);
				legacyReader.loadDocumentSnapshotAndJournal(
					await memory.read("document-values.bin"),
					await memory.read("document-values.wal"),
				);
				legacyReader.loadDocumentSignerSnapshotAndJournal(
					await memory.read("document-signers.bin"),
					await memory.read("document-signers.wal"),
				);
			} catch (error) {
				legacyFailure = error;
			}
			if (testCase.legacyReadable) {
				expect(legacyFailure, `${testCase.id} legacy authority`).equal(
					undefined,
				);
				expectCheckpointFixtures(legacyReader, [
					`${testCase.id}-base`,
					`${testCase.id}-cut`,
				]);
			} else {
				expect(legacyFailure, `${testCase.id} downgrade rejection`).to.exist;
			}

			// Simulate a process crash by abandoning the failed adapter without close.
			// The fresh adapter must choose exactly one authority and remain writable.
			const recovered = await createNativePeerbitBackbone({
				clockId: publicKey,
				privateKey,
				publicKey,
			});
			configureCheckpointDocumentSchema(recovered);
			const recovery = new NativeBackboneCoordinatePersistence(store);
			await recovery.hydrate(recovered);
			expectCheckpointFixtures(recovered, [
				`${testCase.id}-base`,
				`${testCase.id}-cut`,
			]);
			if (testCase.cleanupFailure) {
				expect(
					memory.files.has("coordinates.bin.checkpoint-a"),
					`${testCase.id} retried cleanup debt`,
				).equal(false);
			}
			appendCheckpointFixture(recovered, `${testCase.id}-after`, 3);
			await recovery.flushJournal(recovered);
			if (!testCase.cleanupFailure) {
				await recovery.compact(recovered);
			}
			await recovery.close();

			const state = await memory.read("coordinates.bin.checkpoint-state");
			expect(state, `${testCase.id} completed authority`).to.exist;
			const stateView = new DataView(
				state!.buffer,
				state!.byteOffset,
				state!.byteLength,
			);
			const readU64 = (offset: number) =>
				BigInt(stateView.getUint32(offset, true)) |
				(BigInt(stateView.getUint32(offset + 4, true)) << 32n);
			expect(readU64(16), `${testCase.id} highwater`).equal(
				testCase.expectedCompletedGeneration,
			);
			expect(readU64(24), `${testCase.id} pending`).equal(0n);
			expect(readU64(40), `${testCase.id} completed`).equal(
				testCase.expectedCompletedGeneration,
			);

			const verified = await createNativePeerbitBackbone({
				clockId: publicKey,
				privateKey,
				publicKey,
			});
			configureCheckpointDocumentSchema(verified);
			const verifier = new NativeBackboneCoordinatePersistence(store);
			await verifier.hydrate(verified);
			expectCheckpointFixtures(verified, [
				`${testCase.id}-base`,
				`${testCase.id}-cut`,
				`${testCase.id}-after`,
			]);
			await verifier.close();
		}
	});

	it("never overwrites valid legacy writes after completed checkpoint authority", async () => {
		const [fsPromises, { tmpdir }, { join }] = await Promise.all([
			import("node:fs/promises"),
			import("node:os"),
			import("node:path"),
		]);
		const { appendFile, mkdtemp, readFile, rm, writeFile } = fsPromises;
		const directory = await mkdtemp(
			join(tmpdir(), "peerbit-native-backbone-checkpoint-downgrade-write-"),
		);
		const journalNames = [
			"coordinates.wal",
			"document-values.wal",
			"document-signers.wal",
		] as const;
		try {
			const source = await createNativePeerbitBackbone({
				clockId: publicKey,
				privateKey,
				publicKey,
			});
			configureCheckpointDocumentSchema(source);
			const persistence = new NativeBackboneNodeCoordinatePersistence(
				directory,
			);
			await persistence.hydrate(source);
			appendCheckpointFixture(source, "pre-downgrade", 1);
			await persistence.flushJournal(source);
			const legacyGeneration = new Map<string, Uint8Array>();
			for (const journal of journalNames) {
				legacyGeneration.set(journal, await readFile(join(directory, journal)));
			}
			await persistence.compact(source);
			await persistence.close();

			// A pre-checkpoint process ignores completed authority, restores the last
			// readable legacy generation, and appends a fully valid new suffix.
			const downgraded = await createNativePeerbitBackbone({
				clockId: publicKey,
				privateKey,
				publicKey,
			});
			configureCheckpointDocumentSchema(downgraded);
			downgraded.loadCoordinateSnapshotAndJournal(
				undefined,
				legacyGeneration.get("coordinates.wal"),
			);
			downgraded.loadDocumentSnapshotAndJournal(
				undefined,
				legacyGeneration.get("document-values.wal"),
			);
			downgraded.loadDocumentSignerSnapshotAndJournal(
				undefined,
				legacyGeneration.get("document-signers.wal"),
			);
			downgraded.setCoordinateJournalEnabled(true);
			downgraded.setDocumentJournalEnabled(true);
			downgraded.setDocumentSignerJournalEnabled(true);
			appendCheckpointFixture(downgraded, "post-downgrade", 2);
			const downgradeSuffixes = new Map<string, Uint8Array>([
				["coordinates.wal", downgraded.coordinateJournal()],
				["document-values.wal", downgraded.documentJournal()],
				["document-signers.wal", downgraded.documentSignerJournal()],
			]);
			for (const journal of journalNames) {
				await writeFile(
					join(directory, journal),
					legacyGeneration.get(journal)!,
				);
				await appendFile(
					join(directory, journal),
					downgradeSuffixes.get(journal)!,
				);
			}
			const downgradeCoordinateWal = await readFile(
				join(directory, "coordinates.wal"),
			);

			const current = await createNativePeerbitBackbone({
				clockId: publicKey,
				privateKey,
				publicKey,
			});
			configureCheckpointDocumentSchema(current);
			const reopening = new NativeBackboneNodeCoordinatePersistence(directory);
			const failure = await reopening.hydrate(current).then(
				() => undefined,
				(error: unknown) => error,
			);
			expect(String(failure)).to.contain(
				"downgrade sentinel is missing or has been replaced",
			);
			expect(await readFile(join(directory, "coordinates.wal"))).to.deep.equal(
				downgradeCoordinateWal,
			);

			const legacyVerifier = await createNativePeerbitBackbone({
				clockId: publicKey,
				privateKey,
				publicKey,
			});
			configureCheckpointDocumentSchema(legacyVerifier);
			legacyVerifier.loadCoordinateSnapshotAndJournal(
				undefined,
				await readFile(join(directory, "coordinates.wal")),
			);
			legacyVerifier.loadDocumentSnapshotAndJournal(
				undefined,
				await readFile(join(directory, "document-values.wal")),
			);
			legacyVerifier.loadDocumentSignerSnapshotAndJournal(
				undefined,
				await readFile(join(directory, "document-signers.wal")),
			);
			expectCheckpointFixtures(legacyVerifier, [
				"pre-downgrade",
				"post-downgrade",
			]);
			await reopening.close();
		} finally {
			await rm(directory, { recursive: true, force: true });
		}
	});

	it("queues a concurrent WAL suffix behind Node checkpoint publication", async () => {
		const [fsPromises, { tmpdir }, { join }] = await Promise.all([
			import("node:fs/promises"),
			import("node:os"),
			import("node:path"),
		]);
		const { mkdtemp, readFile, rm } = fsPromises;
		const directory = await mkdtemp(
			join(tmpdir(), "peerbit-native-backbone-node-checkpoint-suffix-"),
		);
		let releasePublication!: () => void;
		const publicationReleased = new Promise<void>((resolve) => {
			releasePublication = resolve;
		});
		let reportPublication!: () => void;
		const publicationEntered = new Promise<void>((resolve) => {
			reportPublication = resolve;
		});
		let gatePublication = false;
		let checkpointStateRenames = 0;
		try {
			const source = await createNativePeerbitBackbone({
				clockId: publicKey,
				privateKey,
				publicKey,
			});
			const restored = await createNativePeerbitBackbone({
				clockId: publicKey,
				privateKey,
				publicKey,
			});
			for (const backbone of [source, restored]) {
				configureCheckpointDocumentSchema(backbone);
			}
			const persistence = new NativeBackboneNodeCoordinatePersistence(
				directory,
				{
					fs: {
						mkdir: async (path, options) => fsPromises.mkdir(path, options),
						readFile: async (path) => fsPromises.readFile(path),
						writeFile: async (path, data) => fsPromises.writeFile(path, data),
						appendFile: async (path, data) => fsPromises.appendFile(path, data),
						openBoundedRead: async (path) => fsPromises.open(path, "r"),
						open: async (path, flags) => fsPromises.open(path, flags),
						rename: async (from, to) => {
							if (
								gatePublication &&
								to.endsWith("coordinates.bin.checkpoint-state")
							) {
								checkpointStateRenames++;
								if (checkpointStateRenames === 2) {
									reportPublication();
									await publicationReleased;
								}
							}
							await fsPromises.rename(from, to);
						},
						rm: async (path, options) => fsPromises.rm(path, options),
					},
				},
			);
			await persistence.hydrate(source);
			appendCheckpointFixture(source, "base", 1);
			await persistence.flushJournal(source);

			gatePublication = true;
			const compacting = persistence.compact(source);
			await publicationEntered;
			appendCheckpointFixture(source, "suffix", 2);
			const flushingSuffix = persistence.flushJournal(source);
			releasePublication();
			await compacting;
			await flushingSuffix;
			await persistence.close();

			const slotSuffix = async (journal: string, headerBytes: number) => {
				for (const slot of ["a", "b"]) {
					try {
						const bytes = await readFile(
							join(directory, `${journal}.checkpoint-${slot}`),
						);
						if (bytes.byteLength > headerBytes) {
							return true;
						}
					} catch (error) {
						if ((error as { code?: string }).code !== "ENOENT") {
							throw error;
						}
					}
				}
				return false;
			};
			expect(
				await slotSuffix(
					"coordinates.wal",
					source.coordinateJournalHeader().byteLength,
				),
				"coordinate suffix",
			).equal(true);
			expect(
				await slotSuffix(
					"document-values.wal",
					source.documentJournalHeader().byteLength,
				),
				"document suffix",
			).equal(true);
			expect(
				await slotSuffix(
					"document-signers.wal",
					source.documentSignerJournalHeader().byteLength,
				),
				"signer suffix",
			).equal(true);

			const reopening = new NativeBackboneNodeCoordinatePersistence(directory);
			await reopening.hydrate(restored);
			expectCheckpointFixtures(restored, ["base", "suffix"]);
			await reopening.close();
		} finally {
			releasePublication();
			await rm(directory, { recursive: true, force: true });
		}
	});

	it("keeps a queued WAL suffix pending after checkpoint publication fails", async () => {
		const [fsPromises, { tmpdir }, { join }] = await Promise.all([
			import("node:fs/promises"),
			import("node:os"),
			import("node:path"),
		]);
		const { mkdtemp, readFile, rm } = fsPromises;
		const directory = await mkdtemp(
			join(tmpdir(), "peerbit-native-backbone-checkpoint-queued-failure-"),
		);
		let releasePublication!: () => void;
		const publicationReleased = new Promise<void>((resolve) => {
			releasePublication = resolve;
		});
		let reportPublication!: () => void;
		const publicationEntered = new Promise<void>((resolve) => {
			reportPublication = resolve;
		});
		const injectedFailure = new Error(
			"injected checkpoint publication failure",
		);
		let checkpointStateRenames = 0;
		let failPublication = false;
		try {
			const source = await createNativePeerbitBackbone({
				clockId: publicKey,
				privateKey,
				publicKey,
			});
			configureCheckpointDocumentSchema(source);
			const persistence = new NativeBackboneNodeCoordinatePersistence(
				directory,
				{
					fs: {
						mkdir: async (path, options) => fsPromises.mkdir(path, options),
						readFile: async (path) => fsPromises.readFile(path),
						writeFile: async (path, data) => fsPromises.writeFile(path, data),
						appendFile: async (path, data) => fsPromises.appendFile(path, data),
						openBoundedRead: async (path) => fsPromises.open(path, "r"),
						open: async (path, flags) => fsPromises.open(path, flags),
						rename: async (from, to) => {
							if (
								failPublication &&
								to.endsWith("coordinates.bin.checkpoint-state") &&
								++checkpointStateRenames === 2
							) {
								reportPublication();
								await publicationReleased;
								throw injectedFailure;
							}
							await fsPromises.rename(from, to);
						},
						rm: async (path, options) => fsPromises.rm(path, options),
					},
				},
			);
			await persistence.hydrate(source);
			appendCheckpointFixture(source, "queued-failure-base", 1);
			await persistence.flushJournal(source);
			const legacyWal = new Map<string, Uint8Array>();
			for (const journal of [
				"coordinates.wal",
				"document-values.wal",
				"document-signers.wal",
			]) {
				legacyWal.set(journal, await readFile(join(directory, journal)));
			}

			failPublication = true;
			const checkpointing = persistence.compact(source);
			await publicationEntered;
			appendCheckpointFixture(source, "queued-failure-suffix", 2);
			const pendingBeforeRelease = {
				coordinateRecords: source.coordinatePendingJournalLength,
				coordinateBytes: source.coordinatePendingJournalByteLength,
				documentRecords: source.documentPendingJournalLength,
				documentBytes: source.documentPendingJournalByteLength,
				signerRecords: source.documentSignerPendingJournalLength,
				signerBytes: source.documentSignerPendingJournalByteLength,
			};
			expect(
				pendingBeforeRelease.coordinateRecords +
					pendingBeforeRelease.documentRecords +
					pendingBeforeRelease.signerRecords,
			).to.be.greaterThan(0);
			const queuedFlush = persistence.flushJournal(source);
			releasePublication();
			const [checkpointFailure, queuedFailure] = await Promise.all([
				checkpointing.then(
					() => undefined,
					(error: unknown) => error,
				),
				queuedFlush.then(
					() => undefined,
					(error: unknown) => error,
				),
			]);
			expect(checkpointFailure).equal(injectedFailure);
			expect(queuedFailure).equal(injectedFailure);
			expect({
				coordinateRecords: source.coordinatePendingJournalLength,
				coordinateBytes: source.coordinatePendingJournalByteLength,
				documentRecords: source.documentPendingJournalLength,
				documentBytes: source.documentPendingJournalByteLength,
				signerRecords: source.documentSignerPendingJournalLength,
				signerBytes: source.documentSignerPendingJournalByteLength,
			}).to.deep.equal(pendingBeforeRelease);
			const legacyCoordinateSentinel = await readFile(
				join(directory, "coordinates.wal"),
			);
			expect(legacyCoordinateSentinel).to.not.deep.equal(
				legacyWal.get("coordinates.wal"),
			);
			const downgrade = await createNativePeerbitBackbone({
				clockId: publicKey,
				privateKey,
				publicKey,
			});
			expect(() =>
				downgrade.loadCoordinateSnapshotAndJournal(
					undefined,
					legacyCoordinateSentinel,
				),
			).to.throw();
			for (const journal of ["document-values.wal", "document-signers.wal"]) {
				expect(await readFile(join(directory, journal)), journal).to.deep.equal(
					legacyWal.get(journal),
				);
			}
			await persistence.close();

			const restored = await createNativePeerbitBackbone({
				clockId: publicKey,
				privateKey,
				publicKey,
			});
			configureCheckpointDocumentSchema(restored);
			const reopening = new NativeBackboneNodeCoordinatePersistence(directory);
			await reopening.hydrate(restored);
			expectCheckpointFixtures(restored, ["queued-failure-base"]);
			expect(
				restored.hasCoordinateIndexHash(
					"hash-checkpoint-queued-failure-suffix",
				),
			).equal(false);
			expect(
				Array.from(
					restored.documentKeysExist(["doc-checkpoint-queued-failure-suffix"]),
				),
			).to.deep.equal([0]);
			await reopening.close();
		} finally {
			releasePublication();
			await rm(directory, { recursive: true, force: true });
		}
	});

	it("fails closed when completed Node checkpoint authority is corrupt", async () => {
		const [fsPromises, { tmpdir }, { join }] = await Promise.all([
			import("node:fs/promises"),
			import("node:os"),
			import("node:path"),
		]);
		const { mkdtemp, readFile, readdir, rm, writeFile } = fsPromises;
		const directory = await mkdtemp(
			join(tmpdir(), "peerbit-native-backbone-node-checkpoint-corrupt-"),
		);
		try {
			const source = await createNativePeerbitBackbone({
				clockId: publicKey,
				privateKey,
				publicKey,
			});
			const restored = await createNativePeerbitBackbone({
				clockId: publicKey,
				privateKey,
				publicKey,
			});
			for (const backbone of [source, restored]) {
				configureCheckpointDocumentSchema(backbone);
			}
			const persistence = new NativeBackboneNodeCoordinatePersistence(
				directory,
			);
			await persistence.hydrate(source);
			appendCheckpointFixture(source, "authority", 1);
			await persistence.flushJournal(source);
			const legacyJournals = new Map<string, Uint8Array>();
			for (const journal of [
				"coordinates.wal",
				"document-values.wal",
				"document-signers.wal",
			]) {
				legacyJournals.set(journal, await readFile(join(directory, journal)));
			}
			await persistence.compact(source);
			await persistence.close();

			const checkpoint = (await readdir(directory)).find((name) =>
				/^coordinates\.bin\.checkpoint-[ab]$/.test(name),
			);
			expect(checkpoint).to.exist;
			const checkpointPath = join(directory, checkpoint!);
			const corrupt = await readFile(checkpointPath);
			expect(corrupt.byteLength).to.be.greaterThan(0);
			const corruptOffset = corrupt.byteLength - 1;
			corrupt[corruptOffset] = corrupt[corruptOffset]! ^ 0xff;
			await writeFile(checkpointPath, corrupt);
			// Leave a complete, valid legacy generation behind. Once checkpoint-state
			// says publication completed, recovery must not silently select it.
			for (const [journal, bytes] of legacyJournals) {
				await writeFile(join(directory, journal), bytes);
			}

			const reopening = new NativeBackboneNodeCoordinatePersistence(directory);
			const failure = await reopening.hydrate(restored).then(
				() => undefined,
				(error: unknown) => error,
			);
			expect(failure).to.exist;
			expect(String(failure)).to.match(
				/checkpoint.*(?:corrupt|checksum)|checksum.*checkpoint/i,
			);
			await reopening.close();
		} finally {
			await rm(directory, { recursive: true, force: true });
		}
	});

	it("fails closed when a completed Node checkpoint WAL is empty", async () => {
		const [fsPromises, { tmpdir }, { join }] = await Promise.all([
			import("node:fs/promises"),
			import("node:os"),
			import("node:path"),
		]);
		const { mkdtemp, readdir, rm, writeFile } = fsPromises;
		const directory = await mkdtemp(
			join(tmpdir(), "peerbit-native-backbone-checkpoint-empty-wal-"),
		);
		try {
			const source = await createNativePeerbitBackbone({
				clockId: publicKey,
				privateKey,
				publicKey,
			});
			configureCheckpointDocumentSchema(source);
			const persistence = new NativeBackboneNodeCoordinatePersistence(
				directory,
			);
			await persistence.hydrate(source);
			appendCheckpointFixture(source, "empty-active-wal", 1);
			await persistence.flushJournal(source);
			await persistence.compact(source);
			await persistence.close();

			const checkpoint = (await readdir(directory)).find((name) =>
				/^coordinates\.bin\.checkpoint-[ab]$/.test(name),
			);
			expect(checkpoint).to.exist;
			const slot = checkpoint!.endsWith("-a") ? "a" : "b";
			await writeFile(
				join(directory, `document-values.wal.checkpoint-${slot}`),
				new Uint8Array(0),
			);

			const restored = await createNativePeerbitBackbone({
				clockId: publicKey,
				privateKey,
				publicKey,
			});
			configureCheckpointDocumentSchema(restored);
			const reopening = new NativeBackboneNodeCoordinatePersistence(directory);
			const failure = await reopening.hydrate(restored).then(
				() => undefined,
				(error: unknown) => error,
			);
			expect(failure).to.exist;
			expect(String(failure)).to.match(
				/completed checkpoint WAL generation.*(?:incomplete|missing|invalid header)/i,
			);
			await reopening.close();
		} finally {
			await rm(directory, { recursive: true, force: true });
		}
	});

	it("keeps WAL records pending and requires a new adapter after append failure", async () => {
		const [fsPromises, { tmpdir }, { join }] = await Promise.all([
			import("node:fs/promises"),
			import("node:os"),
			import("node:path"),
		]);
		const { mkdtemp, rm } = fsPromises;
		const directory = await mkdtemp(
			join(tmpdir(), "peerbit-native-backbone-node-enospc-"),
		);
		try {
			const source = await createNativePeerbitBackbone({
				clockId: publicKey,
				privateKey,
				publicKey,
			});
			const restored = await createNativePeerbitBackbone({
				clockId: publicKey,
				privateKey,
				publicKey,
			});
			let failNextAppend = false;
			// No `open` so appends go through `appendFile`, the injection point.
			const persistence = new NativeBackboneNodeCoordinatePersistence(
				directory,
				{
					fs: {
						mkdir: (path, options) => fsPromises.mkdir(path, options),
						readFile: (path) => fsPromises.readFile(path),
						writeFile: (path, data) => fsPromises.writeFile(path, data),
						appendFile: async (path, data) => {
							if (failNextAppend) {
								failNextAppend = false;
								throw Object.assign(
									new Error("ENOSPC: no space left on device, write"),
									{ code: "ENOSPC" },
								);
							}
							return fsPromises.appendFile(path, data);
						},
						rm: (path, options) => fsPromises.rm(path, options),
					},
				},
			);

			await persistence.hydrate(source);
			source.putEntryCoordinates("hash-a", "gid-a", [1n], false, 1, 1n);
			source.putEntryCoordinates("hash-b", "gid-b", [2n], false, 1, 2n);
			expect(source.coordinatePendingJournalLength).to.equal(2);

			failNextAppend = true;
			let thrown: unknown;
			try {
				await persistence.flushJournal(source);
				expect.fail("flushJournal should reject when the append fails");
			} catch (error) {
				thrown = error;
			}
			expect((thrown as { code?: string })?.code).to.equal("ENOSPC");
			// The journal prefix is only cleared after a successful write, so the
			// unflushed records must still be pending in the wasm journal.
			expect(source.coordinatePendingJournalLength).to.equal(2);

			let repeatedError: unknown;
			try {
				await persistence.flushJournal(source);
			} catch (error) {
				repeatedError = error;
			}
			expect(repeatedError).equal(thrown);
			expect(source.coordinatePendingJournalLength).to.equal(2);
			await persistence.close();

			const retryPersistence = new NativeBackboneNodeCoordinatePersistence(
				directory,
			);
			expect(await retryPersistence.flushJournal(source)).to.be.greaterThan(0);
			expect(source.coordinatePendingJournalLength).to.equal(0);
			await retryPersistence.close();

			const restoredPersistence = new NativeBackboneNodeCoordinatePersistence(
				directory,
			);
			expect(await restoredPersistence.hydrate(restored)).to.equal(2);
			expect(restored.getEntryCoordinateHashes()).to.deep.equal([
				"hash-a",
				"hash-b",
			]);
			await restoredPersistence.close();
		} finally {
			await rm(directory, { recursive: true, force: true });
		}
	});

	it("propagates node mkdir failures and recovers on a later flush", async () => {
		const [fsPromises, { tmpdir }, { join }] = await Promise.all([
			import("node:fs/promises"),
			import("node:os"),
			import("node:path"),
		]);
		const { mkdtemp, rm } = fsPromises;
		const base = await mkdtemp(
			join(tmpdir(), "peerbit-native-backbone-node-mkdir-"),
		);
		// The target directory does not exist yet, so a later append can only
		// succeed if the failed mkdir was not memoized as ensured.
		const directory = join(base, "nested");
		try {
			const source = await createNativePeerbitBackbone({
				clockId: publicKey,
				privateKey,
				publicKey,
			});
			const restored = await createNativePeerbitBackbone({
				clockId: publicKey,
				privateKey,
				publicKey,
			});
			let failNextMkdir = false;
			const persistence = new NativeBackboneNodeCoordinatePersistence(
				directory,
				{
					fs: {
						mkdir: async (path, options) => {
							if (failNextMkdir) {
								failNextMkdir = false;
								throw Object.assign(
									new Error("EACCES: permission denied, mkdir"),
									{ code: "EACCES" },
								);
							}
							return fsPromises.mkdir(path, options);
						},
						readFile: (path) => fsPromises.readFile(path),
						writeFile: (path, data) => fsPromises.writeFile(path, data),
						appendFile: (path, data) => fsPromises.appendFile(path, data),
						rm: (path, options) => fsPromises.rm(path, options),
					},
				},
			);

			await persistence.hydrate(source);
			source.putEntryCoordinates("hash-mkdir", "gid-mkdir", [1n], false, 1, 1n);

			failNextMkdir = true;
			let thrown: unknown;
			try {
				await persistence.flushJournal(source);
				expect.fail("flushJournal should reject when mkdir fails");
			} catch (error) {
				thrown = error;
			}
			expect((thrown as { code?: string })?.code).to.equal("EACCES");
			expect(source.coordinatePendingJournalLength).to.equal(1);

			expect(await persistence.flushJournal(source)).to.be.greaterThan(0);
			expect(source.coordinatePendingJournalLength).to.equal(0);
			await persistence.close();

			const restoredPersistence = new NativeBackboneNodeCoordinatePersistence(
				directory,
			);
			expect(await restoredPersistence.hydrate(restored)).to.equal(1);
			expect(restored.getEntryCoordinateHashes()).to.deep.equal(["hash-mkdir"]);
			await restoredPersistence.close();
		} finally {
			await rm(base, { recursive: true, force: true });
		}
	});

	it("can batch node coordinate WAL appends before flushing", async () => {
		const [{ mkdtemp, rm }, { tmpdir }, { join }] = await Promise.all([
			import("node:fs/promises"),
			import("node:os"),
			import("node:path"),
		]);
		const directory = await mkdtemp(
			join(tmpdir(), "peerbit-native-backbone-buffered-coordinates-"),
		);
		try {
			const source = await createNativePeerbitBackbone({
				clockId: publicKey,
				privateKey,
				publicKey,
			});
			const beforeFlush = await createNativePeerbitBackbone({
				clockId: publicKey,
				privateKey,
				publicKey,
			});
			const afterFlush = await createNativePeerbitBackbone({
				clockId: publicKey,
				privateKey,
				publicKey,
			});
			const nodeStore = new NativeBackboneNodeCoordinatePersistenceStore(
				directory,
			);
			const buffered = new NativeBackboneBufferedCoordinatePersistenceStore(
				nodeStore,
			);
			const persistence = new NativeBackboneCoordinatePersistence(buffered, {
				flushOnAppend: false,
				flushMaxPendingBytes: 1024,
			});

			await persistence.hydrate(source);
			source.putEntryCoordinates("hash-a", "gid-a", [1n], false, 1, 1n);
			expect(await persistence.flushJournalOnAppend(source)).equal(0);

			const writeThroughPersistence = new NativeBackboneCoordinatePersistence(
				new NativeBackboneNodeCoordinatePersistenceStore(directory),
			);
			expect(await writeThroughPersistence.hydrate(beforeFlush)).to.equal(0);
			expect(await persistence.flushJournal(source)).to.be.greaterThan(0);
			expect(await writeThroughPersistence.hydrate(afterFlush)).to.equal(1);
			expect(afterFlush.getEntryCoordinateHashes()).to.deep.equal(["hash-a"]);
			await persistence.close();
		} finally {
			await rm(directory, { recursive: true, force: true });
		}
	});

	it("appends coordinate WAL bytes through OPFS sync access handles", async () => {
		const directory = new FakeOPFSDirectoryHandle(true);
		const store = new NativeBackboneOPFSCoordinatePersistenceStore(directory);

		expect(await store.read("coordinates.wal")).to.equal(undefined);
		await store.append("coordinates.wal", new Uint8Array([1, 2]));
		await store.append("coordinates.wal", new Uint8Array([3]));

		expect([...(await store.read("coordinates.wal"))!]).to.deep.equal([
			1, 2, 3,
		]);
		expect(directory.syncAccessCount).to.equal(2);
		expect(directory.syncWriteCount).to.equal(2);
		expect(directory.syncFlushCount).to.equal(2);
		expect(directory.syncCloseCount).to.equal(2);
		expect(directory.asyncWritableCount).to.equal(0);
	});

	it("completes short OPFS WAL writes and poisons zero-progress writes", async () => {
		const shortDirectory = new FakeOPFSDirectoryHandle(true, [2]);
		const shortStore = new NativeBackboneOPFSCoordinatePersistenceStore(
			shortDirectory,
		);
		await shortStore.append("coordinates.wal", new Uint8Array([1, 2, 3, 4]));
		expect(shortDirectory.syncWriteCount).equal(2);
		expect(await shortStore.read("coordinates.wal")).deep.equal(
			new Uint8Array([1, 2, 3, 4]),
		);

		const zeroDirectory = new FakeOPFSDirectoryHandle(true, [0]);
		const zeroStore = new NativeBackboneOPFSCoordinatePersistenceStore(
			zeroDirectory,
		);
		let firstError: unknown;
		try {
			await zeroStore.append("coordinates.wal", new Uint8Array([5, 6]));
		} catch (error) {
			firstError = error;
		}
		expect(String(firstError)).to.contain(
			"Invalid OPFS coordinate WAL write progress",
		);
		let secondError: unknown;
		try {
			await zeroStore.append("coordinates.wal", new Uint8Array([7]));
		} catch (error) {
			secondError = error;
		}
		expect(secondError).equal(firstError);
		expect(zeroDirectory.syncWriteCount).equal(1);
		expect((await zeroStore.read("coordinates.wal"))?.byteLength).equal(0);
	});

	it("uses buffered coordinate persistence with OPFS stores", async () => {
		const directory = new FakeOPFSDirectoryHandle(true);
		const opfsStore = new NativeBackboneOPFSCoordinatePersistenceStore(
			directory,
		);
		const persistence = createBufferedNativeBackboneCoordinatePersistence(
			opfsStore,
			{ flushMaxPendingBytes: 1024 },
		);
		const source = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});
		const beforeClose = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});
		const afterClose = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});

		await persistence.hydrate(source);
		source.putEntryCoordinates("hash-opfs", "gid-opfs", [1n], false, 1, 1n);
		expect(await persistence.flushJournalOnAppend?.(source)).equal(0);
		expect(await opfsStore.read("coordinates.wal")).equal(undefined);
		await persistence.flushJournal(source);
		expect(
			(await opfsStore.read("coordinates.wal"))?.byteLength,
		).to.be.greaterThan(source.coordinateJournalHeader().byteLength);
		await new NativeBackboneCoordinatePersistence(opfsStore).hydrate(
			beforeClose,
		);
		expect(beforeClose.getEntryCoordinateHashes()).to.deep.equal(["hash-opfs"]);

		await persistence.close?.();
		await new NativeBackboneCoordinatePersistence(opfsStore).hydrate(
			afterClose,
		);
		expect(afterClose.getEntryCoordinateHashes()).to.deep.equal(["hash-opfs"]);
		expect(directory.syncAccessCount).to.be.greaterThan(0);
	});

	it("drops buffered native persistence through OPFS without replaying buffers", async () => {
		const directory = new FakeOPFSDirectoryHandle(true);
		const opfsStore = new NativeBackboneOPFSCoordinatePersistenceStore(
			directory,
		);
		const persistence = createBufferedNativeBackboneCoordinatePersistence(
			opfsStore,
			{ flushMaxPendingBytes: 1024 },
		);
		for (const name of [
			"coordinates.bin",
			"coordinates.wal",
			"document-values.bin",
			"document-values.wal",
			"document-signers.bin",
			"document-signers.wal",
		]) {
			await persistence.intentStore!.append(name, new Uint8Array([1, 2, 3]));
		}
		await persistence.intentStore!.write(
			"strict-intent-a",
			new Uint8Array([4]),
		);
		await persistence.intentStore!.write(
			"strict-intent-b",
			new Uint8Array([5]),
		);

		await persistence.drop!(["strict-intent-a", "strict-intent-b"]);
		await persistence.close?.();
		expect(directory.files.size).equal(0);
	});

	it("persists buffered native document WAL and signer facts through OPFS sync stores", async () => {
		{
			const syncAccess = true;
			const directory = new FakeOPFSDirectoryHandle(syncAccess);
			const opfsStore = new NativeBackboneOPFSCoordinatePersistenceStore(
				directory,
			);
			const persistence = createBufferedNativeBackboneCoordinatePersistence(
				opfsStore,
				{ flushMaxPendingBytes: 1024 },
			);
			const source = await createNativePeerbitBackbone({
				clockId: publicKey,
				privateKey,
				publicKey,
			});
			const restored = await createNativePeerbitBackbone({
				clockId: publicKey,
				privateKey,
				publicKey,
			});
			for (const backbone of [source, restored]) {
				backbone.configureDocumentSchemaIr(contextOnlySchema());
				backbone.setDocumentContextHeadField(3);
				backbone.setDocumentContextFields({
					created: 1,
					modified: 2,
					head: 3,
					gid: 4,
					size: 5,
				});
			}

			await persistence.hydrate(source);
			source.preparePlainCommittedNoNextStorageAppendDocumentIndexCompactTransaction(
				{
					wallTime: 11n,
					logical: 1,
					gid: "gid-opfs-document",
					payloadData: new Uint8Array([1, 2, 3]),
					replicas: 1,
					selfHash: "peer",
					documentIndex: {
						key: "doc-opfs",
						valuePrefixBytes: new Uint8Array(0),
					},
				},
			);
			const documentValue = source.documentValueBytes("doc-opfs");
			expect(documentValue).to.exist;
			source.putDocumentEncodedPartsStored(
				"doc-opfs",
				documentValue!,
				new Uint8Array(0),
			);
			expect(source.documentPendingJournalLength).equal(2);
			expect(source.documentSignerPendingJournalLength).equal(1);

			expect(await persistence.flushJournal(source)).to.be.greaterThan(0);
			expect(
				(await opfsStore.read("document-values.wal"))?.byteLength,
			).to.be.greaterThan(source.documentJournalHeader().byteLength);
			expect(
				(await opfsStore.read("document-signers.wal"))?.byteLength,
			).to.be.greaterThan(source.documentSignerJournalHeader().byteLength);
			await persistence.close?.();
			expect(
				(await opfsStore.read("document-values.wal"))?.byteLength,
			).to.be.greaterThan(source.documentJournalHeader().byteLength);
			expect(
				(await opfsStore.read("document-signers.wal"))?.byteLength,
			).to.be.greaterThan(source.documentSignerJournalHeader().byteLength);

			await new NativeBackboneCoordinatePersistence(opfsStore).hydrate(
				restored,
			);
			expect(restored.documentIndexLength).equal(1);
			expect(
				Array.from(restored.documentKeysExist(["doc-opfs"])),
			).to.deep.equal([1]);
			restored.clearDocumentIndex();
			restored.putDocumentEncodedPartsStored(
				"doc-opfs",
				documentValue!,
				new Uint8Array(0),
			);
			expect(
				Array.from(
					restored.documentPreviousSignaturePublicKey("doc-opfs")?.publicKey ??
						[],
				),
			).to.deep.equal(Array.from(publicKey));
			expect(directory.syncAccessCount).to.be.greaterThan(0);
			expect(directory.asyncWritableCount).equal(0);
		}
	});

	it("rejects a durable ACK when OPFS only exposes writable streams", async () => {
		const directory = new FakeOPFSDirectoryHandle(false);
		const opfsStore = new NativeBackboneOPFSCoordinatePersistenceStore(
			directory,
		);
		const persistence = createBufferedNativeBackboneCoordinatePersistence(
			opfsStore,
			{ flushMaxPendingBytes: 1024 },
		);
		const source = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});
		await persistence.hydrate(source);
		source.putEntryCoordinates(
			"hash-no-opfs-barrier",
			"gid-no-opfs-barrier",
			[1n],
			false,
			1,
			1n,
		);

		const failure = await persistence.flushJournal(source).then(
			() => undefined,
			(error: unknown) => error,
		);
		expect(String(failure)).to.contain("sync handles unavailable");
		expect(source.coordinatePendingJournalLength).equal(1);
		expect(directory.asyncWritableCount).to.be.greaterThan(0);
	});

	it("appends coordinate WAL bytes through OPFS writable fallback", async () => {
		const directory = new FakeOPFSDirectoryHandle(false);
		const store = new NativeBackboneOPFSCoordinatePersistenceStore(directory);

		await store.append("coordinates.wal", new Uint8Array([4, 5]));
		await store.append("coordinates.wal", new Uint8Array([6]));

		expect([...(await store.read("coordinates.wal"))!]).to.deep.equal([
			4, 5, 6,
		]);
		expect(directory.syncWriteCount).to.equal(0);
		expect(directory.asyncWritableCount).to.equal(2);
		expect(directory.keepExistingDataOptions).to.deep.equal([true, true]);

		await store.remove("coordinates.wal");
		expect(await store.read("coordinates.wal")).to.equal(undefined);
		await store.remove("coordinates.wal");
	});
});
