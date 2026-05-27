import { expect } from "chai";
import {
	NativeBackboneBufferedCoordinatePersistenceStore,
	NativeBackboneCoordinatePersistence,
	NativeBackboneMemoryCoordinatePersistenceStore,
	NativeBackboneNodeCoordinatePersistence,
	NativeBackboneNodeCoordinatePersistenceStore,
	NativeBackboneOPFSCoordinatePersistenceStore,
	type NativeBackboneOPFSDirectoryHandle,
	createBufferedNativeBackboneCoordinatePersistence,
	createBufferedNativeBackboneNodeCoordinatePersistence,
	createNativeBackboneCoordinatePersistence,
	createNativePeerbitBackbone,
	defaultNativeBackboneCoordinateCompactMaxJournalBytes,
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
				this.writeAt(options?.at ?? 0, buffer);
				this.directory.syncWriteCount++;
				return buffer.byteLength;
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

	constructor(private readonly syncAccess = false) {}

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

		expect(persistence.compactMaxJournalBytes).equal(
			defaultNativeBackboneCoordinateCompactMaxJournalBytes,
		);
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
		expect(store.files.has("coordinates.wal")).equal(false);
		await persistence.close?.();
		expect(store.files.get("coordinates.wal")?.byteLength).to.be.greaterThan(
			backbone.coordinateJournalHeader().byteLength,
		);
	});

	it("honors buffered store config coordinate WAL checkpoint thresholds", async () => {
		const backbone = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});
		const restored = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});
		const store = new NativeBackboneMemoryCoordinatePersistenceStore();
		const persistence = createNativeBackboneCoordinatePersistence({
			store,
			buffered: true,
			flushOnAppend: false,
			flushMaxPendingBytes: 1,
			compactMaxJournalRecords: 1,
		});

		await persistence.hydrate(backbone);
		backbone.putEntryCoordinates(
			"hash-buffered-config-compact",
			"gid-buffered-config-compact",
			[1n],
			false,
			1,
			1n,
		);

		expect(await persistence.flushJournalOnAppend?.(backbone)).to.be.greaterThan(
			0,
		);
		expect(store.files.has("coordinates.bin")).equal(true);
		expect(store.files.has("coordinates.wal")).equal(false);
		await new NativeBackboneCoordinatePersistence(store).hydrate(restored);
		expect(restored.getEntryCoordinateHashes()).to.deep.equal([
			"hash-buffered-config-compact",
		]);
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
		expect(persistence.compactMaxJournalBytes).equal(
			defaultNativeBackboneCoordinateCompactMaxJournalBytes,
		);
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
		expect(store.files.has("coordinates.wal")).equal(false);
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
		source.preparePlainCommittedNoNextStorageAppendDocumentIndexCompactTransaction({
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
		});
		expect(source.documentPendingJournalLength).equal(1);

		expect(await persistence.flushJournal(source)).to.be.greaterThan(0);
		expect(store.files.has("custom-document-values.wal")).equal(false);
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
		source.preparePlainCommittedNoNextStorageAppendDocumentIndexCompactTransaction({
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
		});
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
		expect(store.files.has("custom-document-signers.wal")).equal(false);
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

	it("checkpoints generic coordinate WAL after compact thresholds", async () => {
		const backbone = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});
		const restored = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});
		const store = new NativeBackboneMemoryCoordinatePersistenceStore();
		const persistence = new NativeBackboneCoordinatePersistence(store, {
			compactMaxJournalRecords: 1,
		});

		await persistence.hydrate(backbone);
		backbone.putEntryCoordinates(
			"hash-compact",
			"gid-compact",
			[1n],
			false,
			1,
			1n,
		);

		expect(await persistence.flushJournal(backbone)).to.be.greaterThan(0);
		expect(store.files.has("coordinates.bin")).equal(true);
		expect(store.files.has("coordinates.wal")).equal(false);
		expect(backbone.coordinatePendingJournalLength).equal(0);
		await new NativeBackboneCoordinatePersistence(store).hydrate(restored);
		expect(restored.getEntryCoordinateHashes()).to.deep.equal(["hash-compact"]);
	});

	it("checkpoints buffered coordinate WAL after compact thresholds", async () => {
		const backbone = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});
		const restored = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});
		const store = new NativeBackboneMemoryCoordinatePersistenceStore();
		const persistence = createBufferedNativeBackboneCoordinatePersistence(
			store,
			{ compactMaxJournalRecords: 1, flushMaxPendingBytes: 1 },
		);

		await persistence.hydrate(backbone);
		backbone.putEntryCoordinates(
			"hash-buffered-compact",
			"gid-buffered-compact",
			[1n],
			false,
			1,
			1n,
		);

		expect(await persistence.flushJournalOnAppend?.(backbone)).to.be.greaterThan(
			0,
		);
		expect(store.files.has("coordinates.bin")).equal(true);
		expect(store.files.has("coordinates.wal")).equal(false);
		expect(backbone.coordinatePendingJournalLength).equal(0);
		await new NativeBackboneCoordinatePersistence(store).hydrate(restored);
		expect(restored.getEntryCoordinateHashes()).to.deep.equal([
			"hash-buffered-compact",
		]);
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
		expect(backbone.hasDocumentExactString(1, "abc", "doc-1")).to.equal(true);
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
		expect(second.entry.bytes).equal(undefined);
		expect(second.entry.next).to.deep.equal([]);
		expect(second.trimmed).to.deep.equal([]);
		expect(second.trimmedHashes).to.deep.equal([first.entry.hash]);
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
		expect(results?.map((result) => result.entry.next)).to.deep.equal([
			[],
			[],
		]);
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
									encodedDocument:
										encodedDocumentWithIdScoreAndBytes("abc", 7),
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
									encodedDocument:
										encodedDocumentWithIdScoreAndBytes("def", 8),
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
			expect(
				backbone.documentExactStringFirstKey(3, result.entry.hash),
			).to.equal("doc-identity-payload");
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
		expect(updated[0]!.coordinate.gid).to.equal(
			"gid-doc-latest-payload-batch",
		);
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
		const compactExpectedColumns =
			target.prepareRawReceiveExpectedColumnsBatch(
				[prepared.bytes],
				[prepared.hash],
			);
		expect(compactExpectedColumns?.[0]).to.deep.equal([]);
		expect(compactExpectedColumns?.[1][0]).to.equal(undefined);
		expect(Array.from(compactExpectedColumns?.[12] ?? [])).to.deep.equal([
			1,
		]);
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
		expect(Array.from(compactUnverifiedColumns?.[12] ?? [])).to.deep.equal([
			0,
		]);
		expect(
			target.graph.verifyPreparedRawReceiveEntries([prepared.hash]),
		).to.deep.equal([true]);
		expect(
			target.graph.verifyPreparedRawReceiveEntries(["missing"]),
		).to.equal(undefined);
		expect(() =>
			target.prepareRawReceiveColumnsBatch([prepared.bytes], ["not-a-cid"]),
		).to.throw("Expected base58btc CID");
		expect(
			target.graph.commitPreparedRawReceiveBatch(
				[prepared.hash],
				[true],
				{
					hashes: [prepared.hash],
					gids: ["gid-raw-receive"],
					hashNumbers: ["9"],
					coordinateBatches: [["11"]],
					nextHashBatches: [[]],
					assignedToRangeBoundaries: new Uint8Array([0]),
					requestedReplicas: [1],
				},
			),
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
		expect(Array.from(indexPlans?.[0].indexes ?? [])).to.deep.equal([
			0,
			1,
		]);
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
		expect(Array.from(leaderPlans?.[0].indexes ?? [])).to.deep.equal([
			0,
			1,
		]);
			expect(leaderPlans?.[0].coordinates).to.have.length(3);
			expect(leaderPlans?.[0].coordinateStrings).to.have.length(3);
			expect(leaderPlans?.[0].leaders).to.be.instanceOf(Map);
			expect(leaderPlans?.[0].leaders.has("peer-keep")).to.equal(true);
			const assignmentPlans =
				target.planPreparedRawReceiveGroupAssignments(
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
			expect(assignmentPlans?.[0].assignedToRangeBoundary).to.be.a(
				"boolean",
			);
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
			expect(Array.from(mixedSelection?.droppedIndexes ?? [])).to.deep.equal([
				2,
			]);
			expect(mixedSelection).to.deep.include({
				groupCount: 2,
				plannedHashCount: 3,
				usedNativeFastDropPlan: false,
				usedLeaderSamplePlans: false,
			});
			expect(mixedSelection?.retainedGroupLeaderPlans).to.have.length(1);
			expect(mixedSelection?.retainedGroupLeaderPlans?.[0]).to.deep.include(
				{
					gid: "gid-raw-group-a",
					latestIndex: 1,
					maxReplicasFromHead: 1,
					maxReplicasFromNewEntries: 3,
					maxMaxReplicas: 3,
				},
			);
			expect(
				Array.from(
					mixedSelection?.retainedGroupLeaderPlans?.[0]?.indexes ?? [],
				),
			).to.deep.equal([0, 1]);
			expect(
				mixedSelection?.retainedGroupLeaderPlans?.[0]?.leaders.has(
					"peer-keep",
				),
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
			expect(
				Array.from(mixedFusedSelection?.droppedIndexes ?? []),
			).to.deep.equal([2]);
			expect(mixedFusedSelection).to.deep.include({
				groupCount: 2,
				plannedHashCount: 3,
				usedNativeFastDropPlan: false,
				usedLeaderSamplePlans: false,
			});
			expect(mixedFusedSelection?.retainedGroupLeaderPlans).to.have.length(
				1,
			);
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
			expect(
				Array.from(mixedPreparedSelection?.columns[12] ?? []),
			).to.deep.equal([0, 0, 0]);
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

		expect(backbone.graph.uniqueReferenceGidRowsFlatBatch([head.hash])).to.deep.equal(
			[
				[0, root.hash, "root-gid"],
				[0, side.hash, "side-gid"],
			],
		);
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
		const requestPruneHintColumns =
			backbone.planRequestPruneLeaderHintColumns(
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
				1,
				0,
			]);
			expect([...requestPruneHintColumns.replicaCounts]).to.deep.equal([1, 0]);
			expect([...requestPruneHintColumns.localLeaderFlags]).to.deep.equal([
				leaderPlan.leaders.has("peer-a") ? 1 : 0,
				0,
			]);
			expect([...requestPruneHintColumns.peerHistoryRemovedFlags]).to.deep.equal(
				[1, 0],
			);
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

	it("benchmarks committed no-next storage appends inside one native loop", async () => {
		const backbone = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});

		backbone.configureDocumentSchemaIr(contextOnlySchema());
		backbone.setCoordinateJournalEnabled(true);
		backbone.resetAppendProfile();
		backbone.setAppendProfileEnabled(true);
		const result =
			backbone.benchmarkPlainCommittedNoNextStorageAppendTransactionLoop({
				iterations: 3,
				wallTimeStart: 100n,
				payloadData: new Uint8Array([1, 2, 3]),
				replicas: 1,
				selfHash: "peer-a",
				useDocumentIndex: true,
			});
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
			gid: "gid-storage-compact-trim",
			payloadData: new Uint8Array([1]),
			replicas: 1,
			selfHash: "peer-a",
		});
		const second = backbone.preparePlainCommittedNoNextStorageAppendTransaction(
			{
				wallTime: 2n,
				gid: "gid-storage-compact-trim",
				payloadData: new Uint8Array([2]),
				replicas: 1,
				selfHash: "peer-a",
				trimLengthTo: 1,
				resolveTrimmedEntries: false,
			},
		);

		expect(second.trimmedHashes).to.deep.equal([first.entry.hash]);
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
		const journal = concatBytes([
			source.coordinateJournalHeader(),
			source.drainCoordinateJournal(),
		]);

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
		const journal = concatBytes([
			source.coordinateJournalHeader(),
			source.drainCoordinateJournal(),
		]);

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
		source.drainCoordinateJournal();
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

	it("flushes and compacts native coordinate state through the persistence adapter", async () => {
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
		const compacted = await createNativePeerbitBackbone({
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
		await persistence.compact(source);
		expect(store.files.has("coordinates.wal")).equal(false);
		expect(await persistence.hydrate(compacted)).to.equal(0);
		expect(compacted.getEntryCoordinateHashes()).to.deep.equal([
			"hash-a",
			"hash-b",
		]);
		expect(compacted.coordinateIndexLength).to.equal(2);
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
		const restoredFromSnapshot = await createNativePeerbitBackbone({
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
		source.preparePlainCommittedNoNextStorageAppendDocumentIndexCompactTransaction({
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
		});
		expect(source.documentPendingJournalLength).to.equal(1);
		await persistence.flushJournal(source);
		expect(store.files.has("document-values.wal")).equal(true);
		expect(store.files.get("document-values.wal")?.byteLength).to.be.greaterThan(
			source.documentJournalHeader().byteLength,
		);

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

		await persistence.compact(source);
		expect(store.files.has("document-values.wal")).equal(false);
		expect(store.files.has("document-values.bin")).equal(true);
		await persistence.hydrate(restoredFromSnapshot);
		restoredFromSnapshot.configureDocumentSchemaIr(contextOnlySchema());
		restoredFromSnapshot.setDocumentContextHeadField(3);
		restoredFromSnapshot.setDocumentContextFields({
			created: 1,
			modified: 2,
			head: 3,
			gid: 4,
			size: 5,
		});
		expect(restoredFromSnapshot.documentIndexLength).to.equal(1);
		expect(
			Array.from(restoredFromSnapshot.documentKeysExist(["doc-value"])),
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
		const restoredFromSnapshot = await createNativePeerbitBackbone({
			clockId: publicKey,
			privateKey,
			publicKey,
		});
		for (const backbone of [source, restoredFromWal, restoredFromSnapshot]) {
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
		expect(store.files.get("document-signers.wal")?.byteLength).to.be.greaterThan(
			source.documentSignerJournalHeader().byteLength,
		);

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

		await persistence.compact(source);
		expect(store.files.has("document-signers.wal")).equal(false);
		expect(store.files.has("document-signers.bin")).equal(true);
		await persistence.hydrate(restoredFromSnapshot);
		restoredFromSnapshot.clearDocumentIndex();
		restoredFromSnapshot.putDocumentEncodedPartsStored(
			"doc-signer",
			documentValue!,
			new Uint8Array(0),
		);
		expect(restoredFromSnapshot.hasBlock(append.entry.hash)).equal(false);
		expect(
			Array.from(
				restoredFromSnapshot.documentPreviousSignaturePublicKey("doc-signer")
					?.publicKey ?? [],
			),
		).to.deep.equal(Array.from(publicKey));
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
			const compacted = await createNativePeerbitBackbone({
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
			await persistence.compact(source);
			expect(await persistence.hydrate(compacted)).to.equal(0);
			expect(compacted.getEntryCoordinateHashes()).to.deep.equal([
				"hash-a",
				"hash-b",
			]);
			expect(compacted.coordinateIndexLength).to.equal(2);
		} finally {
			await rm(directory, { recursive: true, force: true });
		}
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
			const persistence =
				createBufferedNativeBackboneNodeCoordinatePersistence(directory, {
					flushMaxPendingBytes: 1024,
				});

			expect(persistence.flushOnAppend).equal(false);
			expect(persistence.flushMaxPendingBytes).equal(1024);
			expect(persistence.compactMaxJournalBytes).equal(
				defaultNativeBackboneCoordinateCompactMaxJournalBytes,
			);
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

	it("checkpoints node coordinate WAL after compact thresholds", async () => {
		const [{ mkdtemp, rm }, { tmpdir }, { join }] = await Promise.all([
			import("node:fs/promises"),
			import("node:os"),
			import("node:path"),
		]);
		const directory = await mkdtemp(
			join(tmpdir(), "peerbit-native-backbone-node-coordinate-compact-"),
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
			const persistence = new NativeBackboneNodeCoordinatePersistence(
				directory,
				{ compactMaxJournalRecords: 1 },
			);

			await persistence.hydrate(source);
			source.putEntryCoordinates(
				"hash-node-compact",
				"gid-node-compact",
				[1n],
				false,
				1,
				1n,
			);
			expect(await persistence.flushJournal(source)).to.be.greaterThan(0);
			await persistence.close();

			const restoredPersistence = new NativeBackboneNodeCoordinatePersistence(
				directory,
			);
			expect(await restoredPersistence.hydrate(restored)).equal(0);
			expect(restored.getEntryCoordinateHashes()).to.deep.equal([
				"hash-node-compact",
			]);
			await restoredPersistence.close();
		} finally {
			await rm(directory, { recursive: true, force: true });
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
			const persistence = new NativeBackboneCoordinatePersistence(buffered);

			await persistence.hydrate(source);
			source.putEntryCoordinates("hash-a", "gid-a", [1n], false, 1, 1n);
			expect(await persistence.flushJournal(source)).to.be.greaterThan(0);

			const writeThroughPersistence = new NativeBackboneCoordinatePersistence(
				new NativeBackboneNodeCoordinatePersistenceStore(directory),
			);
			expect(await writeThroughPersistence.hydrate(beforeFlush)).to.equal(0);
			await buffered.flush();
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
		expect(await opfsStore.read("coordinates.wal")).equal(undefined);
		await new NativeBackboneCoordinatePersistence(opfsStore).hydrate(
			beforeClose,
		);
		expect(beforeClose.getEntryCoordinateHashes()).to.deep.equal([]);

		await persistence.close?.();
		await new NativeBackboneCoordinatePersistence(opfsStore).hydrate(afterClose);
		expect(afterClose.getEntryCoordinateHashes()).to.deep.equal(["hash-opfs"]);
		expect(directory.syncAccessCount).to.be.greaterThan(0);
	});

	it("persists buffered native document WAL and signer facts through OPFS stores", async () => {
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
		source.preparePlainCommittedNoNextStorageAppendDocumentIndexCompactTransaction({
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
		});
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
		expect(await opfsStore.read("document-values.wal")).equal(undefined);
		expect(await opfsStore.read("document-signers.wal")).equal(undefined);
		await persistence.close?.();
		expect(
			(await opfsStore.read("document-values.wal"))?.byteLength,
		).to.be.greaterThan(source.documentJournalHeader().byteLength);
		expect(
			(await opfsStore.read("document-signers.wal"))?.byteLength,
		).to.be.greaterThan(source.documentSignerJournalHeader().byteLength);

		await new NativeBackboneCoordinatePersistence(opfsStore).hydrate(restored);
		expect(restored.documentIndexLength).equal(1);
		expect(Array.from(restored.documentKeysExist(["doc-opfs"]))).to.deep.equal([
			1,
		]);
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
