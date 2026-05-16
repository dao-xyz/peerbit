import { expect } from "chai";
import {
	NativeBackboneBufferedCoordinatePersistenceStore,
	NativeBackboneCoordinatePersistence,
	NativeBackboneMemoryCoordinatePersistenceStore,
	NativeBackboneNodeCoordinatePersistence,
	NativeBackboneNodeCoordinatePersistenceStore,
	NativeBackboneOPFSCoordinatePersistenceStore,
	type NativeBackboneOPFSDirectoryHandle,
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
		expect(backbone.coordinateIndexLength).to.equal(1);
		expect(backbone.hasCoordinateIndexHash("hash-b")).equal(false);
		expect(backbone.hasCoordinateIndexHash("hash-c")).equal(true);
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
		expect(backbone.hasLogEntry(first.entry.hash)).equal(false);
		expect(backbone.hasBlock(first.entry.hash)).equal(false);
		expect(backbone.hasLogEntry(second.entry.hash)).equal(true);
		expect(backbone.hasBlock(second.entry.hash)).equal(true);
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
