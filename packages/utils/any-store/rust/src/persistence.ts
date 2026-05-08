export type PersistenceDurability = "normal" | "strict";

export interface RustAnyStorePersistenceBackend {
	readSnapshot(): Promise<Uint8Array | undefined>;
	readJournal(): Promise<Uint8Array | undefined>;
	appendJournal(record: Uint8Array, durability: PersistenceDurability): Promise<void>;
	writeSnapshot(snapshot: Uint8Array): Promise<void>;
	removeSublevels(): Promise<void>;
	close(): Promise<void>;
}

const SNAPSHOT_FILE_NAME = "store.bin";
const SNAPSHOT_TEMP_FILE_NAME = "store.bin.tmp";
const JOURNAL_FILE_NAME = "store.wal";
const SUBLEVEL_DIRECTORY_NAME = "sublevels";
const MANIFEST_A_FILE_NAME = "manifest-a.json";
const MANIFEST_B_FILE_NAME = "manifest-b.json";

type CheckpointManifest = {
	epoch: number;
	snapshot: string;
	journal: string;
};

type ActiveManifest = CheckpointManifest & {
	slot: typeof MANIFEST_A_FILE_NAME | typeof MANIFEST_B_FILE_NAME;
};

const encodePathPart = (part: string): string =>
	encodeURIComponent(part).replace(/[!'()*]/g, (char) =>
		`%${char.charCodeAt(0).toString(16).toUpperCase()}`,
	);

const isNodeRuntime = () =>
	Boolean((globalThis as { process?: { versions?: { node?: string } } }).process
		?.versions?.node);

const isNotFoundError = (error: unknown): boolean =>
	Boolean(
		error &&
			((error as { code?: string }).code === "ENOENT" ||
				(error as { name?: string }).name === "NotFoundError"),
	);

const sleep = (ms: number): Promise<void> =>
	new Promise((resolve) => setTimeout(resolve, ms));

const checksumString = (value: string): string => {
	let hash = 0x811c9dc5;
	for (let i = 0; i < value.length; i++) {
		hash ^= value.charCodeAt(i);
		hash = Math.imul(hash, 0x01000193);
	}
	return (hash >>> 0).toString(16).padStart(8, "0");
};

const manifestPayload = (manifest: CheckpointManifest): string =>
	JSON.stringify({
		epoch: manifest.epoch,
		snapshot: manifest.snapshot,
		journal: manifest.journal,
	});

const encodeManifest = (manifest: CheckpointManifest): Uint8Array => {
	const payload = manifestPayload(manifest);
	return new TextEncoder().encode(
		JSON.stringify({ payload: JSON.parse(payload), checksum: checksumString(payload) }),
	);
};

const decodeManifest = (
	bytes: Uint8Array,
	slot: ActiveManifest["slot"],
): ActiveManifest | undefined => {
	try {
		const decoded = JSON.parse(new TextDecoder().decode(bytes)) as {
			payload?: CheckpointManifest;
			checksum?: string;
		};
		if (!decoded.payload || typeof decoded.checksum !== "string") {
			return undefined;
		}
		const payload = manifestPayload(decoded.payload);
		if (checksumString(payload) !== decoded.checksum) {
			return undefined;
		}
		if (
			!Number.isSafeInteger(decoded.payload.epoch) ||
			decoded.payload.epoch < 0 ||
			typeof decoded.payload.snapshot !== "string" ||
			typeof decoded.payload.journal !== "string"
		) {
			return undefined;
		}
		return { ...decoded.payload, slot };
	} catch {
		return undefined;
	}
};

const readNodeFileIfExists = async (path: string): Promise<Uint8Array | undefined> => {
	const fsPromises = "fs/promises";
	const { readFile } = (await import(
		/* @vite-ignore */ fsPromises
	)) as typeof import("fs/promises");
	try {
		return new Uint8Array(await readFile(path));
	} catch (error) {
		if (isNotFoundError(error)) {
			return undefined;
		}
		throw error;
	}
};

class NodePersistenceBackend implements RustAnyStorePersistenceBackend {
	private journalHandle?: import("fs/promises").FileHandle;
	private journalOffset?: number;

	constructor(private readonly rootDirectory: string, private readonly level: string[]) {}

	async readSnapshot(): Promise<Uint8Array | undefined> {
		return readNodeFileIfExists(await this.snapshotPath());
	}

	async readJournal(): Promise<Uint8Array | undefined> {
		const journal = await readNodeFileIfExists(await this.journalPath());
		if (journal) {
			this.journalOffset = journal.byteLength;
		}
		return journal;
	}

	async appendJournal(
		record: Uint8Array,
		durability: PersistenceDurability,
	): Promise<void> {
		await this.ensureLevelDirectory();
		const fsPromises = "fs/promises";
		const { open, stat } = (await import(
			/* @vite-ignore */ fsPromises
		)) as typeof import("fs/promises");
		if (!this.journalHandle) {
			const path = await this.journalPath();
			this.journalHandle = await open(path, "a+");
			if (this.journalOffset == null) {
				try {
					this.journalOffset = (await stat(path)).size;
				} catch {
					this.journalOffset = 0;
				}
			}
		}
		const offset = this.journalOffset ?? 0;
		await this.journalHandle.write(record, 0, record.byteLength, offset);
		this.journalOffset = offset + record.byteLength;
		if (durability === "strict") {
			await this.journalHandle.sync();
		}
	}

	async writeSnapshot(snapshot: Uint8Array): Promise<void> {
		await this.close();
		await this.ensureLevelDirectory();
		const fsPromises = "fs/promises";
		const { rename, writeFile } = (await import(
			/* @vite-ignore */ fsPromises
		)) as typeof import("fs/promises");
		const tempPath = await this.snapshotTempPath();
		await writeFile(tempPath, snapshot);
		await rename(tempPath, await this.snapshotPath());
		await writeFile(await this.journalPath(), new Uint8Array());
		this.journalOffset = 0;
	}

	async removeSublevels(): Promise<void> {
		const fsPromises = "fs/promises";
		const { rm } = (await import(
			/* @vite-ignore */ fsPromises
		)) as typeof import("fs/promises");
		await rm(await this.sublevelsDirectory(), { recursive: true, force: true });
	}

	async close(): Promise<void> {
		if (!this.journalHandle) {
			return;
		}
		const handle = this.journalHandle;
		this.journalHandle = undefined;
		await handle.close();
	}

	private async ensureLevelDirectory(): Promise<void> {
		const fsPromises = "fs/promises";
		const { mkdir } = (await import(
			/* @vite-ignore */ fsPromises
		)) as typeof import("fs/promises");
		await mkdir(await this.levelDirectory(), { recursive: true });
	}

	private async levelDirectory(): Promise<string> {
		const pathModule = "path";
		const path = (await import(/* @vite-ignore */ pathModule)) as typeof import("path");
		let current = this.rootDirectory;
		for (const part of this.level) {
			current = path.join(
				current,
				SUBLEVEL_DIRECTORY_NAME,
				encodePathPart(part),
			);
		}
		return current;
	}

	private async sublevelsDirectory(): Promise<string> {
		const pathModule = "path";
		const path = (await import(/* @vite-ignore */ pathModule)) as typeof import("path");
		return path.join(await this.levelDirectory(), SUBLEVEL_DIRECTORY_NAME);
	}

	private async snapshotPath(): Promise<string> {
		const pathModule = "path";
		const path = (await import(/* @vite-ignore */ pathModule)) as typeof import("path");
		return path.join(await this.levelDirectory(), SNAPSHOT_FILE_NAME);
	}

	private async snapshotTempPath(): Promise<string> {
		const pathModule = "path";
		const path = (await import(/* @vite-ignore */ pathModule)) as typeof import("path");
		return path.join(await this.levelDirectory(), SNAPSHOT_TEMP_FILE_NAME);
	}

	private async journalPath(): Promise<string> {
		const pathModule = "path";
		const path = (await import(/* @vite-ignore */ pathModule)) as typeof import("path");
		return path.join(await this.levelDirectory(), JOURNAL_FILE_NAME);
	}
}

class OpfsPersistenceBackend implements RustAnyStorePersistenceBackend {
	private journalHandle?: FileSystemSyncAccessHandle;
	private journalOffset?: number;
	private journalFileName?: string;
	private activeManifest?: ActiveManifest;
	private manifestLoaded = false;

	constructor(private readonly directory: FileSystemDirectoryHandle) {}

	async readSnapshot(): Promise<Uint8Array | undefined> {
		await this.ensureManifestLoaded();
		if (!this.activeManifest) {
			return this.readFileIfExists(SNAPSHOT_FILE_NAME);
		}
		const snapshot = await this.readFileIfExists(this.activeManifest.snapshot);
		if (!snapshot) {
			throw new Error(
				`OPFS checkpoint snapshot missing: ${this.activeManifest.snapshot}`,
			);
		}
		return snapshot;
	}

	async readJournal(): Promise<Uint8Array | undefined> {
		await this.ensureManifestLoaded();
		const journalFile = this.activeManifest?.journal ?? JOURNAL_FILE_NAME;
		const journal = await this.readFileIfExists(journalFile);
		this.journalFileName = journalFile;
		if (journal) {
			this.journalOffset = journal.byteLength;
		}
		return journal;
	}

	async appendJournal(
		record: Uint8Array,
		durability: PersistenceDurability,
	): Promise<void> {
		await this.ensureManifestLoaded();
		const journalFile = this.activeManifest?.journal ?? JOURNAL_FILE_NAME;
		if (this.journalHandle && this.journalFileName !== journalFile) {
			await this.close();
		}
		if (!this.journalHandle) {
			const file = await this.directory.getFileHandle(journalFile, {
				create: true,
			});
			this.journalHandle = await createSyncAccessHandle(file);
			this.journalFileName = journalFile;
			this.journalOffset ??= this.journalHandle.getSize();
		}
		const offset = this.journalOffset ?? 0;
		this.journalHandle.write(record, { at: offset });
		this.journalOffset = offset + record.byteLength;
		if (durability === "strict") {
			this.journalHandle.flush();
		}
	}

	async writeSnapshot(snapshot: Uint8Array): Promise<void> {
		await this.close();
		await this.ensureManifestLoaded();
		const previous = this.activeManifest;
		const epoch = (previous?.epoch ?? 0) + 1;
		const next: ActiveManifest = {
			epoch,
			snapshot: `snapshot-${epoch}.bin`,
			journal: `journal-${epoch}.wal`,
			slot:
				previous?.slot === MANIFEST_A_FILE_NAME
					? MANIFEST_B_FILE_NAME
					: MANIFEST_A_FILE_NAME,
		};

		await this.writeFile(next.snapshot, snapshot, true);
		await this.writeFile(next.journal, new Uint8Array(), true);
		await this.writeFile(next.slot, encodeManifest(next), true);

		this.activeManifest = next;
		this.journalFileName = next.journal;
		this.journalOffset = 0;
		await this.cleanupCheckpoints(previous, next);
	}

	async removeSublevels(): Promise<void> {
		await this.removeEntryIfExists(SUBLEVEL_DIRECTORY_NAME, true);
	}

	async close(): Promise<void> {
		if (!this.journalHandle) {
			return;
		}
		const handle = this.journalHandle;
		this.journalHandle = undefined;
		this.journalFileName = undefined;
		handle.close();
	}

	private async readFileIfExists(name: string): Promise<Uint8Array | undefined> {
		try {
			const file = await this.directory.getFileHandle(name);
			const handle = await createSyncAccessHandle(file);
			try {
				const bytes = new Uint8Array(handle.getSize());
				handle.read(bytes, { at: 0 });
				return bytes;
			} finally {
				handle.close();
			}
		} catch (error) {
			if (isNotFoundError(error)) {
				return undefined;
			}
			throw error;
		}
	}

	private async writeFile(
		name: string,
		bytes: Uint8Array,
		flush: boolean,
	): Promise<void> {
		const file = await this.directory.getFileHandle(name, { create: true });
		await this.writeHandle(file, bytes, flush);
	}

	private async writeHandle(
		file: FileSystemFileHandle,
		bytes: Uint8Array,
		flush: boolean,
	): Promise<void> {
		const handle = await createSyncAccessHandle(file);
		try {
			handle.write(bytes, { at: 0 });
			handle.truncate(bytes.byteLength);
			if (flush) {
				handle.flush();
			}
		} finally {
			handle.close();
		}
	}

	private async ensureManifestLoaded(): Promise<void> {
		if (this.manifestLoaded) {
			return;
		}
		const manifests = await Promise.all([
			this.readManifest(MANIFEST_A_FILE_NAME),
			this.readManifest(MANIFEST_B_FILE_NAME),
		]);
		const candidates = manifests
			.filter((manifest): manifest is ActiveManifest => manifest != null)
			.sort((a, b) => b.epoch - a.epoch);
		for (const manifest of candidates) {
			if (
				(await this.fileExists(manifest.snapshot)) &&
				(await this.fileExists(manifest.journal))
			) {
				this.activeManifest = manifest;
				break;
			}
		}
		this.manifestLoaded = true;
	}

	private async readManifest(
		slot: ActiveManifest["slot"],
	): Promise<ActiveManifest | undefined> {
		const bytes = await this.readFileIfExists(slot);
		return bytes ? decodeManifest(bytes, slot) : undefined;
	}

	private async fileExists(name: string): Promise<boolean> {
		try {
			await this.directory.getFileHandle(name);
			return true;
		} catch (error) {
			if (isNotFoundError(error)) {
				return false;
			}
			throw error;
		}
	}

	private async cleanupCheckpoints(
		previous: ActiveManifest | undefined,
		current: ActiveManifest,
	): Promise<void> {
		await Promise.allSettled([
			this.removeEntryIfExists(SNAPSHOT_FILE_NAME),
			this.removeEntryIfExists(SNAPSHOT_TEMP_FILE_NAME),
			this.removeEntryIfExists(JOURNAL_FILE_NAME),
			previous?.snapshot && previous.snapshot !== current.snapshot
				? this.removeEntryIfExists(previous.snapshot)
				: Promise.resolve(),
			previous?.journal && previous.journal !== current.journal
				? this.removeEntryIfExists(previous.journal)
				: Promise.resolve(),
		]);
	}

	private async removeEntryIfExists(name: string, recursive = false): Promise<void> {
		try {
			await this.directory.removeEntry(name, { recursive });
		} catch (error) {
			if (!isNotFoundError(error)) {
				throw error;
			}
		}
	}
}

const createSyncAccessHandle = async (
	file: FileSystemFileHandle,
): Promise<FileSystemSyncAccessHandle> => {
	if (typeof file.createSyncAccessHandle !== "function") {
		throw new Error(
			"OPFS persistence requires createSyncAccessHandle, normally available inside a dedicated worker",
		);
	}
	let lastError: unknown;
	for (let attempt = 0; attempt < 20; attempt++) {
		try {
			return await file.createSyncAccessHandle();
		} catch (error) {
			lastError = error;
			await sleep(25);
		}
	}
	throw lastError;
};

const getOpfsLevelDirectory = async (
	rootName: string,
	level: string[],
): Promise<FileSystemDirectoryHandle> => {
	const navigatorLike = globalThis.navigator as
		| { storage?: { getDirectory?: () => Promise<FileSystemDirectoryHandle> } }
		| undefined;
	if (!navigatorLike?.storage?.getDirectory) {
		throw new Error("OPFS persistence requires navigator.storage.getDirectory");
	}
	let current = await navigatorLike.storage.getDirectory();
	current = await current.getDirectoryHandle(encodePathPart(rootName), {
		create: true,
	});
	for (const part of level) {
		current = await current.getDirectoryHandle(SUBLEVEL_DIRECTORY_NAME, {
			create: true,
		});
		current = await current.getDirectoryHandle(encodePathPart(part), {
			create: true,
		});
	}
	return current;
};

export const createPersistenceBackend = async (
	directory: string,
	level: string[],
): Promise<RustAnyStorePersistenceBackend> => {
	if (isNodeRuntime()) {
		return new NodePersistenceBackend(directory, level);
	}
	return new OpfsPersistenceBackend(await getOpfsLevelDirectory(directory, level));
};
