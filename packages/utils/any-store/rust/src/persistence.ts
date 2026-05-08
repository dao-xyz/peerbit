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

const readNodeFileIfExists = async (path: string): Promise<Uint8Array | undefined> => {
	const fsPromises = "fs/promises";
	const { readFile } = (await import(fsPromises)) as typeof import("fs/promises");
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
		const { open, stat } = (await import(fsPromises)) as typeof import("fs/promises");
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
		const { rename, writeFile } = (await import(fsPromises)) as typeof import("fs/promises");
		const tempPath = await this.snapshotTempPath();
		await writeFile(tempPath, snapshot);
		await rename(tempPath, await this.snapshotPath());
		await writeFile(await this.journalPath(), new Uint8Array());
		this.journalOffset = 0;
	}

	async removeSublevels(): Promise<void> {
		const fsPromises = "fs/promises";
		const { rm } = (await import(fsPromises)) as typeof import("fs/promises");
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
		const { mkdir } = (await import(fsPromises)) as typeof import("fs/promises");
		await mkdir(await this.levelDirectory(), { recursive: true });
	}

	private async levelDirectory(): Promise<string> {
		const pathModule = "path";
		const path = (await import(pathModule)) as typeof import("path");
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
		const path = (await import(pathModule)) as typeof import("path");
		return path.join(await this.levelDirectory(), SUBLEVEL_DIRECTORY_NAME);
	}

	private async snapshotPath(): Promise<string> {
		const pathModule = "path";
		const path = (await import(pathModule)) as typeof import("path");
		return path.join(await this.levelDirectory(), SNAPSHOT_FILE_NAME);
	}

	private async snapshotTempPath(): Promise<string> {
		const pathModule = "path";
		const path = (await import(pathModule)) as typeof import("path");
		return path.join(await this.levelDirectory(), SNAPSHOT_TEMP_FILE_NAME);
	}

	private async journalPath(): Promise<string> {
		const pathModule = "path";
		const path = (await import(pathModule)) as typeof import("path");
		return path.join(await this.levelDirectory(), JOURNAL_FILE_NAME);
	}
}

class OpfsPersistenceBackend implements RustAnyStorePersistenceBackend {
	private journalHandle?: FileSystemSyncAccessHandle;
	private journalOffset?: number;

	constructor(private readonly directory: FileSystemDirectoryHandle) {}

	async readSnapshot(): Promise<Uint8Array | undefined> {
		return this.readFileIfExists(SNAPSHOT_FILE_NAME);
	}

	async readJournal(): Promise<Uint8Array | undefined> {
		const journal = await this.readFileIfExists(JOURNAL_FILE_NAME);
		if (journal) {
			this.journalOffset = journal.byteLength;
		}
		return journal;
	}

	async appendJournal(
		record: Uint8Array,
		durability: PersistenceDurability,
	): Promise<void> {
		if (!this.journalHandle) {
			const file = await this.directory.getFileHandle(JOURNAL_FILE_NAME, {
				create: true,
			});
			this.journalHandle = await createSyncAccessHandle(file);
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
		const moved = await this.tryWriteSnapshotWithMove(snapshot);
		if (moved) {
			await this.writeFile(JOURNAL_FILE_NAME, new Uint8Array(), true);
			this.journalOffset = 0;
		} else {
			// Without an atomic OPFS move/replace primitive the WAL remains the
			// source of truth. Replaying a longer WAL is slower than risking a
			// torn checkpoint that shadows valid journal records.
			this.journalOffset = undefined;
		}
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

	private async tryWriteSnapshotWithMove(snapshot: Uint8Array): Promise<boolean> {
		const tempFile = await this.directory.getFileHandle(SNAPSHOT_TEMP_FILE_NAME, {
			create: true,
		});
		await this.writeHandle(tempFile, snapshot, true);
		const movable = tempFile as FileSystemFileHandle & {
			move?: (
				targetOrName: FileSystemDirectoryHandle | string,
				name?: string,
			) => Promise<void>;
		};
		if (typeof movable.move !== "function") {
			await this.removeEntryIfExists(SNAPSHOT_TEMP_FILE_NAME);
			return false;
		}
		try {
			await movable.move(this.directory, SNAPSHOT_FILE_NAME);
		} catch (error) {
			if (!(error instanceof TypeError)) {
				throw error;
			}
			await movable.move(SNAPSHOT_FILE_NAME);
		}
		return true;
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
