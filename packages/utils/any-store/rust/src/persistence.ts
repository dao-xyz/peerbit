export type PersistenceDurability = "normal" | "strict";

export interface RustAnyStorePersistenceBackend {
	readSnapshot(): Promise<Uint8Array | undefined>;
	readJournal(): Promise<Uint8Array | undefined>;
	appendJournal(
		record: Uint8Array,
		durability: PersistenceDurability,
	): Promise<void>;
	/**
	 * Remove an unreadable journal tail and make the new length durable before
	 * any later record can be appended.
	 */
	truncateJournal(byteLength: number): Promise<void>;
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
	encodeURIComponent(part).replace(
		/[!'()*]/g,
		(char) => `%${char.charCodeAt(0).toString(16).toUpperCase()}`,
	);

const isNodeRuntime = () =>
	Boolean(
		(globalThis as { process?: { versions?: { node?: string } } }).process
			?.versions?.node,
	);

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
		JSON.stringify({
			payload: JSON.parse(payload),
			checksum: checksumString(payload),
		}),
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

type NodeFsModule = typeof import("fs/promises");

let nodeFsModule: Promise<NodeFsModule> | undefined;
const importNodeFs = (): Promise<NodeFsModule> => {
	if (!nodeFsModule) {
		const fsPromises = "fs/promises";
		nodeFsModule = import(
			/* @vite-ignore */ fsPromises
		) as Promise<NodeFsModule>;
	}
	return nodeFsModule;
};

let nodePathJoin: Promise<(...parts: string[]) => string> | undefined;
const importNodePathJoin = (): Promise<(...parts: string[]) => string> => {
	if (!nodePathJoin) {
		const pathModule = "path";
		nodePathJoin = (
			import(/* @vite-ignore */ pathModule) as Promise<typeof import("path")>
		).then((mod) => mod.join);
	}
	return nodePathJoin;
};

const readNodeFileIfExists = async (
	path: string,
): Promise<Uint8Array | undefined> => {
	const { readFile } = await importNodeFs();
	try {
		return new Uint8Array(await readFile(path));
	} catch (error) {
		if (isNotFoundError(error)) {
			return undefined;
		}
		throw error;
	}
};

const validateWriteProgress = (
	written: number,
	remaining: number,
	target: string,
): number => {
	if (!Number.isSafeInteger(written) || written <= 0 || written > remaining) {
		throw new Error(
			`${target} write made invalid progress: wrote ${written} of ${remaining} remaining bytes`,
		);
	}
	return written;
};

export class NodePersistenceBackend implements RustAnyStorePersistenceBackend {
	private journalHandle?: import("fs/promises").FileHandle;
	private journalOffset?: number;
	private journalPoison?: unknown;
	private levelDirectoryPath?: string;
	private sublevelsDirectoryPath?: string;
	private snapshotFilePath?: string;
	private snapshotTempFilePath?: string;
	private journalFilePath?: string;
	private directoryEnsured = false;

	constructor(
		private readonly rootDirectory: string,
		private readonly level: string[],
	) {}

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
		if (this.journalPoison !== undefined) {
			throw this.journalPoison;
		}
		await this.ensureLevelDirectory();
		await this.ensureJournalHandle();
		const handle = this.journalHandle!;
		const offset = this.journalOffset ?? 0;
		let written = 0;
		try {
			while (written < record.byteLength) {
				const remaining = record.byteLength - written;
				const result = await handle.write(
					record,
					written,
					remaining,
					offset + written,
				);
				written += validateWriteProgress(
					result.bytesWritten,
					remaining,
					"Node persistence journal",
				);
			}
			if (durability === "strict") {
				await handle.sync();
			}
			this.journalOffset = offset + written;
		} catch (error) {
			try {
				// A rejected record must be completely absent before another write is
				// allowed. Persist the rollback even for normal durability: otherwise a
				// later valid record could be stranded behind an unreadable torn tail.
				await handle.truncate(offset);
				await handle.sync();
				this.journalOffset = offset;
			} catch (rollbackError) {
				this.journalPoison = new AggregateError(
					[error, rollbackError],
					"Node persistence journal rollback failed; reopen is required",
				);
				throw this.journalPoison;
			}
			throw error;
		}
	}

	async truncateJournal(byteLength: number): Promise<void> {
		if (!Number.isSafeInteger(byteLength) || byteLength < 0) {
			throw new Error(`Invalid Node persistence journal length: ${byteLength}`);
		}
		if (this.journalPoison !== undefined) {
			throw this.journalPoison;
		}
		await this.ensureLevelDirectory();
		await this.ensureJournalHandle();
		try {
			await this.journalHandle!.truncate(byteLength);
			await this.journalHandle!.sync();
			this.journalOffset = byteLength;
		} catch (error) {
			this.journalPoison = error;
			throw error;
		}
	}

	async writeSnapshot(snapshot: Uint8Array): Promise<void> {
		await this.close();
		await this.ensureLevelDirectory();
		const { rename, writeFile } = await importNodeFs();
		const tempPath = await this.snapshotTempPath();
		await writeFile(tempPath, snapshot);
		await rename(tempPath, await this.snapshotPath());
		await writeFile(await this.journalPath(), new Uint8Array());
		this.journalOffset = 0;
	}

	async removeSublevels(): Promise<void> {
		const { rm } = await importNodeFs();
		await rm(await this.sublevelsDirectory(), { recursive: true, force: true });
	}

	async close(): Promise<void> {
		this.directoryEnsured = false;
		this.journalOffset = undefined;
		this.journalPoison = undefined;
		if (!this.journalHandle) {
			return;
		}
		const handle = this.journalHandle;
		this.journalHandle = undefined;
		await handle.close();
	}

	private async ensureLevelDirectory(): Promise<void> {
		// Success-only memo: a failed mkdir must retry on the next call.
		if (this.directoryEnsured) {
			return;
		}
		const { mkdir } = await importNodeFs();
		await mkdir(await this.levelDirectory(), { recursive: true });
		this.directoryEnsured = true;
	}

	private async ensureJournalHandle(): Promise<void> {
		if (this.journalHandle) {
			return;
		}
		const { open, stat } = await importNodeFs();
		const path = await this.journalPath();
		try {
			// O_APPEND ignores positional offsets on Linux. Use a positional
			// read/write handle so a retry can never append behind a torn tail.
			this.journalHandle = await open(path, "r+");
		} catch (error) {
			if (!isNotFoundError(error)) {
				throw error;
			}
			this.journalHandle = await open(path, "w+");
		}
		if (this.journalOffset == null) {
			try {
				this.journalOffset = (await stat(path)).size;
			} catch {
				this.journalOffset = 0;
			}
		}
	}

	private async levelDirectory(): Promise<string> {
		if (this.levelDirectoryPath === undefined) {
			const join = await importNodePathJoin();
			let current = this.rootDirectory;
			for (const part of this.level) {
				current = join(current, SUBLEVEL_DIRECTORY_NAME, encodePathPart(part));
			}
			this.levelDirectoryPath = current;
		}
		return this.levelDirectoryPath;
	}

	private async sublevelsDirectory(): Promise<string> {
		this.sublevelsDirectoryPath ??= (await importNodePathJoin())(
			await this.levelDirectory(),
			SUBLEVEL_DIRECTORY_NAME,
		);
		return this.sublevelsDirectoryPath;
	}

	private async snapshotPath(): Promise<string> {
		this.snapshotFilePath ??= (await importNodePathJoin())(
			await this.levelDirectory(),
			SNAPSHOT_FILE_NAME,
		);
		return this.snapshotFilePath;
	}

	private async snapshotTempPath(): Promise<string> {
		this.snapshotTempFilePath ??= (await importNodePathJoin())(
			await this.levelDirectory(),
			SNAPSHOT_TEMP_FILE_NAME,
		);
		return this.snapshotTempFilePath;
	}

	private async journalPath(): Promise<string> {
		this.journalFilePath ??= (await importNodePathJoin())(
			await this.levelDirectory(),
			JOURNAL_FILE_NAME,
		);
		return this.journalFilePath;
	}
}

export class OpfsPersistenceBackend implements RustAnyStorePersistenceBackend {
	private journalHandle?: FileSystemSyncAccessHandle;
	private journalOffset?: number;
	private journalPoison?: unknown;
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
		if (this.journalPoison !== undefined) {
			throw this.journalPoison;
		}
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
		let written = 0;
		try {
			while (written < record.byteLength) {
				const remaining = record.byteLength - written;
				written += validateWriteProgress(
					this.journalHandle.write(record.subarray(written), {
						at: offset + written,
					}),
					remaining,
					"OPFS",
				);
			}
			if (durability === "strict") {
				this.journalHandle.flush();
			}
			this.journalOffset = offset + written;
		} catch (error) {
			try {
				this.journalHandle.truncate(offset);
				this.journalHandle.flush();
				this.journalOffset = offset;
			} catch (rollbackError) {
				this.journalPoison = new AggregateError(
					[error, rollbackError],
					"OPFS persistence journal rollback failed; reopen is required",
				);
				throw this.journalPoison;
			}
			throw error;
		}
	}

	async truncateJournal(byteLength: number): Promise<void> {
		if (!Number.isSafeInteger(byteLength) || byteLength < 0) {
			throw new Error(`Invalid OPFS persistence journal length: ${byteLength}`);
		}
		if (this.journalPoison !== undefined) {
			throw this.journalPoison;
		}
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
		}
		try {
			this.journalHandle.truncate(byteLength);
			this.journalHandle.flush();
			this.journalOffset = byteLength;
		} catch (error) {
			this.journalPoison = error;
			throw error;
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
		this.journalOffset = undefined;
		this.journalPoison = undefined;
		if (!this.journalHandle) {
			return;
		}
		const handle = this.journalHandle;
		this.journalHandle = undefined;
		this.journalFileName = undefined;
		handle.close();
	}

	private async readFileIfExists(
		name: string,
	): Promise<Uint8Array | undefined> {
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
		await this.writeHandle(file, bytes, flush, name);
	}

	private async writeHandle(
		file: FileSystemFileHandle,
		bytes: Uint8Array,
		flush: boolean,
		target: string,
	): Promise<void> {
		const handle = await createSyncAccessHandle(file);
		try {
			let written = 0;
			while (written < bytes.byteLength) {
				const remaining = bytes.byteLength - written;
				written += validateWriteProgress(
					handle.write(bytes.subarray(written), { at: written }),
					remaining,
					`OPFS persistence file ${target}`,
				);
			}
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

	private async removeEntryIfExists(
		name: string,
		recursive = false,
	): Promise<void> {
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
	return new OpfsPersistenceBackend(
		await getOpfsLevelDirectory(directory, level),
	);
};
