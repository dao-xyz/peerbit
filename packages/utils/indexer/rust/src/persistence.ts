import { deserialize, serialize, type AbstractType } from "@dao-xyz/borsh";
import * as types from "@peerbit/indexer-interface";

type NativeFsPromises = typeof import("fs/promises");

type SyncAccessHandle = {
	getSize?: () => number;
	truncate(size: number): void;
	write(buffer: Uint8Array, options?: { at?: number }): number;
	flush(): void;
	close(): void;
};

type SyncFileHandle = FileSystemFileHandle & {
	createSyncAccessHandle?: () => Promise<SyncAccessHandle>;
};

export type SnapshotFile = {
	read<T extends Record<string, any>>(schema: AbstractType<T>): Promise<T[]>;
	appendPut<T extends Record<string, any>>(
		key: string,
		value: T,
		schema: AbstractType<T>,
	): Promise<void>;
	appendDelete(key: string): Promise<void>;
	compact<T extends Record<string, any>>(
		values: T[],
		schema: AbstractType<T>,
	): Promise<void>;
	pendingOperations(): number;
	remove(): Promise<void>;
	persisted: true;
};

const SNAPSHOT_MAGIC = new TextEncoder().encode("PBRIDXS1");
const JOURNAL_MAGIC = new TextEncoder().encode("PBRIDXW1");
const SNAPSHOT_FILE_NAME = "index.bin";
const JOURNAL_FILE_NAME = "index.wal";
const SNAPSHOT_TEMP_FILE_NAME = "index.bin.tmp";

const enum JournalOperation {
	Put = 1,
	Delete = 2,
}

const encodePathPart = (part: string): string =>
	encodeURIComponent(part).replace(/[!'()*]/g, (char) =>
		`%${char.charCodeAt(0).toString(16).toUpperCase()}`,
	);

const isNode = () =>
	Boolean((globalThis as { process?: { versions?: { node?: string } } }).process
		?.versions?.node);

const normalizeBrowserDirectory = (directory: string): string[] =>
	directory
		.replace(/^\./, "")
		.split(/[\\/]+/)
		.filter(Boolean)
		.map(encodePathPart);

const keyToStoreKey = (id: types.IdKey): string => {
	const key = types.toIdeable(id);
	if (key instanceof Uint8Array || ArrayBuffer.isView(key)) {
		return `bytes:${id.primitive.toString()}`;
	}
	return `${typeof key}:${key.toString()}`;
};

const storeKeyFromValue = <T extends Record<string, any>>(
	value: T,
	indexBy: string[],
): string => {
	const id = types.toId(types.extractFieldValue(value, indexBy));
	return keyToStoreKey(id);
};

const setUint32 = (
	output: Uint8Array,
	offset: number,
	value: number,
): number => {
	new DataView(output.buffer, output.byteOffset, output.byteLength).setUint32(
		offset,
		value,
		true,
	);
	return offset + 4;
};

const getUint32 = (bytes: Uint8Array, offset: number): number =>
	new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength).getUint32(
		offset,
		true,
	);

const encodeString = (value: string): Uint8Array => new TextEncoder().encode(value);

const decodeString = (bytes: Uint8Array): string => new TextDecoder().decode(bytes);

const writeBytes = (
	output: Uint8Array,
	offset: number,
	bytes: Uint8Array,
): number => {
	offset = setUint32(output, offset, bytes.byteLength);
	output.set(bytes, offset);
	return offset + bytes.byteLength;
};

const readBytes = (
	bytes: Uint8Array,
	offset: number,
): { bytes: Uint8Array; offset: number } => {
	if (offset + 4 > bytes.byteLength) {
		throw new Error("Truncated length-prefixed bytes");
	}
	const length = getUint32(bytes, offset);
	offset += 4;
	const end = offset + length;
	if (end > bytes.byteLength) {
		throw new Error("Truncated length-prefixed value");
	}
	return { bytes: bytes.slice(offset, end), offset: end };
};

const hasMagic = (bytes: Uint8Array, magic: Uint8Array): boolean =>
	bytes.byteLength >= magic.byteLength &&
	magic.every((byte, i) => bytes[i] === byte);

const fnv1a = (bytes: Uint8Array): number => {
	let hash = 0x811c9dc5;
	for (const byte of bytes) {
		hash ^= byte;
		hash = Math.imul(hash, 0x01000193);
	}
	return hash >>> 0;
};

const encodeSnapshotPayload = <T extends Record<string, any>>(
	values: T[],
	schema: AbstractType<T>,
): Uint8Array => {
	const serialized = values.map((value) => serialize(value));
	const totalSize =
		4 + serialized.reduce((sum, bytes) => sum + 4 + bytes.byteLength, 0);
	const output = new Uint8Array(totalSize);
	const view = new DataView(output.buffer);
	let offset = 0;
	view.setUint32(offset, serialized.length, true);
	offset += 4;
	for (const bytes of serialized) {
		view.setUint32(offset, bytes.byteLength, true);
		offset += 4;
		output.set(bytes, offset);
		offset += bytes.byteLength;
	}
	return output;
};

const encodeSnapshot = <T extends Record<string, any>>(
	values: T[],
	schema: AbstractType<T>,
): Uint8Array => {
	const payload = encodeSnapshotPayload(values, schema);
	const output = new Uint8Array(
		SNAPSHOT_MAGIC.byteLength + 8 + payload.byteLength,
	);
	let offset = 0;
	output.set(SNAPSHOT_MAGIC, offset);
	offset += SNAPSHOT_MAGIC.byteLength;
	offset = setUint32(output, offset, payload.byteLength);
	offset = setUint32(output, offset, fnv1a(payload));
	output.set(payload, offset);
	return output;
};

const decodeSnapshotPayload = <T extends Record<string, any>>(
	bytes: Uint8Array,
	schema: AbstractType<T>,
): T[] => {
	if (bytes.byteLength === 0) {
		return [];
	}
	let offset = 0;
	if (offset + 4 > bytes.byteLength) {
		throw new Error("Truncated rust index snapshot header");
	}
	const count = getUint32(bytes, offset);
	offset += 4;
	const values: T[] = [];
	for (let i = 0; i < count; i++) {
		const next = readBytes(bytes, offset);
		offset = next.offset;
		const valueBytes = next.bytes;
		values.push(deserialize(valueBytes, schema));
	}
	return values;
};

const decodeSnapshot = <T extends Record<string, any>>(
	bytes: Uint8Array,
	schema: AbstractType<T>,
): T[] => {
	if (bytes.byteLength === 0) {
		return [];
	}
	if (!hasMagic(bytes, SNAPSHOT_MAGIC)) {
		return decodeSnapshotPayload(bytes, schema);
	}
	let offset = SNAPSHOT_MAGIC.byteLength;
	if (offset + 8 > bytes.byteLength) {
		throw new Error("Truncated rust index snapshot envelope");
	}
	const length = getUint32(bytes, offset);
	offset += 4;
	const checksum = getUint32(bytes, offset);
	offset += 4;
	const end = offset + length;
	if (end > bytes.byteLength || end !== bytes.byteLength) {
		throw new Error("Truncated rust index snapshot payload");
	}
	const payload = bytes.slice(offset, end);
	if (fnv1a(payload) !== checksum) {
		throw new Error("Rust index snapshot checksum mismatch");
	}
	return decodeSnapshotPayload(payload, schema);
};

const encodeJournalPayload = <T extends Record<string, any>>(
	operation: JournalOperation,
	key: string,
	schema?: AbstractType<T>,
	value?: T,
): Uint8Array => {
	const keyBytes = encodeString(key);
	const valueBytes = value && schema ? serialize(value) : new Uint8Array();
	const output = new Uint8Array(
		1 +
			4 +
			keyBytes.byteLength +
			(operation === JournalOperation.Put ? 4 + valueBytes.byteLength : 0),
	);
	let offset = 0;
	output[offset++] = operation;
	offset = writeBytes(output, offset, keyBytes);
	if (operation === JournalOperation.Put) {
		offset = writeBytes(output, offset, valueBytes);
	}
	return output;
};

const encodeJournalRecord = (payload: Uint8Array): Uint8Array => {
	const output = new Uint8Array(8 + payload.byteLength);
	let offset = 0;
	offset = setUint32(output, offset, payload.byteLength);
	offset = setUint32(output, offset, fnv1a(payload));
	output.set(payload, offset);
	return output;
};

const decodeJournalPayload = <T extends Record<string, any>>(
	payload: Uint8Array,
	schema: AbstractType<T>,
): { operation: JournalOperation; key: string; value?: T } => {
	let offset = 0;
	const operation = payload[offset++] as JournalOperation;
	const keyResult = readBytes(payload, offset);
	offset = keyResult.offset;
	const key = decodeString(keyResult.bytes);
	if (operation === JournalOperation.Delete) {
		return { operation, key };
	}
	if (operation !== JournalOperation.Put) {
		throw new Error(`Unknown rust index journal operation: ${operation}`);
	}
	const valueResult = readBytes(payload, offset);
	return {
		operation,
		key,
		value: deserialize(valueResult.bytes, schema),
	};
};

const decodeJournal = <T extends Record<string, any>>(
	bytes: Uint8Array,
	schema: AbstractType<T>,
): Array<{ operation: JournalOperation; key: string; value?: T }> => {
	if (bytes.byteLength === 0) {
		return [];
	}
	let offset = 0;
	if (hasMagic(bytes, JOURNAL_MAGIC)) {
		offset = JOURNAL_MAGIC.byteLength;
	}
	const operations: Array<{ operation: JournalOperation; key: string; value?: T }> =
		[];
	while (offset < bytes.byteLength) {
		if (offset + 8 > bytes.byteLength) {
			break;
		}
		const length = getUint32(bytes, offset);
		offset += 4;
		const checksum = getUint32(bytes, offset);
		offset += 4;
		const end = offset + length;
		if (end > bytes.byteLength) {
			break;
		}
		const payload = bytes.slice(offset, end);
		offset = end;
		if (fnv1a(payload) !== checksum) {
			break;
		}
		operations.push(decodeJournalPayload(payload, schema));
	}
	return operations;
};

const concatBytes = (chunks: Uint8Array[]): Uint8Array => {
	const total = chunks.reduce((sum, chunk) => sum + chunk.byteLength, 0);
	const output = new Uint8Array(total);
	let offset = 0;
	for (const chunk of chunks) {
		output.set(chunk, offset);
		offset += chunk.byteLength;
	}
	return output;
};

const replaySnapshotAndJournal = <T extends Record<string, any>>(
	snapshotValues: T[],
	journalBytes: Uint8Array,
	schema: AbstractType<T>,
	indexBy: string[],
): { values: T[]; operations: number } => {
	const entries = new Map<string, T>();
	for (const value of snapshotValues) {
		entries.set(storeKeyFromValue(value, indexBy), value);
	}
	const operations = decodeJournal(journalBytes, schema);
	for (const operation of operations) {
		if (operation.operation === JournalOperation.Delete) {
			entries.delete(operation.key);
		} else if (operation.value) {
			entries.set(operation.key, operation.value);
		}
	}
	return { values: [...entries.values()], operations: operations.length };
};

class NativeSnapshotFile implements SnapshotFile {
	readonly persisted = true;
	private operations = 0;
	private journalHandle?: Awaited<ReturnType<NativeFsPromises["open"]>>;
	private journalInitialized = false;

	constructor(
		readonly fs: NativeFsPromises,
		readonly snapshotPath: string,
		readonly journalPath: string,
		readonly tempSnapshotPath: string,
		readonly indexBy: string[],
	) {}

	async read<T extends Record<string, any>>(
		schema: AbstractType<T>,
	): Promise<T[]> {
		const snapshotValues = await this.readSnapshot(schema);
		const journalBytes = await this.readOptional(this.journalPath);
		const replayed = replaySnapshotAndJournal(
			snapshotValues,
			journalBytes,
			schema,
			this.indexBy,
		);
		this.operations = replayed.operations;
		return replayed.values;
	}

	async appendPut<T extends Record<string, any>>(
		key: string,
		value: T,
		schema: AbstractType<T>,
	): Promise<void> {
		await this.appendRecord(encodeJournalPayload(JournalOperation.Put, key, schema, value));
	}

	async appendDelete(key: string): Promise<void> {
		await this.appendRecord(encodeJournalPayload(JournalOperation.Delete, key));
	}

	async compact<T extends Record<string, any>>(
		values: T[],
		schema: AbstractType<T>,
	): Promise<void> {
		await this.closeJournal();
		const bytes = encodeSnapshot(values, schema);
		const dir = this.snapshotPath.slice(0, this.snapshotPath.lastIndexOf("/"));
		await this.fs.mkdir(dir, { recursive: true });
		const handle = await this.fs.open(this.tempSnapshotPath, "w");
		try {
			await handle.writeFile(bytes);
			await handle.sync();
		} finally {
			await handle.close();
		}
		await this.fs.rename(this.tempSnapshotPath, this.snapshotPath);
		await this.syncDirectory(dir);
		await this.fs.rm(this.journalPath, { force: true });
		await this.syncDirectory(dir);
		this.operations = 0;
	}

	pendingOperations(): number {
		return this.operations;
	}

	async remove(): Promise<void> {
		await this.closeJournal();
		await this.fs.rm(this.snapshotPath, { force: true });
		await this.fs.rm(this.journalPath, { force: true });
		await this.fs.rm(this.tempSnapshotPath, { force: true });
	}

	private async readSnapshot<T extends Record<string, any>>(
		schema: AbstractType<T>,
	): Promise<T[]> {
		const snapshot = await this.tryReadSnapshot(this.snapshotPath, schema);
		if (snapshot.ok) {
			return snapshot.values;
		}
		const tempSnapshot = await this.tryReadSnapshot(this.tempSnapshotPath, schema);
		if (tempSnapshot.ok) {
			return tempSnapshot.values;
		}
		if (snapshot.missing) {
			return [];
		}
		throw snapshot.error;
	}

	private async tryReadSnapshot<T extends Record<string, any>>(
		path: string,
		schema: AbstractType<T>,
	): Promise<
		| { ok: true; values: T[] }
		| { ok: false; missing: boolean; error: unknown }
	> {
		try {
			const bytes = await this.fs.readFile(path);
			return { ok: true, values: decodeSnapshot(new Uint8Array(bytes), schema) };
		} catch (error: any) {
			if (error?.code === "ENOENT") {
				return { ok: false, missing: true, error };
			}
			return { ok: false, missing: false, error };
		}
	}

	private async syncDirectory(path: string): Promise<void> {
		let handle:
			| Awaited<ReturnType<NativeFsPromises["open"]>>
			| undefined;
		try {
			handle = await this.fs.open(path, "r");
			await handle.sync();
		} catch {
			// Directory fsync is best-effort because not every platform allows it.
		} finally {
			await handle?.close();
		}
	}

	private async readOptional(path: string): Promise<Uint8Array> {
		try {
			return new Uint8Array(await this.fs.readFile(path));
		} catch (error: any) {
			if (error?.code === "ENOENT") {
				return new Uint8Array();
			}
			throw error;
		}
	}

	private async appendRecord(payload: Uint8Array): Promise<void> {
		const handle = await this.getJournalHandle();
		if (!this.journalInitialized) {
			await handle.write(JOURNAL_MAGIC);
			this.journalInitialized = true;
		}
		await handle.write(encodeJournalRecord(payload));
		await handle.sync();
		this.operations++;
	}

	private async getJournalHandle(): Promise<
		Awaited<ReturnType<NativeFsPromises["open"]>>
	> {
		if (this.journalHandle) {
			return this.journalHandle;
		}
		const dir = this.journalPath.slice(0, this.journalPath.lastIndexOf("/"));
		await this.fs.mkdir(dir, { recursive: true });
		let size = 0;
		try {
			size = (await this.fs.stat(this.journalPath)).size;
		} catch (error: any) {
			if (error?.code !== "ENOENT") {
				throw error;
			}
		}
		this.journalInitialized = size > 0;
		this.journalHandle = await this.fs.open(this.journalPath, "a");
		return this.journalHandle;
	}

	private async closeJournal(): Promise<void> {
		if (!this.journalHandle) {
			return;
		}
		try {
			await this.journalHandle.close();
		} finally {
			this.journalHandle = undefined;
			this.journalInitialized = false;
		}
	}
}

class OpfsSnapshotFile implements SnapshotFile {
	readonly persisted = true;
	private operations = 0;

	constructor(
		readonly path: string[],
		readonly snapshotFileName: string,
		readonly journalFileName: string,
		readonly tempSnapshotFileName: string,
		readonly indexBy: string[],
	) {}

	private async getDirectory(create: boolean): Promise<FileSystemDirectoryHandle> {
		if (!globalThis.navigator?.storage?.getDirectory) {
			throw new Error("OPFS is not available in this runtime");
		}
		let directory = await navigator.storage.getDirectory();
		for (const part of this.path) {
			directory = await directory.getDirectoryHandle(part, { create });
		}
		return directory;
	}

	async read<T extends Record<string, any>>(
		schema: AbstractType<T>,
	): Promise<T[]> {
		const snapshotValues = await this.readSnapshot(schema);
		const journalBytes = await this.readOptional(this.journalFileName);
		const replayed = replaySnapshotAndJournal(
			snapshotValues,
			journalBytes,
			schema,
			this.indexBy,
		);
		this.operations = replayed.operations;
		return replayed.values;
	}

	async appendPut<T extends Record<string, any>>(
		key: string,
		value: T,
		schema: AbstractType<T>,
	): Promise<void> {
		await this.appendRecord(encodeJournalPayload(JournalOperation.Put, key, schema, value));
	}

	async appendDelete(key: string): Promise<void> {
		await this.appendRecord(encodeJournalPayload(JournalOperation.Delete, key));
	}

	async compact<T extends Record<string, any>>(
		values: T[],
		schema: AbstractType<T>,
	): Promise<void> {
		const bytes = encodeSnapshot(values, schema);
		await this.writeFile(this.tempSnapshotFileName, bytes);
		await this.writeFile(this.snapshotFileName, bytes);
		await this.removeEntry(this.tempSnapshotFileName);
		await this.removeEntry(this.journalFileName);
		this.operations = 0;
	}

	pendingOperations(): number {
		return this.operations;
	}

	async remove(): Promise<void> {
		await this.removeEntry(this.snapshotFileName);
		await this.removeEntry(this.journalFileName);
		await this.removeEntry(this.tempSnapshotFileName);
	}

	private async readSnapshot<T extends Record<string, any>>(
		schema: AbstractType<T>,
	): Promise<T[]> {
		const snapshot = await this.tryReadSnapshot(this.snapshotFileName, schema);
		if (snapshot.ok) {
			return snapshot.values;
		}
		const tempSnapshot = await this.tryReadSnapshot(
			this.tempSnapshotFileName,
			schema,
		);
		if (tempSnapshot.ok) {
			return tempSnapshot.values;
		}
		if (snapshot.missing) {
			return [];
		}
		throw snapshot.error;
	}

	private async tryReadSnapshot<T extends Record<string, any>>(
		fileName: string,
		schema: AbstractType<T>,
	): Promise<
		| { ok: true; values: T[] }
		| { ok: false; missing: boolean; error: unknown }
	> {
		try {
			const directory = await this.getDirectory(false);
			const fileHandle = await directory.getFileHandle(fileName);
			const file = await fileHandle.getFile();
			return {
				ok: true,
				values: decodeSnapshot(new Uint8Array(await file.arrayBuffer()), schema),
			};
		} catch (error: any) {
			if (error?.name === "NotFoundError") {
				return { ok: false, missing: true, error };
			}
			return { ok: false, missing: false, error };
		}
	}

	private async readOptional(fileName: string): Promise<Uint8Array> {
		try {
			const directory = await this.getDirectory(false);
			const fileHandle = await directory.getFileHandle(fileName);
			const file = await fileHandle.getFile();
			return new Uint8Array(await file.arrayBuffer());
		} catch (error: any) {
			if (error?.name === "NotFoundError") {
				return new Uint8Array();
			}
			throw error;
		}
	}

	private async writeFile(fileName: string, bytes: Uint8Array): Promise<void> {
		const directory = await this.getDirectory(true);
		const fileHandle = (await directory.getFileHandle(fileName, {
			create: true,
		})) as SyncFileHandle;

		if (fileHandle.createSyncAccessHandle) {
			const access = await fileHandle.createSyncAccessHandle();
			try {
				access.truncate(0);
				access.write(bytes, { at: 0 });
				access.flush();
			} finally {
				access.close();
			}
			return;
		}

		const writable = await fileHandle.createWritable();
		const data = new ArrayBuffer(bytes.byteLength);
		new Uint8Array(data).set(bytes);
		await writable.write(data);
		await writable.close();
	}

	private async appendRecord(payload: Uint8Array): Promise<void> {
		const directory = await this.getDirectory(true);
		const fileHandle = (await directory.getFileHandle(this.journalFileName, {
			create: true,
		})) as SyncFileHandle;
		const record = encodeJournalRecord(payload);

		if (fileHandle.createSyncAccessHandle) {
			const access = await fileHandle.createSyncAccessHandle();
			try {
				const size =
					access.getSize?.() ?? (await fileHandle.getFile()).size;
				let offset = size;
				if (size === 0) {
					access.write(JOURNAL_MAGIC, { at: offset });
					offset += JOURNAL_MAGIC.byteLength;
				}
				access.write(record, { at: offset });
				access.flush();
			} finally {
				access.close();
			}
			this.operations++;
			return;
		}

		const existing = await this.readOptional(this.journalFileName);
		await this.writeFile(
			this.journalFileName,
			concatBytes([
				existing.byteLength === 0 ? JOURNAL_MAGIC : existing,
				record,
			]),
		);
		this.operations++;
	}

	private async removeEntry(fileName: string): Promise<void> {
		try {
			const directory = await this.getDirectory(false);
			await directory.removeEntry(fileName);
		} catch (error: any) {
			if (error?.name === "NotFoundError") {
				return;
			}
			throw error;
		}
	}
}

export const createSnapshotFile = async (
	directory: string | undefined,
	path: string[],
	indexBy: string[],
): Promise<SnapshotFile | undefined> => {
	if (!directory) {
		return undefined;
	}
	const encodedPath = [...path, ...indexBy].map(encodePathPart);
	// The current indexer API can reopen a persisted index with a narrower
	// variant schema than the one it was originally populated with. Keep the
	// first persistence slice index-scoped rather than schema-name-scoped.

	if (isNode()) {
		const fsPromises = "fs/promises";
		const fs = (await import(fsPromises)) as NativeFsPromises;
		const basePath = [
			directory.replace(/\/$/, ""),
			...encodedPath,
		].join("/");
		return new NativeSnapshotFile(
			fs,
			[basePath, SNAPSHOT_FILE_NAME].join("/"),
			[basePath, JOURNAL_FILE_NAME].join("/"),
			[basePath, SNAPSHOT_TEMP_FILE_NAME].join("/"),
			indexBy,
		);
	}

	return new OpfsSnapshotFile(
		["peerbit-indexer-rust", ...normalizeBrowserDirectory(directory), ...encodedPath],
		SNAPSHOT_FILE_NAME,
		JOURNAL_FILE_NAME,
		SNAPSHOT_TEMP_FILE_NAME,
		indexBy,
	);
};
