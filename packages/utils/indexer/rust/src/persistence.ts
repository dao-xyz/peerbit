import { deserialize, serialize, type AbstractType } from "@dao-xyz/borsh";
import * as types from "@peerbit/indexer-interface";

type NativeFs = typeof import("fs");

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
		encodedValue?: EncodedValue,
	): Promise<void>;
	appendPutBatch<T extends Record<string, any>>(
		values: Array<{
			key: string;
			value: T;
			encodedValue?: EncodedValue;
		}>,
		schema: AbstractType<T>,
	): Promise<void>;
	appendPutAndDeleteBatch<T extends Record<string, any>>(
		values: Array<{
			key: string;
			value: T;
			encodedValue?: EncodedValue;
			deleteKeys?: string[];
		}>,
		schema: AbstractType<T>,
	): Promise<void>;
	appendDelete(key: string): Promise<void>;
	appendDeleteBatch(keys: string[]): Promise<void>;
	compact<T extends Record<string, any>>(
		values: T[],
		schema: AbstractType<T>,
	): Promise<void>;
	pendingOperations(): number;
	remove(): Promise<void>;
	persisted: true;
};

export type EncodedValue =
	| Uint8Array
	| {
			prefix: Uint8Array;
			suffix: Uint8Array;
	  };

export type PersistenceDurability = "normal" | "strict";

export type PersistenceOptions = {
	durability?: PersistenceDurability;
	compactAfterOperations?: number;
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

const encodedValueLength = (value: EncodedValue): number =>
	value instanceof Uint8Array
		? value.byteLength
		: value.prefix.byteLength + value.suffix.byteLength;

const writeEncodedValue = (
	output: Uint8Array,
	offset: number,
	value: EncodedValue,
): number => {
	offset = setUint32(output, offset, encodedValueLength(value));
	if (value instanceof Uint8Array) {
		output.set(value, offset);
		return offset + value.byteLength;
	}
	output.set(value.prefix, offset);
	offset += value.prefix.byteLength;
	output.set(value.suffix, offset);
	return offset + value.suffix.byteLength;
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
	encodedValue?: EncodedValue,
): Uint8Array => {
	const keyBytes = encodeString(key);
	const valueBytes =
		encodedValue ?? (value && schema ? serialize(value) : new Uint8Array());
	const output = new Uint8Array(
		1 +
			4 +
			keyBytes.byteLength +
			(operation === JournalOperation.Put
				? 4 + encodedValueLength(valueBytes)
				: 0),
	);
	let offset = 0;
	output[offset++] = operation;
	offset = writeBytes(output, offset, keyBytes);
	if (operation === JournalOperation.Put) {
		offset = writeEncodedValue(output, offset, valueBytes);
	}
	return output;
};

const encodePutAndDeleteRecords = <T extends Record<string, any>>(
	values: Array<{
		key: string;
		value: T;
		encodedValue?: EncodedValue;
		deleteKeys?: string[];
	}>,
	schema: AbstractType<T>,
): Uint8Array[] => {
	const payloads: Uint8Array[] = [];
	for (const entry of values) {
		payloads.push(
			encodeJournalPayload(
				JournalOperation.Put,
				entry.key,
				schema,
				entry.value,
				entry.encodedValue,
			),
		);
		for (const deleteKey of entry.deleteKeys ?? []) {
			payloads.push(
				encodeJournalPayload(JournalOperation.Delete, deleteKey),
			);
		}
	}
	return payloads;
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
	private journalHandle?: number;
	private journalInitialized = false;

	constructor(
		readonly fs: NativeFs,
		readonly snapshotPath: string,
		readonly journalPath: string,
		readonly tempSnapshotPath: string,
		readonly indexBy: string[],
		readonly durability: PersistenceDurability,
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
		encodedValue?: EncodedValue,
	): Promise<void> {
		await this.appendRecord(
			encodeJournalPayload(
				JournalOperation.Put,
				key,
				schema,
				value,
				encodedValue,
			),
		);
	}

	async appendPutBatch<T extends Record<string, any>>(
		values: Array<{
			key: string;
			value: T;
			encodedValue?: EncodedValue;
		}>,
		schema: AbstractType<T>,
	): Promise<void> {
		await this.appendRecords(
			values.map((entry) =>
				encodeJournalPayload(
					JournalOperation.Put,
					entry.key,
					schema,
					entry.value,
					entry.encodedValue,
				),
			),
		);
	}

	async appendPutAndDeleteBatch<T extends Record<string, any>>(
		values: Array<{
			key: string;
			value: T;
			encodedValue?: EncodedValue;
			deleteKeys?: string[];
		}>,
		schema: AbstractType<T>,
	): Promise<void> {
		await this.appendRecords(encodePutAndDeleteRecords(values, schema));
	}

	async appendDelete(key: string): Promise<void> {
		await this.appendRecord(encodeJournalPayload(JournalOperation.Delete, key));
	}

	async appendDeleteBatch(keys: string[]): Promise<void> {
		await this.appendRecords(
			keys.map((key) => encodeJournalPayload(JournalOperation.Delete, key)),
		);
	}

	async compact<T extends Record<string, any>>(
		values: T[],
		schema: AbstractType<T>,
	): Promise<void> {
		await this.closeJournal();
		const bytes = encodeSnapshot(values, schema);
		const dir = this.snapshotPath.slice(0, this.snapshotPath.lastIndexOf("/"));
		this.fs.mkdirSync(dir, { recursive: true });
		const handle = this.fs.openSync(this.tempSnapshotPath, "w");
		try {
			this.writeAllSync(handle, bytes);
			this.fs.fsyncSync(handle);
		} finally {
			this.fs.closeSync(handle);
		}
		this.fs.renameSync(this.tempSnapshotPath, this.snapshotPath);
		this.syncDirectory(dir);
		this.fs.rmSync(this.journalPath, { force: true });
		this.syncDirectory(dir);
		this.operations = 0;
	}

	pendingOperations(): number {
		return this.operations;
	}

	async remove(): Promise<void> {
		await this.closeJournal();
		this.fs.rmSync(this.snapshotPath, { force: true });
		this.fs.rmSync(this.journalPath, { force: true });
		this.fs.rmSync(this.tempSnapshotPath, { force: true });
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
			const bytes = this.fs.readFileSync(path);
			return { ok: true, values: decodeSnapshot(new Uint8Array(bytes), schema) };
		} catch (error: any) {
			if (error?.code === "ENOENT") {
				return { ok: false, missing: true, error };
			}
			return { ok: false, missing: false, error };
		}
	}

	private syncDirectory(path: string): void {
		let handle: number | undefined;
		try {
			handle = this.fs.openSync(path, "r");
			this.fs.fsyncSync(handle);
		} catch {
			// Directory fsync is best-effort because not every platform allows it.
		} finally {
			if (handle !== undefined) {
				this.fs.closeSync(handle);
			}
		}
	}

	private async readOptional(path: string): Promise<Uint8Array> {
		try {
			return new Uint8Array(this.fs.readFileSync(path));
		} catch (error: any) {
			if (error?.code === "ENOENT") {
				return new Uint8Array();
			}
			throw error;
		}
	}

	private async appendRecord(payload: Uint8Array): Promise<void> {
		await this.appendRecords([payload]);
	}

	private async appendRecords(payloads: Uint8Array[]): Promise<void> {
		if (payloads.length === 0) {
			return;
		}
		const handle = await this.getJournalHandle();
		if (!this.journalInitialized) {
			this.writeAllSync(handle, JOURNAL_MAGIC);
			this.journalInitialized = true;
		}
		for (const payload of payloads) {
			this.writeAllSync(handle, encodeJournalRecord(payload));
		}
		if (this.durability === "strict") {
			this.fs.fsyncSync(handle);
		}
		this.operations += payloads.length;
	}

	private async getJournalHandle(): Promise<number> {
		if (this.journalHandle !== undefined) {
			return this.journalHandle;
		}
		const dir = this.journalPath.slice(0, this.journalPath.lastIndexOf("/"));
		this.fs.mkdirSync(dir, { recursive: true });
		let size = 0;
		try {
			size = this.fs.statSync(this.journalPath).size;
		} catch (error: any) {
			if (error?.code !== "ENOENT") {
				throw error;
			}
		}
		this.journalInitialized = size > 0;
		this.journalHandle = this.fs.openSync(this.journalPath, "a");
		return this.journalHandle;
	}

	private async closeJournal(): Promise<void> {
		if (this.journalHandle === undefined) {
			return;
		}
		try {
			this.fs.closeSync(this.journalHandle);
		} finally {
			this.journalHandle = undefined;
			this.journalInitialized = false;
		}
	}

	private writeAllSync(fd: number, bytes: Uint8Array): void {
		let offset = 0;
		while (offset < bytes.byteLength) {
			const written = this.fs.writeSync(
				fd,
				bytes,
				offset,
				bytes.byteLength - offset,
			);
			if (written <= 0) {
				throw new Error("Failed to write rust index persistence record");
			}
			offset += written;
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
		readonly durability: PersistenceDurability,
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
		encodedValue?: EncodedValue,
	): Promise<void> {
		await this.appendRecord(
			encodeJournalPayload(
				JournalOperation.Put,
				key,
				schema,
				value,
				encodedValue,
			),
		);
	}

	async appendPutBatch<T extends Record<string, any>>(
		values: Array<{
			key: string;
			value: T;
			encodedValue?: EncodedValue;
		}>,
		schema: AbstractType<T>,
	): Promise<void> {
		await this.appendRecords(
			values.map((entry) =>
				encodeJournalPayload(
					JournalOperation.Put,
					entry.key,
					schema,
					entry.value,
					entry.encodedValue,
				),
			),
		);
	}

	async appendPutAndDeleteBatch<T extends Record<string, any>>(
		values: Array<{
			key: string;
			value: T;
			encodedValue?: EncodedValue;
			deleteKeys?: string[];
		}>,
		schema: AbstractType<T>,
	): Promise<void> {
		await this.appendRecords(encodePutAndDeleteRecords(values, schema));
	}

	async appendDelete(key: string): Promise<void> {
		await this.appendRecord(encodeJournalPayload(JournalOperation.Delete, key));
	}

	async appendDeleteBatch(keys: string[]): Promise<void> {
		await this.appendRecords(
			keys.map((key) => encodeJournalPayload(JournalOperation.Delete, key)),
		);
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
		await this.appendRecords([payload]);
	}

	private async appendRecords(payloads: Uint8Array[]): Promise<void> {
		if (payloads.length === 0) {
			return;
		}
		const directory = await this.getDirectory(true);
		const fileHandle = (await directory.getFileHandle(this.journalFileName, {
			create: true,
		})) as SyncFileHandle;
		const records = payloads.map(encodeJournalRecord);

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
				for (const record of records) {
					access.write(record, { at: offset });
					offset += record.byteLength;
				}
				if (this.durability === "strict") {
					access.flush();
				}
			} finally {
				access.close();
			}
			this.operations += payloads.length;
			return;
		}

		if (await this.tryAppendWithWritableStream(fileHandle, records)) {
			this.operations += payloads.length;
			return;
		}

		const existing = await this.readOptional(this.journalFileName);
		await this.writeFile(
			this.journalFileName,
			concatBytes([
				existing.byteLength === 0 ? JOURNAL_MAGIC : existing,
				...records,
			]),
		);
		this.operations += payloads.length;
	}

	private async tryAppendWithWritableStream(
		fileHandle: FileSystemFileHandle,
		records: Uint8Array[],
	): Promise<boolean> {
		let writable: FileSystemWritableFileStream | undefined;
		try {
			const size = (await fileHandle.getFile()).size;
			writable = await fileHandle.createWritable({ keepExistingData: true });
			let offset = size;
			if (size === 0) {
				await writable.write({
					type: "write",
					position: offset,
					data: JOURNAL_MAGIC,
				});
				offset += JOURNAL_MAGIC.byteLength;
			}
			for (const record of records) {
				await writable.write({ type: "write", position: offset, data: record });
				offset += record.byteLength;
			}
			await writable.close();
			return true;
		} catch (error: any) {
			try {
				await writable?.abort();
			} catch {
				// Ignore abort failures; preserve the original append error.
			}
			if (!(error instanceof TypeError)) {
				throw error;
			}
			return false;
		}
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
	options: PersistenceOptions = {},
): Promise<SnapshotFile | undefined> => {
	if (!directory) {
		return undefined;
	}
	const durability = options.durability ?? "normal";
	const encodedPath = [...path, ...indexBy].map(encodePathPart);
	// The current indexer API can reopen a persisted index with a narrower
	// variant schema than the one it was originally populated with. Keep the
	// first persistence slice index-scoped rather than schema-name-scoped.

	if (isNode()) {
		const fsModule = "fs";
		const fs = (await import(fsModule)) as NativeFs;
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
			durability,
		);
	}

	return new OpfsSnapshotFile(
		["peerbit-indexer-rust", ...normalizeBrowserDirectory(directory), ...encodedPath],
		SNAPSHOT_FILE_NAME,
		JOURNAL_FILE_NAME,
		SNAPSHOT_TEMP_FILE_NAME,
		indexBy,
		durability,
	);
};
