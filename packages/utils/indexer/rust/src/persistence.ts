import { deserialize, serialize, type AbstractType } from "@dao-xyz/borsh";

type NativeFsPromises = typeof import("fs/promises");

type SyncAccessHandle = {
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
	write<T extends Record<string, any>>(
		values: T[],
		schema: AbstractType<T>,
	): Promise<void>;
	remove(): Promise<void>;
	persisted: true;
};

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

const encodeSnapshot = <T extends Record<string, any>>(
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

const decodeSnapshot = <T extends Record<string, any>>(
	bytes: Uint8Array,
	schema: AbstractType<T>,
): T[] => {
	if (bytes.byteLength === 0) {
		return [];
	}
	const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
	let offset = 0;
	const count = view.getUint32(offset, true);
	offset += 4;
	const values: T[] = [];
	for (let i = 0; i < count; i++) {
		const length = view.getUint32(offset, true);
		offset += 4;
		const valueBytes = bytes.slice(offset, offset + length);
		offset += length;
		values.push(deserialize(valueBytes, schema));
	}
	return values;
};

class NativeSnapshotFile implements SnapshotFile {
	readonly persisted = true;

	constructor(
		readonly fs: NativeFsPromises,
		readonly filePath: string,
	) {}

	async read<T extends Record<string, any>>(
		schema: AbstractType<T>,
	): Promise<T[]> {
		try {
			const bytes = await this.fs.readFile(this.filePath);
			return decodeSnapshot(new Uint8Array(bytes), schema);
		} catch (error: any) {
			if (error?.code === "ENOENT") {
				return [];
			}
			throw error;
		}
	}

	async write<T extends Record<string, any>>(
		values: T[],
		schema: AbstractType<T>,
	): Promise<void> {
		const dir = this.filePath.slice(0, this.filePath.lastIndexOf("/"));
		await this.fs.mkdir(dir, { recursive: true });
		await this.fs.writeFile(this.filePath, encodeSnapshot(values, schema));
	}

	async remove(): Promise<void> {
		await this.fs.rm(this.filePath, { force: true });
	}
}

class OpfsSnapshotFile implements SnapshotFile {
	readonly persisted = true;

	constructor(
		readonly path: string[],
		readonly fileName: string,
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
		try {
			const directory = await this.getDirectory(false);
			const fileHandle = await directory.getFileHandle(this.fileName);
			const file = await fileHandle.getFile();
			return decodeSnapshot(new Uint8Array(await file.arrayBuffer()), schema);
		} catch (error: any) {
			if (error?.name === "NotFoundError") {
				return [];
			}
			throw error;
		}
	}

	async write<T extends Record<string, any>>(
		values: T[],
		schema: AbstractType<T>,
	): Promise<void> {
		const directory = await this.getDirectory(true);
		const fileHandle = (await directory.getFileHandle(this.fileName, {
			create: true,
		})) as SyncFileHandle;
		const bytes = encodeSnapshot(values, schema);

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

	async remove(): Promise<void> {
		try {
			const directory = await this.getDirectory(false);
			await directory.removeEntry(this.fileName);
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
	const fileName = "index.bin";

	if (isNode()) {
		const fsPromises = "fs/promises";
		const fs = (await import(fsPromises)) as NativeFsPromises;
		const filePath = [
			directory.replace(/\/$/, ""),
			...encodedPath,
			fileName,
		].join("/");
		return new NativeSnapshotFile(fs, filePath);
	}

	return new OpfsSnapshotFile(
		["peerbit-indexer-rust", ...normalizeBrowserDirectory(directory), ...encodedPath],
		fileName,
	);
};
