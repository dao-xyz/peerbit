import { deserialize, serialize } from "@dao-xyz/borsh";
import { AnyStore } from "./interface.js";
import * as memory from "./opfs-worker-messages.js";
import { fromBase64URL, toBase64URL } from "@peerbit/crypto";
import { BinaryReader, BinaryWriter } from "@dao-xyz/borsh";
import { waitForResolved } from "@peerbit/time";
const encodeName = (name: string): string => {
	// since "/" and perhaps other characters might not be allowed we do encode
	const writer = new BinaryWriter();
	writer.string(name);
	return toBase64URL(writer.finalize());
};

const decodeName = (name: string): string => {
	// since "/" and perhaps other characters might not be allowed we do encode
	const writer = new BinaryReader(fromBase64URL(name));
	return writer.string();
};

const waitForSyncAcccess = async (
	fileHandle: FileSystemFileHandle
): Promise<FileSystemSyncAccessHandle> => {
	try {
		const handle = await fileHandle.createSyncAccessHandle();
		return handle;
	} catch (error) {
		const handle = await waitForResolved(() =>
			fileHandle.createSyncAccessHandle()
		);
		if (!handle) {
			throw error;
		}
		return handle;
	}
};

const createWriteHandle = async (fileHandle: FileSystemFileHandle) => {
	//  In Chrome on GET DOMException: Failed to execute 'createWritable' on 'FileSystemFileHandle': Failed to create swap file
	// hence below is not used for now
	/*
	if (fileHandle.createWritable != null) {
		return fileHandle.createWritable({ keepExistingData: false });
	} */
	return waitForSyncAcccess(fileHandle);
};

export class OPFSStoreWorker {
	level: string[];
	private _levels: Map<string, AnyStore>;
	private _rootStore: AnyStore;

	private _memoryIterator: Map<
		string,
		AsyncIterator<[string, ArrayBuffer], void, void>
	>;

	constructor() {
		const postMessageFn = postMessage;
		this._memoryIterator = new Map();
		this._levels = new Map();
		const createMemory = (
			root?: FileSystemDirectoryHandle,
			level: string[] = []
		): AnyStore => {
			let isOpen = false;

			let m: FileSystemDirectoryHandle = root!;

			// 'open' | 'closed' is just a virtual thing since OPFS is always open as soone as we get the FileSystemDirectoryHandle
			// TODO remove status? or assume not storage adapters can be closed?
			const open = async () => {
				isOpen = true;
				m = m || (await navigator.storage.getDirectory());
			};

			return {
				clear: async () => {
					for await (const key of m.keys()) {
						m.removeEntry(key, { recursive: true });
					}
				},

				del: async (key: string) => {
					try {
						await m.removeEntry(encodeName(key), { recursive: true });
					} catch (error) {
						if (
							error instanceof DOMException &&
							error.name === "NotFoundError"
						) {
							return;
						} else {
							throw error;
						}
					}
				},

				get: async (key: string) => {
					try {
						const fileHandle = await m.getFileHandle(encodeName(key));
						const buffer = await (await fileHandle.getFile()).arrayBuffer();

						return new Uint8Array(buffer);
					} catch (error) {
						if (
							error instanceof DOMException &&
							error.name === "NotFoundError"
						) {
							return;
						} else {
							throw error;
						}
					}
				},
				put: async (key: string, value: Uint8Array) => {
					const fileHandle = await m.getFileHandle(encodeName(key), {
						create: true
					});
					const writeFileHandle = await createWriteHandle(fileHandle);
					await writeFileHandle.write(value);
					writeFileHandle.close();
				},

				size: async () => {
					let size = 0;
					for await (const value of m.values()) {
						if (value.kind === "file") {
							const handle = await waitForSyncAcccess(value);
							size += handle.getSize();
							handle.close();
						}
					}
					return size;
				},
				status: () => (isOpen ? "open" : "closed"),

				sublevel: async (name) => {
					const encodedName = encodeName(name);
					const fileHandle = await m.getDirectoryHandle(encodedName, {
						create: true
					});
					const sublevel = [...level, encodedName];
					const subMemory = createMemory(fileHandle, sublevel);
					this._levels.set(memory.levelKey(sublevel), subMemory);
					await subMemory.open();
					return subMemory;
				},

				async *iterator(): AsyncGenerator<[string, Uint8Array], void, void> {
					for await (const v of m.values()) {
						if (v.kind == "file") {
							yield [
								decodeName(v.name),
								new Uint8Array(await (await v.getFile()).arrayBuffer())
							];
						}
					}
				},
				close: async () => {
					isOpen = false;
					this._memoryIterator.clear();
				},
				open
			};
		};

		this._rootStore = createMemory();

		self.addEventListener("message", async (ev) => {
			const message = deserialize(ev["data"], memory.MemoryRequest);
			if (message instanceof memory.MemoryMessage) {
				const m =
					message.level.length === 0
						? this._rootStore
						: this._levels.get(
								memory.levelKey(message.level.map((x) => encodeName(x)))
							);
				if (!m) {
					throw new Error("Recieved memory message for an undefined level");
				} else if (message instanceof memory.REQ_Clear) {
					await m.clear();
					await this.respond(
						message,
						new memory.RESP_Clear({ level: message.level }),
						postMessageFn
					);
				} else if (message instanceof memory.REQ_Close) {
					await m.close();
					await this.respond(
						message,
						new memory.RESP_Close({ level: message.level }),
						postMessageFn
					);
				} else if (message instanceof memory.REQ_Del) {
					await m.del(message.key);
					await this.respond(
						message,
						new memory.RESP_Del({ level: message.level }),
						postMessageFn
					);
				} else if (message instanceof memory.REQ_Iterator_Next) {
					let iterator = this._memoryIterator.get(message.id);
					if (!iterator) {
						iterator = m.iterator()[Symbol.asyncIterator]();
						this._memoryIterator.set(message.id, iterator);
					}
					const next = await iterator.next();
					await this.respond(
						message,
						new memory.RESP_Iterator_Next({
							keys: next.done ? [] : [next.value[0]],
							values: next.done ? [] : [new Uint8Array(next.value[1])],
							level: message.level
						}),
						postMessageFn
					);
					if (next.done) {
						this._memoryIterator.delete(message.id);
					}
				} else if (message instanceof memory.REQ_Iterator_Stop) {
					this._memoryIterator.delete(message.id);
					await this.respond(
						message,
						new memory.RESP_Iterator_Stop({ level: message.level }),
						postMessageFn
					);
				} else if (message instanceof memory.REQ_Get) {
					const value = await m.get(message.key);
					await this.respond(
						message,
						new memory.RESP_Get({
							bytes: value ? new Uint8Array(value) : undefined,
							level: message.level
						}),
						postMessageFn
					);
				} else if (message instanceof memory.REQ_Open) {
					await m.open();
					await this.respond(
						message,
						new memory.RESP_Open({ level: message.level }),
						postMessageFn
					);
				} else if (message instanceof memory.REQ_Put) {
					await m.put(message.key, message.bytes);
					await this.respond(
						message,
						new memory.RESP_Put({ level: message.level }),
						postMessageFn
					);
				} else if (message instanceof memory.REQ_Size) {
					await this.respond(
						message,
						new memory.RESP_Size({
							size: await m.size(),
							level: message.level
						}),
						postMessageFn
					);
				} else if (message instanceof memory.REQ_Status) {
					await this.respond(
						message,
						new memory.RESP_Status({
							status: await m.status(),
							level: message.level
						}),
						postMessageFn
					);
				} else if (message instanceof memory.REQ_Sublevel) {
					await m.sublevel(message.name);

					await this.respond(
						message,
						new memory.RESP_Sublevel({ level: message.level }),
						postMessageFn
					);
				}
			}
		});
	}

	async respond(
		request: memory.MemoryRequest,
		response: memory.MemoryRequest,
		postMessageFn = postMessage
	) {
		response.messageId = request.messageId;
		const bytes = serialize(response);
		postMessageFn(bytes, { transfer: [bytes.buffer] });
	}
}

new OPFSStoreWorker();
