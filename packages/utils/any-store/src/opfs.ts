import { AnyStore, MaybePromise } from "./interface.js";
import * as memory from "./opfs-worker-messages.js";
import { v4 as uuid } from "uuid";
import { serialize, deserialize } from "@dao-xyz/borsh";

function memoryIterator(
	client: {
		request<T extends memory.MemoryRequest>(
			request: memory.MemoryRequest
		): Promise<T>;
	},
	level: string[]
): {
	[Symbol.asyncIterator]: () => AsyncIterator<[string, Uint8Array], void, void>;
} {
	return {
		[Symbol.asyncIterator]() {
			const iteratorId = uuid();
			return {
				next: async () => {
					const resp = await client.request<memory.RESP_Iterator_Next>(
						new memory.REQ_Iterator_Next({ id: iteratorId, level })
					);
					if (resp.keys.length > 1) {
						throw new Error("Unsupported iteration response");
					}
					// Will only have 0 or 1 element for now
					for (let i = 0; i < resp.keys.length; i++) {
						return {
							done: false,
							value: [resp.keys[i], resp.values[i]] as [string, Uint8Array]
						} as { done: false; value: [string, Uint8Array] };
					}
					return { done: true, value: undefined } as {
						done: true;
						value: undefined;
					};
				},
				async return() {
					await client.request<memory.RESP_Iterator_Next>(
						new memory.REQ_Iterator_Stop({ id: iteratorId, level })
					);
					return { done: true, value: undefined } as {
						done: true;
						value: undefined;
					};
				}
			};
		}
	};
}

const workerURL = new URL("./opfs-worker.js", import.meta.url);

/* new Worker(workerURL, { type: 'module' }) */
const createWorker = () => new Worker(workerURL, { type: "module" });
export class OPFSStore implements AnyStore {
	worker: Worker;
	level: string[];
	levelMap: Map<string, AnyStore>;
	root: AnyStore;

	private _responseCallbacks: Map<
		string,
		{ fn: (message: memory.MemoryRequest) => any; once: boolean }
	> = new Map();

	private _createMemory: (level: string[]) => AnyStore;
	constructor(level: string[] = []) {
		this.level = level;
		this.levelMap = new Map();
		this._createMemory = (level: string[] = []): AnyStore => {
			return {
				clear: async () => {
					await this.request<memory.RESP_Clear>(
						new memory.REQ_Clear({ level })
					);
				},
				del: async (key) => {
					await this.request<memory.RESP_Del>(
						new memory.REQ_Del({ level, key })
					);
				},
				get: async (key) => {
					return (
						await this.request<memory.RESP_Get>(
							new memory.REQ_Get({ level, key })
						)
					).bytes;
				},
				put: async (key, value) => {
					await this.request<memory.RESP_Put>(
						new memory.REQ_Put({ level, key, bytes: value })
					);
				},
				status: async () =>
					(
						await this.request<memory.RESP_Status>(
							new memory.REQ_Status({ level })
						)
					).status,
				sublevel: async (name) => {
					await this.request<memory.RESP_Sublevel>(
						new memory.REQ_Sublevel({ level, name })
					);
					const newLevels = [...level, name];
					const sublevel = this._createMemory(newLevels);
					this.levelMap.set(memory.levelKey(newLevels), sublevel);
					return sublevel;
				},

				iterator: () => memoryIterator(this, level),
				close: async () => {
					await this.request<memory.RESP_Close>(
						new memory.REQ_Close({ level })
					);
					/*     this.levelMap.delete(memory.levelKey(level)); */
				},
				open: async () => {
					await this.request<memory.RESP_Open>(new memory.REQ_Open({ level }));
				}
			};
		};
	}
	status() {
		return this.worker ? this.root.status() : "closed";
	}
	async close(): Promise<void> {
		this.worker.terminate();
		this.worker = undefined!;
		this._responseCallbacks.clear();
		this.levelMap.clear();
	}
	async open(): Promise<void> {
		if (!this.worker) {
			this.root = this._createMemory([]);
			this.worker = createWorker();
			this.worker.addEventListener("message", async (ev) => {
				const message = deserialize(ev.data, memory.MemoryMessage);
				this._responseCallbacks.get(message.messageId)!.fn(message);
			});
			await this.root.open();
		}
	}
	async get(key: string): Promise<Uint8Array | undefined> {
		return this.root.get(key);
	}
	async put(key: string, value: Uint8Array) {
		return this.root.put(key, value);
	}
	del(key: any): MaybePromise<void> {
		return this.root.del(key);
	}
	sublevel(name: string): AnyStore | Promise<AnyStore> {
		return this.root.sublevel(name);
	}
	iterator(): {
		[Symbol.asyncIterator]: () => AsyncIterator<
			[string, Uint8Array],
			void,
			void
		>;
	} {
		return this.root.iterator();
	}
	clear(): MaybePromise<void> {
		return this.root.clear();
	}

	async request<T extends memory.MemoryRequest>(
		request: memory.MemoryRequest
	): Promise<T> {
		return new Promise<T>((resolve, reject) => {
			const onResponse = (message: memory.MemoryRequest) => {
				this._responseCallbacks.delete(request.messageId);
				if (message instanceof memory.RESP_Error) {
					reject(message.error);
				} else {
					resolve(message as T);
				}
			};
			this._responseCallbacks.set(request.messageId, {
				fn: onResponse,
				once: true
			});
			const bytes = serialize(request);
			this.worker.postMessage(bytes, [bytes.buffer]);
		});
	}
}
