import { deserialize, serialize } from "@dao-xyz/borsh";
import { type AnyStore, type MaybePromise } from "@peerbit/any-store-interface";
import * as memory from "@peerbit/any-store-interface/messages";
import { v4 as uuid } from "uuid";
import { createWorker } from "./create.js";

function memoryIterator(
	client: {
		request<T extends memory.MemoryRequest>(
			request: memory.MemoryRequest,
		): Promise<T>;
	},
	level: string[],
): {
	[Symbol.asyncIterator]: () => AsyncIterator<[string, Uint8Array], void, void>;
} {
	return {
		[Symbol.asyncIterator]() {
			const iteratorId = uuid();
			return {
				next: async () => {
					const resp = await client.request<memory.RESP_Iterator_Next>(
						new memory.REQ_Iterator_Next({ id: iteratorId, level }),
					);
					if (resp.keys.length > 1) {
						throw new Error("Unsupported iteration response");
					}
					// Will only have 0 or 1 element for now
					// eslint-disable-next-line no-unreachable-loop
					for (let i = 0; i < resp.keys.length; i++) {
						return {
							done: false,
							value: [resp.keys[i], resp.values[i]] as [string, Uint8Array],
						} as { done: false; value: [string, Uint8Array] };
					}
					return { done: true, value: undefined } as {
						done: true;
						value: undefined;
					};
				},
				async return() {
					await client.request<memory.RESP_Iterator_Next>(
						new memory.REQ_Iterator_Stop({ id: iteratorId, level }),
					);
					return { done: true, value: undefined } as {
						done: true;
						value: undefined;
					};
				},
			};
		},
	};
}

export class OPFSStore implements AnyStore {
	worker: Worker;
	levelMap: Map<string, AnyStore>;
	root: AnyStore;

	private _responseCallbacks: Map<
		string,
		{ fn: (message: memory.MemoryRequest) => any; once: boolean }
	> = new Map();

	private _createStorage: (level: string[]) => AnyStore;
	constructor(readonly directory?: string) {
		this.levelMap = new Map();
		this._createStorage = (level: string[] = []): AnyStore => {
			return {
				clear: async () => {
					await this.request<memory.RESP_Clear>(
						new memory.REQ_Clear({ level }),
					);
				},
				del: async (key) => {
					await this.request<memory.RESP_Del>(
						new memory.REQ_Del({ level, key }),
					);
				},
				get: async (key) => {
					return (
						await this.request<memory.RESP_Get>(
							new memory.REQ_Get({ level, key }),
						)
					).bytes;
				},
				put: async (key, value) => {
					await this.request<memory.RESP_Put>(
						new memory.REQ_Put({ level, key, bytes: value }),
					);
				},
				status: async () =>
					(
						await this.request<memory.RESP_Status>(
							new memory.REQ_Status({ level }),
						)
					).status,
				sublevel: async (name) => {
					await this.request(new memory.REQ_Sublevel({ level, name }));
					const newLevels = [...level, name];
					const sublevel = this._createStorage(newLevels);
					this.levelMap.set(memory.levelKey(newLevels), sublevel);
					return sublevel;
				},

				iterator: () => memoryIterator(this, level),
				close: async () => {
					await this.request<memory.RESP_Close>(
						new memory.REQ_Close({ level }),
					);
					/*     this.levelMap.delete(memory.levelKey(level)); */
				},
				open: async () => {
					await this.request<memory.RESP_Open>(new memory.REQ_Open({ level }));
				},

				size: async () => {
					const size = await this.request<memory.RESP_Size>(
						new memory.REQ_Size({ level }),
					);
					return size.size;
				},
				persisted: directory != null ? () => true : () => false,
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
			/* if (
				!(globalThis as any)["__playwright_test__"] &&
				(await navigator.storage.persist()) === false
			) {
				throw new Error("OPFS not allowed to persist data");
			} */
			this.worker = createWorker(this.directory);
			this.root = this._createStorage([]);
			this.worker.addEventListener("message", async (ev) => {
				const message = deserialize(ev.data, memory.MemoryMessage);
				this._responseCallbacks.get(message.messageId)!.fn(message);
			});
			await this.root.open();
		}
	}
	get(key: string): MaybePromise<Uint8Array | undefined> {
		return this.root.get(key);
	}
	put(key: string, value: Uint8Array) {
		return this.root.put(key, value);
	}
	del(key: any): MaybePromise<void> {
		return this.root.del(key);
	}
	sublevel(name: string): AnyStore | MaybePromise<AnyStore> {
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

	size(): MaybePromise<number> {
		return this.root.size();
	}

	async request<T extends memory.MemoryRequest>(
		request: memory.MemoryRequest,
	): Promise<T> {
		return new Promise<T>((resolve, reject) => {
			const onResponse = (message: memory.MemoryRequest) => {
				this._responseCallbacks.delete(request.messageId);
				if (message instanceof memory.RESP_Error) {
					reject(new Error(message.error));
				} else {
					resolve(message as T);
				}
			};
			this._responseCallbacks.set(request.messageId, {
				fn: onResponse,
				once: true,
			});
			const bytes = serialize(request);
			this.worker.postMessage(bytes, [bytes.buffer]);
		});
	}

	persisted() {
		return this.root.persisted();
	}
}
