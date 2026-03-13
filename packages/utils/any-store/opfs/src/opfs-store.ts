import { deserialize, serialize } from "@dao-xyz/borsh";
import { type AnyStore, type MaybePromise } from "@peerbit/any-store-interface";
import * as memory from "@peerbit/any-store-interface/messages";
import { v4 as uuid } from "uuid";
import { createWorker } from "./create.js";
import {
	getTransferables,
	type OPFSRequest,
	type OPFSResponse,
	type OPFSStoreProtocol,
	isOPFSResponse,
} from "./protocol.js";

function storeIterator(
	client: {
		request(
			request: OPFSRequest | memory.MemoryRequest,
		): Promise<OPFSResponse | memory.MemoryRequest>;
	},
	protocol: OPFSStoreProtocol,
	level: string[],
): {
	[Symbol.asyncIterator]: () => AsyncIterator<[string, Uint8Array], void, void>;
} {
	return {
		[Symbol.asyncIterator]() {
			const iteratorId = uuid();
			return {
				next: async () => {
					const response = await client.request(
						protocol === "legacy"
							? new memory.REQ_Iterator_Next({ id: iteratorId, level })
							: {
									type: "iterator-next",
									id: iteratorId,
									level,
									messageId: uuid(),
								},
					);
					const cloneResponse =
						isOPFSResponse(response) && response.type === "iterator-next"
							? response
							: undefined;
					const keys =
						response instanceof memory.RESP_Iterator_Next
							? response.keys
							: cloneResponse?.keys ?? [];
					const values =
						response instanceof memory.RESP_Iterator_Next
							? response.values
							: cloneResponse?.values ?? [];
					if (keys.length > 1) {
						throw new Error("Unsupported iteration response");
					}
					// Will only have 0 or 1 element for now
					// eslint-disable-next-line no-unreachable-loop
					for (let i = 0; i < keys.length; i++) {
						return {
							done: false,
							value: [keys[i], values[i]] as [string, Uint8Array],
						} as { done: false; value: [string, Uint8Array] };
					}
					return { done: true, value: undefined } as {
						done: true;
						value: undefined;
					};
				},
				async return() {
					await client.request(
						protocol === "legacy"
							? new memory.REQ_Iterator_Stop({ id: iteratorId, level })
							: {
									type: "iterator-stop",
									id: iteratorId,
									level,
									messageId: uuid(),
								},
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
	private readonly protocol: OPFSStoreProtocol;

	private _responseCallbacks: Map<
		string,
		{
			fn: (message: OPFSResponse | memory.MemoryRequest) => any;
			once: boolean;
		}
	> = new Map();

	private _createStorage: (level: string[]) => AnyStore;
	constructor(
		readonly directory?: string,
		options: { protocol?: OPFSStoreProtocol } = {},
	) {
		this.protocol = options.protocol ?? "clone";
		this.levelMap = new Map();
		this._createStorage = (level: string[] = []): AnyStore => {
			return {
				clear: async () => {
					await this.request(
						this.protocol === "legacy"
							? new memory.REQ_Clear({ level })
							: { type: "clear", level, messageId: uuid() },
					);
				},
				del: async (key) => {
					await this.request(
						this.protocol === "legacy"
							? new memory.REQ_Del({ level, key })
							: { type: "del", level, key, messageId: uuid() },
					);
				},
				get: async (key) => {
					const response = await this.request(
						this.protocol === "legacy"
							? new memory.REQ_Get({ level, key })
							: { type: "get", level, key, messageId: uuid() },
					);
					if (response instanceof memory.RESP_Get) {
						return response.bytes;
					}
					if (isOPFSResponse(response) && response.type === "get") {
						return response.bytes;
					}
					return undefined;
				},
				put: async (key, value) => {
					await this.request(
						this.protocol === "legacy"
							? new memory.REQ_Put({ level, key, bytes: value })
							: {
									type: "put",
									level,
									key,
									bytes: value,
									messageId: uuid(),
								},
					);
				},
				status: async () => {
					const response = await this.request(
						this.protocol === "legacy"
							? new memory.REQ_Status({ level })
							: { type: "status", level, messageId: uuid() },
					);
					if (response instanceof memory.RESP_Status) {
						return response.status;
					}
					if (!isOPFSResponse(response) || response.type !== "status") {
						throw new Error("Unexpected OPFS status response");
					}
					return response.status;
				},
				sublevel: async (name) => {
					await this.request(
						this.protocol === "legacy"
							? new memory.REQ_Sublevel({ level, name })
							: { type: "sublevel", level, name, messageId: uuid() },
					);
					const newLevels = [...level, name];
					const sublevel = this._createStorage(newLevels);
					this.levelMap.set(memory.levelKey(newLevels), sublevel);
					return sublevel;
				},

				iterator: () => storeIterator(this, this.protocol, level),
				close: async () => {
					await this.request(
						this.protocol === "legacy"
							? new memory.REQ_Close({ level })
							: { type: "close", level, messageId: uuid() },
					);
					/*     this.levelMap.delete(memory.levelKey(level)); */
				},
				open: async () => {
					await this.request(
						this.protocol === "legacy"
							? new memory.REQ_Open({ level })
							: { type: "open", level, messageId: uuid() },
					);
				},

				size: async () => {
					const response = await this.request(
						this.protocol === "legacy"
							? new memory.REQ_Size({ level })
							: { type: "size", level, messageId: uuid() },
					);
					if (response instanceof memory.RESP_Size) {
						return response.size;
					}
					if (!isOPFSResponse(response) || response.type !== "size") {
						throw new Error("Unexpected OPFS size response");
					}
					return response.size;
				},
				persisted: directory != null ? () => true : () => false,
			};
		};
	}
	status() {
		return this.worker ? this.root.status() : "closed";
	}
	async close(): Promise<void> {
		this.worker?.terminate();
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
				const message = isOPFSResponse(ev.data)
					? ev.data
					: deserialize(
							ev.data instanceof Uint8Array
								? ev.data
								: new Uint8Array(ev.data as ArrayBuffer),
							memory.MemoryMessage,
						);
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

	async request(
		request: OPFSRequest | memory.MemoryRequest,
	): Promise<OPFSResponse | memory.MemoryRequest> {
		return new Promise<OPFSResponse | memory.MemoryRequest>((resolve, reject) => {
			const onResponse = (message: OPFSResponse | memory.MemoryRequest) => {
				this._responseCallbacks.delete(request.messageId);
				if (message instanceof memory.RESP_Error) {
					reject(new Error(message.error));
				} else if (isOPFSResponse(message) && message.type === "error") {
					reject(new Error(message.error));
				} else {
					resolve(message);
				}
			};
			this._responseCallbacks.set(request.messageId, {
				fn: onResponse,
				once: true,
			});
			if (request instanceof memory.MemoryRequest) {
				const bytes = serialize(request);
				this.worker.postMessage(bytes, [bytes.buffer]);
				return;
			}
			this.worker.postMessage(request, getTransferables(request));
		});
	}

	persisted() {
		return this.root.persisted();
	}
}
