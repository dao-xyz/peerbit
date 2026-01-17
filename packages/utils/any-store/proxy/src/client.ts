import { deserialize, serialize } from "@dao-xyz/borsh";
import type { AnyStore } from "@peerbit/any-store-interface";
import * as memory from "@peerbit/any-store-interface/messages";
import type { CanonicalClient } from "@peerbit/canonical-client";
import { v4 as uuid } from "uuid";

type Pending<T> = {
	resolve: (value: T) => void;
	reject: (error: Error) => void;
};

export type AnyStoreProxy = AnyStore & {
	closePort: () => void;
};

const memoryIterator = (
	client: {
		request<T extends memory.MemoryRequest>(
			request: memory.MemoryRequest,
		): Promise<T>;
	},
	level: string[],
): {
	[Symbol.asyncIterator]: () => AsyncIterator<[string, Uint8Array], void, void>;
} => {
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
};

export const openAnyStore = async (options: {
	client: CanonicalClient;
	moduleName?: string;
	payload?: Uint8Array;
}): Promise<AnyStoreProxy> => {
	const moduleName = options.moduleName ?? "@peerbit/any-store";
	const payload = options.payload ?? new Uint8Array();

	const channel = await options.client.openPort(moduleName, payload);

	const pending = new Map<string, Pending<memory.MemoryRequest>>();
	let closed = false;

	const onMessage = (bytes: Uint8Array) => {
		const message = deserialize(bytes, memory.MemoryRequest);
		const entry = pending.get(message.messageId);
		if (!entry) return;
		pending.delete(message.messageId);
		if (message instanceof memory.RESP_Error) {
			entry.reject(new Error(message.error));
			return;
		}
		entry.resolve(message);
	};

	let unsubscribe = () => {};
	let unsubscribeClose: (() => void) | undefined;

	const closePort = () => {
		if (closed) return;
		closed = true;
		unsubscribe();
		unsubscribeClose?.();
		channel.close?.();
		for (const [id, p] of pending) {
			p.reject(new Error("AnyStore proxy closed"));
			pending.delete(id);
		}
	};

	unsubscribe = channel.onMessage(onMessage);
	unsubscribeClose = channel.onClose?.(() => {
		closePort();
	});

	const request = async <T extends memory.MemoryRequest>(
		req: memory.MemoryRequest,
	): Promise<T> => {
		if (closed) {
			throw new Error("AnyStore proxy closed");
		}
		return new Promise<T>((resolve, reject) => {
			pending.set(req.messageId, { resolve: resolve as any, reject });
			try {
				channel.send(serialize(req));
			} catch (e: any) {
				pending.delete(req.messageId);
				reject(e);
			}
		});
	};

	const levelMap = new Map<string, AnyStoreProxy>();

	const createStore = (level: string[]): AnyStoreProxy => {
		const store: AnyStoreProxy = {
			status: async () => {
				const resp = await request<memory.RESP_Status>(
					new memory.REQ_Status({ level }),
				);
				return resp.status;
			},
			open: async () => {
				await request(new memory.REQ_Open({ level }));
			},
			close: async () => {
				await request(new memory.REQ_Close({ level }));
			},
			get: async (key) => {
				const resp = await request<memory.RESP_Get>(
					new memory.REQ_Get({ level, key }),
				);
				return resp.bytes;
			},
			put: async (key, value) => {
				await request(new memory.REQ_Put({ level, key, bytes: value }));
			},
			del: async (key) => {
				await request(new memory.REQ_Del({ level, key }));
			},
			sublevel: async (name) => {
				await request(new memory.REQ_Sublevel({ level, name }));
				const newLevel = [...level, name];
				const key = memory.levelKey(newLevel);
				const existing = levelMap.get(key);
				if (existing) return existing;
				const next = createStore(newLevel);
				levelMap.set(key, next);
				return next;
			},
			iterator: () => memoryIterator({ request }, level),
			clear: async () => {
				await request(new memory.REQ_Clear({ level }));
			},
			size: async () => {
				const resp = await request<memory.RESP_Size>(
					new memory.REQ_Size({ level }),
				);
				return resp.size;
			},
			persisted: async () => {
				const resp = await request<memory.RESP_Persisted>(
					new memory.REQ_Persisted({ level }),
				);
				return resp.persisted;
			},
			closePort,
		};
		return store;
	};

	const root = createStore([]);
	levelMap.set(memory.levelKey([]), root);
	return root;
};
