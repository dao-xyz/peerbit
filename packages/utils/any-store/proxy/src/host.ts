import { deserialize, serialize } from "@dao-xyz/borsh";
import type { AnyStore } from "@peerbit/any-store-interface";
import * as memory from "@peerbit/any-store-interface/messages";
import type {
	CanonicalChannel,
	CanonicalContext,
	CanonicalModule,
} from "@peerbit/canonical-host";

export type AnyStoreModuleOptions = {
	name?: string;
	createStore: (
		ctx: CanonicalContext,
		payload: Uint8Array,
	) => AnyStore | Promise<AnyStore>;
};

const requestLevel = (req: memory.MemoryRequest): string[] => {
	return req instanceof memory.MemoryMessage ? req.level : [];
};

export const createAnyStoreModule = (
	options: AnyStoreModuleOptions,
): CanonicalModule => {
	return {
		name: options.name ?? "@peerbit/any-store",
		open: async (
			ctx: CanonicalContext,
			channel: CanonicalChannel,
			payload: Uint8Array,
		) => {
			const store = await options.createStore(ctx, payload);
			let closed = false;
			const levels = new Map<string, AnyStore>();
			levels.set(memory.levelKey([]), store);

			const iterators = new Map<
				string,
				AsyncIterator<[string, Uint8Array], void, void>
			>();

			const respond = (
				req: memory.MemoryRequest,
				resp: memory.MemoryRequest,
			) => {
				resp.messageId = req.messageId;
				channel.send(serialize(resp));
			};

			const respondError = (req: memory.MemoryRequest, error: unknown) => {
				const level = requestLevel(req);
				const resp = new memory.RESP_Error({
					level,
					error: String((error as any)?.message ?? error),
				});
				respond(req, resp);
			};

			const getStore = (level: string[]): AnyStore | undefined => {
				if (level.length === 0) return store;
				return levels.get(memory.levelKey(level));
			};

			const closeAll = async () => {
				if (closed) return;
				closed = true;
				iterators.clear();
				const uniqueStores = new Set(levels.values());
				await Promise.allSettled(
					[...uniqueStores].map((store) =>
						typeof store?.close === "function" ? store.close() : undefined,
					),
				);
				levels.clear();
			};

			channel.onClose?.(() => {
				void closeAll();
			});

			channel.onMessage((bytes) => {
				if (closed) return;
				void (async () => {
					const message = deserialize(bytes, memory.MemoryRequest);
					if (!(message instanceof memory.MemoryMessage)) return;

					try {
						const m = getStore(message.level);
						if (!m) {
							throw new Error("Received request for unknown level");
						}

						if (message instanceof memory.REQ_Clear) {
							await m.clear();
							respond(message, new memory.RESP_Clear({ level: message.level }));
							return;
						}

						if (message instanceof memory.REQ_Close) {
							await m.close();
							respond(message, new memory.RESP_Close({ level: message.level }));
							return;
						}

						if (message instanceof memory.REQ_Del) {
							await m.del(message.key);
							respond(message, new memory.RESP_Del({ level: message.level }));
							return;
						}

						if (message instanceof memory.REQ_Iterator_Next) {
							let iterator = iterators.get(message.id);
							if (!iterator) {
								iterator = m.iterator()[Symbol.asyncIterator]();
								iterators.set(message.id, iterator);
							}
							const next: any = await iterator.next();
							respond(
								message,
								new memory.RESP_Iterator_Next({
									keys: next.done ? [] : [next.value[0]],
									values: next.done ? [] : [next.value[1]],
									level: message.level,
								}),
							);
							if (next.done) {
								iterators.delete(message.id);
							}
							return;
						}

						if (message instanceof memory.REQ_Iterator_Stop) {
							iterators.delete(message.id);
							respond(
								message,
								new memory.RESP_Iterator_Stop({ level: message.level }),
							);
							return;
						}

						if (message instanceof memory.REQ_Get) {
							respond(
								message,
								new memory.RESP_Get({
									bytes: await m.get(message.key),
									level: message.level,
								}),
							);
							return;
						}

						if (message instanceof memory.REQ_Open) {
							await m.open();
							respond(message, new memory.RESP_Open({ level: message.level }));
							return;
						}

						if (message instanceof memory.REQ_Put) {
							await m.put(message.key, message.bytes);
							respond(message, new memory.RESP_Put({ level: message.level }));
							return;
						}

						if (message instanceof memory.REQ_Status) {
							respond(
								message,
								new memory.RESP_Status({
									status: await m.status(),
									level: message.level,
								}),
							);
							return;
						}

						if (message instanceof memory.REQ_Sublevel) {
							const sublevel = await m.sublevel(message.name);
							const nextLevel = [...message.level, message.name];
							levels.set(memory.levelKey(nextLevel), sublevel);
							respond(
								message,
								new memory.RESP_Sublevel({ level: message.level }),
							);
							return;
						}

						if (message instanceof memory.REQ_Size) {
							respond(
								message,
								new memory.RESP_Size({
									size: await m.size(),
									level: message.level,
								}),
							);
							return;
						}

						if (message instanceof memory.REQ_Persisted) {
							respond(
								message,
								new memory.RESP_Persisted({
									persisted: await m.persisted(),
									level: message.level,
								}),
							);
							return;
						}

						if (message instanceof memory.REQ_Idle) {
							respond(message, new memory.RESP_Idle({ level: message.level }));
							return;
						}
					} catch (error) {
						respondError(message, error);
					}
				})();
			});
		},
	};
};
