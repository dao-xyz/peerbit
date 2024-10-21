import { type AbstractType, deserialize, serialize } from "@dao-xyz/borsh";
import { type PeerId } from "@libp2p/interface";
import { type Multiaddr } from "@multiformats/multiaddr";
import { type AnyStore } from "@peerbit/any-store-interface";
import { type Blocks } from "@peerbit/blocks-interface";
import { Ed25519Keypair } from "@peerbit/crypto";
import type { Indices } from "@peerbit/indexer-interface";
import { type Keychain } from "@peerbit/keychain";
import { type ProgramClient } from "@peerbit/program";
import {
	DataEvent,
	type PubSub,
	PublishEvent,
} from "@peerbit/pubsub-interface";
import * as blocks from "./blocks.js";
import type * as connection from "./connection.js";
import * as keychain from "./keychain.js";
import * as lifecycle from "./lifecycle.js";
import { Message } from "./message.js";
import * as native from "./native.js";
import * as network from "./network.js";
import * as pubsub from "./pubsub.js";
import * as memory from "./storage.js";

const levelKey = (level: string[]) => JSON.stringify(level);
const CUSTOM_EVENT_ORIGIN_PROPERTY = "__origin";
const CUSTOM_EVENT_ORIGIN_PROXY = "proxy";

export class PeerbitProxyHost implements ProgramClient {
	private _levels: Map<string, AnyStore>;
	private _eventListenerSubscribeCounter: Map<
		string,
		Map<string, { counter: number; fn: (event: any) => void }>
	> = new Map();

	private _pubsubTopicSubscriptions: Map<string, Set<string>>;
	private _memoryIterator: Map<
		string,
		AsyncIterator<[string, Uint8Array], void, void>
	>;

	constructor(
		readonly hostClient: ProgramClient,
		readonly messages: connection.Node,
	) {
		if (hostClient.identity instanceof Ed25519Keypair === false) {
			throw new Error("Expecting identity to be a Ed25519Keypair keypair");
		}
		this._levels = new Map();
		this._pubsubTopicSubscriptions = new Map();
		this._memoryIterator = new Map();

		const dispatchFunction = this.hostClient.services.pubsub.dispatchEvent.bind(
			this.hostClient.services.pubsub,
		);

		// Override pubsub dispatchEvent so that data that is published from one client
		// appears in other clients as incoming data messages.
		// this allows multiple clients to subscribe to share the same host and
		// also have same databases open
		this.hostClient.services.pubsub.dispatchEvent = (evt: CustomEvent<any>) => {
			if (evt.type === "publish" && evt.detail.client) {
				dispatchFunction(new CustomEvent("data", { detail: evt.detail }));
			}
			return dispatchFunction(evt);
		};
		this.messages.start();
	}

	get peerId(): PeerId {
		return this.hostClient.peerId;
	}

	get identity(): Ed25519Keypair {
		return this.hostClient.identity as Ed25519Keypair;
	}
	getMultiaddrs(): Multiaddr[] {
		return this.hostClient.getMultiaddrs();
	}

	dial(address: string | Multiaddr | Multiaddr[]): Promise<boolean> {
		return this.hostClient.dial(address);
	}

	get services(): { pubsub: PubSub; blocks: Blocks; keychain: Keychain } {
		return this.hostClient.services;
	}
	get storage(): AnyStore {
		return this.hostClient.storage;
	}

	get indexer(): Indices {
		return this.hostClient.indexer;
	}

	start(): Promise<void> {
		return this.hostClient.start();
	}
	async stop(): Promise<void> {
		await this.messages.stop();
		await this.hostClient.stop();
		this._levels.clear();
		this._pubsubTopicSubscriptions.clear();
	}

	async open(program: any, options: any) {
		await this.messages.start();
		return this.hostClient.open(program, options);
	}

	async init() {
		this.messages.subscribe("data", (dataMessage, from) => {
			this.onMessage(deserialize(dataMessage.data, Message), from);
		});
	}

	async respond(request: Message, response: Message, to: connection.From) {
		response.messageId = request.messageId;
		await this.messages.send(serialize(response), to.id);
	}

	async onMessage(message: Message, from: connection.From) {
		try {
			if (message instanceof network.REQ_PeerId) {
				await this.respond(message, new network.RESP_PeerId(this.peerId), from);
			} else if (message instanceof network.REQ_Identity) {
				await this.respond(
					message,
					new network.RESP_Identity(this.identity),
					from,
				);
			} else if (message instanceof network.REQ_GetMultiaddrs) {
				const respo = this.getMultiaddrs();
				await this.respond(
					message,
					new network.RESP_GetMultiAddrs(respo),
					from,
				);
			} else if (message instanceof network.REQ_Dial) {
				await this.respond(
					message,
					new network.RESP_DIAL(await this.dial(message.multiaddr)),
					from,
				);
			} else if (message instanceof lifecycle.REQ_Start) {
				await this.start();
				await this.respond(message, new lifecycle.RESP_Start(), from);
			} else if (message instanceof lifecycle.REQ_Stop) {
				await this.stop();
				await this.respond(message, new lifecycle.RESP_Stop(), from);
			} else if (message instanceof keychain.REQ_ExportKeypairById) {
				await this.respond(
					message,
					new keychain.RESP_ExportKeypairById(
						await this.services.keychain?.exportById(
							message.keyId,
							message.type as AbstractType<any>,
						),
					),
					from,
				);
			} else if (message instanceof keychain.REQ_ExportKeypairByKey) {
				await this.respond(
					message,
					new keychain.RESP_ExportKeypairByKey(
						await this.services.keychain?.exportByKey(message.publicKey.key),
					),
					from,
				);
			} else if (message instanceof keychain.REQ_ImportKey) {
				await this.services.keychain?.import({
					keypair: message.keypair,
					id: message.keyId,
				});
				await this.respond(
					message,
					new keychain.RESP_ImportKey(message.messageId),
					from,
				);
			} else if (message instanceof memory.StorageMessage) {
				const request = message.message as memory.api.MemoryMessage;
				const m =
					request.level.length === 0
						? this.storage
						: this._levels.get(levelKey(request.level));
				if (!m) {
					throw new Error("Recieved memory message for an undefined level");
				} else if (request instanceof memory.api.REQ_Clear) {
					await m.clear();
					await this.respond(
						message,
						new memory.StorageMessage(
							new memory.api.RESP_Clear({ level: request.level }),
						),
						from,
					);
				} else if (request instanceof memory.api.REQ_Close) {
					await m.close();
					await this.respond(
						message,
						new memory.StorageMessage(
							new memory.api.RESP_Close({ level: request.level }),
						),
						from,
					);
				} else if (request instanceof memory.api.REQ_Del) {
					await m.del(request.key);
					await this.respond(
						message,
						new memory.StorageMessage(
							new memory.api.RESP_Del({ level: request.level }),
						),
						from,
					);
				} else if (request instanceof memory.api.REQ_Iterator_Next) {
					let iterator = this._memoryIterator.get(request.id);
					if (!iterator) {
						iterator = m.iterator()[Symbol.asyncIterator]();
						this._memoryIterator.set(request.id, iterator);
					}
					const next: any = await iterator.next();
					await this.respond(
						message,
						new memory.StorageMessage(
							new memory.api.RESP_Iterator_Next({
								keys: next.done ? [] : [next.value[0]],
								values: next.done ? [] : [next.value[1]],
								level: request.level,
							}),
						),
						from,
					);
					if (next.done) {
						this._memoryIterator.delete(request.id);
					}
				} else if (request instanceof memory.api.REQ_Iterator_Stop) {
					this._memoryIterator.delete(request.id);
					await this.respond(
						message,
						new memory.StorageMessage(
							new memory.api.RESP_Iterator_Stop({ level: request.level }),
						),
						from,
					);
				} else if (request instanceof memory.api.REQ_Get) {
					await this.respond(
						message,
						new memory.StorageMessage(
							new memory.api.RESP_Get({
								bytes: await m.get(request.key),
								level: request.level,
							}),
						),
						from,
					);
				} else if (request instanceof memory.api.REQ_Open) {
					await m.open();
					await this.respond(
						message,
						new memory.StorageMessage(
							new memory.api.RESP_Open({ level: request.level }),
						),
						from,
					);
				} else if (request instanceof memory.api.REQ_Put) {
					await m.put(request.key, request.bytes);
					await this.respond(
						message,
						new memory.StorageMessage(
							new memory.api.RESP_Put({ level: request.level }),
						),
						from,
					);
				} else if (request instanceof memory.api.REQ_Status) {
					await this.respond(
						message,
						new memory.StorageMessage(
							new memory.api.RESP_Status({
								status: await m.status(),
								level: request.level,
							}),
						),
						from,
					);
				} else if (request instanceof memory.api.REQ_Sublevel) {
					const sublevel = await m.sublevel(request.name);
					this._levels.set(
						levelKey([...request.level, request.name]),
						sublevel,
					);
					await this.respond(
						message,
						new memory.StorageMessage(
							new memory.api.RESP_Sublevel({ level: request.level }),
						),
						from,
					);
				} else if (request instanceof memory.api.REQ_Size) {
					await this.respond(
						message,
						new memory.StorageMessage(
							new memory.api.RESP_Size({
								size: await m.size(),
								level: request.level,
							}),
						),
						from,
					);
				} else if (request instanceof memory.api.REQ_Persisted) {
					await this.respond(
						message,
						new memory.StorageMessage(
							new memory.api.RESP_Persisted({
								persisted: await m.persisted(),
								level: request.level,
							}),
						),
						from,
					);
				}
			} else if (message instanceof blocks.REQ_BlockWaitFor) {
				await this.services.blocks.waitFor(message.hash);
				await this.respond(message, new blocks.RESP_BlockWaitFor(), from);
			} else if (message instanceof blocks.REQ_BlockSize) {
				await this.respond(
					message,
					new blocks.RESP_BlockSize(await this.services.blocks.size()),
					from,
				);
			} else if (message instanceof blocks.REQ_GetBlock) {
				await this.respond(
					message,
					new blocks.RESP_GetBlock(
						await this.services.blocks.get(message.cid, {
							remote: message.remote,
						}),
					),
					from,
				);
			} else if (message instanceof blocks.REQ_HasBlock) {
				await this.respond(
					message,
					new blocks.RESP_HasBlock(await this.services.blocks.has(message.cid)),
					from,
				);
			} else if (message instanceof blocks.REQ_PutBlock) {
				await this.respond(
					message,
					new blocks.RESP_PutBlock(
						await this.services.blocks.put(message.bytes),
					),
					from,
				);
			} else if (message instanceof blocks.REQ_RmBlock) {
				await this.services.blocks.rm(message.cid);
				await this.respond(message, new blocks.RESP_RmBlock(), from);
			} else if (message instanceof blocks.REQ_Persisted) {
				await this.respond(
					message,
					new blocks.RESP_Persisted({
						persisted: await this.services.blocks.persisted(),
					}),
					from,
				);
			} else if (message instanceof pubsub.REQ_AddEventListener) {
				let map = this._eventListenerSubscribeCounter.get(from.id);
				if (!map) {
					map = new Map();
					this._eventListenerSubscribeCounter.set(from.id, map);
				}
				let subscription = map.get(message.type);
				if (!subscription) {
					const cb = async (e: CustomEvent<any>) => {
						// TODO what if many clients whants the same data, dedup serialization invokations?
						if (
							e.detail instanceof PublishEvent &&
							e.detail.client === from.id &&
							message.type === "data"
						) {
							// ignore 'publish' events routed to 'data' events if the dispatcher is the same as the receiver
							return;
						}

						if (
							e.detail instanceof DataEvent ||
							e.detail instanceof PublishEvent
						) {
							const subscriptions = this._pubsubTopicSubscriptions.get(from.id);
							let found = false;
							if (subscriptions) {
								for (const topic of e.detail.data.topics) {
									found = subscriptions.has(topic);
									if (found) {
										break;
									}
								}
							}

							if (!found) {
								// Ignore this message, since the client is not subscribing to any of the topics
								return;
							}
						}

						const request = new pubsub.RESP_EmitEvent(
							message.type,
							serialize(e.detail),
						);
						request.messageId = message.emitMessageId; // Same message id so that receiver can subscribe to all events emitted from this listener
						await this.messages.send(serialize(request), from.id);
					};
					subscription = {
						counter: 1,
						fn: cb,
					};

					await this.services.pubsub.addEventListener(
						message.type,
						subscription.fn,
					);
					map.set(message.type, subscription);
				} else {
					subscription.counter += 1;
				}
				await this.respond(message, new pubsub.RESP_AddEventListener(), from);
			} else if (message instanceof pubsub.REQ_RemoveEventListener) {
				const subscription = this._eventListenerSubscribeCounter
					.get(from.id)
					?.get(message.type);

				if (subscription) {
					subscription.counter -= 1;
					if (subscription.counter === 0) {
						this.services.pubsub.removeEventListener(
							message.type,
							subscription.fn,
						);
					}
					this._eventListenerSubscribeCounter.delete(message.type);
					if (this._eventListenerSubscribeCounter.get(from.id)?.size === 0) {
						this._eventListenerSubscribeCounter.delete(from.id);
					}
				}
				await this.respond(
					message,
					new pubsub.RESP_RemoveEventListener(),
					from,
				);
			} else if (message instanceof pubsub.REQ_DispatchEvent) {
				const customEvent: any = pubsub.createCustomEventFromType(
					message.type,
					message.data,
				);
				customEvent[CUSTOM_EVENT_ORIGIN_PROPERTY] = CUSTOM_EVENT_ORIGIN_PROXY;
				const dispatched =
					await this.services.pubsub.dispatchEvent(customEvent);

				await this.respond(
					message,
					new pubsub.RESP_DispatchEvent(dispatched),
					from,
				);
			} else if (message instanceof pubsub.REQ_GetSubscribers) {
				await this.respond(
					message,
					new pubsub.RESP_GetSubscribers(
						await this.services.pubsub.getSubscribers(message.topic),
					),
					from,
				);
			} else if (message instanceof pubsub.REQ_Publish) {
				await this.respond(
					message,
					new pubsub.RESP_Publish(
						await this.services.pubsub.publish(message.data, {
							mode: message.mode!,
							topics: message.topics!,
							client: from.id,
						}),
					),
					from,
				); // TODO types));
			} else if (message instanceof pubsub.REQ_PubsubWaitFor) {
				await this.services.pubsub.waitFor(message.hash);
				await this.respond(message, new pubsub.RESP_PubsubWaitFor(), from);
			} else if (message instanceof pubsub.REQ_RequestSubscribers) {
				await this.services.pubsub.requestSubscribers(message.topic);
				await this.respond(message, new pubsub.RESP_RequestSubscribers(), from);
			} else if (message instanceof pubsub.REQ_Subscribe) {
				await this.services.pubsub.subscribe(message.topic);

				let set = this._pubsubTopicSubscriptions.get(from.id);
				if (!set) {
					set = new Set();
					this._pubsubTopicSubscriptions.set(from.id, set);
				}
				set.add(message.topic);
				await this.respond(message, new pubsub.RESP_Subscribe(), from);
			} else if (message instanceof pubsub.REQ_Unsubscribe) {
				const set = this._pubsubTopicSubscriptions.get(from.id);
				set?.delete(message.topic);

				await this.respond(
					message,
					new pubsub.RESP_Unsubscribe(
						await this.services.pubsub.unsubscribe(message.topic, {
							force: message.force,
							data: message.data,
						}),
					),
					from,
				);
			} else if (message instanceof pubsub.REQ_GetPublicKey) {
				await this.respond(
					message,
					new pubsub.RESP_GetPublicKey(
						await this.services.pubsub.getPublicKey(message.hash),
					),
					from,
				);
			} else {
				throw new Error("Unknown message type: " + message.constructor.name);
			}
		} catch (error: any) {
			await this.respond(message, new native.RESP_Error(error), from);
		}
	}
}
