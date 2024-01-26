import { AnyStore } from "@peerbit/any-store";
import { PeerId } from "@libp2p/interface";
import { Multiaddr } from "@multiformats/multiaddr";
import { Blocks } from "@peerbit/blocks-interface";
import {
	Identity,
	Ed25519PublicKey,
	PublicSignKey,
	X25519PublicKey,
	randomBytes,
	ready,
	PublicKeyEncryptionKey,
	Secp256k1PublicKey,
	Ed25519Keypair,
	Secp256k1Keypair,
	X25519Keypair,
	ByteKey
} from "@peerbit/crypto";
import { PubSub, PubSubEvents } from "@peerbit/pubsub-interface";
import {
	Address,
	OpenOptions,
	Program,
	ProgramHandler
} from "@peerbit/program";
import { ProgramClient } from "@peerbit/program";
import { sha256Base64 } from "@peerbit/crypto";
import * as blocks from "./blocks.js";
import * as keychain from "./keychain.js";
import * as lifecycle from "./lifecycle.js";
import * as memory from "./memory.js";
import * as native from "./native.js";
import { Message } from "./message.js";
import * as network from "./network.js";
import * as pubsub from "./pubsub.js";
import * as connection from "./connection.js";
import { v4 as uuid } from "uuid";
import { TypedEventEmitter } from "@libp2p/interface";
import { serialize, deserialize, AbstractType } from "@dao-xyz/borsh";
import { Keychain, KeypairFromPublicKey } from "@peerbit/keychain";
import { ExtractArgs } from "@peerbit/program";

const messageIdString = (messageId: Uint8Array) => sha256Base64(messageId);
const levelKey = (level: string[]) => JSON.stringify(level);

async function* blocksIterator(client: {
	request<T extends Message>(request: Message): Promise<T>;
}) {
	while (true) {
		const resp = await client.request<blocks.RESP_Iterator>(
			new blocks.REQ_Iterator()
		);
		for (let i = 0; i < resp.keys.length; i++) {
			yield [resp.keys[i], resp.values[i]] as [string, Uint8Array];
		}
		if (resp.keys.length === 0) {
			break;
		}
	}
}

function memoryIterator(
	client: { request<T extends Message>(request: Message): Promise<T> },
	level: string[]
): {
	[Symbol.asyncIterator]: () => AsyncIterator<[string, Uint8Array], void, void>;
} {
	return {
		[Symbol.asyncIterator]() {
			const iteratorId = uuid();
			return {
				next: async () => {
					const resp = await client.request<
						memory.MemoryMessage<memory.api.RESP_Iterator_Next>
					>(
						new memory.MemoryMessage(
							new memory.api.REQ_Iterator_Next({ id: iteratorId, level })
						)
					);
					if (resp.message.keys.length > 1) {
						throw new Error("Unsupported iteration response");
					}

					// Will only have 0 or 1 element for now
					for (let i = 0; i < resp.message.keys.length; i++) {
						return {
							done: false,
							value: [resp.message.keys[i], resp.message.values[i]] as [
								string,
								Uint8Array
							]
						} as { done: false; value: [string, Uint8Array] };
					}
					return { done: true, value: undefined } as {
						done: true;
						value: undefined;
					};
				},
				async return() {
					await client.request<
						memory.MemoryMessage<memory.api.RESP_Iterator_Next>
					>(
						new memory.MemoryMessage(
							new memory.api.REQ_Iterator_Stop({ id: iteratorId, level })
						)
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

export class PeerbitProxyClient implements ProgramClient {
	peerId: PeerId;
	identity: Identity<Ed25519PublicKey>;

	private _multiaddr: Multiaddr[];
	private _services: { pubsub: PubSub; blocks: Blocks; keychain: Keychain };
	private _memory: AnyStore;
	private _handler: ProgramHandler;

	constructor(readonly messages: connection.Node) {
		const pubsubEventEmitter = new TypedEventEmitter();
		const eventListenerSubscribeCounter: Map<
			string,
			{ messageId: string; counter: number }
		> = new Map();
		this._services = {
			pubsub: {
				addEventListener: async (type, lister, options) => {
					pubsubEventEmitter.addEventListener(type, lister, options);
					let subscription = eventListenerSubscribeCounter.get(type);
					if (!subscription) {
						const emitMessageId = randomBytes(32);
						const request = new pubsub.REQ_AddEventListener(
							type,
							emitMessageId
						);
						const messageId = await messageIdString(request.messageId);
						const emitMessageIdStr = await messageIdString(emitMessageId);

						subscription = {
							counter: 1,
							messageId: emitMessageIdStr
						};
						eventListenerSubscribeCounter.set(type, subscription);
						this._responseCallbacks.set(emitMessageIdStr, {
							fn: (e) => {
								if (e instanceof pubsub.RESP_EmitEvent) {
									pubsubEventEmitter.dispatchEvent(
										pubsub.createCustomEventFromType(e.type, e.data)
									);
								} else {
									throw new Error(
										"No event handler for message with id: " + messageId
									);
								}
							},
							once: false
						});
						await this.request<pubsub.RESP_AddEventListener>(request);
					} else {
						subscription.counter += 1;
					}
				},
				dispatchEvent: async (event) => {
					const resp = await this.request<pubsub.RESP_DispatchEvent>(
						new pubsub.REQ_DispatchEvent(
							event.type as keyof PubSubEvents,
							serialize((event as CustomEvent<any>).detail)
						)
					);
					return resp.value;
				},

				removeEventListener: async (type, listener, options) => {
					pubsubEventEmitter.removeEventListener(type, listener, options);
					const subscription = eventListenerSubscribeCounter.get(type);
					if (subscription) {
						subscription.counter -= 1;
						if (subscription.counter === 0) {
							this._responseCallbacks.delete(subscription.messageId);
							await this.request<pubsub.RESP_RemoveEventListener>(
								new pubsub.REQ_RemoveEventListener(type)
							);
						}
					}
				},

				getSubscribers: async (topic) => {
					const resp = await this.request<pubsub.RESP_GetSubscribers>(
						new pubsub.REQ_GetSubscribers(topic)
					);
					return resp.subscribers;
				},
				publish: async (data, options) => {
					const resp = await this.request<pubsub.RESP_Publish>(
						new pubsub.REQ_Publish(data, options)
					);
					return resp.messageId;
				},
				requestSubscribers: async (topic: string) => {
					await this.request<pubsub.RESP_RequestSubscribers>(
						new pubsub.REQ_RequestSubscribers(topic)
					);
				},
				subscribe: async (topic) => {
					await this.request<pubsub.RESP_Subscribe>(
						new pubsub.REQ_Subscribe(topic)
					);
				},
				unsubscribe: async (topic, options) => {
					const resp = await this.request<pubsub.RESP_Unsubscribe>(
						new pubsub.REQ_Unsubscribe(topic, options)
					);
					return resp.value;
				},
				waitFor: async (publicKey: PeerId | PublicSignKey) => {
					await this.request<pubsub.RESP_PubsubWaitFor>(
						new pubsub.REQ_PubsubWaitFor(publicKey)
					);
				}
			},
			blocks: {
				get: async (cid, options) => {
					const resp = await this.request<blocks.RESP_GetBlock>(
						new blocks.REQ_GetBlock(cid, options)
					);
					return resp.bytes;
				},

				has: async (cid) => {
					const resp = await this.request<blocks.RESP_HasBlock>(
						new blocks.REQ_HasBlock(cid)
					);
					return resp.has;
				},
				put: async (bytes) => {
					const resp = await this.request<blocks.RESP_PutBlock>(
						new blocks.REQ_PutBlock(bytes)
					);
					return resp.cid;
				},
				rm: async (cid) => {
					await this.request<blocks.RESP_RmBlock>(new blocks.REQ_RmBlock(cid));
				},

				iterator: () => blocksIterator(this),

				waitFor: async (publicKey) => {
					await this.request<blocks.RESP_BlockWaitFor>(
						new blocks.REQ_BlockWaitFor(publicKey)
					);
				},
				size: async () => {
					return (
						await this.request<blocks.RESP_BlockSize>(
							new blocks.REQ_BlockSize()
						)
					).size;
				}
			},
			keychain: {
				exportById: async <
					T extends Ed25519Keypair | Secp256k1Keypair | X25519Keypair | ByteKey
				>(
					id: Uint8Array,
					type: AbstractType<T>
				) => {
					const resp = await this.request<keychain.RESP_ExportKeypairById>(
						new keychain.REQ_ExportKeypairById(id, type)
					);
					return resp.keypair?.key as T;
				},
				exportByKey: async <
					T extends
						| Ed25519PublicKey
						| X25519PublicKey
						| Secp256k1PublicKey
						| PublicSignKey
						| PublicKeyEncryptionKey,
					Q = KeypairFromPublicKey<T>
				>(
					publicKey: T
				) => {
					const resp = await this.request<keychain.RESP_ExportKeypairByKey>(
						new keychain.REQ_ExportKeypairByKey(publicKey)
					);
					return resp.keypair?.key as Q;
				},
				import: async (properties: { keypair; id }) => {
					await this.request<keychain.RESP_ImportKey>(
						new keychain.REQ_ImportKey(properties.keypair, properties.id)
					);
				}
			}
		};
		const levelMap: Map<string, AnyStore> = new Map();
		const createMemory = (level: string[] = []): AnyStore => {
			return {
				clear: async () => {
					await this.request<memory.MemoryMessage<memory.api.RESP_Clear>>(
						new memory.MemoryMessage(new memory.api.REQ_Clear({ level }))
					);
				},
				del: async (key) => {
					await this.request<memory.MemoryMessage<memory.api.RESP_Del>>(
						new memory.MemoryMessage(new memory.api.REQ_Del({ level, key }))
					);
				},
				get: async (key) => {
					return (
						await this.request<memory.MemoryMessage<memory.api.RESP_Get>>(
							new memory.MemoryMessage(new memory.api.REQ_Get({ level, key }))
						)
					).message.bytes;
				},
				put: async (key, value) => {
					await this.request<memory.MemoryMessage<memory.api.RESP_Put>>(
						new memory.MemoryMessage(
							new memory.api.REQ_Put({ level, key, bytes: value })
						)
					);
				},
				status: async () =>
					(
						await this.request<memory.MemoryMessage<memory.api.RESP_Status>>(
							new memory.MemoryMessage(new memory.api.REQ_Status({ level }))
						)
					).message.status,
				sublevel: async (name) => {
					await this.request<memory.MemoryMessage<memory.api.RESP_Sublevel>>(
						new memory.MemoryMessage(
							new memory.api.REQ_Sublevel({ level, name })
						)
					);
					const newLevels = [...level, name];
					const sublevel = createMemory(newLevels);
					levelMap.set(levelKey(newLevels), sublevel);
					return sublevel;
				},

				iterator: () => memoryIterator(this, level),
				close: async () => {
					await this.request<memory.MemoryMessage<memory.api.RESP_Close>>(
						new memory.MemoryMessage(new memory.api.REQ_Close({ level }))
					);
					levelMap.delete(levelKey(level));
				},
				open: async () => {
					await this.request<memory.MemoryMessage<memory.api.RESP_Open>>(
						new memory.MemoryMessage(new memory.api.REQ_Open({ level }))
					);
				},
				size: async () => {
					return (
						await this.request<memory.MemoryMessage<memory.api.RESP_Size>>(
							new memory.MemoryMessage(new memory.api.REQ_Size({ level }))
						)
					).message.size;
				}
			};
		};
		this._memory = createMemory();
	}

	async connect() {
		await ready;

		await this.messages.connect({ waitForParent: true });

		this.messages.subscribe("data", this.onMessage.bind(this));
		this.peerId = (
			await this.request<network.RESP_PeerId>(new network.REQ_PeerId())
		).peerId;
		this.identity = (
			await this.request<network.RESP_Identity>(new network.REQ_Identity())
		).identity;
		this._multiaddr = (
			await this.request<network.RESP_GetMultiAddrs>(
				new network.REQ_GetMultiaddrs()
			)
		).multiaddr;
	}

	getMultiaddrs(): Multiaddr[] {
		return this._multiaddr;
	}

	async dial(address: string | Multiaddr | Multiaddr[]): Promise<boolean> {
		const response = await this.request<network.RESP_DIAL>(
			new network.REQ_Dial(address)
		);
		return response.value;
	}

	get services(): { pubsub: PubSub; blocks: Blocks; keychain: Keychain } {
		return this._services;
	}

	get memory(): AnyStore {
		return this._memory;
	}

	async start(): Promise<void> {
		await this.messages.start();
		await this.request<lifecycle.RESP_Start>(new lifecycle.REQ_Start());
	}
	async stop(): Promise<void> {
		await this.messages.stop();
		await this._handler?.stop();
		await this.request<lifecycle.RESP_Stop>(new lifecycle.REQ_Stop());
	}
	async open<S extends Program<ExtractArgs<S>>>(
		storeOrAddress: S | Address | string,
		options?: OpenOptions<Program>
	): Promise<S> {
		return (
			this._handler || (this._handler = new ProgramHandler({ client: this }))
		).open(storeOrAddress, options) as Promise<S>;
	}

	private _responseCallbacks: Map<
		string,
		{ fn: (message: Message) => any; once: boolean }
	> = new Map();
	async onMessage(dataMessage: connection.DataMessage, _from: connection.From) {
		const message = deserialize(dataMessage.data, Message);
		const messageId = await messageIdString(message.messageId);
		const fns = this._responseCallbacks.get(messageId);
		if (!fns) {
			throw new Error("Recieve response without a response handler");
		}
		fns.fn(message);
		if (fns.once) {
			this._responseCallbacks.delete(messageId);
		}
	}

	async request<T extends Message>(request: Message): Promise<T> {
		const messageId = await messageIdString(request.messageId);
		return new Promise<T>((resolve, reject) => {
			const onResponse = (message: Message) => {
				this._responseCallbacks.delete(messageId);
				if (message instanceof native.RESP_Error) {
					reject(message.error);
				} else {
					resolve(message as T);
				}
			};
			this._responseCallbacks.set(messageId, { fn: onResponse, once: true });
			const resp = this.messages.send(serialize(request));
			if (resp instanceof Promise) {
				resp.catch((e) => {
					this._responseCallbacks.delete(messageId);
					reject(e);
				});
			}
			// try catch above?
		});
	}
}
