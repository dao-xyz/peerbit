import { SimpleLevel } from "@peerbit/lazy-level";
import { PeerId } from "@libp2p/interface/peer-id";
import { Multiaddr } from "@multiformats/multiaddr";
import { Blocks } from "@peerbit/blocks-interface";
import {
	Identity,
	Ed25519PublicKey,
	Keychain,
	Ed25519Keypair,
	PublicSignKey,
	X25519PublicKey,
	KeypairFromPublicKey,
	randomBytes,
} from "@peerbit/crypto";
import { PubSub, PubSubEvents } from "@peerbit/pubsub-interface";
import {
	Address,
	OpenOptions,
	Program,
	ProgramHandler,
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

import { EventEmitter } from "@libp2p/interface/events";
import { X25519Keypair } from "@peerbit/crypto";
import { serialize, deserialize } from "@dao-xyz/borsh";

import sodium from "libsodium-wrappers";

const messageIdString = (messageId: Uint8Array) => sha256Base64(messageId);
const levelKey = (level: string[]) => JSON.stringify(level);

export class PeerbitProxyClient implements ProgramClient {
	peerId: PeerId;
	identity: Identity<Ed25519PublicKey>;

	private _multiaddr: Multiaddr[];
	private _services: { pubsub: PubSub; blocks: Blocks };
	private _keychain: Keychain;
	private _memory: SimpleLevel;
	private _handler: ProgramHandler;

	constructor(readonly messages: connection.Node) {
		const pubsubEventEmitter = new EventEmitter();
		const eventListenerSubscribeCounter: Map<
			string,
			{ messageId: string; counter: number }
		> = new Map();
		this._services = {
			pubsub: {
				emitSelf: false, // might be changed to true on connect()
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
							messageId: emitMessageIdStr,
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
							once: false,
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
					return resp.map || undefined;
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
				subscribe: async (topic, options) => {
					await this.request<pubsub.RESP_Subscribe>(
						new pubsub.REQ_Subscribe(topic, options)
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
				},
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
				waitFor: async (publicKey) => {
					await this.request<blocks.RESP_BlockWaitFor>(
						new blocks.REQ_BlockWaitFor(publicKey)
					);
				},
			},
		};
		this._keychain = {
			exportById: async <
				T = "ed25519" | "x25519",
				Q = T extends "ed25519" ? Ed25519Keypair : X25519Keypair
			>(
				id: Uint8Array,
				type: T
			) => {
				const resp = await this.request<keychain.RESP_ExportKeypairById>(
					new keychain.REQ_ExportKeypairById(id, type as "ed25519" | "x25519")
				);
				return resp.keypair as Q;
			},
			exportByKey: async <
				T extends Ed25519PublicKey | X25519PublicKey,
				Q = KeypairFromPublicKey<T>
			>(
				publicKey: T
			) => {
				const resp = await this.request<keychain.RESP_ExportKeypairByKey>(
					new keychain.REQ_ExportKeypairByKey(publicKey)
				);
				return resp.keypair as Q;
			},
			import: async (keypair, id) => {
				await this.request<keychain.RESP_ImportKey>(
					new keychain.REQ_ImportKey(keypair, id)
				);
			},
		};
		const levelMap: Map<string, SimpleLevel> = new Map();
		const createMemory = (level: string[] = []): SimpleLevel => {
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
					const sublevel = createMemory(newLevels);
					levelMap.set(levelKey(newLevels), sublevel);
					return sublevel;
				},
				idle: async () => {
					await this.request<memory.RESP_Idle>(new memory.REQ_Idle({ level }));
				},
				close: async () => {
					await this.request<memory.RESP_Close>(
						new memory.REQ_Close({ level })
					);
					levelMap.delete(levelKey(level));
				},
				open: async () => {
					await this.request<memory.RESP_Open>(new memory.REQ_Open({ level }));
				},
			};
		};
		this._memory = createMemory();
	}

	async connect() {
		await sodium.ready;

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

		this.services.pubsub.emitSelf = (
			await this.request<pubsub.RESP_EmitSelf>(new pubsub.REQ_EmitSelf())
		).value;
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

	get services(): { pubsub: PubSub; blocks: Blocks } {
		return this._services;
	}
	get keychain(): Keychain {
		return this._keychain;
	}

	get memory(): SimpleLevel {
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
	async open<S extends Program<Args>, Args = any>(
		storeOrAddress: S | Address | string,
		options?: OpenOptions<Args, Program>
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
