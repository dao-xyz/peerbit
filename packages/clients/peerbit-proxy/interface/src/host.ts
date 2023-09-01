import { SimpleLevel } from "@peerbit/lazy-level";
import { PeerId } from "@libp2p/interface/peer-id";
import { Multiaddr } from "@multiformats/multiaddr";
import { Blocks } from "@peerbit/blocks-interface";
import { Keychain, Ed25519Keypair } from "@peerbit/crypto";
import { DataEvent, PubSub } from "@peerbit/pubsub-interface";
import { ProgramClient } from "@peerbit/program";
import * as blocks from "./blocks.js";
import * as keychain from "./keychain.js";
import * as lifecycle from "./lifecycle.js";
import * as memory from "./memory.js";
import * as native from "./native.js";
import { Message } from "./message.js";
import * as network from "./network.js";
import * as pubsub from "./pubsub.js";
import * as connection from "./connection.js";

import { serialize, deserialize } from "@dao-xyz/borsh";

const levelKey = (level: string[]) => JSON.stringify(level);

export class PeerbitProxyHost implements ProgramClient {
	private _levels: Map<string, SimpleLevel>;
	private _eventListenerSubscribeCounter: Map<
		string,
		Map<string, { counter: number; fn: (event: any) => void }>
	> = new Map();

	private _pubsubTopicSubscriptions: Map<string, Set<string>>;

	constructor(
		readonly hostClient: ProgramClient,
		readonly messages: connection.Node
	) {
		if (hostClient.identity instanceof Ed25519Keypair === false) {
			throw new Error("Expecting identity to be a Ed25519Keypair keypair");
		}
		this._levels = new Map();
		this._pubsubTopicSubscriptions = new Map();
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

	get services(): { pubsub: PubSub; blocks: Blocks } {
		return this.hostClient.services;
	}
	get keychain(): Keychain {
		return this.hostClient.keychain;
	}

	get memory(): SimpleLevel {
		return this.hostClient.memory;
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

	async open(program, options) {
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
					from
				);
			} else if (message instanceof network.REQ_GetMultiaddrs) {
				const respo = this.getMultiaddrs();
				await this.respond(
					message,
					new network.RESP_GetMultiAddrs(respo),
					from
				);
			} else if (message instanceof network.REQ_Dial) {
				await this.respond(
					message,
					new network.RESP_DIAL(await this.dial(message.multiaddr)),
					from
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
						await this.keychain?.exportById(message.keyId, message.type)
					),
					from
				);
			} else if (message instanceof keychain.REQ_ExportKeypairByKey) {
				await this.respond(
					message,
					new keychain.RESP_ExportKeypairByKey(
						await this.keychain?.exportByKey(message.publicKey)
					),
					from
				);
			} else if (message instanceof keychain.REQ_ImportKey) {
				await this.keychain?.import(message.keypair, message.keyId);
				await this.respond(
					message,
					new keychain.RESP_ImportKey(message.messageId),
					from
				);
			} else if (message instanceof memory.MemoryMessage) {
				const m =
					message.level.length === 0
						? this.memory
						: this._levels.get(levelKey(message.level));
				if (!m) {
					throw new Error("Recieved memory message for an undefined level");
				} else if (message instanceof memory.REQ_Clear) {
					await m.clear();
					await this.respond(
						message,
						new memory.RESP_Clear({ level: message.level }),
						from
					);
				} else if (message instanceof memory.REQ_Close) {
					await m.close();
					await this.respond(
						message,
						new memory.RESP_Close({ level: message.level }),
						from
					);
				} else if (message instanceof memory.REQ_Del) {
					await m.del(message.key);
					await this.respond(
						message,
						new memory.RESP_Del({ level: message.level }),
						from
					);
				} else if (message instanceof memory.REQ_Get) {
					await this.respond(
						message,
						new memory.RESP_Get({
							bytes: await m.get(message.key),
							level: message.level
						}),
						from
					);
				} else if (message instanceof memory.REQ_Idle) {
					await m.idle?.();
					await this.respond(
						message,
						new memory.RESP_Idle({ level: message.level }),
						from
					);
				} else if (message instanceof memory.REQ_Open) {
					await m.open();
					await this.respond(
						message,
						new memory.RESP_Open({ level: message.level }),
						from
					);
				} else if (message instanceof memory.REQ_Put) {
					await m.put(message.key, message.bytes);
					await this.respond(
						message,
						new memory.RESP_Put({ level: message.level }),
						from
					);
				} else if (message instanceof memory.REQ_Status) {
					await this.respond(
						message,
						new memory.RESP_Status({
							status: await m.status(),
							level: message.level
						}),
						from
					);
				} else if (message instanceof memory.REQ_Sublevel) {
					const sublevel = await m.sublevel(message.name);
					this._levels.set(
						levelKey([...message.level, message.name]),
						sublevel
					);
					await this.respond(
						message,
						new memory.RESP_Sublevel({ level: message.level }),
						from
					);
				}
			} else if (message instanceof blocks.REQ_BlockWaitFor) {
				await this.services.blocks.waitFor(message.publicKey);
				await this.respond(message, new blocks.RESP_BlockWaitFor(), from);
			} else if (message instanceof blocks.REQ_GetBlock) {
				await this.respond(
					message,
					new blocks.RESP_GetBlock(
						await this.services.blocks.get(message.cid, {
							replicate: message.replicate,
							timeout: message.timeout
						})
					),
					from
				);
			} else if (message instanceof blocks.REQ_HasBlock) {
				await this.respond(
					message,
					new blocks.RESP_HasBlock(await this.services.blocks.has(message.cid)),
					from
				);
			} else if (message instanceof blocks.REQ_PutBlock) {
				await this.respond(
					message,
					new blocks.RESP_PutBlock(
						await this.services.blocks.put(message.bytes)
					),
					from
				);
			} else if (message instanceof blocks.REQ_RmBlock) {
				await this.services.blocks.rm(message.cid);
				await this.respond(message, new blocks.RESP_RmBlock(), from);
			} else if (message instanceof pubsub.REQ_AddEventListener) {
				let map = this._eventListenerSubscribeCounter.get(from.id);
				if (!map) {
					map = new Map();
					this._eventListenerSubscribeCounter.set(from.id, map);
				}
				let subscription = map.get(message.type);
				if (!subscription) {
					subscription = {
						counter: 1,
						fn: async (e) => {
							// TODO what if many clients whants the same data, dedup serialization invokations?
							if (e.detail instanceof DataEvent) {
								const subscriptions = this._pubsubTopicSubscriptions.get(
									from.id
								);
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
								serialize(e.detail)
							);
							request.messageId = message.emitMessageId; // Same message id so that receiver can subscribe to all events emitted from this listener
							await this.messages.send(serialize(request), from.id);
						}
					};
					await this.services.pubsub.addEventListener(
						message.type,
						subscription.fn
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
							subscription.fn
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
					from
				);
			} else if (message instanceof pubsub.REQ_DispatchEvent) {
				const customEvent = pubsub.createCustomEventFromType(
					message.type,
					message.data
				);
				await this.respond(
					message,
					new pubsub.RESP_DispatchEvent(
						await this.services.pubsub.dispatchEvent(customEvent)
					),
					from
				);
			} else if (message instanceof pubsub.REQ_GetSubscribers) {
				await this.respond(
					message,
					new pubsub.RESP_GetSubscribers(
						await this.services.pubsub.getSubscribers(message.topic)
					),
					from
				);
			} else if (message instanceof pubsub.REQ_Publish) {
				await this.respond(
					message,
					new pubsub.RESP_Publish(
						await this.services.pubsub.publish(message.data, {
							strict: message.strict,
							to: message.to!,
							topics: message.topics!
						})
					),
					from
				); // TODO types));
			} else if (message instanceof pubsub.REQ_PubsubWaitFor) {
				await this.services.pubsub.waitFor(message.publicKey);
				await this.respond(message, new pubsub.RESP_PubsubWaitFor(), from);
			} else if (message instanceof pubsub.REQ_RequestSubscribers) {
				await this.services.pubsub.requestSubscribers(message.topic);
				await this.respond(message, new pubsub.RESP_RequestSubscribers(), from);
			} else if (message instanceof pubsub.REQ_Subscribe) {
				await this.services.pubsub.subscribe(message.topic, {
					data: message.data
				});

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
							data: message.data
						})
					),
					from
				);
			} else if (message instanceof pubsub.REQ_EmitSelf) {
				await this.respond(
					message,
					new pubsub.RESP_EmitSelf(this.services.pubsub.emitSelf),
					from
				);
			} else {
				throw new Error("Unknown message type: " + message.constructor.name);
			}
		} catch (error: any) {
			await this.respond(message, new native.RESP_Error(error), from);
		}
	}
}
