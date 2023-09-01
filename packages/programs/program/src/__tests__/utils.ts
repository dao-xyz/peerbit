import {
	Ed25519Keypair,
	PublicSignKey,
	randomBytes,
	sha256Base64Sync
} from "@peerbit/crypto";
import { ProgramClient, ProgramHandler } from "../program";
import {
	Subscription,
	SubscriptionEvent,
	UnsubcriptionEvent,
	Unsubscription
} from "@peerbit/pubsub-interface";
import { CustomEvent } from "@libp2p/interface/events";

export const createPeer = async (
	state: {
		subsribers: Map<
			string,
			Map<
				string,
				{
					publicKey: PublicSignKey;
					timestamp: bigint;
					data?: Uint8Array | undefined;
				}
			>
		>;
		pubsubEventHandlers: Map<string, { fn: any; publicKey: PublicSignKey }[]>;
		peers: Map<string, ProgramClient>;
	} = {
		pubsubEventHandlers: new Map(),
		subsribers: new Map(),
		peers: new Map()
	}
): Promise<ProgramClient> => {
	const keypair = await Ed25519Keypair.create();

	const blocks: Map<string, Uint8Array> = new Map();

	const dispatchEvent = (e: CustomEvent<any>, emitSelf = false) => {
		const handlers = state.pubsubEventHandlers.get(e.type);
		if (handlers) {
			handlers.forEach(({ fn, publicKey }) => {
				if (!publicKey.equals(keypair.publicKey) || emitSelf) {
					fn(e);
				}
			});
			return true;
		}
		return false;
	};

	let handler: ProgramHandler | undefined = undefined;
	const peer: ProgramClient = {
		peerId: await keypair.toPeerId(),
		identity: keypair,
		getMultiaddrs: () => [],
		dial: () => Promise.resolve(false),
		services: {
			blocks: {
				get: (c) => blocks.get(c),
				has: (c) => blocks.has(c),
				put: (c) => {
					const hash = sha256Base64Sync(c);
					blocks.set(hash, c);
					return hash;
				},
				rm: (c) => {
					blocks.delete(c);
				},
				waitFor: () => Promise.resolve()
			},
			pubsub: {
				emitSelf: false,
				subscribe: async (topic, opts) => {
					let map = state.subsribers.get(topic);
					if (!map) {
						map = new Map();
						state.subsribers.set(topic, map);
					}
					map.set(keypair.publicKey.hashcode(), {
						publicKey: keypair.publicKey,
						timestamp: BigInt(+new Date()),
						data: opts?.data
					});
					dispatchEvent(
						new CustomEvent<SubscriptionEvent>("subscribe", {
							detail: new SubscriptionEvent(keypair.publicKey, [
								new Subscription(topic, opts?.data)
							])
						})
					);
				},
				getSubscribers: (topic) => {
					return state.subsribers.get(topic);
				},

				unsubscribe: async (topic) => {
					const map = state.subsribers.get(topic);
					if (!map) {
						return false;
					}
					const ret = map.delete(keypair.publicKey.hashcode());
					if (ret) {
						dispatchEvent(
							new CustomEvent<UnsubcriptionEvent>("unsubscribe", {
								detail: new UnsubcriptionEvent(keypair.publicKey, [
									new Unsubscription(topic)
								])
							})
						);
					}
					return ret;
				},

				publish: (d, o) => Promise.resolve(randomBytes(32)),

				addEventListener: (type, fn) => {
					const arr = state.pubsubEventHandlers.get(type) || [];
					arr.push({ fn, publicKey: keypair.publicKey });
					state.pubsubEventHandlers.set(type, arr);
				},

				removeEventListener: (type, e) => {
					const fns = state.pubsubEventHandlers.get(type);
					const idx = fns?.findIndex((x) => x.fn == e);
					if (idx == null || idx < 0) {
						throw new Error("Missing handler");
					}
					fns?.splice(idx, 1);
				},
				dispatchEvent,

				requestSubscribers: async () => {
					for (const [topic, data] of state.subsribers) {
						for (const [hash, opts] of data) {
							if (hash !== keypair.publicKey.hashcode()) {
								dispatchEvent(
									new CustomEvent<SubscriptionEvent>("subscribe", {
										// TODO undefined checks
										detail: new SubscriptionEvent(
											state.peers.get(hash)!.identity.publicKey!,
											[new Subscription(topic, opts?.data)]
										)
									}),
									true
								);
							}
						}
					}
				},
				waitFor: () => Promise.resolve()
			}
		},
		memory: undefined as any, // TODO
		keychain: undefined as any, // TODO
		start: () => Promise.resolve(),
		stop: async () => {
			return handler?.stop();
		},
		open: async (p, o) => {
			return (handler || (handler = new ProgramHandler({ client: peer }))).open(
				p,
				o
			);
		}
	};
	state.peers.set(peer.identity.publicKey.hashcode(), peer);
	return peer;
};
