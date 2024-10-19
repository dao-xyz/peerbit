import {
	Ed25519Keypair,
	type PublicSignKey,
	randomBytes,
	sha256Base64Sync,
} from "@peerbit/crypto";
import {
	SubscriptionEvent,
	UnsubcriptionEvent,
} from "@peerbit/pubsub-interface";
import { type ProgramClient, ProgramHandler } from "../src/program";

export const createPeer = async (
	state: {
		subsribers: Map<
			string,
			Map<
				string,
				{
					publicKey: PublicSignKey;
					timestamp: bigint;
				}
			>
		>;
		pubsubEventHandlers: Map<string, { fn: any; publicKey: PublicSignKey }[]>;
		peers: Map<string, ProgramClient>;
	} = {
		pubsubEventHandlers: new Map(),
		subsribers: new Map(),
		peers: new Map(),
	},
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
				waitFor: () => Promise.resolve(),
				iterator: () => {
					return undefined as any; // TODO
				},
				size: () => Promise.resolve(0),
				persisted: () => Promise.resolve(false),
			},
			pubsub: {
				subscribe: async (topic: any) => {
					let map = state.subsribers.get(topic);
					if (!map) {
						map = new Map();
						state.subsribers.set(topic, map);
					}
					map.set(keypair.publicKey.hashcode(), {
						publicKey: keypair.publicKey,
						timestamp: BigInt(+new Date()),
					});
					dispatchEvent(
						new CustomEvent<SubscriptionEvent>("subscribe", {
							detail: new SubscriptionEvent(keypair.publicKey, [topic]),
						}),
					);
				},
				getSubscribers: (topic: any) => {
					return [...(state.subsribers.get(topic)?.values() || [])].map(
						(x) => x.publicKey,
					);
				},

				unsubscribe: async (topic: any) => {
					const map = state.subsribers.get(topic);
					if (!map) {
						return false;
					}
					const ret = map.delete(keypair.publicKey.hashcode());
					if (ret) {
						dispatchEvent(
							new CustomEvent<UnsubcriptionEvent>("unsubscribe", {
								detail: new UnsubcriptionEvent(keypair.publicKey, [topic]),
							}),
						);
					}
					return ret;
				},

				publish: (d: any, o: any) => Promise.resolve(randomBytes(32)),

				addEventListener: (type: any, fn: any) => {
					const arr = state.pubsubEventHandlers.get(type) || [];
					arr.push({ fn, publicKey: keypair.publicKey });
					state.pubsubEventHandlers.set(type, arr);
				},

				removeEventListener: (type: any, e: any) => {
					const fns = state.pubsubEventHandlers.get(type);
					const idx = fns?.findIndex((x) => x.fn === e);
					if (idx == null || idx < 0) {
						return; // already removed
					}
					fns?.splice(idx, 1);
				},
				dispatchEvent,

				requestSubscribers: async () => {
					for (const [topic, data] of state.subsribers) {
						for (const [hash, _opts] of data) {
							if (hash !== keypair.publicKey.hashcode()) {
								dispatchEvent(
									new CustomEvent<SubscriptionEvent>("subscribe", {
										// TODO undefined checks
										detail: new SubscriptionEvent(
											state.peers.get(hash)!.identity.publicKey!,
											[topic],
										),
									}),
									true,
								);
							}
						}
					}
				},
				waitFor: () => Promise.resolve(),
				getPublicKey: (_hash: string) => keypair.publicKey, // TODO
			},
			keychain: undefined as any, // TODO
		},
		storage: undefined as any, // TODO
		indexer: undefined as any, // TODO
		start: () => Promise.resolve(),
		stop: async () => {
			return handler?.stop();
		},
		open: async (p, o) => {
			return (handler || (handler = new ProgramHandler({ client: peer }))).open(
				p,
				o,
			);
		},
	};
	state.peers.set(peer.identity.publicKey.hashcode(), peer);
	return peer;
};
