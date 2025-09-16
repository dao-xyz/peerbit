import { isMultiaddr, multiaddr } from "@multiformats/multiaddr";
import { DirectBlock } from "@peerbit/blocks";
import { calculateRawCid } from "@peerbit/blocks-interface";
import {
	Ed25519Keypair,
	type PublicSignKey,
	getKeypairFromPrivateKey,
	randomBytes,
} from "@peerbit/crypto";
import {
	DefaultCryptoKeychain,
	type IPeerbitKeychain,
} from "@peerbit/keychain";
import { TestSession } from "@peerbit/libp2p-test-utils";
import { DirectSub } from "@peerbit/pubsub";
import {
	SubscriptionEvent,
	UnsubcriptionEvent,
} from "@peerbit/pubsub-interface";
import { type ProgramClient, ProgramHandler } from "../src/program";

export const creatMockPeer = async (
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
		hangUp: () => Promise.resolve(),
		services: {
			blocks: {
				get: (c) => blocks.get(c),
				has: (c) => blocks.has(c),
				put: async (c) => {
					if (c instanceof Uint8Array) {
						const out = await calculateRawCid(c);
						blocks.set(out.cid, c);
						return out.cid;
					}
					blocks.set(c.cid, c.block.bytes);
					return c.cid;
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

export const createLibp2pPeer = async (): Promise<ProgramClient> => {
	const client = (
		await TestSession.connected<
			{
				pubsub: DirectSub;
				blocks: DirectBlock;
				keychain: IPeerbitKeychain;
			} & any
		>(1, {
			services: {
				pubsub: (c) => new DirectSub(c),
				blocks: (c) => new DirectBlock(c),
				keychain: () =>
					new DefaultCryptoKeychain() as unknown as IPeerbitKeychain,
			},
		})
	).peers[0];

	const identity = getKeypairFromPrivateKey(
		(client as any)["components"].privateKey, // TODO can we export privateKey in a better way?
	);

	let handler: ProgramHandler | undefined = undefined;
	const peer: ProgramClient = {
		peerId: await identity.toPeerId(),
		services: client.services,
		hangUp: async (_address) => {
			throw new Error("Not implemented");
		},
		getMultiaddrs: () => {
			return client.getMultiaddrs();
		},
		identity,
		storage: undefined as any, // TODO
		indexer: undefined as any, // TODO
		open: async (p, o) => {
			return (handler || (handler = new ProgramHandler({ client: peer }))).open(
				p,
				o,
			);
		},
		start: async () => {
			await client.start();
			handler = new ProgramHandler({ client: peer });
			return peer as any; // TODO
		},
		stop: async () => {
			await handler?.stop();
			await client.stop();
		},
		dial: async (address) => {
			const maddress =
				typeof address === "string"
					? multiaddr(address)
					: isMultiaddr(address) || Array.isArray(address)
						? address
						: undefined;

			if (!maddress) {
				throw new Error("Invalid address");
			}
			const out = await client.dial(maddress);
			return !!out;
		},
	};
	return peer;
};
