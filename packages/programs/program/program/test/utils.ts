import {
	type Multiaddr,
	isMultiaddr,
	multiaddr,
} from "@multiformats/multiaddr";
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
import { FanoutTree, TopicControlPlane, TopicRootControlPlane } from "@peerbit/pubsub";
import {
	SubscriptionEvent,
	UnsubcriptionEvent,
} from "@peerbit/pubsub-interface";
import { type ProgramClient, ProgramHandler } from "../src/program.js";

type PubsubHandler = (event: CustomEvent<unknown>) => void;
type StoredBlock = Uint8Array | { cid: string; block: { bytes: Uint8Array } };

// Program tests create libp2p peers independently (not in a shared session) and
// then dial them explicitly. Sharded pubsub requires a consistent shard-root
// candidate set across peers; otherwise each peer may resolve roots to itself
// and never converge on the same overlay.
const TEST_TOPIC_ROOT_PLANES = new Set<TopicRootControlPlane>();
const TEST_TOPIC_ROOT_CANDIDATES = new Set<string>();
let TEST_ACTIVE_LIBP2P_PEERS = 0;
const updateTestTopicRootCandidates = () => {
	const candidates = [...TEST_TOPIC_ROOT_CANDIDATES].sort((a, b) =>
		a < b ? -1 : a > b ? 1 : 0,
	);
	for (const plane of TEST_TOPIC_ROOT_PLANES) {
		plane.setTopicRootCandidates(candidates);
	}
};

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
		pubsubEventHandlers: Map<
			string,
			{ fn: PubsubHandler; publicKey: PublicSignKey }[]
		>;
		peers: Map<string, ProgramClient>;
	} = {
		pubsubEventHandlers: new Map(),
		subsribers: new Map(),
		peers: new Map(),
	},
): Promise<ProgramClient> => {
	const keypair = await Ed25519Keypair.create();

	const blocks: Map<string, Uint8Array> = new Map();

	const dispatchEvent = (
		event: CustomEvent<unknown>,
		emitSelf = false,
	): boolean => {
		const handlers = state.pubsubEventHandlers.get(event.type);
		if (handlers) {
			handlers.forEach(({ fn, publicKey }) => {
				if (!publicKey.equals(keypair.publicKey) || emitSelf) {
					fn(event);
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
				get: (cid: string) => blocks.get(cid),
				has: (cid: string) => blocks.has(cid),
				put: async (content: StoredBlock) => {
					if (content instanceof Uint8Array) {
						const out = await calculateRawCid(content);
						blocks.set(out.cid, content);
						return out.cid;
					}
					blocks.set(content.cid, content.block.bytes);
					return content.cid;
				},
				rm: (cid: string) => {
					blocks.delete(cid);
				},
				waitFor: () => Promise.resolve([] as string[]),
				iterator: () => {
					return undefined as any; // TODO
				},
				size: () => Promise.resolve(0),
				persisted: () => Promise.resolve(false),
			},
			pubsub: {
				subscribe: async (topic: string) => {
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
				getSubscribers: (topic: string) => {
					return [...(state.subsribers.get(topic)?.values() || [])].map(
						(x) => x.publicKey,
					);
				},

				unsubscribe: async (topic: string) => {
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

				publish: (_data: Uint8Array, _options?: unknown) =>
					Promise.resolve(randomBytes(32)),

				addEventListener: (type: string, fn: PubsubHandler) => {
					const arr = state.pubsubEventHandlers.get(type) || [];
					arr.push({ fn, publicKey: keypair.publicKey });
					state.pubsubEventHandlers.set(type, arr);
				},

				removeEventListener: (type: string, handler: PubsubHandler) => {
					const fns = state.pubsubEventHandlers.get(type);
					const idx = fns?.findIndex((x) => x.fn === handler);
					if (idx == null || idx < 0) {
						return; // already removed
					}
					fns?.splice(idx, 1);
				},
				dispatchEvent,

				requestSubscribers: async (): Promise<void> => {
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
				waitFor: () => Promise.resolve([] as string[]),
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
		open: async (program: any, options?: any) => {
			const currentHandler =
				handler || (handler = new ProgramHandler({ client: peer }));
			return currentHandler.open(program, options);
		},
	};
	state.peers.set(peer.identity.publicKey.hashcode(), peer);
	return peer;
};

export const createLibp2pPeer = async (): Promise<ProgramClient> => {
	const topicRootControlPlane = new TopicRootControlPlane();
	let fanoutInstance: FanoutTree | undefined;
	const getOrCreateFanout = (c: any) => {
		if (!fanoutInstance) {
			fanoutInstance = new FanoutTree(c, {
				connectionManager: false,
				topicRootControlPlane,
			});
		}
		return fanoutInstance;
	};

	const client = (
		await TestSession.connected<
			{
				pubsub: TopicControlPlane;
				fanout: FanoutTree;
				blocks: DirectBlock;
				keychain: IPeerbitKeychain;
			} & any
		>(1, {
			services: {
				pubsub: (c) =>
					new TopicControlPlane(c, {
						topicRootControlPlane,
						fanout: getOrCreateFanout(c),
					}),
				fanout: (c) => getOrCreateFanout(c),
				blocks: (c) => new DirectBlock(c),
				keychain: () =>
					new DefaultCryptoKeychain() as unknown as IPeerbitKeychain,
			},
		})
	).peers[0];

	// Keep shard-root resolution consistent across all peers created by this helper.
	// NOTE: Callers should create all peers for a given test before subscribing/opening
	// programs, otherwise changing the candidate set can change shard roots.
	try {
		const selfHash = (client as any)?.services?.pubsub?.publicKeyHash as
			| string
			| undefined;
		if (typeof selfHash === "string" && selfHash.length > 0) {
			TEST_TOPIC_ROOT_PLANES.add(topicRootControlPlane);
			TEST_TOPIC_ROOT_CANDIDATES.add(selfHash);
			updateTestTopicRootCandidates();
		}
	} catch {
		// ignore
	}

	const identity = getKeypairFromPrivateKey(
		(client as any)["components"].privateKey, // TODO can we export privateKey in a better way?
	);

	let handler: ProgramHandler | undefined = undefined;
	let stopped = false;
	const peer: ProgramClient = {
		peerId: await identity.toPeerId(),
		services: client.services,
		hangUp: async (_address: unknown) => {
			throw new Error("Not implemented");
		},
		getMultiaddrs: () => {
			return client.getMultiaddrs();
		},
		identity,
		storage: undefined as any, // TODO
		indexer: undefined as any, // TODO
		open: async (program: any, options?: any) => {
			const currentHandler =
				handler || (handler = new ProgramHandler({ client: peer }));
			return currentHandler.open(program, options);
		},
		start: async () => {
			await client.start();
			handler = new ProgramHandler({ client: peer });
			return peer as any; // TODO
		},
		stop: async () => {
			if (stopped) return;
			stopped = true;
			try {
				await handler?.stop();
				await client.stop();
			} finally {
				TEST_ACTIVE_LIBP2P_PEERS = Math.max(0, TEST_ACTIVE_LIBP2P_PEERS - 1);
				if (TEST_ACTIVE_LIBP2P_PEERS === 0) {
					TEST_TOPIC_ROOT_PLANES.clear();
					TEST_TOPIC_ROOT_CANDIDATES.clear();
				}
			}
		},
		dial: async (address: string | Multiaddr | Multiaddr[]) => {
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
	TEST_ACTIVE_LIBP2P_PEERS += 1;
	return peer;
};
