import { field, variant, vec } from "@dao-xyz/borsh";
import { Cache } from "@peerbit/cache";
import type { PublicSignKey } from "@peerbit/crypto";
import {
	Compare,
	type Index,
	IntegerCompare,
	Or,
} from "@peerbit/indexer-interface";
import { Entry, Log } from "@peerbit/log";
import type { RPC, RequestContext } from "@peerbit/rpc";
import { SilentDelivery } from "@peerbit/stream-interface";
import type { SyncableKey, Syncronizer } from "./index.js";
import {
	EntryWithRefs,
	createExchangeHeadsMessages,
} from "../exchange-heads.js";
import { TransportMessage } from "../message.js";
import type { EntryReplicated } from "../ranges.js";

@variant([0, 1])
export class RequestMaybeSync extends TransportMessage {
	@field({ type: vec("string") })
	hashes: string[];

	constructor(props: { hashes: string[] }) {
		super();
		this.hashes = props.hashes;
	}
}

@variant([0, 2])
export class ResponseMaybeSync extends TransportMessage {
	@field({ type: vec("string") })
	hashes: string[];

	constructor(props: { hashes: string[] }) {
		super();
		this.hashes = props.hashes;
	}
}

@variant([0, 5])
export class RequestMaybeSyncCoordinate extends TransportMessage {
	@field({ type: vec("u64") })
	hashNumbers: bigint[];

	constructor(props: { hashNumbers: bigint[] }) {
		super();
		this.hashNumbers = props.hashNumbers;
	}
}

const getHashesFromSymbols = async (
	symbols: bigint[],
	entryIndex: Index<EntryReplicated<any>, any>,
	coordinateToHash: Cache<string>,
) => {
	let queries: IntegerCompare[] = [];
	let batchSize = 1; // TODO arg
	let results = new Set<string>();
	const handleBatch = async (end = false) => {
		if (queries.length >= batchSize || (end && queries.length > 0)) {
			const entries = await entryIndex
				.iterate(
					{ query: queries.length > 1 ? new Or(queries) : queries },
					{ shape: { hash: true, hashNumber: true } },
				)
				.all();
			queries = [];

			for (const entry of entries) {
				results.add(entry.value.hash);
				coordinateToHash.add(entry.value.hashNumber, entry.value.hash);
			}
		}
	};
	for (let i = 0; i < symbols.length; i++) {
		const fromCache = coordinateToHash.get(symbols[i]);
		if (fromCache) {
			results.add(fromCache);
			continue;
		}
		const matchQuery = new IntegerCompare({
			key: "hashNumber",
			compare: Compare.Equal,
			value: symbols[i],
		});

		queries.push(matchQuery);
		await handleBatch();
	}
	await handleBatch(true);

	return results;
};

export class SimpleSyncronizer<R extends "u32" | "u64">
	implements Syncronizer<R>
{
	// map of hash to public keys that we can ask for entries
	syncInFlightQueue: Map<SyncableKey, PublicSignKey[]>;
	syncInFlightQueueInverted: Map<string, Set<SyncableKey>>;

	// map of hash to public keys that we have asked for entries
	syncInFlight!: Map<string, Map<SyncableKey, { timestamp: number }>>;

	rpc: RPC<TransportMessage, TransportMessage>;
	log: Log<any>;
	entryIndex: Index<EntryReplicated<R>, any>;
	coordinateToHash: Cache<string>;

	// Syncing and dedeplucation work
	syncMoreInterval?: ReturnType<typeof setTimeout>;

	closed!: boolean;

	constructor(properties: {
		rpc: RPC<TransportMessage, TransportMessage>;
		entryIndex: Index<EntryReplicated<R>, any>;
		log: Log<any>;
		coordinateToHash: Cache<string>;
	}) {
		this.syncInFlightQueue = new Map();
		this.syncInFlightQueueInverted = new Map();
		this.syncInFlight = new Map();
		this.rpc = properties.rpc;
		this.log = properties.log;
		this.entryIndex = properties.entryIndex;
		this.coordinateToHash = properties.coordinateToHash;
	}

	onMaybeMissingEntries(properties: {
		entries: Map<string, EntryReplicated<R>>;
		targets: string[];
	}): Promise<void> {
		return this.rpc.send(
			new RequestMaybeSync({ hashes: [...properties.entries.keys()] }),
			{
				priority: 1,
				mode: new SilentDelivery({ to: properties.targets, redundancy: 1 }),
			},
		);
	}

	async onMessage(
		msg: TransportMessage,
		context: RequestContext,
	): Promise<boolean> {
		const from = context.from!;
		if (msg instanceof RequestMaybeSync) {
			await this.queueSync(msg.hashes, from);
			return true;
		} else if (msg instanceof ResponseMaybeSync) {
			// TODO perhaps send less messages to more receivers for performance reasons?
			// TODO wait for previous send to target before trying to send more?

			for await (const message of createExchangeHeadsMessages(
				this.log,
				msg.hashes,
			)) {
				await this.rpc.send(message, {
					mode: new SilentDelivery({ to: [context.from!], redundancy: 1 }),
				});
			}
			return true;
		} else if (msg instanceof RequestMaybeSyncCoordinate) {
			const hashes = await getHashesFromSymbols(
				msg.hashNumbers,
				this.entryIndex,
				this.coordinateToHash,
			);
			for await (const message of createExchangeHeadsMessages(
				this.log,
				hashes,
			)) {
				await this.rpc.send(message, {
					mode: new SilentDelivery({ to: [context.from!], redundancy: 1 }),
					// dont set priority 1 here because this will block other messages that should higher priority
				});
			}

			return true;
		} else {
			return false; // no message was consumed
		}
	}

	onReceivedEntries(properties: {
		entries: EntryWithRefs<any>[];
		from: PublicSignKey;
	}): Promise<void> | void {
		for (const entry of properties.entries) {
			const set = this.syncInFlight.get(properties.from.hashcode());
			if (set) {
				set.delete(entry.entry.hash);
				if (set?.size === 0) {
					this.syncInFlight.delete(properties.from.hashcode());
				}
			}
		}
	}

	async queueSync(
		keys: string[] | bigint[],
		from: PublicSignKey,
		options?: { skipCheck?: boolean },
	) {
		const requestHashes: SyncableKey[] = [];

		for (const coordinateOrHash of keys) {
			const inFlight = this.syncInFlightQueue.get(coordinateOrHash);
			if (inFlight) {
				if (!inFlight.find((x) => x.hashcode() === from.hashcode())) {
					inFlight.push(from);
					let inverted = this.syncInFlightQueueInverted.get(from.hashcode());
					if (!inverted) {
						inverted = new Set();
						this.syncInFlightQueueInverted.set(from.hashcode(), inverted);
					}
					inverted.add(coordinateOrHash);
				}
			} else if (
				options?.skipCheck ||
				!(await this.checkHasCoordinateOrHash(coordinateOrHash))
			) {
				this.syncInFlightQueue.set(coordinateOrHash, []);
				requestHashes.push(coordinateOrHash); // request immediately (first time we have seen this hash)
			}
		}

		requestHashes.length > 0 &&
			(await this.requestSync(requestHashes as string[] | bigint[], [
				from!.hashcode(),
			]));
	}

	private async requestSync(
		hashes: string[] | bigint[],
		to: Set<string> | string[],
	) {
		if (hashes.length === 0) {
			return;
		}

		const now = +new Date();
		for (const node of to) {
			let map = this.syncInFlight.get(node);
			if (!map) {
				map = new Map();
				this.syncInFlight.set(node, map);
			}
			for (const hash of hashes) {
				map.set(hash, { timestamp: now });
			}
		}

		const isBigInt = typeof hashes[0] === "bigint";

		await this.rpc.send(
			isBigInt
				? new RequestMaybeSyncCoordinate({ hashNumbers: hashes as bigint[] })
				: new ResponseMaybeSync({ hashes: hashes as string[] }),
			{
				mode: new SilentDelivery({ to, redundancy: 1 }),
				priority: 1,
			},
		);
	}
	private async checkHasCoordinateOrHash(key: string | bigint) {
		return typeof key === "bigint"
			? (await this.entryIndex.count({ query: { hashNumber: key } })) > 0
			: this.log.has(key);
	}
	async open() {
		this.closed = false;
		const requestSyncLoop = async () => {
			/**
			 * This method fetches entries that we potentially want.
			 * In a case in which we become replicator of a segment,
			 * multiple remote peers might want to send us entries
			 * This method makes sure that we only request on entry from the remotes at a time
			 * so we don't get flooded with the same entry
			 */

			const requestHashes: SyncableKey[] = [];
			const from: Set<string> = new Set();
			for (const [key, value] of this.syncInFlightQueue) {
				if (this.closed) {
					return;
				}

				const has = await this.checkHasCoordinateOrHash(key);

				if (!has) {
					// TODO test that this if statement actually does anymeaningfull
					if (value.length > 0) {
						requestHashes.push(key);
						const publicKeyHash = value.shift()!.hashcode();
						from.add(publicKeyHash);
						const invertedSet =
							this.syncInFlightQueueInverted.get(publicKeyHash);
						if (invertedSet) {
							if (invertedSet.delete(key)) {
								if (invertedSet.size === 0) {
									this.syncInFlightQueueInverted.delete(publicKeyHash);
								}
							}
						}
					}
					if (value.length === 0) {
						this.syncInFlightQueue.delete(key); // no-one more to ask for this entry
					}
				} else {
					this.syncInFlightQueue.delete(key);
				}
			}

			const nowMin10s = +new Date() - 2e4;
			for (const [key, map] of this.syncInFlight) {
				// cleanup "old" missing syncs
				for (const [hash, { timestamp }] of map) {
					if (timestamp < nowMin10s) {
						map.delete(hash);
					}
				}
				if (map.size === 0) {
					this.syncInFlight.delete(key);
				}
			}
			this.requestSync(requestHashes as string[] | bigint[], from).finally(
				() => {
					if (this.closed) {
						return;
					}
					this.syncMoreInterval = setTimeout(requestSyncLoop, 3e3);
				},
			);
		};

		requestSyncLoop();
	}

	async close() {
		this.closed = true;
		this.syncInFlightQueue.clear();
		this.syncInFlightQueueInverted.clear();
		this.syncInFlight.clear();
		clearTimeout(this.syncMoreInterval);
	}
	onEntryAdded(entry: Entry<any>): void {
		return this.clearSyncProcess(entry.hash);
	}

	onEntryRemoved(hash: string): void {
		return this.clearSyncProcess(hash);
	}

	private clearSyncProcess(hash: string) {
		const inflight = this.syncInFlightQueue.get(hash);
		if (inflight) {
			for (const key of inflight) {
				const map = this.syncInFlightQueueInverted.get(key.hashcode());
				if (map) {
					map.delete(hash);
					if (map.size === 0) {
						this.syncInFlightQueueInverted.delete(key.hashcode());
					}
				}
			}

			this.syncInFlightQueue.delete(hash);
		}
	}

	onPeerDisconnected(key: PublicSignKey): Promise<void> | void {
		return this.clearSyncProcessPublicKey(key);
	}
	private clearSyncProcessPublicKey(publicKey: PublicSignKey) {
		this.syncInFlight.delete(publicKey.hashcode());
		const map = this.syncInFlightQueueInverted.get(publicKey.hashcode());
		if (map) {
			for (const hash of map) {
				const arr = this.syncInFlightQueue.get(hash);
				if (arr) {
					const filtered = arr.filter((x) => !x.equals(publicKey));
					if (filtered.length > 0) {
						this.syncInFlightQueue.set(hash, filtered);
					} else {
						this.syncInFlightQueue.delete(hash);
					}
				}
			}
			this.syncInFlightQueueInverted.delete(publicKey.hashcode());
		}
	}

	get pending() {
		return this.syncInFlightQueue.size;
	}
}
