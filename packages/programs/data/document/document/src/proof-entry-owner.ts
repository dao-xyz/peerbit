import { deserialize, serialize } from "@dao-xyz/borsh";
import {
	Compare,
	IdKey,
	IntegerCompare,
	Sort,
	SortDirection,
} from "@peerbit/indexer-interface";
import {
	type AppendOptions,
	type Change,
	Entry,
	type EntryIndexHashMutationLockOwner,
	EntryType,
	EntryV0,
	type JoinOptions,
	LamportClock,
	type Log,
} from "@peerbit/log";
import { type createProofCommitCoordinator } from "./proof-commit.js";
import {
	type ProofDeleteContract,
	deriveProofDeleteScope,
} from "./proof-delete-manifest.js";
import {
	BORSH_ENCODING_PROOF_OPERATION,
	MAX_PROOF_HISTORY_EDGES,
	type OriginalProofEntry,
	ProofDeleteOperation,
	ProofOperation,
	ProofPutOperation,
	createProofOperationVerifier,
	deriveProofKeyGid,
} from "./proof-operations.js";
import { createProofRetainedState } from "./proof-retained-state.js";

type Remote = NonNullable<Parameters<typeof Entry.fromMultihash>[2]>["remote"];
type JoinContext = {
	references: Map<string, Entry<ProofOperation>>;
	options?: JoinOptions<ProofOperation>;
	signal: AbortSignal;
	remote: Exclude<Remote, boolean | undefined>;
	resolveRemoteFrom(
		hash: string,
		signal?: AbortSignal,
	): Promise<string[] | undefined>;
};
type Prepared = {
	operation: ProofPutOperation | ProofDeleteOperation;
	next: Entry<ProofOperation>[];
};
const PAGE_SIZE = 32;
// Bound the working window, not total history depth. Evicted prefixes are
// re-read after a suffix commits; long chains trade extra reads for bounded state.
const MAX_JOIN_ENTRIES = 4096;
const MAX_JOIN_BYTES = 32 * 1024 * 1024;
const materialize = (original: OriginalProofEntry) => {
	const entry = deserialize(original.bytes, Entry) as Entry<ProofOperation>;
	entry.hash = Entry.prepareMultihashBytes(entry, original.bytes, original.cid);
	entry.init({ encoding: BORSH_ENCODING_PROOF_OPERATION });
	return entry;
};
const queryType = (value: EntryType) =>
	new IntegerCompare({ key: ["meta", "type"], value, compare: Compare.Equal });

/** Private lower-log owner for the persisted owner-proofs Documents format. */
export const createProofEntryOwner = (properties: {
	log: Log<ProofOperation>;
	contract: ProofDeleteContract;
	coordinator: ReturnType<typeof createProofCommitCoordinator>;
	assertActive(): void;
	// Private deterministic tests may shrink the working window, never expand it.
	traversalLimits?: { entries: number; bytes: number };
}) => {
	const { log, coordinator, assertActive } = properties;
	const maxEntries = properties.traversalLimits?.entries ?? MAX_JOIN_ENTRIES;
	const maxBytes = properties.traversalLimits?.bytes ?? MAX_JOIN_BYTES;
	if (
		!Number.isInteger(maxEntries) ||
		maxEntries <= 0 ||
		maxEntries > MAX_JOIN_ENTRIES ||
		!Number.isInteger(maxBytes) ||
		maxBytes <= 0 ||
		maxBytes > MAX_JOIN_BYTES
	)
		throw new Error("Invalid private proof traversal limits");
	const copy32 = (value: Uint8Array) => {
		if (Reflect.get(Uint8Array.prototype, "byteLength", value) !== 32)
			throw new Error("Expected 32-byte proof contract field");
		const copy = new Uint8Array(32);
		Uint8Array.prototype.set.call(copy, value);
		return copy;
	};
	const { version, owner: keyOwner, nonce, keyKind } = properties.contract;
	const contract = {
		version,
		owner: copy32(keyOwner),
		nonce: copy32(nonce),
		keyKind,
	};
	const scope = deriveProofDeleteScope(contract);
	const index = log.entryIndex;
	const verifier = createProofOperationVerifier({ contract });
	const retained = createProofRetainedState({ log, contract, assertActive });
	let ready = false;
	let recovering = false;
	const active = () => {
		assertActive();
		if (log.entryIndex !== index) throw new Error("Stale proof entry owner");
	};
	const requireReady = () => {
		active();
		if (!ready) throw new Error("Proof entry owner has not recovered");
	};
	const lower = log as unknown as {
		withNativeCommittedAppendAdmission<T>(
			operation: () => Promise<T>,
		): Promise<T>;
		runWithMutationCallback<T>(operation: () => Promise<T>): Promise<T>;
		bindEntryOwner(owner: {
			append(
				data: ProofOperation,
				options: AppendOptions<ProofOperation>,
				onLocalCommit: (entry: Entry<ProofOperation>) => void,
			): ReturnType<Log<ProofOperation>["append"]>;
			join(
				entries: Entry<ProofOperation>[],
				context: JoinContext,
			): Promise<void>;
		}): void;
	};
	const withLease = <T>(
		work: (owner: EntryIndexHashMutationLockOwner) => Promise<T>,
	) =>
		lower.withNativeCommittedAppendAdmission(async () => {
			active();
			const owner = await index.acquireExclusiveMutationLock();
			try {
				active();
				return await work(owner);
			} finally {
				index.releaseHashMutationLocks(owner);
			}
		});
	const checkEdge = (
		child: Entry<ProofOperation>,
		parent: Entry<ProofOperation>,
	) => {
		if (child.meta.clock.timestamp.compare(parent.meta.clock.timestamp) <= 0)
			throw new Error("Proof PUT parent must have an earlier signed clock");
	};
	const notify = async (
		result: Awaited<ReturnType<typeof coordinator.put>>,
		context: JoinContext,
	) => {
		if (!result.admitted || !result.changed || !context.options?.onChange)
			return;
		const change: Change<ProofOperation> = {
			added: [{ entry: result.entry, head: true }],
			removed: result.removed,
		};
		await lower.runWithMutationCallback(async () => {
			await context.options!.onChange!(change);
		});
	};
	const commit = (
		entry: Entry<ProofOperation>,
		original: OriginalProofEntry,
		signal?: AbortSignal,
		onLocalCommit?: (entry: Entry<ProofOperation>) => void,
	) =>
		entry.meta.type === EntryType.CUT
			? coordinator.deletion(original, { signal, onLocalCommit })
			: coordinator.put(original, { signal, onLocalCommit });

	lower.bindEntryOwner({
		async append(data, options, onLocalCommit) {
			requireReady();
			const operationType: unknown = Reflect.get(data, "constructor");
			if (
				(operationType !== ProofPutOperation &&
					operationType !== ProofDeleteOperation) ||
				options.identity ||
				options.signers ||
				options.encryption ||
				options.canAppend ||
				options.trim ||
				options.deferIndexWrite ||
				options.durability === "buffered" ||
				options.meta?.gidSeed ||
				options.meta?.timestamp
			)
				throw new Error("Unsupported proof owner append options");
			// Reconstruct through owning constructors before any await.
			const operation =
				operationType === ProofPutOperation
					? new ProofPutOperation(data as ProofPutOperation)
					: new ProofDeleteOperation(data as ProofDeleteOperation);
			const hashes = options.meta?.next?.map((parent) => parent.hash) ?? [];
			if (hashes.length > MAX_PROOF_HISTORY_EDGES)
				throw new Error("Proof history edge limit exceeded");
			const metadata = options.meta?.data && Uint8Array.from(options.meta.data);
			const type =
				operation instanceof ProofDeleteOperation
					? EntryType.CUT
					: EntryType.APPEND;
			if (options.meta?.type != null && options.meta.type !== type)
				throw new Error("Wrong proof owner append entry type");
			let next: Entry<ProofOperation>[];
			if (operation instanceof ProofDeleteOperation) {
				const history = await verifier.history(operation);
				next = history.targets
					.map((fact) => materialize({ cid: fact.hash, bytes: fact.bytes }))
					.sort((a, b) => (a.hash < b.hash ? -1 : a.hash > b.hash ? 1 : 0));
				if (
					hashes.length !== next.length ||
					hashes.some((hash, i) => hash !== next[i]!.hash)
				)
					throw new Error(
						"Proof delete append must name exact prepared targets",
					);
			} else {
				next = await withLease(async (owner) => {
					const parents: Entry<ProofOperation>[] = [];
					for (const hash of hashes) {
						const bytes = await retained.parentOriginal({ owner, hash });
						if (!bytes) throw new Error(`Missing proof PUT parent: ${hash}`);
						await verifier.put({ cid: hash, bytes });
						parents.push(materialize({ cid: hash, bytes }));
					}
					return parents;
				});
			}
			requireReady();
			for (const parent of next) log.hlc.update(parent.meta.clock.timestamp);
			const entry = await (
				EntryV0 as unknown as {
					createWithExplicitGid<T>(
						args: Parameters<typeof EntryV0.create<T>>[0],
						gid: string,
					): Promise<Entry<T>>;
				}
			).createWithExplicitGid(
				{
					store: log.blocks,
					identity: log.identity,
					encoding: BORSH_ENCODING_PROOF_OPERATION,
					data: operation,
					deferStore: true,
					meta: {
						type,
						data: metadata,
						next,
						clock: new LamportClock({
							id: log.identity.publicKey.bytes,
							timestamp: log.hlc.now(),
						}),
					},
				},
				deriveProofKeyGid(scope, operation.key),
			);
			const original = {
				cid: entry.hash,
				bytes: Entry.getPreparedStorageBytes(entry)!,
			};
			const result = await commit(entry, original, undefined, onLocalCommit);
			return { entry: result.entry, removed: result.removed };
		},
		async join(entries, context) {
			requireReady();
			if (context.options?.reset || context.options?.trim)
				throw new Error("Unsupported proof owner join options");
			type Frame = {
				entry: Entry<ProofOperation>;
				original: OriginalProofEntry;
				missing: string[];
				next: number;
			};
			for (const root of entries) {
				const requested = await verifier.incoming({
					entry: root,
					resolveLocal: async (hash) => log.blocks.get(hash, { remote: false }),
				});
				let finished = false;
				const frames: Frame[] = [];
				let bytes = requested.bytes.byteLength;
				const push = async (original: OriginalProofEntry) => {
					context.signal.throwIfAborted();
					requireReady();
					const entry = materialize(original);
					const missing =
						entry.meta.type === EntryType.CUT
							? []
							: await withLease(async (owner) => {
									if (
										await retained.retiredOriginal({
											owner,
											hash: original.cid,
										})
									)
										return [];
									const pending: string[] = [];
									for (const hash of entry.meta.next) {
										const bytes = await retained.parentOriginal({
											owner,
											hash,
										});
										if (!bytes) {
											pending.push(hash);
											continue;
										}
										await verifier.put({ cid: hash, bytes });
										checkEdge(entry, materialize({ cid: hash, bytes }));
									}
									return pending;
								});
					// Evict only uncommitted prefixes. The retained suffix commits a ready
					// ancestor before restarting the authenticated requested root.
					while (
						frames.length &&
						(frames.length >= maxEntries ||
							bytes + original.bytes.byteLength > maxBytes)
					) {
						bytes -= frames.shift()!.original.bytes.byteLength;
					}
					if (bytes + original.bytes.byteLength > maxBytes)
						throw new Error("Proof join staging byte budget exceeded");
					frames.push({ entry, original, missing, next: 0 });
					bytes += original.bytes.byteLength;
				};
				while (!finished) {
					await push(requested);
					let progressed = false;
					while (frames.length) {
						context.signal.throwIfAborted();
						requireReady();
						const frame = frames[frames.length - 1]!;
						if (frame.next < frame.missing.length) {
							const hash = frame.missing[frame.next++]!;
							let parent = context.references.get(hash);
							if (!parent) {
								const from = await context.resolveRemoteFrom(
									hash,
									context.signal,
								);
								parent = await Entry.fromMultihash<ProofOperation>(
									log.blocks,
									hash,
									{
										remote: {
											...context.remote,
											signal: context.signal,
											...(from?.length ? { from } : {}),
										},
									},
								);
							}
							const original = await verifier.incoming({
								entry: parent,
								resolveLocal: async (cid) =>
									log.blocks.get(cid, { remote: false }),
							});
							if (original.cid !== hash)
								throw new Error("Wrong proof parent reference");
							const fact = await verifier.put(original);
							checkEdge(
								frame.entry,
								materialize({ cid: fact.hash, bytes: fact.bytes }),
							);
							await push(original);
							continue;
						}
						const result = await commit(
							frame.entry,
							frame.original,
							context.signal,
						);
						await notify(result, context);
						// A frame may have been admitted by another branch after its missing
						// snapshot. Its successful durable completion is still new knowledge;
						// the next root pass sees that admitted row or retirement proof.
						progressed = true;
						finished ||= frame.original.cid === requested.cid;
						frames.pop();
						bytes -= frame.original.bytes.byteLength;
					}
					if (!finished && !progressed)
						throw new Error("Proof join made no durable progress");
				}
			}
		},
	});
	return {
		assertReady: requireReady,
		async preparePut(key: IdKey, data: Uint8Array): Promise<Prepared> {
			requireReady();
			const operation = new ProofPutOperation({ scope, key, data });
			return withLease(async (owner) => {
				const winner = await retained.winner({ owner, key: operation.key });
				return {
					operation,
					next: winner
						? [materialize({ cid: winner.hash, bytes: winner.bytes })]
						: [],
				};
			});
		},
		async prepareDelete(key: IdKey): Promise<Prepared> {
			requireReady();
			const ownedKey = deserialize(serialize(key), IdKey);
			return withLease(async (owner) => {
				const winner = await retained.winner({ owner, key: ownedKey });
				if (!winner) throw new Error("Proof document not found");
				const history = await verifier.collectHistory({
					target: winner.hash,
					key: ownedKey,
					resolve: (hash) => retained.parentOriginal({ owner, hash }),
				});
				return {
					operation: new ProofDeleteOperation({
						scope,
						key: ownedKey,
						target: winner.hash,
						witnesses: [...history.targets, ...history.boundaries].map(
							(fact) => fact.bytes,
						),
					}),
					next: history.targets
						.map((fact) => materialize({ cid: fact.hash, bytes: fact.bytes }))
						.sort((a, b) => (a.hash < b.hash ? -1 : a.hash > b.hash ? 1 : 0)),
				};
			});
		},
		async recover() {
			active();
			if (ready || recovering)
				throw new Error("Proof owner recovery already started");
			recovering = true;
			try {
				for (const type of [EntryType.CUT, EntryType.APPEND]) {
					const sort = [
						new Sort({ key: ["meta", "gid"], direction: SortDirection.ASC }),
						...[
							["meta", "clock", "timestamp", "wallTime"],
							["meta", "clock", "timestamp", "logical"],
							["hash"],
						].map((key) => new Sort({ key, direction: SortDirection.DESC })),
					];
					const iterator = index.iterate([queryType(type)], sort, {
						type: "shape",
						shape: { hash: true, meta: { gid: true } },
					});
					let previousGid: string | undefined;
					try {
						for (;;) {
							active();
							const page = await iterator.next(PAGE_SIZE);
							if (!page.length) break;
							for (const row of page) {
								active();
								const bytes = await log.blocks.get(row.hash, { remote: false });
								if (!bytes)
									throw new Error("Missing indexed proof recovery block");
								const original = { cid: row.hash, bytes };
								if (type === EntryType.CUT)
									await coordinator.deletion(original);
								else {
									const put = await verifier.put(original);
									if (put.gid !== row.meta.gid)
										throw new Error(
											"Proof recovery row disagrees with signed gid",
										);
									const entry = materialize(original);
									if (put.gid === previousGid) {
										await coordinator.validatePut(put);
										await withLease(async (owner) => {
											for (const hash of put.next) {
												const bytes = await retained.parentOriginal({
													owner,
													hash,
												});
												if (!bytes)
													throw new Error(
														`Missing proof recovery parent: ${hash}`,
													);
												await verifier.put({ cid: hash, bytes });
												checkEdge(entry, materialize({ cid: hash, bytes }));
											}
										});
									}
									log.hlc.update(entry.meta.clock.timestamp);
									if (put.gid !== previousGid) await coordinator.put(original);
									previousGid = put.gid;
								}
							}
						}
					} finally {
						await iterator.close();
					}
				}
				active();
				ready = true;
			} finally {
				recovering = false;
			}
		},
	};
};
