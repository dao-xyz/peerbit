import { deserialize } from "@dao-xyz/borsh";
import { type IdKey, toId } from "@peerbit/indexer-interface";
import {
	Entry,
	type EntryIndexHashMutationLockOwner,
	type Log,
	type ShallowEntry,
} from "@peerbit/log";
import type { ProofDeleteContract } from "./proof-delete-manifest.js";
import {
	BORSH_ENCODING_PROOF_OPERATION,
	type OriginalProofEntry,
	type ProofOperation,
	type VerifiedProofPut,
	createProofOperationVerifier,
} from "./proof-operations.js";
import { createProofRetainedState } from "./proof-retained-state.js";

type Lease = EntryIndexHashMutationLockOwner;
type CommitFacts = {
	entry: Entry<ProofOperation>;
	removed: ShallowEntry[];
	owner: Lease;
};
type Admission = { poison(error: unknown): void; release(): void };
type PreparedCoordinates = {
	complete(input: Pick<CommitFacts, "removed" | "owner">): Promise<void>;
	/** Fences both coordinates and the caller's projection, after project(). */
	barrier(): Promise<void>;
	release(): void;
};
type CommitOptions = {
	signal?: AbortSignal;
	/** Local row admission evidence, not proof of remote or coupled durability. */
	onLocalCommit?(entry: Entry<ProofOperation>): void;
};
export type ProofCommitResult = Omit<CommitFacts, "owner"> & {
	/** A new lower row was admitted, rather than an existing operation replayed. */
	changed: boolean;
	/** False only for an authenticated PUT already covered by retained proof. */
	admitted: boolean;
};
const byteLength = Object.getOwnPropertyDescriptor(
	Object.getPrototypeOf(Uint8Array.prototype),
	"byteLength",
)!.get!;
const copy32 = (value: Uint8Array) => {
	if (byteLength.call(value) !== 32)
		throw new Error("Expected 32-byte proof contract field");
	const copy = new Uint8Array(32);
	Uint8Array.prototype.set.call(copy, value);
	return copy;
};
const detached = (block: { hash: string; bytes: Uint8Array }) => {
	const entry = deserialize(block.bytes, Entry) as Entry<ProofOperation>;
	entry.hash = Entry.prepareMultihashBytes(entry, block.bytes, block.hash);
	entry.init({ encoding: BORSH_ENCODING_PROOF_OPERATION });
	return entry;
};
const earlier = (parent: VerifiedProofPut, child: VerifiedProofPut) =>
	parent.wallTime < child.wallTime ||
	(parent.wallTime === child.wallTime && parent.logical < child.logical);

/**
 * Private protocol-owner commit path for owner-proofs Documents. Callbacks
 * are mandatory trusted owner work, not application authorization hooks. The
 * block/lower-index marker barriers precede any exact deletion; coupled state
 * and its barrier finish before receipt admission is released. A failed write
 * keeps poison until close/reopen recovery, never an in-place blind retry.
 * Cancellation before writes rejects; after writes start this returns the
 * completed durable result (or failure), not a misleading cancelled outcome.
 */
export const createProofCommitCoordinator = (properties: {
	log: Log<ProofOperation>;
	contract: ProofDeleteContract;
	assertActive(): void;
	beginAdmission(): Admission;
	/** Trusted projection codec/id consistency check, never application permission. */
	validatePut?(fact: VerifiedProofPut): void | Promise<void>;
	prepareCoordinates(
		entry: Entry<ProofOperation>,
	): Promise<PreparedCoordinates>;
	project(input: {
		key: IdKey;
		winner: VerifiedProofPut | undefined;
		owner: Lease;
	}): Promise<void | (() => void)>;
}) => {
	const {
		log,
		assertActive,
		beginAdmission,
		validatePut,
		prepareCoordinates,
		project,
	} = properties;
	const index = log.entryIndex;
	const store = index.properties.store;
	const blockDurability = store.crashSafeDurability;
	const indexDurability = index.properties.index.crashSafeDurability;
	if (
		blockDurability?.crashSafe !== true ||
		typeof blockDurability.barrier !== "function" ||
		indexDurability?.crashSafe !== true ||
		typeof indexDurability.barrier !== "function"
	) {
		throw new Error(
			"Proof commits require crash-safe block and index barriers",
		);
	}
	const { version, nonce, owner: keyOwner, keyKind } = properties.contract;
	const contract = {
		version,
		nonce: copy32(nonce),
		owner: copy32(keyOwner),
		keyKind,
	};
	const verifier = createProofOperationVerifier({ contract });
	const lower = log as unknown as {
		withNativeCommittedAppendAdmission<T>(
			operation: () => Promise<T>,
		): Promise<T>;
		runWithMutationCallback<T>(operation: () => Promise<T>): Promise<T>;
	};
	const checkCurrent = () => {
		if (log.entryIndex !== index)
			throw new Error("Stale proof commit lower lifecycle");
	};
	const validatePutFact = async (fact: VerifiedProofPut) => {
		if (validatePut)
			await lower.runWithMutationCallback(async () => validatePut(fact));
	};
	const commit = (
		kind: "put" | "delete",
		input: OriginalProofEntry,
		options?: CommitOptions,
	): Promise<ProofCommitResult> =>
		lower.withNativeCommittedAppendAdmission(async () => {
			checkCurrent();
			assertActive();
			options?.signal?.throwIfAborted();
			const admission = beginAdmission();
			let owner: Lease | undefined;
			let coordinates: PreparedCoordinates | undefined;
			let mutated = false;
			let admittedScope = true;
			const preflight = () => {
				checkCurrent();
				assertActive();
				options?.signal?.throwIfAborted();
			};
			const assertAdmitted = () => {
				checkCurrent();
				if (!admittedScope || !owner)
					throw new Error("Proof commit no longer owns lower admission");
			};
			try {
				// These verifiers copy caller bytes synchronously before their first await.
				const fact = await (kind === "put"
					? verifier.put(input)
					: verifier.deletion(input));
				preflight();
				if (kind === "put") {
					await validatePutFact(fact as VerifiedProofPut);
					preflight();
				}
				const entry = detached(fact);
				owner = await index.acquireExclusiveMutationLock();
				preflight();
				const retained = createProofRetainedState({
					log,
					contract,
					assertActive: checkCurrent,
					assertAdmitted,
				});
				if (kind === "put") {
					if (await retained.retiredOriginal({ owner, hash: fact.hash })) {
						preflight();
						return { entry, changed: false, admitted: false, removed: [] };
					}
					const put = fact as VerifiedProofPut;
					for (const hash of put.next) {
						preflight();
						const bytes = await retained.parentOriginal({ owner, hash });
						preflight();
						if (!bytes) throw new Error(`Missing proof PUT parent: ${hash}`);
						const parent = await verifier.put({ cid: hash, bytes });
						preflight();
						if (!earlier(parent, put))
							throw new Error(
								"Proof PUT parent must have an earlier signed clock",
							);
					}
				}
				const removed =
					"targets" in fact
						? fact.targets.map((target) => detached(target).toShallow(false))
						: [];
				const existing = await index.properties.index.get(toId(fact.hash));
				preflight();
				coordinates = await lower.runWithMutationCallback(() =>
					prepareCoordinates(entry),
				);
				preflight();
				log.hlc.update(entry.meta.clock.timestamp);
				// From this point cancellation/terminal requests drain the exact admitted
				// operation. Never turn a partial local commit into cancelled-success.
				mutated = true;
				const cid = await store.put(
					Entry.preparedBlockFromBytes(fact.bytes, fact.hash),
				);
				if (cid !== fact.hash)
					throw new Error("Proof committed block CID mismatch");
				// Never persist a marker row before its original signed block is durable.
				await blockDurability.barrier();
				if (!existing) {
					await index.put(entry, {
						unique: true,
						isHead: true,
						toMultiHash: false,
						hashMutationLockOwner: owner,
					});
				}
				if (options?.onLocalCommit)
					await lower.runWithMutationCallback(async () =>
						options.onLocalCommit!(entry),
					);
				await indexDurability.barrier();
				if (removed.length) await index.completeExactDelete(removed, owner);
				await lower.runWithMutationCallback(() =>
					coordinates!.complete({ removed, owner: owner! }),
				);
				const winner = await retained.winner({ owner, key: fact.key });
				const notify = await lower.runWithMutationCallback(() =>
					project({ key: fact.key, winner, owner: owner! }),
				);
				await blockDurability.barrier();
				await indexDurability.barrier();
				await lower.runWithMutationCallback(() => coordinates!.barrier());
				if (notify) await lower.runWithMutationCallback(async () => notify());
				return { entry, removed, changed: !existing, admitted: true };
			} catch (error) {
				const failure = error ?? new Error("Proof commit failed");
				if (mutated) {
					index.poisonNativeDurableTransactionMutations(failure);
					admission.poison(failure);
				}
				throw failure;
			} finally {
				admittedScope = false;
				try {
					if (owner) index.releaseHashMutationLocks(owner);
					coordinates?.release();
				} finally {
					admission.release();
				}
			}
		});
	return {
		/** Recovery also checks nonwinning rows without reprojecting each row. */
		validatePut: validatePutFact,
		put: (input: OriginalProofEntry, options?: CommitOptions) =>
			commit("put", input, options),
		deletion: (input: OriginalProofEntry, options?: CommitOptions) =>
			commit("delete", input, options),
	};
};
