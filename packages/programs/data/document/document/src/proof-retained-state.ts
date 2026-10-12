import { deserialize, serialize } from "@dao-xyz/borsh";
import {
	Compare,
	IdKey,
	IntegerCompare,
	Sort,
	SortDirection,
	StringKey,
	StringMatch,
	StringMatchMethod,
	Uint8ArrayKey,
	toId,
} from "@peerbit/indexer-interface";
import {
	type EntryIndexHashMutationLockOwner,
	EntryType,
	type Log,
} from "@peerbit/log";
import { equals } from "uint8arrays";
import { MAX_BATCH_SIZE } from "./constants.js";
import {
	type ProofDeleteContract,
	copyProofDeleteContract,
	deriveProofDeleteScope,
} from "./proof-delete-manifest.js";
import {
	type ProofOperation,
	type VerifiedProofPut,
	createProofOperationVerifier,
	deriveProofKeyGid,
} from "./proof-operations.js";

const PAGE_SIZE = 32;
const MAX_PUT_CANDIDATES = 256;
const MAX_MARKER_CANDIDATES = 1024;
const MAX_READ_BYTES = 32 * 1024 * 1024;
const byteLength = Object.getOwnPropertyDescriptor(
	Object.getPrototypeOf(Uint8Array.prototype),
	"byteLength",
)!.get!;
const copyBytes = (value: Uint8Array, maximum: number) => {
	const length = byteLength.call(value) as number;
	if (length > maximum)
		throw new Error("Proof retained-state byte budget exceeded");
	const result = new Uint8Array(length);
	Uint8Array.prototype.set.call(result, value);
	return result;
};
const exact = (key: string[], value: string) =>
	new StringMatch({
		key,
		value,
		method: StringMatchMethod.exact,
		caseInsensitive: false,
	});
const entryType = (value: EntryType) =>
	new IntegerCompare({ key: ["meta", "type"], value, compare: Compare.Equal });
const winnerSort = [
	["meta", "clock", "timestamp", "wallTime"],
	["meta", "clock", "timestamp", "logical"],
	["hash"],
].map((key) => new Sort({ key, direction: SortDirection.DESC }));
type ReadOptions = {
	owner: EntryIndexHashMutationLockOwner;
	signal?: AbortSignal;
};

/**
 * Private read side of the Documents protocol owner. The caller holds one
 * exclusive lower lease, does not mutate under it concurrently, and supplies
 * the current Documents lifecycle fence. No witness is admitted by these reads.
 * The owner first recovers uncertain durable intents; these reads do not prove
 * fsync. Exhausted scan/byte budgets throw, never imply absence or a winner.
 *
 * Ordering uses the locally derived admitted-row index, then verifies each
 * selected row against signed bytes. This is not a malicious-local-DB integrity
 * proof. Queries may materialize backend state beyond one result page.
 */
export const createProofRetainedState = (properties: {
	log: Log<ProofOperation>;
	contract: ProofDeleteContract;
	assertActive: () => void;
	// Only an already-admitted physical lower mutation may drain after close
	// starts. This must throw once that captured admission/lease is no longer held.
	assertAdmitted?: () => void;
}) => {
	const { log, assertActive, assertAdmitted } = properties;
	const index = log.entryIndex;
	const contract = copyProofDeleteContract(properties.contract);
	const { keyKind } = contract;
	const scope = deriveProofDeleteScope(contract);
	const verifier = createProofOperationVerifier({ contract });
	// A commit can resolve many retired parents and then select its winner under
	// the same lease. Method-local budgets would multiply that work per parent.
	const budgets = new WeakMap<
		EntryIndexHashMutationLockOwner,
		{ bytes: number; markers: number; puts: number }
	>();
	const captureKey = (key: IdKey) => {
		if (key.constructor !== (keyKind === 0 ? StringKey : Uint8ArrayKey)) {
			throw new Error("Wrong proof retained-state key kind");
		}
		return deserialize(serialize(key), IdKey);
	};
	const begin = ({ owner, signal }: ReadOptions) => {
		const check = () => {
			signal?.throwIfAborted();
			assertActive();
			if (log.entryIndex !== index || (!assertAdmitted && log.closed)) {
				throw new Error("Stale proof retained-state lifecycle");
			}
			assertAdmitted?.();
			// One private cross-package owner check; queued future admissions do not
			// invalidate this held lease. captureMutationGeneration excludes owners.
			const state = (
				index as unknown as {
					getHashMutationLockOwnerState(
						owner: EntryIndexHashMutationLockOwner,
					): {
						exclusive: boolean;
					};
				}
			).getHashMutationLockOwnerState(owner);
			if (!state.exclusive)
				throw new Error("Proof retained-state requires exclusive ownership");
		};
		const wait = async <T>(operation: () => T | Promise<T>): Promise<T> => {
			check();
			const result = await operation();
			check();
			return result;
		};
		check();
		let budget = budgets.get(owner);
		if (!budget) {
			budget = { bytes: 0, markers: 0, puts: 0 };
			budgets.set(owner, budget);
		}
		const heldBudget = budget;
		const read = async (hash: string) => {
			// Block length is not known until fetched. Reserve the maximum supported
			// entry first: an incomplete budget fails closed, never fetches another
			// valid 5MB block beyond the aggregate cap or implies target absence.
			if (MAX_READ_BYTES - heldBudget.bytes < MAX_BATCH_SIZE)
				throw new Error("Proof retained-state byte budget exceeded");
			heldBudget.bytes += MAX_BATCH_SIZE;
			const value = await wait(() =>
				index.properties.store.get(hash, { remote: false }),
			);
			if (!value) throw new Error("Missing indexed proof retained-state block");
			// Own bytes before signature verification yields or the source reuses a view.
			const bytes = copyBytes(value, MAX_BATCH_SIZE);
			heldBudget.bytes -= MAX_BATCH_SIZE - bytes.byteLength;
			return bytes;
		};
		const retired = async (hash: string, requiredKey?: IdKey) => {
			const iterator = index.iterate(
				[entryType(EntryType.CUT), exact(["meta", "next"], hash)],
				undefined,
				{ type: "shape", shape: { hash: true } },
			);
			try {
				const [row] = await wait(() => iterator.next(1));
				if (!row) return undefined;
				if (++heldBudget.markers > MAX_MARKER_CANDIDATES) {
					throw new Error("Proof retained-state marker budget exceeded");
				}
				const bytes = await read(row.hash);
				const proof = await wait(() =>
					verifier.deletion({ cid: row.hash, bytes }),
				);
				const fact = proof.targets.find((target) => target.hash === hash);
				if (!fact)
					throw new Error("Indexed proof marker does not retire target");
				if (
					requiredKey &&
					!equals(serialize(requiredKey), serialize(fact.key))
				) {
					throw new Error("Wrong retained proof target key");
				}
				return fact;
			} finally {
				await iterator.close();
				check();
			}
		};
		return { check, wait, read, retired, budget: heldBudget };
	};
	return {
		// A raw block alone is not admission. Prefer indexed originals, otherwise
		// recover only authenticated retained proof. The caller still verifies
		// the original PUT and its causal clock before following any parent edge.
		async parentOriginal(
			input: ReadOptions & { hash: string },
		): Promise<Uint8Array | undefined> {
			const { hash, owner, signal } = input;
			const operation = begin({ owner, signal });
			await operation.wait(() => index.flushPendingWrites(undefined, owner));
			const row = await operation.wait(() =>
				index.properties.index.get(toId(hash)),
			);
			if (row) return operation.read(hash);
			const fact = await operation.retired(hash);
			operation.check();
			return fact && copyBytes(fact.bytes, MAX_BATCH_SIZE);
		},
		async winner(
			input: ReadOptions & { key: IdKey },
		): Promise<VerifiedProofPut | undefined> {
			const { owner, signal } = input;
			const key = captureKey(input.key);
			const keyBytes = serialize(key);
			const gid = deriveProofKeyGid(scope, key);
			const operation = begin({ owner, signal });
			// iterate() otherwise flushes ownerlessly, which can wait on our own lease.
			await operation.wait(() => index.flushPendingWrites(undefined, owner));
			const iterator = index.iterate(
				[entryType(EntryType.APPEND), exact(["meta", "gid"], gid)],
				winnerSort,
				{
					type: "shape",
					shape: { hash: true, meta: { gid: true, clock: true } },
				},
			);
			try {
				for (;;) {
					const page = await operation.wait(() => iterator.next(PAGE_SIZE));
					if (page.length === 0) return undefined;
					for (const row of page) {
						if (++operation.budget.puts > MAX_PUT_CANDIDATES) {
							throw new Error("Proof retained-state PUT budget exceeded");
						}
						const bytes = await operation.read(row.hash);
						const fact = await operation.wait(() =>
							verifier.put({ cid: row.hash, bytes }),
						);
						if (
							!equals(serialize(fact.key), keyBytes) ||
							fact.gid !== row.meta.gid ||
							fact.wallTime !== row.meta.clock.timestamp.wallTime ||
							fact.logical !== row.meta.clock.timestamp.logical
						)
							throw new Error(
								"Proof retained-state row disagrees with signed PUT",
							);
						if (!(await operation.retired(fact.hash, key))) return fact;
					}
				}
			} finally {
				await iterator.close();
				operation.check();
			}
		},
		async retiredOriginal(
			input: ReadOptions & { hash: string; key?: IdKey },
		): Promise<Uint8Array | undefined> {
			const { hash, owner } = input;
			const key = input.key === undefined ? undefined : captureKey(input.key);
			const operation = begin(input);
			await operation.wait(() => index.flushPendingWrites(undefined, owner));
			const fact = await operation.retired(hash, key);
			operation.check();
			return fact && copyBytes(fact.bytes, MAX_BATCH_SIZE);
		},
	};
};
