import {
	deserialize,
	field,
	fixedArray,
	serialize,
	variant,
} from "@dao-xyz/borsh";
import { PublicSignKey, sha256Sync } from "@peerbit/crypto";
import { compare, concat, equals } from "uint8arrays";
import type {
	PolicyAdmissionResultV2,
	PolicyForkEvidenceV2,
	PolicyHeadProjectionV2,
	PolicyReducerDurableStateV2,
	PolicySnapshotResolverV2,
} from "./v2-policy-engine.js";
import {
	TrustedNetworkV2PolicyReducer,
	authenticatePolicySnapshotEntryV2,
} from "./v2-policy-engine.js";
import {
	NetworkDescriptorV2,
	PolicySubjectBindingV2,
	TRUSTED_NETWORK_V2_KNOWN_ROLE_BITS,
	TRUSTED_NETWORK_V2_MAX_POLICY_ENTRY_BYTES,
	assertNetworkDescriptorV2,
} from "./v2.js";

/**
 * Internal crash-safe policy-anchor storage format.
 *
 * This module is intentionally absent from the package entry point. Its store
 * must already be open and scoped to this namespace and descriptor. Exactly
 * one wrapper may write that scope: this append-only format deliberately does
 * not pretend that get-then-put is a multi-process compare-and-swap.
 */

export const TRUSTED_NETWORK_V2_POLICY_ANCHOR_STORE_OWNER =
	"peerbit/trusted-network/v2/policy-anchor/v1";
export const TRUSTED_NETWORK_V2_MAX_POLICY_ANCHOR_GENERATION_BYTES =
	TRUSTED_NETWORK_V2_MAX_POLICY_ENTRY_BYTES * 4;

const GENERATION_KEY_PREFIX = `${TRUSTED_NETWORK_V2_POLICY_ANCHOR_STORE_OWNER}/generation/`;
const FORMAT_VERSION = 1;
const MAX_U64 = 0xffffffffffffffffn;
const MAX_UNAVAILABLE_REASON_LENGTH = 512;
const ZERO_DIGEST = new Uint8Array(32);
const textEncoder = new TextEncoder();
const DESCRIPTOR_DIGEST_DOMAIN = textEncoder.encode(
	"peerbit/trusted-network/v2/policy-anchor/descriptor/v1",
);
const GENERATION_CHECKSUM_DOMAIN = textEncoder.encode(
	"peerbit/trusted-network/v2/policy-anchor/generation/v1",
);
const ANCHOR_STATE = Object.freeze({
	ACTIVE: 1,
	UNAVAILABLE: 2,
	FORKED: 3,
} as const);
const GENERATION_KIND = Object.freeze({
	STATE: 1,
	FORK_OBSERVATION: 2,
} as const);
const MAX_STATE_GENERATION_PAYLOAD_BYTES =
	TRUSTED_NETWORK_V2_MAX_POLICY_ENTRY_BYTES * 4;
const MAX_OBSERVATION_GENERATION_PAYLOAD_BYTES =
	TRUSTED_NETWORK_V2_MAX_POLICY_ENTRY_BYTES + 1024;

type MaybePromiseV2<T> = T | Promise<T>;

export type CrashSafePolicyAnchorStoreV2 = {
	get(key: string): MaybePromiseV2<Uint8Array | undefined>;
	put(key: string, value: Uint8Array): MaybePromiseV2<void>;
	iterator(): AsyncIterable<[string, Uint8Array]>;
	readonly crashSafeDurability: {
		readonly crashSafe: true;
		barrier(): MaybePromiseV2<void>;
	};
};

type DurableReducerOptionsV2 = {
	descriptor: NetworkDescriptorV2;
	resolvePolicyEntry: PolicySnapshotResolverV2;
	resolveTimeoutMs?: number;
	signal?: AbortSignal;
	maxPending?: number;
	maxPendingPolicyBytes?: number;
};

export type TrustedNetworkV2DurablePolicyReducerOptions =
	DurableReducerOptionsV2 & {
		store: CrashSafePolicyAnchorStoreV2;
	};

@variant([2, 16, 0])
class PolicyAnchorCoreStateRecordV2 {
	@field({ type: "u8" })
	state: number;

	@field({ type: Uint8Array })
	acceptedHeadEntryBytes: Uint8Array;

	@field({ type: Uint8Array })
	comparisonCandidateEntryBytes: Uint8Array;

	@field({ type: fixedArray("u8", 32) })
	acceptedAncestorDigest: Uint8Array;

	@field({ type: "string" })
	unavailableReason: string;

	@field({ type: Uint8Array })
	forkChildEntryBytes0: Uint8Array;

	@field({ type: Uint8Array })
	forkChildEntryBytes1: Uint8Array;

	constructor(properties?: {
		state: number;
		acceptedHeadEntryBytes: Uint8Array;
		comparisonCandidateEntryBytes: Uint8Array;
		acceptedAncestorDigest: Uint8Array;
		unavailableReason: string;
		forkChildEntryBytes0: Uint8Array;
		forkChildEntryBytes1: Uint8Array;
	}) {
		if (properties) Object.assign(this, properties);
	}
}

@variant([2, 16, 3])
class PolicyAnchorStateGenerationPayloadV2 {
	@field({ type: PolicyAnchorCoreStateRecordV2 })
	coreState: PolicyAnchorCoreStateRecordV2;

	constructor(properties?: { coreState: PolicyAnchorCoreStateRecordV2 }) {
		if (properties) Object.assign(this, properties);
	}
}

@variant([2, 16, 4])
class PolicyAnchorObservationGenerationPayloadV2 {
	@field({ type: Uint8Array })
	entryBytes: Uint8Array;

	constructor(properties?: { entryBytes: Uint8Array }) {
		if (properties) Object.assign(this, properties);
	}
}

@variant([2, 16, 1])
class PolicyAnchorGenerationBodyV2 {
	@field({ type: "u8" })
	formatVersion: number;

	@field({ type: "u64" })
	generation: bigint;

	@field({ type: fixedArray("u8", 32) })
	descriptorDigest: Uint8Array;

	@field({ type: fixedArray("u8", 32) })
	previousGenerationChecksum: Uint8Array;

	@field({ type: "u8" })
	kind: number;

	@field({ type: Uint8Array })
	payloadBytes: Uint8Array;

	constructor(properties?: {
		formatVersion: number;
		generation: bigint;
		descriptorDigest: Uint8Array;
		previousGenerationChecksum: Uint8Array;
		kind: number;
		payloadBytes: Uint8Array;
	}) {
		if (properties) Object.assign(this, properties);
	}
}

@variant([2, 16, 2])
class PolicyAnchorGenerationRecordV2 {
	@field({ type: PolicyAnchorGenerationBodyV2 })
	body: PolicyAnchorGenerationBodyV2;

	@field({ type: fixedArray("u8", 32) })
	checksum: Uint8Array;

	constructor(properties?: {
		body: PolicyAnchorGenerationBodyV2;
		checksum: Uint8Array;
	}) {
		if (properties) Object.assign(this, properties);
	}
}

type CanonicalForkChildV2 = {
	digest: Uint8Array;
	entryBytes: Uint8Array;
};

type PublishedProjectionV2 = {
	state: "EMPTY" | "ACTIVE" | "UNAVAILABLE" | "FORKED";
	head?: PolicyHeadProjectionV2;
	forkEvidence?: PolicyForkEvidenceV2;
	roles: Map<string, number>;
};

const copyBytes = (bytes: Uint8Array): Uint8Array => Uint8Array.from(bytes);

const bytesKey = (bytes: Uint8Array): string => {
	let key = "";
	for (const byte of bytes) key += byte.toString(16).padStart(2, "0");
	return key;
};

const copyBinding = (binding: PolicySubjectBindingV2): PolicySubjectBindingV2 =>
	deserialize(serialize(binding), PolicySubjectBindingV2);

const copyHead = (
	head: PolicyHeadProjectionV2 | undefined,
): PolicyHeadProjectionV2 | undefined =>
	head === undefined
		? undefined
		: {
				sequence: head.sequence,
				digest: copyBytes(head.digest),
				bindings: head.bindings.map(copyBinding),
			};

const copyForkEvidence = (
	evidence: PolicyForkEvidenceV2 | undefined,
): PolicyForkEvidenceV2 | undefined =>
	evidence === undefined
		? undefined
		: {
				commonParent: copyHead(evidence.commonParent)!,
				children: [
					{
						sequence: evidence.children[0].sequence,
						digest: copyBytes(evidence.children[0].digest),
						entryBytes: copyBytes(evidence.children[0].entryBytes),
					},
					{
						sequence: evidence.children[1].sequence,
						digest: copyBytes(evidence.children[1].digest),
						entryBytes: copyBytes(evidence.children[1].entryBytes),
					},
				],
			};

const keyId = (key: PublicSignKey): string => bytesKey(serialize(key));

const descriptorDigest = (descriptor: NetworkDescriptorV2): Uint8Array =>
	sha256Sync(concat([DESCRIPTOR_DIGEST_DOMAIN, serialize(descriptor)]));

const generationChecksum = (body: PolicyAnchorGenerationBodyV2): Uint8Array =>
	sha256Sync(concat([GENERATION_CHECKSUM_DOMAIN, serialize(body)]));

const observationContentHash = (entryBytes: Uint8Array): Uint8Array =>
	sha256Sync(entryBytes);

const generationKey = (generation: bigint): string =>
	`${GENERATION_KEY_PREFIX}${generation.toString().padStart(20, "0")}`;

const assertOpenNotAborted = (signal: AbortSignal | undefined): void => {
	if (signal?.aborted) {
		throw new Error("TrustedNetwork v2 durable policy open was aborted");
	}
};

const assertEntryBytes = (bytes: Uint8Array, label: string): void => {
	if (
		!(bytes instanceof Uint8Array) ||
		bytes.byteLength === 0 ||
		bytes.byteLength > TRUSTED_NETWORK_V2_MAX_POLICY_ENTRY_BYTES
	) {
		throw new Error(
			`${label} must contain 1-${TRUSTED_NETWORK_V2_MAX_POLICY_ENTRY_BYTES} bytes`,
		);
	}
};

const retainCanonicalForkChild = (
	children: CanonicalForkChildV2[],
	candidate: CanonicalForkChildV2,
): void => {
	const existingIndex = children.findIndex(({ digest }) =>
		equals(digest, candidate.digest),
	);
	if (existingIndex >= 0) {
		if (
			compare(candidate.entryBytes, children[existingIndex]!.entryBytes) >= 0
		) {
			return;
		}
		children.splice(existingIndex, 1);
	}
	children.push({
		digest: copyBytes(candidate.digest),
		entryBytes: copyBytes(candidate.entryBytes),
	});
	children.sort((left, right) => {
		const digestOrder = compare(left.digest, right.digest);
		return digestOrder === 0
			? compare(left.entryBytes, right.entryBytes)
			: digestOrder;
	});
	if (children.length > 2) children.length = 2;
};

const assertEmptyBytes = (bytes: Uint8Array, label: string): void => {
	if (!(bytes instanceof Uint8Array) || bytes.byteLength !== 0) {
		throw new Error(`${label} must be empty`);
	}
};

const coreRecordFromState = (
	state: Exclude<PolicyReducerDurableStateV2, { state: "EMPTY" }>,
): PolicyAnchorCoreStateRecordV2 => {
	if (state.state === "ACTIVE") {
		return new PolicyAnchorCoreStateRecordV2({
			state: ANCHOR_STATE.ACTIVE,
			acceptedHeadEntryBytes: copyBytes(state.acceptedHeadEntryBytes),
			comparisonCandidateEntryBytes: new Uint8Array(0),
			acceptedAncestorDigest: copyBytes(ZERO_DIGEST),
			unavailableReason: "",
			forkChildEntryBytes0: new Uint8Array(0),
			forkChildEntryBytes1: new Uint8Array(0),
		});
	}
	if (state.state === "UNAVAILABLE") {
		return new PolicyAnchorCoreStateRecordV2({
			state: ANCHOR_STATE.UNAVAILABLE,
			acceptedHeadEntryBytes: copyBytes(state.acceptedHeadEntryBytes),
			comparisonCandidateEntryBytes: copyBytes(
				state.comparisonCandidateEntryBytes,
			),
			acceptedAncestorDigest: copyBytes(state.acceptedAncestorDigest),
			unavailableReason: state.reason,
			forkChildEntryBytes0: new Uint8Array(0),
			forkChildEntryBytes1: new Uint8Array(0),
		});
	}
	return new PolicyAnchorCoreStateRecordV2({
		state: ANCHOR_STATE.FORKED,
		acceptedHeadEntryBytes: copyBytes(state.commonParentEntryBytes),
		comparisonCandidateEntryBytes: new Uint8Array(0),
		acceptedAncestorDigest: copyBytes(ZERO_DIGEST),
		unavailableReason: "",
		forkChildEntryBytes0: copyBytes(state.childEntryBytes[0]),
		forkChildEntryBytes1: copyBytes(state.childEntryBytes[1]),
	});
};

const durableStateFromCoreRecord = (
	record: PolicyAnchorCoreStateRecordV2,
): Exclude<PolicyReducerDurableStateV2, { state: "EMPTY" }> => {
	assertEntryBytes(record.acceptedHeadEntryBytes, "accepted head entry");
	if (record.state === ANCHOR_STATE.ACTIVE) {
		assertEmptyBytes(
			record.comparisonCandidateEntryBytes,
			"active comparison candidate",
		);
		if (!equals(record.acceptedAncestorDigest, ZERO_DIGEST)) {
			throw new Error("Active accepted-ancestor digest must be zero");
		}
		if (record.unavailableReason !== "") {
			throw new Error("Active unavailable reason must be empty");
		}
		assertEmptyBytes(record.forkChildEntryBytes0, "active fork child 0");
		assertEmptyBytes(record.forkChildEntryBytes1, "active fork child 1");
		return {
			formatVersion: FORMAT_VERSION,
			state: "ACTIVE",
			acceptedHeadEntryBytes: copyBytes(record.acceptedHeadEntryBytes),
		};
	}
	if (record.state === ANCHOR_STATE.UNAVAILABLE) {
		assertEntryBytes(
			record.comparisonCandidateEntryBytes,
			"unavailable comparison candidate",
		);
		if (record.acceptedAncestorDigest.byteLength !== 32) {
			throw new Error("Unavailable accepted-ancestor digest must be 32 bytes");
		}
		if (
			record.unavailableReason.length === 0 ||
			record.unavailableReason.length > MAX_UNAVAILABLE_REASON_LENGTH
		) {
			throw new Error(
				"Unavailable reason is empty or exceeds its character ceiling",
			);
		}
		assertEmptyBytes(record.forkChildEntryBytes0, "unavailable fork child 0");
		assertEmptyBytes(record.forkChildEntryBytes1, "unavailable fork child 1");
		return {
			formatVersion: FORMAT_VERSION,
			state: "UNAVAILABLE",
			acceptedHeadEntryBytes: copyBytes(record.acceptedHeadEntryBytes),
			comparisonCandidateEntryBytes: copyBytes(
				record.comparisonCandidateEntryBytes,
			),
			acceptedAncestorDigest: copyBytes(record.acceptedAncestorDigest),
			reason: record.unavailableReason,
		};
	}
	if (record.state === ANCHOR_STATE.FORKED) {
		assertEmptyBytes(
			record.comparisonCandidateEntryBytes,
			"forked comparison candidate",
		);
		if (!equals(record.acceptedAncestorDigest, ZERO_DIGEST)) {
			throw new Error("Forked accepted-ancestor digest must be zero");
		}
		if (record.unavailableReason !== "") {
			throw new Error("Forked unavailable reason must be empty");
		}
		assertEntryBytes(record.forkChildEntryBytes0, "fork child 0");
		assertEntryBytes(record.forkChildEntryBytes1, "fork child 1");
		return {
			formatVersion: FORMAT_VERSION,
			state: "FORKED",
			commonParentEntryBytes: copyBytes(record.acceptedHeadEntryBytes),
			childEntryBytes: [
				copyBytes(record.forkChildEntryBytes0),
				copyBytes(record.forkChildEntryBytes1),
			],
		};
	}
	throw new Error("Unknown durable policy-anchor state");
};

type DecodedGenerationPayloadV2 =
	| {
			kind: "state";
			payload: PolicyAnchorStateGenerationPayloadV2;
			durableState: Exclude<PolicyReducerDurableStateV2, { state: "EMPTY" }>;
	  }
	| {
			kind: "fork-observation";
			payload: PolicyAnchorObservationGenerationPayloadV2;
	  };

const decodeCanonicalPayload = <T>(
	input: Uint8Array,
	type: new (...args: any[]) => T,
	maxBytes: number,
	label: string,
): T => {
	if (
		!(input instanceof Uint8Array) ||
		input.byteLength === 0 ||
		input.byteLength > maxBytes
	) {
		throw new Error(`${label} exceeds its byte ceiling`);
	}
	const bytes = copyBytes(input);
	const payload = deserialize(bytes, type);
	if (!equals(bytes, serialize(payload))) {
		throw new Error(`${label} is not canonical`);
	}
	return payload;
};

const decodeGenerationPayload = (
	body: PolicyAnchorGenerationBodyV2,
): DecodedGenerationPayloadV2 => {
	if (body.kind === GENERATION_KIND.STATE) {
		const payload = decodeCanonicalPayload(
			body.payloadBytes,
			PolicyAnchorStateGenerationPayloadV2,
			MAX_STATE_GENERATION_PAYLOAD_BYTES,
			"Policy-anchor state payload",
		);
		return {
			kind: "state",
			payload,
			durableState: durableStateFromCoreRecord(payload.coreState),
		};
	}
	if (body.kind === GENERATION_KIND.FORK_OBSERVATION) {
		const payload = decodeCanonicalPayload(
			body.payloadBytes,
			PolicyAnchorObservationGenerationPayloadV2,
			MAX_OBSERVATION_GENERATION_PAYLOAD_BYTES,
			"Policy fork-observation payload",
		);
		assertEntryBytes(payload.entryBytes, "fork observation entry");
		return { kind: "fork-observation", payload };
	}
	throw new Error("Unknown policy-anchor generation kind");
};

const decodeGenerationRecord = (
	input: Uint8Array,
): {
	record: PolicyAnchorGenerationRecordV2;
	payload: DecodedGenerationPayloadV2;
} => {
	if (
		!(input instanceof Uint8Array) ||
		input.byteLength === 0 ||
		input.byteLength > TRUSTED_NETWORK_V2_MAX_POLICY_ANCHOR_GENERATION_BYTES
	) {
		throw new Error("Policy-anchor generation record exceeds its byte ceiling");
	}
	const bytes = copyBytes(input);
	const record = deserialize(bytes, PolicyAnchorGenerationRecordV2);
	if (!equals(bytes, serialize(record))) {
		throw new Error("Policy-anchor generation record is not canonical");
	}
	if (record.body.formatVersion !== FORMAT_VERSION) {
		throw new Error("Unsupported policy-anchor generation format");
	}
	if (!equals(record.checksum, generationChecksum(record.body))) {
		throw new Error("Policy-anchor generation checksum mismatch");
	}
	return { record, payload: decodeGenerationPayload(record.body) };
};

const publishedFromReducer = (
	reducer: TrustedNetworkV2PolicyReducer,
): PublishedProjectionV2 => {
	if (reducer.state === "HALTED") {
		throw new Error("Cannot publish a halted policy reducer");
	}
	const head = copyHead(reducer.head);
	const roles = new Map<string, number>();
	for (const binding of head?.bindings ?? []) {
		roles.set(keyId(binding.signingKey), binding.roles);
	}
	return {
		state: reducer.state,
		head,
		forkEvidence: copyForkEvidence(reducer.forkEvidence),
		roles,
	};
};

const assertCanonicalCoreRestore = (
	reducer: TrustedNetworkV2PolicyReducer,
	storedCoreBytes: Uint8Array,
): void => {
	const restoredState = reducer.exportDurableState();
	if (restoredState.state === "EMPTY") {
		throw new Error("Restored policy-anchor state is unexpectedly empty");
	}
	const restoredCoreBytes = serialize(coreRecordFromState(restoredState));
	if (!equals(restoredCoreBytes, storedCoreBytes)) {
		throw new Error("Restored policy-anchor state is not canonical");
	}
};

/**
 * Crash-safe publication wrapper for the internal v2 reducer.
 *
 * The mutable reducer is never exposed. Authorization reads use only the last
 * projection published after the store barrier. An admission places a
 * fail-closed fence around both reducer mutation and durable publication.
 */
export class TrustedNetworkV2DurablePolicyReducer {
	private readonly store: CrashSafePolicyAnchorStoreV2;
	private readonly durability: CrashSafePolicyAnchorStoreV2["crashSafeDurability"];
	private readonly descriptor: NetworkDescriptorV2;
	private readonly descriptorHash: Uint8Array;
	private core: TrustedNetworkV2PolicyReducer;
	private published: PublishedProjectionV2;
	private generation = 0n;
	private previousGenerationChecksum = copyBytes(ZERO_DIGEST);
	private durableCoreBytes?: Uint8Array;
	private observedHashes = new Set<string>();
	private operationTail: Promise<void> = Promise.resolve();
	private authorizationFences = 0;
	private terminalError?: Error;

	private constructor(properties: {
		store: CrashSafePolicyAnchorStoreV2;
		durability: CrashSafePolicyAnchorStoreV2["crashSafeDurability"];
		descriptor: NetworkDescriptorV2;
		core: TrustedNetworkV2PolicyReducer;
		generation?: bigint;
		previousGenerationChecksum?: Uint8Array;
		durableCoreBytes?: Uint8Array;
		observedHashes?: ReadonlySet<string>;
	}) {
		this.store = properties.store;
		this.durability = properties.durability;
		this.descriptor = deserialize(
			serialize(properties.descriptor),
			NetworkDescriptorV2,
		);
		this.descriptorHash = descriptorDigest(this.descriptor);
		this.core = properties.core;
		this.published = publishedFromReducer(this.core);
		this.generation = properties.generation ?? 0n;
		this.previousGenerationChecksum = copyBytes(
			properties.previousGenerationChecksum ?? ZERO_DIGEST,
		);
		this.durableCoreBytes =
			properties.durableCoreBytes === undefined
				? undefined
				: copyBytes(properties.durableCoreBytes);
		this.observedHashes = new Set(properties.observedHashes);
	}

	static async open(
		options: TrustedNetworkV2DurablePolicyReducerOptions,
	): Promise<TrustedNetworkV2DurablePolicyReducer> {
		assertNetworkDescriptorV2(options.descriptor);
		const descriptor = deserialize(
			serialize(options.descriptor),
			NetworkDescriptorV2,
		);
		const signal = options.signal;
		const store = options.store;
		const coreProperties: DurableReducerOptionsV2 = {
			descriptor,
			resolvePolicyEntry: options.resolvePolicyEntry,
			resolveTimeoutMs: options.resolveTimeoutMs,
			signal,
			maxPending: options.maxPending,
			maxPendingPolicyBytes: options.maxPendingPolicyBytes,
		};
		assertOpenNotAborted(signal);
		const durability = store?.crashSafeDurability;
		if (
			durability?.crashSafe !== true ||
			typeof durability.barrier !== "function"
		) {
			throw new Error(
				"TrustedNetwork v2 durable policy requires a crash-safe store barrier",
			);
		}

		// A successful fresh barrier is required on every open, including an empty
		// store; a generic flush acknowledgement is not this physical fence.
		await durability.barrier();
		assertOpenNotAborted(signal);

		const expectedDescriptorHash = descriptorDigest(descriptor);
		// Keep only immutable keys while discovering the history, then use get()
		// to decode one record at a time. This avoids retaining the raw log and its
		// decoded payloads together during restore.
		const generationKeys: string[] = [];
		for await (const [key, input] of store.iterator()) {
			assertOpenNotAborted(signal);
			if (key.startsWith(GENERATION_KEY_PREFIX)) {
				const suffix = key.slice(GENERATION_KEY_PREFIX.length);
				if (!/^\d{20}$/.test(suffix)) {
					throw new Error("Malformed policy-anchor generation key");
				}
				if (
					!(input instanceof Uint8Array) ||
					input.byteLength === 0 ||
					input.byteLength >
						TRUSTED_NETWORK_V2_MAX_POLICY_ANCHOR_GENERATION_BYTES
				) {
					throw new Error(
						"Policy-anchor generation record exceeds its byte ceiling",
					);
				}
				const generation = BigInt(suffix);
				if (generation === 0n || generation > MAX_U64) {
					throw new Error("Policy-anchor generation key is outside u64");
				}
				generationKeys.push(key);
				continue;
			}
			if (
				key === TRUSTED_NETWORK_V2_POLICY_ANCHOR_STORE_OWNER ||
				key.startsWith(`${TRUSTED_NETWORK_V2_POLICY_ANCHOR_STORE_OWNER}/`)
			) {
				throw new Error("Unknown policy-anchor record in the owned namespace");
			}
		}
		assertOpenNotAborted(signal);

		generationKeys.sort();
		let previousChecksum = copyBytes(ZERO_DIGEST);
		let latestCoreBytes: Uint8Array | undefined;
		let latestDurableState:
			| Exclude<PolicyReducerDurableStateV2, { state: "EMPTY" }>
			| undefined;
		let core: TrustedNetworkV2PolicyReducer | undefined;
		let durableCoreBytes: Uint8Array | undefined;
		let forkEvidence: PolicyForkEvidenceV2 | undefined;
		const observedHashes = new Set<string>();
		const authenticatedEvidenceEntryByHash = new Map<string, Uint8Array>();
		const canonicalChildren: CanonicalForkChildV2[] = [];
		try {
			for (let index = 0; index < generationKeys.length; index++) {
				assertOpenNotAborted(signal);
				const key = generationKeys[index]!;
				const expectedGeneration = BigInt(index + 1);
				const keyedGeneration = BigInt(key.slice(GENERATION_KEY_PREFIX.length));
				if (keyedGeneration !== expectedGeneration) {
					throw new Error("Policy-anchor generation history is gapped");
				}
				const input = await store.get(key);
				assertOpenNotAborted(signal);
				if (input === undefined) {
					throw new Error(
						"Policy-anchor generation disappeared during restore",
					);
				}
				const { record, payload } = decodeGenerationRecord(input);
				if (
					record.body.generation !== keyedGeneration ||
					generationKey(record.body.generation) !== key
				) {
					throw new Error("Policy-anchor generation key does not match record");
				}
				if (!equals(record.body.descriptorDigest, expectedDescriptorHash)) {
					throw new Error(
						"Policy-anchor generation belongs to another descriptor",
					);
				}
				if (!equals(record.body.previousGenerationChecksum, previousChecksum)) {
					throw new Error("Policy-anchor generation checksum chain is broken");
				}

				if (payload.kind === "state") {
					if (latestDurableState?.state === "FORKED") {
						throw new Error(
							"A FORKED policy anchor may only append observation deltas",
						);
					}
					latestCoreBytes = serialize(payload.payload.coreState);
					latestDurableState = payload.durableState;
					if (latestDurableState.state === "FORKED") {
						core = await TrustedNetworkV2PolicyReducer.restore({
							...coreProperties,
							durableState: latestDurableState,
						});
						assertOpenNotAborted(signal);
						assertCanonicalCoreRestore(core, latestCoreBytes);
						durableCoreBytes = copyBytes(latestCoreBytes);
						forkEvidence = core.forkEvidence;
						if (forkEvidence === undefined) {
							throw new Error(
								"Restored forked policy anchor has no fork evidence",
							);
						}
						for (const child of forkEvidence.children) {
							const hashKey = bytesKey(
								observationContentHash(child.entryBytes),
							);
							const retained = authenticatedEvidenceEntryByHash.get(hashKey);
							if (
								retained !== undefined &&
								!equals(retained, child.entryBytes)
							) {
								throw new Error(
									"Policy fork-observation content hash collision",
								);
							}
							authenticatedEvidenceEntryByHash.set(hashKey, child.entryBytes);
							observedHashes.add(hashKey);
							retainCanonicalForkChild(canonicalChildren, child);
						}
					}
				} else {
					if (
						latestDurableState?.state !== "FORKED" ||
						core === undefined ||
						forkEvidence === undefined
					) {
						throw new Error(
							"Policy fork-observation delta requires a preceding FORKED state",
						);
					}
					const entryBytes = payload.payload.entryBytes;
					const hashKey = bytesKey(observationContentHash(entryBytes));
					if (observedHashes.has(hashKey)) {
						const evidenceEntry = authenticatedEvidenceEntryByHash.get(hashKey);
						if (
							evidenceEntry !== undefined &&
							!equals(evidenceEntry, entryBytes)
						) {
							throw new Error("Policy fork-observation content hash collision");
						}
					} else {
						const authenticated = await authenticatePolicySnapshotEntryV2(
							entryBytes,
							descriptor,
						);
						assertOpenNotAborted(signal);
						if (
							authenticated.body.sequence !==
								forkEvidence.commonParent.sequence + 1n ||
							!equals(
								authenticated.body.previousPolicyDigest,
								forkEvidence.commonParent.digest,
							)
						) {
							throw new Error(
								"Stored policy fork observation is not a direct child of the common parent",
							);
						}
						observedHashes.add(hashKey);
						retainCanonicalForkChild(canonicalChildren, {
							digest: authenticated.digest,
							entryBytes,
						});
					}
				}
				previousChecksum = copyBytes(record.checksum);
			}

			const highestGeneration =
				generationKeys.length === 0 ? undefined : BigInt(generationKeys.length);
			if (highestGeneration !== undefined && latestCoreBytes === undefined) {
				throw new Error(
					"Policy-anchor history has no durable state generation",
				);
			}
			if (core === undefined) {
				if (highestGeneration === undefined) {
					core = new TrustedNetworkV2PolicyReducer(coreProperties);
				} else {
					core = await TrustedNetworkV2PolicyReducer.restore({
						...coreProperties,
						durableState: latestDurableState!,
					});
					assertOpenNotAborted(signal);
					assertCanonicalCoreRestore(core, latestCoreBytes!);
					durableCoreBytes = copyBytes(latestCoreBytes!);
				}
			}

			if (latestDurableState?.state === "FORKED") {
				if (forkEvidence === undefined) {
					throw new Error("Restored forked policy anchor has no fork evidence");
				}
				if (canonicalChildren.length !== 2) {
					throw new Error(
						"Stored policy fork evidence has fewer than two distinct children",
					);
				}
				const normalizedForkState: Extract<
					PolicyReducerDurableStateV2,
					{ state: "FORKED" }
				> = {
					formatVersion: FORMAT_VERSION,
					state: "FORKED",
					commonParentEntryBytes: copyBytes(
						latestDurableState.commonParentEntryBytes,
					),
					childEntryBytes: [
						copyBytes(canonicalChildren[0]!.entryBytes),
						copyBytes(canonicalChildren[1]!.entryBytes),
					],
				};
				const pairChanged = canonicalChildren.some(
					(child, index) =>
						!equals(child.digest, forkEvidence!.children[index]!.digest) ||
						!equals(
							child.entryBytes,
							forkEvidence!.children[index]!.entryBytes,
						),
				);
				if (pairChanged) {
					core.abort();
					core = await TrustedNetworkV2PolicyReducer.restore({
						...coreProperties,
						durableState: normalizedForkState,
					});
					assertOpenNotAborted(signal);
					assertCanonicalCoreRestore(
						core,
						serialize(coreRecordFromState(normalizedForkState)),
					);
				}
				latestDurableState = normalizedForkState;
				durableCoreBytes = serialize(coreRecordFromState(normalizedForkState));
			} else if (observedHashes.size !== 0) {
				throw new Error("Non-forked policy anchor has fork observations");
			}

			return new TrustedNetworkV2DurablePolicyReducer({
				store,
				durability,
				descriptor,
				core,
				generation: highestGeneration,
				previousGenerationChecksum: previousChecksum,
				durableCoreBytes,
				observedHashes,
			});
		} catch (error) {
			core?.abort();
			throw error;
		}
	}

	get state(): "EMPTY" | "ACTIVE" | "UNAVAILABLE" | "FORKED" | "HALTED" {
		if (this.terminalError !== undefined || this.core.state === "HALTED") {
			return "HALTED";
		}
		return this.published.state;
	}

	get head(): PolicyHeadProjectionV2 | undefined {
		return copyHead(this.published.head);
	}

	get forkEvidence(): PolicyForkEvidenceV2 | undefined {
		return copyForkEvidence(this.published.forkEvidence);
	}

	get pendingCount(): number {
		return this.core.pendingCount;
	}

	get pendingBytes(): number {
		return this.core.pendingBytes;
	}

	get pendingDigests(): Uint8Array[] {
		return this.core.pendingDigests.map(copyBytes);
	}

	/** Projection query only; use isAuthorized() for the fail-closed gate. */
	rolesFor(subject: PublicSignKey): number {
		return this.published.roles.get(keyId(subject)) ?? 0;
	}

	isAuthorized(subject: PublicSignKey, roles: number): boolean {
		if (
			this.authorizationFences !== 0 ||
			this.state !== "ACTIVE" ||
			!Number.isInteger(roles) ||
			roles === 0 ||
			(roles & ~TRUSTED_NETWORK_V2_KNOWN_ROLE_BITS) !== 0
		) {
			return false;
		}
		return (this.rolesFor(subject) & roles) === roles;
	}

	abort(): void {
		this.core.abort();
	}

	ingest(entryBytes: Uint8Array): Promise<PolicyAdmissionResultV2> {
		const captured =
			entryBytes instanceof Uint8Array &&
			entryBytes.byteLength <= TRUSTED_NETWORK_V2_MAX_POLICY_ENTRY_BYTES
				? copyBytes(entryBytes)
				: entryBytes;
		return this.enqueue(async () => {
			const result = await this.core.ingest(captured);
			await this.persistCorePublication(result);
			return result;
		});
	}

	retryUnavailable(): Promise<PolicyAdmissionResultV2> {
		return this.enqueue(async () => {
			const result = await this.core.retryUnavailable();
			await this.persistCorePublication(result);
			return result;
		});
	}

	private enqueue<T>(operation: () => Promise<T>): Promise<T> {
		this.authorizationFences++;
		const result = this.operationTail.then(async () => {
			if (this.terminalError !== undefined) throw this.terminalError;
			try {
				return await operation();
			} catch (error) {
				throw this.halt(error);
			}
		});
		this.operationTail = result.then(
			(): void => {},
			(): void => {},
		);
		return result.finally(() => {
			this.authorizationFences--;
		});
	}

	private halt(error: unknown): Error {
		if (this.terminalError !== undefined) return this.terminalError;
		const cause = error instanceof Error ? error : new Error(String(error));
		const terminal = new Error(
			`TrustedNetwork v2 durable policy publication is ambiguous and halted: ${cause.message}`,
		);
		terminal.cause = cause;
		this.terminalError = terminal;
		this.core.abort();
		return terminal;
	}

	private async persistCorePublication(
		result: PolicyAdmissionResultV2,
	): Promise<void> {
		const durableState = this.core.exportDurableState();
		if (durableState.state === "EMPTY") {
			if (this.durableCoreBytes !== undefined) {
				throw this.halt("Durable policy state cannot return to EMPTY");
			}
			return;
		}

		const coreRecord = coreRecordFromState(durableState);
		const coreBytes = serialize(coreRecord);
		// FORKED is terminal. Once its common-parent anchor exists, later child
		// proofs are observation deltas even when they change the live canonical
		// pair; reopen derives that pair from the complete delta history.
		const stateChanged =
			!(this.published.state === "FORKED" && durableState.state === "FORKED") &&
			(this.durableCoreBytes === undefined ||
				!equals(this.durableCoreBytes, coreBytes));
		const suppliedObservations = new Map<string, Uint8Array>();
		for (const proof of result.forkObservations ?? []) {
			const contentHash = observationContentHash(proof.entryBytes);
			const hashKey = bytesKey(contentHash);
			const retained = suppliedObservations.get(hashKey);
			if (retained !== undefined && !equals(retained, proof.entryBytes)) {
				throw this.halt("Policy fork-observation content hash collision");
			}
			suppliedObservations.set(hashKey, copyBytes(proof.entryBytes));
		}
		if (durableState.state !== "FORKED") {
			if (suppliedObservations.size !== 0 || this.observedHashes.size !== 0) {
				throw this.halt("Only a forked policy anchor may contain observations");
			}
		} else {
			for (const entryBytes of durableState.childEntryBytes) {
				const hashKey = bytesKey(observationContentHash(entryBytes));
				if (stateChanged) suppliedObservations.delete(hashKey);
				else if (
					!this.observedHashes.has(hashKey) &&
					!suppliedObservations.has(hashKey)
				) {
					throw this.halt(
						"Published fork state is missing a canonical observation",
					);
				}
			}
		}
		for (const hashKey of this.observedHashes) {
			suppliedObservations.delete(hashKey);
		}

		const observationEntries = [...suppliedObservations.entries()]
			.sort(([left], [right]) => (left < right ? -1 : left > right ? 1 : 0))
			.map(([hashKey, entryBytes]) => ({ hashKey, entryBytes }));
		const recordCount = (stateChanged ? 1 : 0) + observationEntries.length;
		if (recordCount === 0) return;
		if (BigInt(recordCount) > MAX_U64 - this.generation) {
			throw this.halt("Policy-anchor generation exhausted u64");
		}

		let nextGeneration = this.generation;
		let previousChecksum = copyBytes(this.previousGenerationChecksum);
		const stagedObservedHashes = new Set(this.observedHashes);
		const drafts: Array<{
			kind: number;
			payloadBytes: Uint8Array;
			applyObservationHash?: string;
		}> = [];
		if (stateChanged) {
			const payloadBytes = serialize(
				new PolicyAnchorStateGenerationPayloadV2({ coreState: coreRecord }),
			);
			if (payloadBytes.byteLength > MAX_STATE_GENERATION_PAYLOAD_BYTES) {
				throw this.halt("Policy-anchor state payload exceeds its byte ceiling");
			}
			drafts.push({ kind: GENERATION_KIND.STATE, payloadBytes });
		}
		for (const { hashKey, entryBytes } of observationEntries) {
			const payloadBytes = serialize(
				new PolicyAnchorObservationGenerationPayloadV2({
					entryBytes: copyBytes(entryBytes),
				}),
			);
			if (payloadBytes.byteLength > MAX_OBSERVATION_GENERATION_PAYLOAD_BYTES) {
				throw this.halt(
					"Policy fork-observation payload exceeds its byte ceiling",
				);
			}
			drafts.push({
				kind: GENERATION_KIND.FORK_OBSERVATION,
				payloadBytes,
				applyObservationHash: hashKey,
			});
		}

		for (const draft of drafts) {
			nextGeneration += 1n;
			if (draft.kind === GENERATION_KIND.STATE) {
				if (durableState.state === "FORKED") {
					for (const entryBytes of durableState.childEntryBytes) {
						stagedObservedHashes.add(
							bytesKey(observationContentHash(entryBytes)),
						);
					}
				}
			} else {
				stagedObservedHashes.add(draft.applyObservationHash!);
			}
			const body = new PolicyAnchorGenerationBodyV2({
				formatVersion: FORMAT_VERSION,
				generation: nextGeneration,
				descriptorDigest: copyBytes(this.descriptorHash),
				previousGenerationChecksum: copyBytes(previousChecksum),
				kind: draft.kind,
				payloadBytes: copyBytes(draft.payloadBytes),
			});
			const record = new PolicyAnchorGenerationRecordV2({
				body,
				checksum: generationChecksum(body),
			});
			const recordBytes = serialize(record);
			if (
				recordBytes.byteLength >
				TRUSTED_NETWORK_V2_MAX_POLICY_ANCHOR_GENERATION_BYTES
			) {
				throw this.halt(
					"Policy-anchor generation record exceeds its byte ceiling",
				);
			}

			const key = generationKey(nextGeneration);
			if ((await this.store.get(key)) !== undefined) {
				throw this.halt("Policy-anchor generation must never be overwritten");
			}
			await this.store.put(key, copyBytes(recordBytes));
			// Each immutable generation gets one physical fence. The projection is
			// published only after every state/observation delta is fenced.
			await this.durability.barrier();
			previousChecksum = copyBytes(record.checksum);
		}

		this.generation = nextGeneration;
		this.previousGenerationChecksum = previousChecksum;
		this.durableCoreBytes = copyBytes(coreBytes);
		this.observedHashes = stagedObservedHashes;
		try {
			this.published = publishedFromReducer(this.core);
		} catch (error) {
			throw this.halt(error);
		}
	}
}
