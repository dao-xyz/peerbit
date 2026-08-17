import { sha256Base64Sync, sha256Sync, toHexString } from "@peerbit/crypto";
import { MAX_U32, MAX_U64, type NumberFromType } from "./integers.js";
import {
	ReplicationIntent,
	ReplicationRangeIndexableU32,
	ReplicationRangeIndexableU64,
} from "./ranges.js";

type Resolution = "u32" | "u64";

const FORMAT = "peerbit-shared-log-local-placement-view" as const;
const VERSION = 1 as const;
const MAX_REPLICAS = 100;
const MAX_SAFE_WALL_TIME = BigInt(Number.MAX_SAFE_INTEGER);
const encoder = new TextEncoder();
const issuedCanonicalViews = new WeakSet<object>();

class BoundedBinaryWriter {
	private readonly value: Uint8Array;
	private offset = 0;

	constructor(maximumBytes: number) {
		this.value = new Uint8Array(maximumBytes);
	}

	private reserve(length: number): number {
		if (
			!Number.isSafeInteger(length) ||
			length < 0 ||
			this.offset + length > this.value.byteLength
		) {
			throw new Error("Local placement view exceeds the encoded byte limit");
		}
		const start = this.offset;
		this.offset += length;
		return start;
	}

	u8(value: number): void {
		this.value[this.reserve(1)] = value;
	}

	bool(value: boolean): void {
		this.u8(value ? 1 : 0);
	}

	u32(value: number): void {
		const start = this.reserve(4);
		new DataView(this.value.buffer).setUint32(start, value, true);
	}

	u64(value: bigint): void {
		const start = this.reserve(8);
		new DataView(this.value.buffer).setBigUint64(start, value, true);
	}

	raw(bytes: Uint8Array): void {
		this.value.set(bytes, this.reserve(bytes.byteLength));
	}

	bytes(bytes: Uint8Array): void {
		this.u32(bytes.byteLength);
		this.raw(bytes);
	}

	string(value: string): void {
		this.bytes(encoder.encode(value));
	}

	finish(): Uint8Array {
		return this.value.slice(0, this.offset);
	}
}

export type LocalPlacementViewLimits = Readonly<{
	maxEncodedBytes: number;
	maxOwners: number;
	maxRanges: number;
	maxRangesPerOwner: number;
	maxIdentifierBytes: number;
	maxPublicKeyBytes: number;
}>;

/**
 * Frozen hard ceilings. A caller that exceeds them must retain the source and
 * fail closed; truncating owners or ranges can change placement decisions.
 */
export const MAX_LOCAL_PLACEMENT_VIEW_LIMITS: LocalPlacementViewLimits =
	Object.freeze({
		maxEncodedBytes: 4 * 1024 * 1024,
		maxOwners: 4096,
		maxRanges: 16_384,
		maxRangesPerOwner: 4096,
		maxIdentifierBytes: 512,
		maxPublicKeyBytes: 512,
	});

export const DEFAULT_LOCAL_PLACEMENT_VIEW_LIMITS =
	MAX_LOCAL_PLACEMENT_VIEW_LIMITS;

export type LocalPlacementOwner = Readonly<{
	/** Canonical authenticated PublicSignKey encoding (`publicKey.bytes`). */
	publicKey: Uint8Array;
	/** Optional redundant assertion from the caller/runtime row. */
	hash?: string;
}>;

export type LocalPlacementRangeFact<R extends Resolution = Resolution> =
	Readonly<{
		owner: string;
		id: Uint8Array;
		timestamp: bigint;
		start1: NumberFromType<R>;
		end1: NumberFromType<R>;
		start2: NumberFromType<R>;
		end2: NumberFromType<R>;
		width: NumberFromType<R>;
		mode: ReplicationIntent;
	}>;

export type LocalPlacementSnapshotInput<R extends Resolution = Resolution> =
	Readonly<{
		logId: Uint8Array;
		resolution: R;
		planner: Readonly<{ id: string; version: number }>;
		domain: Readonly<{ type: string; version: number; configId: string }>;
		policy: Readonly<{
			/** Stable descriptor for custom eligibility/replica policy behavior. */
			id: string;
			minReplicas: number;
			maxReplicas?: number;
			roleAgeMs: number;
			expandPeerFilter: boolean;
			fullReplicaFallback: boolean;
			includeStrictFullReplica: boolean;
		}>;
		capturedAtMs: bigint;
		/**
		 * Expiry of receiver-local subscriber/authentication provenance. Runtime
		 * capture must supply this when its candidate snapshot has a TTL.
		 */
		freshUntilMs?: bigint;
		self: Readonly<{ owner: string; replicating: boolean }>;
		/**
		 * undefined means the built-in planner may consider every owner in the
		 * complete snapshot. An empty array is an explicit empty filter.
		 */
		basePeerFilter?: readonly string[];
		/**
		 * Complete identity set for every range owner and peer-filter member.
		 * The caller must admit only freshly authenticated and authorized facts.
		 */
		owners: readonly LocalPlacementOwner[];
		/** Complete local range snapshot; never a truncated page. */
		ranges: readonly LocalPlacementRangeFact<R>[];
	}>;

export type CanonicalLocalPlacementViewBody = Readonly<{
	format: typeof FORMAT;
	version: typeof VERSION;
	logId: string;
	resolution: Resolution;
	planner: Readonly<{ id: string; version: number }>;
	domain: Readonly<{ type: string; version: number; configId: string }>;
	policy: Readonly<{
		id: string;
		minReplicas: number;
		maxReplicas: number | null;
		roleAgeMs: string;
		expandPeerFilter: boolean;
		fullReplicaFallback: boolean;
		includeStrictFullReplica: boolean;
	}>;
	self: Readonly<{ owner: string; replicating: boolean }>;
	owners: readonly Readonly<{ hash: string; publicKey: string }>[];
	basePeerFilter: readonly string[] | null;
	maturityValidUntilMs: string | null;
	ranges: readonly Readonly<{
		owner: string;
		id: string;
		timestamp: string;
		start1: string;
		end1: string;
		start2: string;
		end2: string;
		width: string;
		mode: ReplicationIntent;
		mature: boolean;
	}>[];
}>;

export type CanonicalLocalPlacementView = Readonly<{
	body: CanonicalLocalPlacementViewBody;
	bytes: Uint8Array;
	digest: string;
	capturedAtMs: bigint;
	validUntilMs?: bigint;
}>;

const hexBytes = (value: string): Uint8Array => {
	if (value.length % 2 !== 0 || !/^[0-9a-f]*$/.test(value)) {
		throw new Error("Invalid canonical local placement view bytes");
	}
	const bytes = new Uint8Array(value.length / 2);
	for (let index = 0; index < bytes.length; index++) {
		bytes[index] = Number.parseInt(value.slice(index * 2, index * 2 + 2), 16);
	}
	return bytes;
};

const encodeLocalPlacementViewBody = (
	body: CanonicalLocalPlacementViewBody,
	maximumBytes: number,
): Uint8Array => {
	const writer = new BoundedBinaryWriter(maximumBytes);
	writer.string(body.format);
	writer.u32(body.version);
	writer.bytes(hexBytes(body.logId));
	writer.u8(body.resolution === "u32" ? 0 : 1);
	writer.string(body.planner.id);
	writer.u32(body.planner.version);
	writer.string(body.domain.type);
	writer.u32(body.domain.version);
	writer.bytes(hexBytes(body.domain.configId));
	writer.bytes(hexBytes(body.policy.id));
	writer.u32(body.policy.minReplicas);
	writer.bool(body.policy.maxReplicas != null);
	if (body.policy.maxReplicas != null) {
		writer.u32(body.policy.maxReplicas);
	}
	writer.u64(BigInt(body.policy.roleAgeMs));
	writer.bool(body.policy.expandPeerFilter);
	writer.bool(body.policy.fullReplicaFallback);
	writer.bool(body.policy.includeStrictFullReplica);
	writer.string(body.self.owner);
	writer.bool(body.self.replicating);
	writer.u32(body.owners.length);
	for (const owner of body.owners) {
		writer.string(owner.hash);
		writer.bytes(hexBytes(owner.publicKey));
	}
	writer.bool(body.basePeerFilter != null);
	if (body.basePeerFilter != null) {
		writer.u32(body.basePeerFilter.length);
		for (const owner of body.basePeerFilter) writer.string(owner);
	}
	writer.bool(body.maturityValidUntilMs != null);
	if (body.maturityValidUntilMs != null) {
		writer.u64(BigInt(body.maturityValidUntilMs));
	}
	writer.u32(body.ranges.length);
	for (const range of body.ranges) {
		writer.string(range.owner);
		writer.bytes(hexBytes(range.id));
		writer.u64(BigInt(range.timestamp));
		if (body.resolution === "u32") {
			writer.u32(Number(range.start1));
			writer.u32(Number(range.end1));
			writer.u32(Number(range.start2));
			writer.u32(Number(range.end2));
			writer.u32(Number(range.width));
		} else {
			writer.u64(BigInt(range.start1));
			writer.u64(BigInt(range.end1));
			writer.u64(BigInt(range.start2));
			writer.u64(BigInt(range.end2));
			writer.u64(BigInt(range.width));
		}
		writer.u8(range.mode);
		writer.bool(range.mature);
	}
	return writer.finish();
};

/**
 * Per-open/local execution identity. A restart, ownership revision, role
 * generation, or semantic view change must create a different fence.
 *
 * This fence is not a network-consensus epoch, transfer receipt, durable
 * possession proof, or pruning authorization. A source still needs a durable
 * destination acknowledgement and a fresh exact owner/replica revalidation.
 */
export type LocalPlacementExecutionFence = Readonly<{
	viewId: string;
	executionEpoch: string;
	ownershipRevision: bigint;
	roleGeneration: number;
	validFromMs: bigint;
	validUntilMs?: bigint;
	fenceId: string;
}>;

const own = (value: readonly unknown[], index: number) =>
	Object.prototype.hasOwnProperty.call(value, index);

const assertArray = <T>(
	value: readonly T[],
	maximum: number,
	name: string,
): readonly T[] => {
	if (!Array.isArray(value) || value.length > maximum) {
		throw new Error(`Invalid local placement view ${name}`);
	}
	return value;
};

const boundedString = (value: unknown, maximum: number, name: string) => {
	if (
		typeof value !== "string" ||
		value.length === 0 ||
		value.length > maximum
	) {
		throw new Error(`Invalid local placement view ${name}`);
	}
	for (let index = 0; index < value.length; index++) {
		const code = value.charCodeAt(index);
		if (code >= 0xd800 && code <= 0xdbff) {
			const next = value.charCodeAt(index + 1);
			if (!(next >= 0xdc00 && next <= 0xdfff)) {
				throw new Error(`Invalid local placement view ${name}`);
			}
			index++;
		} else if (code >= 0xdc00 && code <= 0xdfff) {
			throw new Error(`Invalid local placement view ${name}`);
		}
	}
	if (encoder.encode(value).byteLength > maximum) {
		throw new Error(`Invalid local placement view ${name}`);
	}
	return value;
};

const boundedBytes = (value: unknown, maximum: number, name: string) => {
	if (!(value instanceof Uint8Array) || value.byteLength === 0) {
		throw new Error(`Invalid local placement view ${name}`);
	}
	if (value.byteLength > maximum) {
		throw new Error(`Invalid local placement view ${name}`);
	}
	return toHexString(value);
};

const positiveU32 = (value: unknown, name: string) => {
	if (
		typeof value !== "number" ||
		!Number.isSafeInteger(value) ||
		value <= 0 ||
		value > MAX_U32
	) {
		throw new Error(`Invalid local placement view ${name}`);
	}
	return value;
};

const nonNegativeSafeInteger = (value: unknown, name: string) => {
	if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0) {
		throw new Error(`Invalid local placement view ${name}`);
	}
	return value;
};

const booleanValue = (value: unknown, name: string) => {
	if (typeof value !== "boolean") {
		throw new Error(`Invalid local placement view ${name}`);
	}
	return value;
};

const replicaCount = (value: unknown, name: string) => {
	const normalized = positiveU32(value, name);
	if (normalized > MAX_REPLICAS) {
		throw new Error(`Invalid local placement view ${name}`);
	}
	return normalized;
};

const nonNegativeU64 = (value: unknown, name: string) => {
	if (typeof value !== "bigint" || value < 0n || value > MAX_U64) {
		throw new Error(`Invalid local placement view ${name}`);
	}
	return value;
};

const wallTime = (value: unknown, name: string) => {
	const normalized = nonNegativeU64(value, name);
	if (normalized > MAX_SAFE_WALL_TIME) {
		throw new Error(`Invalid local placement view ${name}`);
	}
	return normalized;
};

const coordinate = <R extends Resolution>(
	resolution: R,
	value: unknown,
	name: string,
): NumberFromType<R> => {
	if (resolution === "u32") {
		if (
			typeof value !== "number" ||
			!Number.isInteger(value) ||
			value < 0 ||
			value > MAX_U32
		) {
			throw new Error(`Invalid local placement view ${name}`);
		}
		return value as NumberFromType<R>;
	}
	if (typeof value !== "bigint" || value < 0n || value > MAX_U64) {
		throw new Error(`Invalid local placement view ${name}`);
	}
	return value as NumberFromType<R>;
};

const decimal = (value: number | bigint) => value.toString();

const normalizeLimits = (
	limits?: Partial<LocalPlacementViewLimits>,
): LocalPlacementViewLimits => {
	const values = { ...DEFAULT_LOCAL_PLACEMENT_VIEW_LIMITS, ...limits };
	const bounded = (value: unknown, maximum: number, name: string) => {
		if (
			typeof value !== "number" ||
			!Number.isSafeInteger(value) ||
			value <= 0 ||
			value > maximum
		) {
			throw new Error(`Invalid local placement view limit: ${name}`);
		}
		return value;
	};
	return Object.freeze({
		maxEncodedBytes: bounded(
			values.maxEncodedBytes,
			MAX_LOCAL_PLACEMENT_VIEW_LIMITS.maxEncodedBytes,
			"maxEncodedBytes",
		),
		maxOwners: bounded(
			values.maxOwners,
			MAX_LOCAL_PLACEMENT_VIEW_LIMITS.maxOwners,
			"maxOwners",
		),
		maxRanges: bounded(
			values.maxRanges,
			MAX_LOCAL_PLACEMENT_VIEW_LIMITS.maxRanges,
			"maxRanges",
		),
		maxRangesPerOwner: bounded(
			values.maxRangesPerOwner,
			MAX_LOCAL_PLACEMENT_VIEW_LIMITS.maxRangesPerOwner,
			"maxRangesPerOwner",
		),
		maxIdentifierBytes: bounded(
			values.maxIdentifierBytes,
			MAX_LOCAL_PLACEMENT_VIEW_LIMITS.maxIdentifierBytes,
			"maxIdentifierBytes",
		),
		maxPublicKeyBytes: bounded(
			values.maxPublicKeyBytes,
			MAX_LOCAL_PLACEMENT_VIEW_LIMITS.maxPublicKeyBytes,
			"maxPublicKeyBytes",
		),
	});
};

const compareStrings = (left: string, right: string) =>
	left < right ? -1 : left > right ? 1 : 0;

const assertDigest = (value: unknown, name: string) => {
	const digest = boundedString(value, 64, name);
	if (!/^[0-9a-f]{64}$/.test(digest)) {
		throw new Error(`Invalid local placement view ${name}`);
	}
	return digest;
};

/**
 * Canonicalize a complete, frozen LOCAL placement snapshot. Authentication and
 * freshness are caller obligations: this codec cannot recover signatures,
 * sender epochs, or V2 sequence provenance from persisted range rows.
 *
 * The digest identifies semantic planner inputs only. Different healthy peers
 * may legitimately produce different digests during churn or a partition.
 */
export const canonicalizeLocalPlacementSnapshotV1 = <R extends Resolution>(
	input: LocalPlacementSnapshotInput<R>,
	options?: { limits?: Partial<LocalPlacementViewLimits> },
): CanonicalLocalPlacementView => {
	const limits = normalizeLimits(options?.limits);
	if (input?.resolution !== "u32" && input?.resolution !== "u64") {
		throw new Error("Invalid local placement view resolution");
	}
	const resolution = input.resolution;
	const capturedAtMs = wallTime(input.capturedAtMs, "capture timestamp");
	const freshUntilMs =
		input.freshUntilMs == null
			? undefined
			: wallTime(input.freshUntilMs, "capture freshness deadline");
	if (freshUntilMs != null && freshUntilMs <= capturedAtMs) {
		throw new Error("Local placement view capture freshness has expired");
	}
	const logId = boundedBytes(input.logId, limits.maxIdentifierBytes, "log id");
	const planner = Object.freeze({
		id: boundedString(
			input.planner?.id,
			limits.maxIdentifierBytes,
			"planner id",
		),
		version: positiveU32(input.planner?.version, "planner version"),
	});
	const domain = Object.freeze({
		type: boundedString(
			input.domain?.type,
			limits.maxIdentifierBytes,
			"domain type",
		),
		version: positiveU32(input.domain?.version, "domain version"),
		configId: assertDigest(input.domain?.configId, "domain config id"),
	});
	const roleAgeMs = nonNegativeSafeInteger(input.policy?.roleAgeMs, "role age");
	const minReplicas = replicaCount(
		input.policy?.minReplicas,
		"minimum replicas",
	);
	const maxReplicas =
		input.policy?.maxReplicas == null
			? null
			: replicaCount(input.policy.maxReplicas, "maximum replicas");
	if (maxReplicas != null && maxReplicas < minReplicas) {
		throw new Error("Invalid local placement view replica interval");
	}
	const policy = Object.freeze({
		id: assertDigest(input.policy?.id, "policy id"),
		minReplicas,
		maxReplicas,
		roleAgeMs: roleAgeMs.toString(),
		expandPeerFilter: booleanValue(
			input.policy?.expandPeerFilter,
			"expand-peer-filter policy",
		),
		fullReplicaFallback: booleanValue(
			input.policy?.fullReplicaFallback,
			"full-replica-fallback policy",
		),
		includeStrictFullReplica: booleanValue(
			input.policy?.includeStrictFullReplica,
			"strict-full-replica policy",
		),
	});

	const ownerInputs = assertArray(input.owners, limits.maxOwners, "owners");
	const owners: Array<{ hash: string; publicKey: string }> = [];
	const ownerByHash = new Map<string, string>();
	for (let index = 0; index < ownerInputs.length; index++) {
		if (!own(ownerInputs, index)) {
			throw new Error("Invalid local placement view sparse owners");
		}
		const owner = ownerInputs[index]!;
		const publicKey = boundedBytes(
			owner?.publicKey,
			limits.maxPublicKeyBytes,
			`owner ${index} public key`,
		);
		const derivedHash = sha256Base64Sync(hexBytes(publicKey));
		const hash = boundedString(
			derivedHash,
			limits.maxIdentifierBytes,
			`owner ${index} hash`,
		);
		if (
			owner.hash != null &&
			boundedString(
				owner.hash,
				limits.maxIdentifierBytes,
				`owner ${index} asserted hash`,
			) !== hash
		) {
			throw new Error(`Local placement view owner ${index} hash mismatch`);
		}
		if (ownerByHash.has(hash)) {
			throw new Error(`Duplicate local placement view owner: ${hash}`);
		}
		ownerByHash.set(hash, publicKey);
		owners.push({ hash, publicKey });
	}
	owners.sort(
		(left, right) =>
			compareStrings(left.hash, right.hash) ||
			compareStrings(left.publicKey, right.publicKey),
	);

	const selfOwner = boundedString(
		input.self?.owner,
		limits.maxIdentifierBytes,
		"self owner",
	);
	if (!ownerByHash.has(selfOwner)) {
		throw new Error("Local placement view self identity is missing");
	}
	const self = Object.freeze({
		owner: selfOwner,
		replicating: booleanValue(input.self?.replicating, "self role"),
	});

	let basePeerFilter: string[] | null = null;
	if (input.basePeerFilter !== undefined) {
		const filter = assertArray(
			input.basePeerFilter,
			limits.maxOwners,
			"base peer filter",
		);
		const seen = new Set<string>();
		basePeerFilter = [];
		for (let index = 0; index < filter.length; index++) {
			if (!own(filter, index)) {
				throw new Error("Invalid local placement view sparse peer filter");
			}
			const hash = boundedString(
				filter[index],
				limits.maxIdentifierBytes,
				`base peer filter ${index}`,
			);
			if (!ownerByHash.has(hash)) {
				throw new Error(`Unknown local placement view peer: ${hash}`);
			}
			if (seen.has(hash)) {
				throw new Error(`Duplicate local placement view peer: ${hash}`);
			}
			seen.add(hash);
			basePeerFilter.push(hash);
		}
		basePeerFilter.sort(compareStrings);
	}

	const rangeInputs = assertArray(input.ranges, limits.maxRanges, "ranges");
	const ranges: Array<CanonicalLocalPlacementViewBody["ranges"][number]> = [];
	const rangesByOwner = new Map<string, number>();
	const rangeIds = new Set<string>();
	let maturityValidUntilMs: bigint | undefined;
	for (let index = 0; index < rangeInputs.length; index++) {
		if (!own(rangeInputs, index)) {
			throw new Error("Invalid local placement view sparse ranges");
		}
		const range = rangeInputs[index]!;
		const owner = boundedString(
			range?.owner,
			limits.maxIdentifierBytes,
			`range ${index} owner`,
		);
		if (!ownerByHash.has(owner)) {
			throw new Error(`Unknown local placement view range owner: ${owner}`);
		}
		const ownerCount = (rangesByOwner.get(owner) ?? 0) + 1;
		if (ownerCount > limits.maxRangesPerOwner) {
			throw new Error(
				`Local placement view owner range limit exceeded: ${owner}`,
			);
		}
		rangesByOwner.set(owner, ownerCount);
		const id = boundedBytes(
			range?.id,
			limits.maxIdentifierBytes,
			`range ${index} id`,
		);
		if (rangeIds.has(id)) {
			throw new Error(`Duplicate local placement view range id: ${id}`);
		}
		rangeIds.add(id);
		const timestamp = wallTime(range?.timestamp, `range ${index} timestamp`);
		const start1 = coordinate(
			resolution,
			range?.start1,
			`range ${index} start1`,
		);
		const end1 = coordinate(resolution, range?.end1, `range ${index} end1`);
		const start2 = coordinate(
			resolution,
			range?.start2,
			`range ${index} start2`,
		);
		const end2 = coordinate(resolution, range?.end2, `range ${index} end2`);
		const width = coordinate(resolution, range?.width, `range ${index} width`);
		if (
			range?.mode !== ReplicationIntent.NonStrict &&
			range?.mode !== ReplicationIntent.Strict
		) {
			throw new Error(`Invalid local placement view range ${index} mode`);
		}
		const reconstructed =
			resolution === "u32"
				? new ReplicationRangeIndexableU32({
						id: range.id,
						publicKeyHash: owner,
						offset: Number(start1),
						width: Number(width),
						timestamp,
						mode: range.mode,
					})
				: new ReplicationRangeIndexableU64({
						id: range.id,
						publicKeyHash: owner,
						offset: BigInt(start1),
						width: BigInt(width),
						timestamp,
						mode: range.mode,
					});
		if (
			reconstructed.start1.toString() !== start1.toString() ||
			reconstructed.end1.toString() !== end1.toString() ||
			reconstructed.start2.toString() !== start2.toString() ||
			reconstructed.end2.toString() !== end2.toString() ||
			reconstructed.width.toString() !== width.toString()
		) {
			throw new Error(`Invalid local placement view range ${index} geometry`);
		}
		const maturityAt = timestamp + BigInt(roleAgeMs);
		if (maturityAt > MAX_SAFE_WALL_TIME) {
			throw new Error(`Invalid local placement view range ${index} maturity`);
		}
		const mature = capturedAtMs >= maturityAt;
		if (
			!mature &&
			(maturityValidUntilMs === undefined || maturityAt < maturityValidUntilMs)
		) {
			maturityValidUntilMs = maturityAt;
		}
		ranges.push({
			owner,
			id,
			timestamp: timestamp.toString(),
			start1: decimal(start1),
			end1: decimal(end1),
			start2: decimal(start2),
			end2: decimal(end2),
			width: decimal(width),
			mode: range.mode,
			mature,
		});
	}
	ranges.sort((left, right) => {
		const stringFields = [
			"owner",
			"id",
			"timestamp",
			"start1",
			"end1",
			"start2",
			"end2",
			"width",
		] as const;
		for (const field of stringFields) {
			const result = compareStrings(left[field], right[field]);
			if (result !== 0) return result;
		}
		return left.mode - right.mode;
	});

	const body: CanonicalLocalPlacementViewBody = Object.freeze({
		format: FORMAT,
		version: VERSION,
		logId,
		resolution,
		planner,
		domain,
		policy,
		self,
		owners: Object.freeze(owners.map((owner) => Object.freeze(owner))),
		basePeerFilter:
			basePeerFilter == null ? null : Object.freeze([...basePeerFilter]),
		maturityValidUntilMs: maturityValidUntilMs?.toString() ?? null,
		ranges: Object.freeze(ranges.map((range) => Object.freeze(range))),
	});
	const bytes = encodeLocalPlacementViewBody(body, limits.maxEncodedBytes);
	const validUntilMs =
		freshUntilMs == null
			? maturityValidUntilMs
			: maturityValidUntilMs == null || freshUntilMs < maturityValidUntilMs
				? freshUntilMs
				: maturityValidUntilMs;
	const view = Object.freeze({
		body,
		bytes,
		digest: toHexString(sha256Sync(bytes)),
		capturedAtMs,
		validUntilMs,
	});
	issuedCanonicalViews.add(view);
	return view;
};

const fenceBody = (properties: {
	viewId: string;
	executionEpoch: string;
	ownershipRevision: bigint;
	roleGeneration: number;
	validFromMs: bigint;
	validUntilMs?: bigint;
}) => ({
	format: "peerbit-shared-log-local-placement-execution-fence",
	version: 1,
	viewId: properties.viewId,
	executionEpoch: properties.executionEpoch,
	ownershipRevision: properties.ownershipRevision.toString(),
	roleGeneration: properties.roleGeneration,
	validFromMs: properties.validFromMs.toString(),
	validUntilMs: properties.validUntilMs?.toString() ?? null,
});

const encodeFenceBody = (
	properties: ReturnType<typeof fenceBody>,
): Uint8Array => {
	const writer = new BoundedBinaryWriter(512);
	writer.string(properties.format);
	writer.u32(properties.version);
	writer.bytes(hexBytes(properties.viewId));
	writer.bytes(hexBytes(properties.executionEpoch));
	writer.u64(BigInt(properties.ownershipRevision));
	writer.u64(BigInt(properties.roleGeneration));
	writer.u64(BigInt(properties.validFromMs));
	writer.bool(properties.validUntilMs != null);
	if (properties.validUntilMs != null) {
		writer.u64(BigInt(properties.validUntilMs));
	}
	return writer.finish();
};

export const createLocalPlacementExecutionFence = (properties: {
	view: CanonicalLocalPlacementView;
	/** Fresh random 32-byte value generated once for this SharedLog.open(). */
	executionEpoch: Uint8Array;
	ownershipRevision: bigint;
	roleGeneration: number;
}): LocalPlacementExecutionFence => {
	const view = properties?.view;
	if (!view || !issuedCanonicalViews.has(view)) {
		throw new Error("Invalid local placement view execution fence view");
	}
	const viewId = assertDigest(view.digest, "execution fence view id");
	if (
		!(view.bytes instanceof Uint8Array) ||
		toHexString(sha256Sync(view.bytes)) !== viewId
	) {
		throw new Error("Invalid local placement view execution fence digest");
	}
	const executionEpoch = boundedBytes(
		properties?.executionEpoch,
		32,
		"execution epoch",
	);
	if (properties.executionEpoch.byteLength !== 32) {
		throw new Error("Invalid local placement view execution epoch");
	}
	const ownershipRevision = nonNegativeU64(
		properties?.ownershipRevision,
		"ownership revision",
	);
	const roleGeneration = nonNegativeSafeInteger(
		properties?.roleGeneration,
		"role generation",
	);
	const validFromMs = wallTime(view.capturedAtMs, "fence validity start");
	const validUntilMs =
		view.validUntilMs == null
			? undefined
			: wallTime(view.validUntilMs, "fence validity end");
	if (validUntilMs != null && validUntilMs <= validFromMs) {
		throw new Error("Invalid local placement view execution fence validity");
	}
	const maturityValidUntilMs =
		view.body.maturityValidUntilMs == null
			? undefined
			: wallTime(
					BigInt(view.body.maturityValidUntilMs),
					"fence maturity validity",
				);
	if (
		maturityValidUntilMs != null &&
		(validUntilMs == null || validUntilMs > maturityValidUntilMs)
	) {
		throw new Error("Invalid local placement view execution fence validity");
	}
	const normalized = {
		viewId,
		executionEpoch,
		ownershipRevision,
		roleGeneration,
		validFromMs,
		validUntilMs,
	};
	const bytes = encodeFenceBody(fenceBody(normalized));
	return Object.freeze({
		...normalized,
		fenceId: toHexString(sha256Sync(bytes)),
	});
};

/** Time-only portion of the runtime fence guard; ownership/session checks remain required. */
export const isLocalPlacementExecutionFenceTimeValid = (
	fence: LocalPlacementExecutionFence,
	nowMs: bigint,
): boolean => {
	const now = wallTime(nowMs, "fence check timestamp");
	return (
		now >= fence.validFromMs &&
		(fence.validUntilMs == null || now < fence.validUntilMs)
	);
};
