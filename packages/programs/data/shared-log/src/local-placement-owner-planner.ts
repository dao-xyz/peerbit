import {
	fromHexString,
	sha256Sync,
	toBase64,
	toHexString,
} from "@peerbit/crypto";
import type {
	NativeReplicationRange,
	SharedLogRangePlanner,
} from "@peerbit/shared-log-rust";
import { MAX_U32, MAX_U64 } from "./integers.js";
import {
	type CanonicalLocalPlacementView,
	assertIssuedCanonicalLocalPlacementView,
} from "./local-placement-view.js";

const BUILT_IN_PLANNER_ID = "peerbit-built-in-owner-planner" as const;
const BUILT_IN_PLANNER_VERSION = 1 as const;
const PLAN_FORMAT = "peerbit-shared-log-local-placement-owner-plan" as const;
const PLAN_VERSION = 1 as const;
const MAX_REPLICAS = 100;
const MAX_PLAN_ID_BYTES = 4 * 1024 * 1024;
const encoder = new TextEncoder();

type Resolution = "u32" | "u64";

type CoordinateInput<R extends Resolution> = R extends "u32"
	? readonly number[]
	: readonly bigint[];

export type LocalPlacementOwnerPlanInput<R extends Resolution = Resolution> =
	Readonly<{
		resolution: R;
		coordinates: CoordinateInput<R>;
		requestedReplicas: number;
	}>;

export type LocalPlacementOwnerRow = Readonly<{
	ownerHash: string;
	/** Canonical lower-case hex encoding of the authenticated public-key bytes. */
	publicKey: string;
	intersecting: boolean;
}>;

export type LocalPlacementOwnerPlanFor<R extends Resolution> = Readonly<{
	format: typeof PLAN_FORMAT;
	version: typeof PLAN_VERSION;
	/** Ownership-decision identity only; never custody or pruning authority. */
	planId: string;
	viewId: string;
	resolution: R;
	coordinates: CoordinateInput<R>;
	requestedReplicas: number;
	effectiveReplicas: number;
	owners: readonly LocalPlacementOwnerRow[];
}>;

export type LocalPlacementOwnerPlan =
	| LocalPlacementOwnerPlanFor<"u32">
	| LocalPlacementOwnerPlanFor<"u64">;

export type LocalPlacementViewOwnerPlanner = Readonly<{
	viewId: string;
	resolution: Resolution;
	validUntil?: bigint;
	effectiveReplicas(requestedReplicas: number): number;
	plan<R extends Resolution>(
		input: LocalPlacementOwnerPlanInput<R>,
	): LocalPlacementOwnerPlanFor<R>;
	close(): void;
}>;

type NativeOwnerPlanner = Pick<
	SharedLogRangePlanner,
	"length" | "put" | "findLeaders" | "close"
>;

type LocalPlacementOwnerPlannerDependencies = Readonly<{
	createRangePlanner(resolution: Resolution): Promise<NativeOwnerPlanner>;
}>;

class BoundedPlanWriter {
	private value = new Uint8Array(256);
	private offset = 0;

	private reserve(length: number): number {
		if (!Number.isSafeInteger(length) || length < 0) {
			throw new Error("Invalid local placement owner plan encoding length");
		}
		const required = this.offset + length;
		if (required > MAX_PLAN_ID_BYTES) {
			throw new Error("Local placement owner plan exceeds the byte limit");
		}
		if (required > this.value.byteLength) {
			let capacity = this.value.byteLength;
			while (capacity < required) {
				capacity = Math.min(MAX_PLAN_ID_BYTES, capacity * 2);
			}
			const next = new Uint8Array(capacity);
			next.set(this.value.subarray(0, this.offset));
			this.value = next;
		}
		const start = this.offset;
		this.offset = required;
		return start;
	}

	u8(value: number): void {
		const start = this.reserve(1);
		this.value[start] = value;
	}

	u32(value: number): void {
		const start = this.reserve(4);
		new DataView(this.value.buffer).setUint32(start, value, true);
	}

	u64(value: bigint): void {
		const start = this.reserve(8);
		new DataView(this.value.buffer).setBigUint64(start, value, true);
	}

	raw(value: Uint8Array): void {
		const start = this.reserve(value.byteLength);
		this.value.set(value, start);
	}

	bytes(value: Uint8Array): void {
		this.u32(value.byteLength);
		this.raw(value);
	}

	string(value: string): void {
		this.bytes(encoder.encode(value));
	}

	finish(): Uint8Array {
		return this.value.slice(0, this.offset);
	}
}

const own = (value: readonly unknown[], index: number) =>
	Object.prototype.hasOwnProperty.call(value, index);

const compareStrings = (left: string, right: string) =>
	left < right ? -1 : left > right ? 1 : 0;

const requestedReplicaCount = (value: unknown): number => {
	if (
		typeof value !== "number" ||
		!Number.isSafeInteger(value) ||
		Object.is(value, -0) ||
		value < 0 ||
		value > MAX_REPLICAS
	) {
		throw new Error("Invalid local placement owner plan requested replicas");
	}
	return value;
};

const effectiveReplicaCount = (
	view: CanonicalLocalPlacementView,
	requestedReplicas: unknown,
): number => {
	const requested = requestedReplicaCount(requestedReplicas);
	const maximum = view.body.policy.maxReplicas ?? MAX_REPLICAS;
	return Math.max(view.body.policy.minReplicas, Math.min(maximum, requested));
};

const canonicalCoordinates = <R extends Resolution>(
	resolution: R,
	value: unknown,
	effectiveReplicas: number,
): CoordinateInput<R> => {
	if (!Array.isArray(value)) {
		throw new Error(
			"Local placement owner plan coordinates must match effective replicas",
		);
	}
	const length = value.length;
	if (length !== effectiveReplicas) {
		throw new Error(
			"Local placement owner plan coordinates must match effective replicas",
		);
	}
	const coordinates: Array<number | bigint> = [];
	for (let index = 0; index < length; index++) {
		if (!own(value, index)) {
			throw new Error("Invalid sparse local placement owner plan coordinates");
		}
		const coordinate = value[index];
		if (resolution === "u32") {
			if (
				typeof coordinate !== "number" ||
				!Number.isInteger(coordinate) ||
				Object.is(coordinate, -0) ||
				coordinate < 0 ||
				coordinate > MAX_U32
			) {
				throw new Error("Invalid u32 local placement owner plan coordinate");
			}
		} else if (
			typeof coordinate !== "bigint" ||
			coordinate < 0n ||
			coordinate > MAX_U64
		) {
			throw new Error("Invalid u64 local placement owner plan coordinate");
		}
		coordinates.push(coordinate);
	}
	return Object.freeze(coordinates) as CoordinateInput<R>;
};

const nativeOwnerRows = (
	value: unknown,
	ownersByHash: ReadonlyMap<string, string>,
): readonly LocalPlacementOwnerRow[] => {
	if (!(value instanceof Map) || value.size > ownersByHash.size) {
		throw new Error("Invalid native local placement owner plan output");
	}
	const rows: LocalPlacementOwnerRow[] = [];
	for (const [ownerHash, sample] of value) {
		if (typeof ownerHash !== "string") {
			throw new Error("Invalid native local placement owner hash");
		}
		const publicKey = ownersByHash.get(ownerHash);
		if (publicKey == null) {
			throw new Error(`Unknown native local placement owner: ${ownerHash}`);
		}
		if (
			!sample ||
			typeof sample !== "object" ||
			Array.isArray(sample) ||
			!Object.prototype.hasOwnProperty.call(sample, "intersecting") ||
			typeof (sample as { intersecting?: unknown }).intersecting !==
				"boolean" ||
			Object.keys(sample).length !== 1
		) {
			throw new Error("Invalid native local placement owner sample");
		}
		rows.push(
			Object.freeze({
				ownerHash,
				publicKey,
				intersecting: (sample as { intersecting: boolean }).intersecting,
			}),
		);
	}
	rows.sort(
		(left, right) =>
			compareStrings(left.ownerHash, right.ownerHash) ||
			compareStrings(left.publicKey, right.publicKey),
	);
	return Object.freeze(rows);
};

const encodePlanIdentity = (
	view: CanonicalLocalPlacementView,
	coordinates: readonly (number | bigint)[],
	requestedReplicas: number,
	effectiveReplicas: number,
	owners: readonly LocalPlacementOwnerRow[],
): Uint8Array => {
	const writer = new BoundedPlanWriter();
	writer.string(PLAN_FORMAT);
	writer.u32(PLAN_VERSION);
	writer.raw(fromHexString(view.digest));
	writer.string(view.body.planner.id);
	writer.u32(view.body.planner.version);
	writer.u8(view.body.resolution === "u32" ? 0 : 1);
	writer.u32(coordinates.length);
	for (const coordinate of coordinates) {
		if (view.body.resolution === "u32") writer.u32(Number(coordinate));
		else writer.u64(BigInt(coordinate));
	}
	writer.u32(requestedReplicas);
	writer.u32(effectiveReplicas);
	writer.u32(owners.length);
	for (const owner of owners) {
		writer.string(owner.ownerHash);
		writer.u8(owner.intersecting ? 1 : 0);
	}
	return writer.finish();
};

const defaultDependencies: LocalPlacementOwnerPlannerDependencies = {
	async createRangePlanner(resolution) {
		const { createRangePlanner } = await import(
			/* @vite-ignore */ "@peerbit/shared-log-rust"
		);
		return createRangePlanner(resolution);
	},
};

const closeAfterConstructionFailure = (
	native: NativeOwnerPlanner | undefined,
	error: unknown,
): never => {
	if (!native || typeof native.close !== "function") throw error;
	try {
		native.close();
	} catch (closeError) {
		throw new AggregateError(
			[error, closeError],
			"Failed to construct and release local placement owner planner",
		);
	}
	throw error;
};

/**
 * Hydrate a pure owner planner from one complete, issued local placement view.
 * The returned object never reads an Index and performs no transfer or pruning.
 *
 * @internal
 */
const createLocalPlacementViewOwnerPlannerWithDependencies = async (
	viewValue: unknown,
	dependencies: LocalPlacementOwnerPlannerDependencies,
): Promise<LocalPlacementViewOwnerPlanner> => {
	const view = assertIssuedCanonicalLocalPlacementView(viewValue);
	if (
		view.body.planner.id !== BUILT_IN_PLANNER_ID ||
		view.body.planner.version !== BUILT_IN_PLANNER_VERSION
	) {
		throw new Error(
			`Unsupported local placement owner planner: ${view.body.planner.id}@${view.body.planner.version}`,
		);
	}
	if (!dependencies || typeof dependencies.createRangePlanner !== "function") {
		throw new Error("Invalid local placement owner planner dependencies");
	}

	let native: NativeOwnerPlanner | undefined;
	try {
		native = await dependencies.createRangePlanner(view.body.resolution);
		if (
			!native ||
			typeof native.put !== "function" ||
			typeof native.findLeaders !== "function" ||
			typeof native.close !== "function"
		) {
			throw new Error("Invalid native local placement owner planner");
		}
		for (const range of view.body.ranges) {
			const nativeRange: NativeReplicationRange = {
				id: toBase64(fromHexString(range.id)),
				hash: range.owner,
				timestamp: range.timestamp,
				start1: range.start1,
				end1: range.end1,
				start2: range.start2,
				end2: range.end2,
				width: range.width,
				mode: range.mode,
			};
			native.put(nativeRange);
		}
		if (native.length !== view.body.ranges.length) {
			throw new Error(
				"Native local placement owner planner hydration mismatch",
			);
		}
	} catch (error) {
		return closeAfterConstructionFailure(native, error);
	}

	const hydratedNative = native;
	const ownersByHash = new Map(
		view.body.owners.map((owner) => [owner.hash, owner.publicKey]),
	);
	const state = { closed: false };
	const assertOpen = () => {
		if (state.closed) {
			throw new Error("Local placement owner planner is closed");
		}
	};
	const planner: LocalPlacementViewOwnerPlanner = {
		viewId: view.digest,
		resolution: view.body.resolution,
		validUntil: view.validUntilMs,
		effectiveReplicas(requestedReplicas) {
			assertOpen();
			return effectiveReplicaCount(view, requestedReplicas);
		},
		plan<R extends Resolution>(
			input: LocalPlacementOwnerPlanInput<R>,
		): LocalPlacementOwnerPlanFor<R> {
			assertOpen();
			if (!input || typeof input !== "object") {
				throw new Error("Local placement owner plan resolution mismatch");
			}
			const resolution = input.resolution;
			const requestedReplicasValue = input.requestedReplicas;
			const coordinatesValue = input.coordinates;
			if (resolution !== view.body.resolution) {
				throw new Error("Local placement owner plan resolution mismatch");
			}
			const requestedReplicas = requestedReplicaCount(requestedReplicasValue);
			const effectiveReplicas = effectiveReplicaCount(view, requestedReplicas);
			const coordinates = canonicalCoordinates(
				resolution,
				coordinatesValue,
				effectiveReplicas,
			);
			const nativeRows = hydratedNative.findLeaders(
				coordinates,
				effectiveReplicas,
				{
					roleAge: Number(view.body.policy.roleAgeMs),
					now: view.capturedAtMs,
					peerFilter: view.body.basePeerFilter ?? undefined,
					expandPeerFilter: view.body.policy.expandPeerFilter,
					selfHash: view.body.self.owner,
					selfReplicating: view.body.self.replicating,
					fullReplicaFallback: view.body.policy.fullReplicaFallback,
					includeStrictFullReplica: view.body.policy.includeStrictFullReplica,
				},
			);
			const owners = nativeOwnerRows(nativeRows, ownersByHash);
			const planId = toHexString(
				sha256Sync(
					encodePlanIdentity(
						view,
						coordinates,
						requestedReplicas,
						effectiveReplicas,
						owners,
					),
				),
			);
			return Object.freeze({
				format: PLAN_FORMAT,
				version: PLAN_VERSION,
				planId,
				viewId: view.digest,
				resolution,
				coordinates,
				requestedReplicas,
				effectiveReplicas,
				owners,
			});
		},
		close() {
			if (state.closed) return;
			state.closed = true;
			hydratedNative.close();
		},
	};
	return Object.freeze(planner);
};

export const createLocalPlacementViewOwnerPlanner = (
	viewValue: unknown,
): Promise<LocalPlacementViewOwnerPlanner> =>
	createLocalPlacementViewOwnerPlannerWithDependencies(
		viewValue,
		defaultDependencies,
	);
