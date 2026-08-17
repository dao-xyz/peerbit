import {
	sha256Base64Sync,
	sha256Sync,
	toBase64,
	toHexString,
} from "@peerbit/crypto";
import { SharedLogRangePlanner } from "@peerbit/shared-log-rust";
import { expect } from "chai";
import { MAX_U32, MAX_U64 } from "../src/integers.js";
import {
	type LocalPlacementOwnerPlan,
	createLocalPlacementViewOwnerPlanner,
} from "../src/local-placement-owner-planner.js";
import {
	type LocalPlacementSnapshotInput,
	canonicalizeLocalPlacementSnapshotV1,
} from "../src/local-placement-view.js";
import {
	ReplicationIntent,
	ReplicationRangeIndexableU32,
	ReplicationRangeIndexableU64,
} from "../src/ranges.js";

const textEncoder = new TextEncoder();
const digest = (value: string) =>
	toHexString(sha256Sync(textEncoder.encode(value)));

type Owner = Readonly<{
	hash: string;
	publicKey: Uint8Array;
}>;

const owner = (value: number): Owner => {
	const publicKey = Uint8Array.of(value);
	return { publicKey, hash: sha256Base64Sync(publicKey) };
};

const u32Range = (
	ownerValue: Owner,
	id: number,
	offset: number,
	properties?: {
		width?: number;
		mode?: ReplicationIntent;
		timestamp?: bigint;
	},
) => {
	const range = new ReplicationRangeIndexableU32({
		id: Uint8Array.of(id),
		publicKeyHash: ownerValue.hash,
		offset,
		width: properties?.width ?? 20,
		timestamp: properties?.timestamp ?? 0n,
		mode: properties?.mode ?? ReplicationIntent.NonStrict,
	});
	return {
		owner: range.hash,
		id: range.id,
		timestamp: range.timestamp,
		start1: range.start1,
		end1: range.end1,
		start2: range.start2,
		end2: range.end2,
		width: range.width,
		mode: range.mode,
	};
};

const u64Range = (
	ownerValue: Owner,
	id: number,
	offset: bigint,
	properties?: { width?: bigint; mode?: ReplicationIntent },
) => {
	const range = new ReplicationRangeIndexableU64({
		id: Uint8Array.of(id),
		publicKeyHash: ownerValue.hash,
		offset,
		width: properties?.width ?? 20n,
		timestamp: 0n,
		mode: properties?.mode ?? ReplicationIntent.NonStrict,
	});
	return {
		owner: range.hash,
		id: range.id,
		timestamp: range.timestamp,
		start1: range.start1,
		end1: range.end1,
		start2: range.start2,
		end2: range.end2,
		width: range.width,
		mode: range.mode,
	};
};

const policy = (properties?: {
	minReplicas?: number;
	maxReplicas?: number;
	expandPeerFilter?: boolean;
	fullReplicaFallback?: boolean;
	includeStrictFullReplica?: boolean;
	roleAgeMs?: number;
}) => ({
	id: digest("local-owner-planner-policy-v1"),
	minReplicas: properties?.minReplicas ?? 1,
	maxReplicas: properties?.maxReplicas,
	roleAgeMs: properties?.roleAgeMs ?? 0,
	expandPeerFilter: properties?.expandPeerFilter ?? false,
	fullReplicaFallback: properties?.fullReplicaFallback ?? false,
	includeStrictFullReplica: properties?.includeStrictFullReplica ?? true,
});

const inputU32 = (properties: {
	owners: readonly Owner[];
	ranges: ReturnType<typeof u32Range>[];
	self?: Owner;
	selfReplicating?: boolean;
	basePeerFilter?: readonly string[];
	planner?: Readonly<{ id: string; version: number }>;
	policy?: ReturnType<typeof policy>;
}): LocalPlacementSnapshotInput<"u32"> => ({
	logId: Uint8Array.of(1, 2, 3),
	resolution: "u32",
	planner: properties.planner ?? {
		id: "peerbit-built-in-owner-planner",
		version: 1,
	},
	domain: { type: "hash", version: 1, configId: digest("u32-domain-v1") },
	policy: properties.policy ?? policy(),
	capturedAtMs: 1_000n,
	self: {
		owner: (properties.self ?? properties.owners[0]!).hash,
		replicating: properties.selfReplicating ?? true,
	},
	basePeerFilter: properties.basePeerFilter,
	owners: properties.owners.map((value) => ({ publicKey: value.publicKey })),
	ranges: properties.ranges,
});

const inputU64 = (properties: {
	owners: readonly Owner[];
	ranges: ReturnType<typeof u64Range>[];
	policy?: ReturnType<typeof policy>;
}): LocalPlacementSnapshotInput<"u64"> => ({
	logId: Uint8Array.of(4, 5, 6),
	resolution: "u64",
	planner: { id: "peerbit-built-in-owner-planner", version: 1 },
	domain: { type: "hash", version: 1, configId: digest("u64-domain-v1") },
	policy: properties.policy ?? policy(),
	capturedAtMs: 1_000n,
	self: { owner: properties.owners[0]!.hash, replicating: true },
	owners: properties.owners.map((value) => ({ publicKey: value.publicKey })),
	ranges: properties.ranges,
});

const hashes = (plan: LocalPlacementOwnerPlan) =>
	plan.owners.map((value) => value.ownerHash);

describe("local placement view owner planner", () => {
	it("hydrates real u32 WASM, clamps replicas, freezes output, and is permutation stable", async () => {
		const owners = [owner(3), owner(1), owner(2)];
		const ranges = owners.map((value, index) =>
			u32Range(value, index + 1, index * 100),
		);
		const firstView = canonicalizeLocalPlacementSnapshotV1(
			inputU32({
				owners,
				ranges,
				policy: policy({ minReplicas: 2, maxReplicas: 3 }),
			}),
		);
		const secondView = canonicalizeLocalPlacementSnapshotV1(
			inputU32({
				owners: [...owners].reverse(),
				ranges: [...ranges].reverse(),
				self: owners[0],
				policy: policy({ minReplicas: 2, maxReplicas: 3 }),
			}),
		);
		expect(secondView.digest).to.equal(firstView.digest);

		const first = await createLocalPlacementViewOwnerPlanner(firstView);
		const second = await createLocalPlacementViewOwnerPlanner(secondView);
		try {
			expect(Object.isFrozen(first)).to.equal(true);
			expect(first.viewId).to.equal(firstView.digest);
			expect(first.resolution).to.equal("u32");
			expect(first.validUntil).to.equal(undefined);
			expect(first.effectiveReplicas(0)).to.equal(2);
			expect(first.effectiveReplicas(100)).to.equal(3);

			const inputCoordinates = [5, 105];
			const firstPlan = first.plan({
				resolution: "u32",
				coordinates: inputCoordinates,
				requestedReplicas: 1,
			});
			inputCoordinates[0] = 999;
			const secondPlan = second.plan({
				resolution: "u32",
				coordinates: [5, 105],
				requestedReplicas: 1,
			});
			expect(firstPlan).to.deep.equal(secondPlan);
			expect(firstPlan.coordinates).to.deep.equal([5, 105]);
			expect(firstPlan.requestedReplicas).to.equal(1);
			expect(firstPlan.effectiveReplicas).to.equal(2);
			expect(new Set(hashes(firstPlan))).to.deep.equal(
				new Set([owners[0]!.hash, owners[1]!.hash]),
			);
			expect(firstPlan.owners.map((value) => value.ownerHash)).to.deep.equal(
				[...firstPlan.owners].map((value) => value.ownerHash).sort(),
			);
			expect(firstPlan.owners.every((value) => value.intersecting)).to.equal(
				true,
			);
			expect(firstPlan.owners[0]!.publicKey).to.match(/^[0-9a-f]+$/);
			expect(Object.isFrozen(firstPlan)).to.equal(true);
			expect(Object.isFrozen(firstPlan.coordinates)).to.equal(true);
			expect(Object.isFrozen(firstPlan.owners)).to.equal(true);
			expect(firstPlan.owners.every(Object.isFrozen)).to.equal(true);
			expect(firstPlan.format).to.equal(
				"peerbit-shared-log-local-placement-owner-plan",
			);
			expect(firstPlan.version).to.equal(1);
			expect(firstPlan.planId).to.match(/^[0-9a-f]{64}$/);
			expect(firstPlan.planId).to.equal(
				"2c3a358cb3e8eaa78b000041b01024ac8d8f629cb2610e5ceea415af05f0a3df",
			);

			const sameEffective = first.plan({
				resolution: "u32",
				coordinates: [5, 105],
				requestedReplicas: 0,
			});
			expect(sameEffective.effectiveReplicas).to.equal(
				firstPlan.effectiveReplicas,
			);
			expect(sameEffective.owners).to.deep.equal(firstPlan.owners);
			expect(sameEffective.planId).to.not.equal(firstPlan.planId);
			const movedCoordinates = first.plan({
				resolution: "u32",
				coordinates: [6, 106],
				requestedReplicas: 1,
			});
			expect(movedCoordinates.owners).to.deep.equal(firstPlan.owners);
			expect(movedCoordinates.planId).to.not.equal(firstPlan.planId);

			const high = first.plan({
				resolution: "u32",
				coordinates: [5, 105, 205],
				requestedReplicas: 100,
			});
			expect(high.effectiveReplicas).to.equal(3);
		} finally {
			first.close();
			second.close();
		}
	});

	it("hydrates real u64 WASM and preserves bigint coordinates", async () => {
		const owners = [owner(4), owner(5)];
		const view = canonicalizeLocalPlacementSnapshotV1(
			inputU64({
				owners,
				ranges: [u64Range(owners[0]!, 1, 10n), u64Range(owners[1]!, 2, 100n)],
				policy: policy({ minReplicas: 2, maxReplicas: 2 }),
			}),
		);
		const planner = await createLocalPlacementViewOwnerPlanner(view);
		try {
			const plan = planner.plan({
				resolution: "u64",
				coordinates: [15n, 105n],
				requestedReplicas: 2,
			});
			expect(plan.resolution).to.equal("u64");
			expect(plan.coordinates).to.deep.equal([15n, 105n]);
			expect(hashes(plan)).to.have.members(owners.map((value) => value.hash));
		} finally {
			planner.close();
		}
	});

	it("binds plan identity to semantic view changes with unchanged owners", async () => {
		const owners = [owner(19)];
		const ranges = [u32Range(owners[0]!, 1, 0)];
		const firstPolicy = policy();
		const first = await createLocalPlacementViewOwnerPlanner(
			canonicalizeLocalPlacementSnapshotV1(
				inputU32({ owners, ranges, policy: firstPolicy }),
			),
		);
		const second = await createLocalPlacementViewOwnerPlanner(
			canonicalizeLocalPlacementSnapshotV1(
				inputU32({
					owners,
					ranges,
					policy: {
						...firstPolicy,
						id: digest("same-semantics-new-policy-revision"),
					},
				}),
			),
		);
		try {
			const input = {
				resolution: "u32" as const,
				coordinates: [5],
				requestedReplicas: 1,
			};
			const firstPlan = first.plan(input);
			const secondPlan = second.plan(input);
			expect(secondPlan.owners).to.deep.equal(firstPlan.owners);
			expect(secondPlan.planId).to.not.equal(firstPlan.planId);
		} finally {
			first.close();
			second.close();
		}
	});

	it("hydrates runtime-compatible base64 range ids", async () => {
		const owners = [owner(16)];
		const rangeId = 0xfa;
		const view = canonicalizeLocalPlacementSnapshotV1(
			inputU32({
				owners,
				ranges: [u32Range(owners[0]!, rangeId, 0)],
			}),
		);
		const originalPut = SharedLogRangePlanner.prototype.put;
		const ids: string[] = [];
		SharedLogRangePlanner.prototype.put = function (range) {
			ids.push(range.id);
			return originalPut.call(this, range);
		};
		let planner: Awaited<
			ReturnType<typeof createLocalPlacementViewOwnerPlanner>
		> | null = null;
		try {
			planner = await createLocalPlacementViewOwnerPlanner(view);
			expect(ids).to.deep.equal([toBase64(Uint8Array.of(rangeId))]);
		} finally {
			SharedLogRangePlanner.prototype.put = originalPut;
			planner?.close();
		}
	});

	it("uses captured view time and role age for immature intersections", async () => {
		const owners = [owner(17), owner(18)];
		const view = canonicalizeLocalPlacementSnapshotV1(
			inputU32({
				owners,
				ranges: [
					u32Range(owners[0]!, 1, 0, { timestamp: 950n }),
					u32Range(owners[1]!, 2, 100),
				],
				policy: policy({ roleAgeMs: 100 }),
			}),
		);
		const planner = await createLocalPlacementViewOwnerPlanner(view);
		try {
			expect(planner.validUntil).to.equal(1_050n);
			const plan = planner.plan({
				resolution: "u32",
				coordinates: [5],
				requestedReplicas: 1,
			});
			const rows = new Map(
				plan.owners.map((value) => [value.ownerHash, value.intersecting]),
			);
			expect(rows.size).to.equal(2);
			expect(rows.get(owners[0]!.hash)).to.equal(true);
			expect(rows.get(owners[1]!.hash)).to.equal(false);
		} finally {
			planner.close();
		}
	});

	it("preserves absent versus empty peer filters", async () => {
		const owners = [owner(6), owner(7)];
		const ranges = [u32Range(owners[0]!, 1, 0), u32Range(owners[1]!, 2, 100)];
		const absent = await createLocalPlacementViewOwnerPlanner(
			canonicalizeLocalPlacementSnapshotV1(inputU32({ owners, ranges })),
		);
		const empty = await createLocalPlacementViewOwnerPlanner(
			canonicalizeLocalPlacementSnapshotV1(
				inputU32({ owners, ranges, basePeerFilter: [] }),
			),
		);
		try {
			expect(
				hashes(
					absent.plan({
						resolution: "u32",
						coordinates: [5],
						requestedReplicas: 1,
					}),
				),
			).to.deep.equal([owners[0]!.hash]);
			expect(
				empty.plan({
					resolution: "u32",
					coordinates: [5],
					requestedReplicas: 1,
				}).owners,
			).to.deep.equal([]);
		} finally {
			absent.close();
			empty.close();
		}
	});

	it("pins strict-full fallback and self-filter semantics", async () => {
		const owners = [owner(8), owner(9)];
		const strictRanges = [
			u32Range(owners[0]!, 1, 0),
			u32Range(owners[1]!, 2, 100, { mode: ReplicationIntent.Strict }),
		];
		const withoutStrict = await createLocalPlacementViewOwnerPlanner(
			canonicalizeLocalPlacementSnapshotV1(
				inputU32({
					owners,
					ranges: strictRanges,
					policy: policy({
						minReplicas: 2,
						maxReplicas: 2,
						fullReplicaFallback: true,
						includeStrictFullReplica: false,
					}),
				}),
			),
		);
		const withStrict = await createLocalPlacementViewOwnerPlanner(
			canonicalizeLocalPlacementSnapshotV1(
				inputU32({
					owners,
					ranges: strictRanges,
					policy: policy({
						minReplicas: 2,
						maxReplicas: 2,
						fullReplicaFallback: true,
						includeStrictFullReplica: true,
					}),
				}),
			),
		);
		const selfRanges = [
			u32Range(owners[0]!, 1, 0),
			u32Range(owners[1]!, 2, 100),
		];
		const selfIncluded = await createLocalPlacementViewOwnerPlanner(
			canonicalizeLocalPlacementSnapshotV1(
				inputU32({
					owners,
					ranges: selfRanges,
					self: owners[1],
					selfReplicating: true,
					basePeerFilter: [owners[0]!.hash],
					policy: policy({
						minReplicas: 2,
						maxReplicas: 2,
						expandPeerFilter: true,
					}),
				}),
			),
		);
		const selfExcluded = await createLocalPlacementViewOwnerPlanner(
			canonicalizeLocalPlacementSnapshotV1(
				inputU32({
					owners,
					ranges: selfRanges,
					self: owners[1],
					selfReplicating: false,
					basePeerFilter: [owners[0]!.hash],
					policy: policy({
						minReplicas: 2,
						maxReplicas: 2,
						expandPeerFilter: true,
					}),
				}),
			),
		);
		try {
			const coordinates = [50, 60] as const;
			expect(
				hashes(
					withoutStrict.plan({
						resolution: "u32",
						coordinates,
						requestedReplicas: 2,
					}),
				),
			).to.deep.equal([owners[0]!.hash]);
			expect(
				hashes(
					withStrict.plan({
						resolution: "u32",
						coordinates,
						requestedReplicas: 2,
					}),
				),
			).to.have.members(owners.map((value) => value.hash));
			expect(
				hashes(
					selfIncluded.plan({
						resolution: "u32",
						coordinates: [5, 105],
						requestedReplicas: 2,
					}),
				),
			).to.have.members(owners.map((value) => value.hash));
			expect(
				hashes(
					selfExcluded.plan({
						resolution: "u32",
						coordinates: [5, 105],
						requestedReplicas: 2,
					}),
				),
			).to.deep.equal([owners[0]!.hash]);
		} finally {
			withoutStrict.close();
			withStrict.close();
			selfIncluded.close();
			selfExcluded.close();
		}
	});

	it("rejects unsupported or forged views before loading WASM", async () => {
		const owners = [owner(10)];
		const unknown = canonicalizeLocalPlacementSnapshotV1(
			inputU32({
				owners,
				ranges: [u32Range(owners[0]!, 1, 0)],
				planner: { id: "unknown", version: 1 },
			}),
		);
		await expect(
			createLocalPlacementViewOwnerPlanner(unknown),
		).to.be.rejectedWith("Unsupported local placement owner planner");

		const issued = canonicalizeLocalPlacementSnapshotV1(
			inputU32({ owners, ranges: [u32Range(owners[0]!, 1, 0)] }),
		);
		await expect(
			createLocalPlacementViewOwnerPlanner({ ...issued }),
		).to.be.rejectedWith("Invalid issued canonical local placement view");
		issued.bytes[0] ^= 1;
		await expect(
			createLocalPlacementViewOwnerPlanner(issued),
		).to.be.rejectedWith(
			"Invalid issued canonical local placement view digest",
		);
	});

	it("validates dense typed coordinates and the 100-replica ceiling", async () => {
		const owners = [owner(11)];
		const planner = await createLocalPlacementViewOwnerPlanner(
			canonicalizeLocalPlacementSnapshotV1(
				inputU32({ owners, ranges: [u32Range(owners[0]!, 1, 0)] }),
			),
		);
		try {
			expect(() => planner.effectiveReplicas(101)).to.throw(
				"Invalid local placement owner plan requested replicas",
			);
			expect(() => planner.effectiveReplicas(-0)).to.throw(
				"Invalid local placement owner plan requested replicas",
			);
			expect(() =>
				planner.plan({
					resolution: "u32",
					coordinates: [],
					requestedReplicas: 1,
				}),
			).to.throw("coordinates must match effective replicas");
			const sparse = new Array<number>(1);
			expect(() =>
				planner.plan({
					resolution: "u32",
					coordinates: sparse,
					requestedReplicas: 1,
				}),
			).to.throw("Invalid sparse");
			expect(() =>
				planner.plan({
					resolution: "u32",
					coordinates: [MAX_U32 + 1],
					requestedReplicas: 1,
				}),
			).to.throw("Invalid u32");
			expect(() =>
				planner.plan({
					resolution: "u32",
					coordinates: [-0],
					requestedReplicas: 1,
				}),
			).to.throw("Invalid u32");
			expect(() =>
				planner.plan({
					resolution: "u64",
					coordinates: [MAX_U64],
					requestedReplicas: 1,
				}),
			).to.throw("resolution mismatch");

			let lengthReads = 0;
			const boundedProxy = new Proxy([5], {
				get(target, property, receiver) {
					if (property === "length") {
						lengthReads++;
						return lengthReads === 1 ? 1 : Number.MAX_SAFE_INTEGER;
					}
					return Reflect.get(target, property, receiver);
				},
			});
			let resolutionReads = 0;
			const aliasedInput = new Proxy(
				{
					resolution: "u32" as const,
					coordinates: boundedProxy,
					requestedReplicas: 1,
				},
				{
					get(target, property, receiver) {
						if (property === "resolution") {
							resolutionReads++;
							return resolutionReads === 1 ? "u32" : "u64";
						}
						return Reflect.get(target, property, receiver);
					},
				},
			);
			const boundedPlan = planner.plan(aliasedInput);
			expect(boundedPlan.resolution).to.equal("u32");
			expect(lengthReads).to.equal(1);
			expect(resolutionReads).to.equal(1);
		} finally {
			planner.close();
		}
	});

	it("rejects unknown and malformed native owner rows", async () => {
		const owners = [owner(12)];
		const view = canonicalizeLocalPlacementSnapshotV1(
			inputU32({ owners, ranges: [u32Range(owners[0]!, 1, 0)] }),
		);
		const planner = await createLocalPlacementViewOwnerPlanner(view);
		const original = SharedLogRangePlanner.prototype.findLeaders;
		try {
			SharedLogRangePlanner.prototype.findLeaders = (() =>
				new Map([
					["unknown-owner", { intersecting: true }],
				])) as typeof original;
			expect(() =>
				planner.plan({
					resolution: "u32",
					coordinates: [5],
					requestedReplicas: 1,
				}),
			).to.throw("Unknown native local placement owner");

			SharedLogRangePlanner.prototype.findLeaders = (() =>
				new Map([
					[owners[0]!.hash, { intersecting: "yes" }],
				])) as unknown as typeof original;
			expect(() =>
				planner.plan({
					resolution: "u32",
					coordinates: [5],
					requestedReplicas: 1,
				}),
			).to.throw("Invalid native local placement owner sample");
		} finally {
			SharedLogRangePlanner.prototype.findLeaders = original;
			planner.close();
		}
	});

	it("frees partial hydration once and preserves cleanup failure causes", async () => {
		const owners = [owner(13), owner(14)];
		const view = canonicalizeLocalPlacementSnapshotV1(
			inputU32({
				owners,
				ranges: [u32Range(owners[0]!, 1, 0), u32Range(owners[1]!, 2, 100)],
			}),
		);
		const originalPut = SharedLogRangePlanner.prototype.put;
		const originalClose = SharedLogRangePlanner.prototype.close;
		let puts = 0;
		let closes = 0;
		try {
			SharedLogRangePlanner.prototype.put = function (...args) {
				puts++;
				if (puts === 2) throw new Error("hydrate failed");
				return originalPut.apply(this, args);
			};
			SharedLogRangePlanner.prototype.close = function () {
				closes++;
				return originalClose.call(this);
			};
			await expect(
				createLocalPlacementViewOwnerPlanner(view),
			).to.be.rejectedWith("hydrate failed");
			expect(closes).to.equal(1);

			puts = 0;
			closes = 0;
			SharedLogRangePlanner.prototype.close = function () {
				closes++;
				originalClose.call(this);
				throw new Error("cleanup failed");
			};
			let failure: unknown;
			try {
				await createLocalPlacementViewOwnerPlanner(view);
			} catch (error) {
				failure = error;
			}
			expect(failure).to.be.instanceOf(AggregateError);
			expect((failure as AggregateError).errors).to.have.length(2);
			expect(closes).to.equal(1);
		} finally {
			SharedLogRangePlanner.prototype.put = originalPut;
			SharedLogRangePlanner.prototype.close = originalClose;
		}
	});

	it("is fail-closed and idempotent when native close throws", async () => {
		const owners = [owner(15)];
		const view = canonicalizeLocalPlacementSnapshotV1(
			inputU32({ owners, ranges: [u32Range(owners[0]!, 1, 0)] }),
		);
		const planner = await createLocalPlacementViewOwnerPlanner(view);
		const originalClose = SharedLogRangePlanner.prototype.close;
		let closes = 0;
		SharedLogRangePlanner.prototype.close = function () {
			closes++;
			originalClose.call(this);
			throw new Error("close failed");
		};
		try {
			expect(() => planner.close()).to.throw("close failed");
			expect(() => planner.close()).to.not.throw();
			expect(closes).to.equal(1);
			expect(() => planner.effectiveReplicas(1)).to.throw("planner is closed");
			let consumed = false;
			const coordinates = new Proxy([5], {
				get(target, property, receiver) {
					if (property === "length" || property === "0") consumed = true;
					return Reflect.get(target, property, receiver);
				},
			});
			expect(() =>
				planner.plan({
					resolution: "u32",
					coordinates,
					requestedReplicas: 1,
				}),
			).to.throw("planner is closed");
			expect(consumed).to.equal(false);
		} finally {
			SharedLogRangePlanner.prototype.close = originalClose;
		}
	});
});
