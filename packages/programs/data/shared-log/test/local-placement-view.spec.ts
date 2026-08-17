import {
	Ed25519Keypair,
	type PublicSignKey,
	sha256Base64Sync,
	sha256Sync,
	toHexString,
} from "@peerbit/crypto";
import { expect } from "chai";
import { MAX_U32, MAX_U64 } from "../src/integers.js";
import {
	type LocalPlacementSnapshotInput,
	canonicalizeLocalPlacementSnapshotV1,
	createLocalPlacementExecutionFence,
	isLocalPlacementExecutionFenceTimeValid,
} from "../src/local-placement-view.js";
import {
	ReplicationIntent,
	ReplicationRangeIndexableU32,
	ReplicationRangeIndexableU64,
} from "../src/ranges.js";

const textEncoder = new TextEncoder();
const digest = (value: string) =>
	toHexString(sha256Sync(textEncoder.encode(value)));
const bytes = (...values: number[]) => Uint8Array.from(values);

const u32Range = (
	owner: PublicSignKey,
	id: Uint8Array,
	properties?: {
		offset?: number;
		width?: number;
		timestamp?: bigint;
		mode?: ReplicationIntent;
	},
) => {
	const row = new ReplicationRangeIndexableU32({
		id,
		publicKeyHash: owner.hashcode(),
		offset: properties?.offset ?? 10,
		width: properties?.width ?? 20,
		timestamp: properties?.timestamp ?? 100n,
		mode: properties?.mode ?? ReplicationIntent.NonStrict,
	});
	return {
		owner: row.hash,
		id: row.id,
		timestamp: row.timestamp,
		start1: row.start1,
		end1: row.end1,
		start2: row.start2,
		end2: row.end2,
		width: row.width,
		mode: row.mode,
	};
};

const u64Range = (
	owner: PublicSignKey,
	id: Uint8Array,
	properties?: {
		offset?: bigint;
		width?: bigint;
		timestamp?: bigint;
		mode?: ReplicationIntent;
	},
) => {
	const row = new ReplicationRangeIndexableU64({
		id,
		publicKeyHash: owner.hashcode(),
		offset: properties?.offset ?? 10n,
		width: properties?.width ?? 20n,
		timestamp: properties?.timestamp ?? 100n,
		mode: properties?.mode ?? ReplicationIntent.NonStrict,
	});
	return {
		owner: row.hash,
		id: row.id,
		timestamp: row.timestamp,
		start1: row.start1,
		end1: row.end1,
		start2: row.start2,
		end2: row.end2,
		width: row.width,
		mode: row.mode,
	};
};

const inputU32 = (
	keys: readonly PublicSignKey[],
	properties?: {
		capturedAtMs?: bigint;
		basePeerFilter?: readonly string[];
		ranges?: ReturnType<typeof u32Range>[];
	},
): LocalPlacementSnapshotInput<"u32"> => ({
	logId: bytes(1, 2, 3),
	resolution: "u32",
	planner: { id: "peerbit-built-in-owner-planner", version: 1 },
	domain: { type: "hash", version: 1, configId: digest("hash-domain-v1") },
	policy: {
		id: digest("default-policy-v1"),
		minReplicas: 2,
		maxReplicas: 3,
		roleAgeMs: 50,
		expandPeerFilter: true,
		fullReplicaFallback: true,
		includeStrictFullReplica: true,
	},
	capturedAtMs: properties?.capturedAtMs ?? 120n,
	self: { owner: keys[0]!.hashcode(), replicating: true },
	basePeerFilter: properties?.basePeerFilter,
	owners: keys.map((publicKey) => ({ publicKey: publicKey.bytes })),
	ranges:
		properties?.ranges ??
		keys.map((key, index) =>
			u32Range(key, bytes(index + 1), {
				offset: index * 100,
				timestamp: BigInt(100 + index * 100),
			}),
		),
});

describe("local placement view", () => {
	let keys: PublicSignKey[];

	before(async () => {
		keys = await Promise.all(
			[0, 1, 2].map(async () => (await Ed25519Keypair.create()).publicKey),
		);
	});

	it("canonicalizes complete facts independently of input order", () => {
		const base = inputU32(keys, {
			basePeerFilter: keys.map((key) => key.hashcode()),
		});
		const ownerOrder = [...base.owners];
		const rangeOrder = [...base.ranges];
		const filterOrder = [...base.basePeerFilter!];
		const first = canonicalizeLocalPlacementSnapshotV1(base);
		const second = canonicalizeLocalPlacementSnapshotV1({
			...base,
			owners: [...base.owners].reverse(),
			ranges: [...base.ranges].reverse(),
			basePeerFilter: [...base.basePeerFilter!].reverse(),
		});

		expect(second.digest).to.equal(first.digest);
		expect([...second.bytes]).to.deep.equal([...first.bytes]);
		expect(first.digest).to.equal(toHexString(sha256Sync(first.bytes)));
		expect(first.body.owners[0]!.hash).to.equal(
			sha256Base64Sync(
				Uint8Array.from(
					keys.find((key) => key.hashcode() === first.body.owners[0]!.hash)!
						.bytes,
				),
			),
		);
		expect(base.owners).to.deep.equal(ownerOrder);
		expect(base.ranges).to.deep.equal(rangeOrder);
		expect(base.basePeerFilter).to.deep.equal(filterOrder);
		expect(first.bytes[0]).to.not.equal("{".charCodeAt(0));
	});

	it("pins the V1 binary layout and execution-fence identity", () => {
		const publicKey = bytes(2);
		const owner = sha256Base64Sync(publicKey);
		const row = new ReplicationRangeIndexableU32({
			id: bytes(3),
			publicKeyHash: owner,
			offset: 1,
			width: 2,
			timestamp: 10n,
			mode: ReplicationIntent.Strict,
		});
		const view = canonicalizeLocalPlacementSnapshotV1({
			logId: bytes(1),
			resolution: "u32",
			planner: { id: "p", version: 1 },
			domain: { type: "h", version: 1, configId: "00".repeat(32) },
			policy: {
				id: "11".repeat(32),
				minReplicas: 1,
				roleAgeMs: 5,
				expandPeerFilter: true,
				fullReplicaFallback: false,
				includeStrictFullReplica: true,
			},
			capturedAtMs: 20n,
			self: { owner, replicating: true },
			owners: [{ publicKey }],
			ranges: [
				{
					owner,
					id: row.id,
					timestamp: row.timestamp,
					start1: row.start1,
					end1: row.end1,
					start2: row.start2,
					end2: row.end2,
					width: row.width,
					mode: row.mode,
				},
			],
		});
		const expectedBytes =
			"27000000706565726269742d7368617265642d6c6f672d6c6f63616c2d706c61" +
			"63656d656e742d76696577010000000100000001000100000070010000000100" +
			"0000680100000020000000000000000000000000000000000000000000000000" +
			"0000000000000000000000200000001111111111111111111111111111111111" +
			"111111111111111111111111111111010000000005000000000000000100012c" +
			"000000323847307951442f354931585731326c786a6745415358325862442b50" +
			"69524a533362716d4752583259593d01010000002c000000323847307951442f" +
			"354931585731326c786a6745415358325862442b5069524a533362716d475258" +
			"3259593d01000000020000010000002c000000323847307951442f3549315857" +
			"31326c786a6745415358325862442b5069524a533362716d4752583259593d01" +
			"000000030a000000000000000100000003000000010000000300000002000000" +
			"0101";
		expect(toHexString(view.bytes)).to.equal(expectedBytes);
		expect(view.digest).to.equal(
			"27bc4c0c3275b271ccb42b518298dd8b572b169c5750904995ac3ef82a5ca258",
		);
		const fence = createLocalPlacementExecutionFence({
			view,
			executionEpoch: new Uint8Array(32),
			ownershipRevision: 0n,
			roleGeneration: 0,
		});
		expect(fence.fenceId).to.equal(
			"e9fdfd50456f9b3dec54ba7643539643986b3a094ddb8621a47171653817901f",
		);
	});

	it("keeps semantic identity independent from a shorter freshness deadline", () => {
		const semantic = canonicalizeLocalPlacementSnapshotV1(inputU32(keys));
		const short = canonicalizeLocalPlacementSnapshotV1({
			...inputU32(keys),
			freshUntilMs: 130n,
		});
		expect(short.digest).to.equal(semantic.digest);
		expect(short.body.maturityValidUntilMs).to.equal("150");
		expect(short.validUntilMs).to.equal(130n);
		const fence = createLocalPlacementExecutionFence({
			view: short,
			executionEpoch: new Uint8Array(32).fill(3),
			ownershipRevision: 0n,
			roleGeneration: 0,
		});
		expect(isLocalPlacementExecutionFenceTimeValid(fence, 129n)).to.equal(true);
		expect(isLocalPlacementExecutionFenceTimeValid(fence, 130n)).to.equal(
			false,
		);
	});

	it("distinguishes absent and explicitly empty peer filters", () => {
		const absent = canonicalizeLocalPlacementSnapshotV1(inputU32(keys));
		const empty = canonicalizeLocalPlacementSnapshotV1(
			inputU32(keys, { basePeerFilter: [] }),
		);
		expect(empty.digest).to.not.equal(absent.digest);
		expect(absent.body.basePeerFilter).to.equal(null);
		expect(empty.body.basePeerFilter).to.deep.equal([]);
	});

	it("keeps a semantic digest stable until the next maturity transition", () => {
		const ranges = [
			u32Range(keys[0]!, bytes(1), { timestamp: 100n }),
			u32Range(keys[1]!, bytes(2), { timestamp: 200n }),
		];
		const at120 = canonicalizeLocalPlacementSnapshotV1(
			inputU32(keys, { capturedAtMs: 120n, ranges }),
		);
		const at149 = canonicalizeLocalPlacementSnapshotV1(
			inputU32(keys, { capturedAtMs: 149n, ranges }),
		);
		const at150 = canonicalizeLocalPlacementSnapshotV1(
			inputU32(keys, { capturedAtMs: 150n, ranges }),
		);
		const at250 = canonicalizeLocalPlacementSnapshotV1(
			inputU32(keys, { capturedAtMs: 250n, ranges }),
		);

		expect(at120.digest).to.equal(at149.digest);
		expect(at120.validUntilMs).to.equal(150n);
		expect(
			Object.fromEntries(
				at120.body.ranges.map((range) => [range.timestamp, range.mature]),
			),
		).to.deep.equal({ "100": false, "200": false });
		expect(at150.digest).to.not.equal(at149.digest);
		expect(at150.validUntilMs).to.equal(250n);
		expect(
			Object.fromEntries(
				at150.body.ranges.map((range) => [range.timestamp, range.mature]),
			),
		).to.deep.equal({ "100": true, "200": false });
		expect(at250.validUntilMs).to.equal(undefined);
		expect(at250.body.maturityValidUntilMs).to.equal(null);
	});

	it("binds every placement policy and identity input", () => {
		const base = inputU32(keys, { basePeerFilter: [keys[0]!.hashcode()] });
		const expected = canonicalizeLocalPlacementSnapshotV1(base).digest;
		const variants: LocalPlacementSnapshotInput<"u32">[] = [
			{ ...base, planner: { ...base.planner, version: 2 } },
			{
				...base,
				domain: { ...base.domain, configId: digest("different-domain") },
			},
			{
				...base,
				policy: { ...base.policy, fullReplicaFallback: false },
			},
			{ ...base, self: { ...base.self, replicating: false } },
			{ ...base, basePeerFilter: [keys[1]!.hashcode()] },
			{
				...base,
				ranges: [
					{ ...base.ranges[0]!, mode: ReplicationIntent.Strict },
					...base.ranges.slice(1),
				],
			},
		];
		for (const variant of variants) {
			expect(canonicalizeLocalPlacementSnapshotV1(variant).digest).to.not.equal(
				expected,
			);
		}
	});

	it("supports exact u64 placement facts and boundaries", () => {
		const input: LocalPlacementSnapshotInput<"u64"> = {
			...inputU32(keys),
			resolution: "u64",
			capturedAtMs: 1_000n,
			policy: { ...inputU32(keys).policy, roleAgeMs: 0 },
			ranges: [
				u64Range(keys[0]!, bytes(9), {
					offset: MAX_U64 - 10n,
					width: 10n,
					timestamp: 1_000n,
				}),
			],
		};
		const view = canonicalizeLocalPlacementSnapshotV1(input);
		expect(view.body.resolution).to.equal("u64");
		expect(view.body.ranges[0]!.start1).to.equal((MAX_U64 - 10n).toString());
	});

	it("rejects a resolution mismatch and noncanonical range geometry", () => {
		const base = inputU32(keys);
		expect(() =>
			canonicalizeLocalPlacementSnapshotV1({
				...base,
				ranges: [{ ...base.ranges[0]!, start1: 1n }] as any,
			}),
		).to.throw("range 0 start1");
		expect(() =>
			canonicalizeLocalPlacementSnapshotV1({
				...base,
				ranges: [{ ...base.ranges[0]!, end1: MAX_U32 }],
			}),
		).to.throw("range 0 geometry");
	});

	it("verifies full public-key identity and rejects unknown owners", () => {
		const base = inputU32(keys);
		expect(() =>
			canonicalizeLocalPlacementSnapshotV1({
				...base,
				owners: [{ publicKey: keys[0]!.bytes, hash: keys[1]!.hashcode() }],
			}),
		).to.throw("hash mismatch");
		expect(() =>
			canonicalizeLocalPlacementSnapshotV1({
				...base,
				basePeerFilter: ["unknown"],
			}),
		).to.throw("Unknown local placement view peer");
	});

	it("rejects duplicate range ids even across different owners", () => {
		const duplicateId = bytes(7);
		expect(() =>
			canonicalizeLocalPlacementSnapshotV1(
				inputU32(keys, {
					ranges: [
						u32Range(keys[0]!, duplicateId),
						u32Range(keys[1]!, duplicateId),
					],
				}),
			),
		).to.throw("Duplicate local placement view range id");
	});

	it("rejects sparse inputs and every configured collection bound", () => {
		const base = inputU32(keys);
		const sparseOwners = new Array(1) as any;
		expect(() =>
			canonicalizeLocalPlacementSnapshotV1({ ...base, owners: sparseOwners }),
		).to.throw("sparse owners");
		expect(() =>
			canonicalizeLocalPlacementSnapshotV1(base, {
				limits: { maxOwners: 2 },
			}),
		).to.throw("owners");
		expect(() =>
			canonicalizeLocalPlacementSnapshotV1(base, {
				limits: { maxRanges: 2 },
			}),
		).to.throw("ranges");
		expect(() =>
			canonicalizeLocalPlacementSnapshotV1(base, {
				limits: { maxRangesPerOwner: 1, maxRanges: 3 },
			}),
		).not.to.throw();
		expect(() =>
			canonicalizeLocalPlacementSnapshotV1(
				{ ...base, ranges: [base.ranges[0]!, base.ranges[0]!] },
				{ limits: { maxRangesPerOwner: 1 } },
			),
		).to.throw("owner range limit");
		expect(() =>
			canonicalizeLocalPlacementSnapshotV1(base, {
				limits: { maxEncodedBytes: 128 },
			}),
		).to.throw("encoded byte limit");
		expect(() =>
			canonicalizeLocalPlacementSnapshotV1(base, {
				limits: { maxPublicKeyBytes: 1 },
			}),
		).to.throw("public key");
	});

	it("requires explicit stable descriptors for domain and policy semantics", () => {
		const base = inputU32(keys);
		expect(() =>
			canonicalizeLocalPlacementSnapshotV1({
				...base,
				domain: { ...base.domain, configId: "custom-closure" },
			}),
		).to.throw("domain config id");
		expect(() =>
			canonicalizeLocalPlacementSnapshotV1({
				...base,
				policy: { ...base.policy, id: "mutable-policy" },
			}),
		).to.throw("policy id");
		expect(() =>
			canonicalizeLocalPlacementSnapshotV1({
				...base,
				planner: { ...base.planner, id: "bad\ud800" },
			}),
		).to.throw("planner id");
		expect(() =>
			canonicalizeLocalPlacementSnapshotV1({
				...base,
				capturedAtMs: BigInt(Number.MAX_SAFE_INTEGER) + 1n,
			}),
		).to.throw("capture timestamp");
	});

	it("creates deterministic per-open execution fences without prune authority", () => {
		const view = canonicalizeLocalPlacementSnapshotV1(inputU32(keys));
		const epoch = new Uint8Array(32).fill(1);
		const first = createLocalPlacementExecutionFence({
			view,
			executionEpoch: epoch,
			ownershipRevision: 3n,
			roleGeneration: 4,
		});
		const replay = createLocalPlacementExecutionFence({
			view,
			executionEpoch: epoch,
			ownershipRevision: 3n,
			roleGeneration: 4,
		});
		const nextOpen = createLocalPlacementExecutionFence({
			view,
			executionEpoch: new Uint8Array(32).fill(2),
			ownershipRevision: 3n,
			roleGeneration: 4,
		});
		expect(replay.fenceId).to.equal(first.fenceId);
		expect(nextOpen.fenceId).to.not.equal(first.fenceId);
		expect(isLocalPlacementExecutionFenceTimeValid(first, 120n)).to.equal(true);
		expect(isLocalPlacementExecutionFenceTimeValid(first, 150n)).to.equal(
			false,
		);
		expect(() =>
			createLocalPlacementExecutionFence({
				view,
				executionEpoch: bytes(1),
				ownershipRevision: 0n,
				roleGeneration: 0,
			}),
		).to.throw("execution epoch");
	});
});
