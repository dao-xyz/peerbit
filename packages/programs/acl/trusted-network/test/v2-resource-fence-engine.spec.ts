import { serialize } from "@dao-xyz/borsh";
import { calculateRawCid } from "@peerbit/blocks-interface";
import { Ed25519Keypair } from "@peerbit/crypto";
import { Entry, EntryV0, LamportClock, Timestamp } from "@peerbit/log";
import { expect } from "chai";
import {
	type AcceptedResourceFencePolicyV2,
	TrustedNetworkV2ResourceFenceReducer,
} from "../src/v2-resource-fence-engine.js";
import {
	NetworkDescriptorV2,
	ResourceFenceV2,
	TRUSTED_NETWORK_V2_ENTRY_V0_AUTHORITY_ONLY_SIGNATURE_PROFILE,
	TRUSTED_NETWORK_V2_POLICY_HASH_SHA256,
	TRUSTED_NETWORK_V2_PROTOCOL_VERSION,
	deriveNetworkIdV2,
} from "../src/v2.js";

const bytes32 = (value: number): Uint8Array => new Uint8Array(32).fill(value);
const ZERO = bytes32(0);
const RESOURCE_ID = bytes32(0x71);
const RESOURCE_GID = "resource-fence-engine";

const keyFor = (bytes: Uint8Array): string => {
	let key = "";
	for (const byte of bytes) key += byte.toString(16).padStart(2, "0");
	return key;
};

type FenceFixture = {
	body: ResourceFenceV2;
	entryBytes: Uint8Array;
	entryCid: string;
	digest: Uint8Array;
};

type Harness = {
	authority: Ed25519Keypair;
	descriptor: NetworkDescriptorV2;
	fencesByDigest: Map<string, Uint8Array>;
	entriesByCid: Map<string, Uint8Array>;
	reducer: () => TrustedNetworkV2ResourceFenceReducer;
};

const createHarness = async (): Promise<Harness> => {
	const authority = await Ed25519Keypair.create();
	const descriptor = new NetworkDescriptorV2({
		protocolVersion: TRUSTED_NETWORK_V2_PROTOCOL_VERSION,
		networkNonce: bytes32(0x31),
		policyAuthority: authority.publicKey,
		genesisPolicyDigest: bytes32(0x41),
		policyHashProfile: TRUSTED_NETWORK_V2_POLICY_HASH_SHA256,
		entrySignatureProfile:
			TRUSTED_NETWORK_V2_ENTRY_V0_AUTHORITY_ONLY_SIGNATURE_PROFILE,
	});
	const fencesByDigest = new Map<string, Uint8Array>();
	const entriesByCid = new Map<string, Uint8Array>();
	return {
		authority,
		descriptor,
		fencesByDigest,
		entriesByCid,
		reducer: () =>
			new TrustedNetworkV2ResourceFenceReducer({
				descriptor,
				expectedResourceId: RESOURCE_ID,
				expectedGid: RESOURCE_GID,
				resolveFenceEntry: async (digest) => fencesByDigest.get(keyFor(digest)),
				resolveEntryV0: async (cids) =>
					new Map(cids.map((cid) => [cid, entriesByCid.get(cid)] as const)),
			}),
	};
};

const fenceBody = (
	descriptor: NetworkDescriptorV2,
	properties: {
		sequence: bigint;
		previousDigest?: Uint8Array;
		contentEpoch?: bigint;
		manifest?: number;
	},
): ResourceFenceV2 =>
	new ResourceFenceV2({
		networkId: deriveNetworkIdV2(descriptor),
		resourceId: RESOURCE_ID,
		fenceSequence: properties.sequence,
		previousFenceDigest: properties.previousDigest ?? ZERO,
		policySequence: 0n,
		policyDigest: descriptor.genesisPolicyDigest,
		contentEpoch: properties.contentEpoch ?? properties.sequence,
		epochManifestDigest: bytes32(properties.manifest ?? 0x51),
	});

const createFence = async (
	harness: Harness,
	body: ResourceFenceV2,
	parentCids: string[] = [],
): Promise<FenceFixture> => {
	const parentClock = new LamportClock({
		id: harness.authority.publicKey.bytes,
		timestamp: new Timestamp({ wallTime: 1n }),
	});
	const entry = (await EntryV0.create({
		store: {} as never,
		data: serialize(body),
		identity: harness.authority,
		deferStore: true,
		meta: {
			gid: parentCids.length === 0 ? RESOURCE_GID : undefined,
			next: parentCids.map((hash) => ({
				hash,
				meta: { gid: RESOURCE_GID, clock: parentClock },
			})) as never,
		},
	})) as EntryV0<Uint8Array>;
	const stored = Entry.getPreparedStorageBytes(entry);
	if (stored === undefined) throw new Error("Fixture has no prepared bytes");
	const entryBytes = new Uint8Array(stored);
	const prepared = await calculateRawCid(entryBytes);
	const fixture: FenceFixture = {
		body,
		entryBytes,
		entryCid: prepared.cid,
		digest: new Uint8Array(prepared.block.cid.multihash.digest),
	};
	harness.fencesByDigest.set(keyFor(fixture.digest), fixture.entryBytes);
	harness.entriesByCid.set(fixture.entryCid, fixture.entryBytes);
	return fixture;
};

const createCausalEntry = async (
	harness: Harness,
	parentCids: string[],
): Promise<{ entryBytes: Uint8Array; entryCid: string }> => {
	const parentClock = new LamportClock({
		id: harness.authority.publicKey.bytes,
		timestamp: new Timestamp({ wallTime: 1n }),
	});
	const entry = (await EntryV0.create({
		store: {} as never,
		data: Uint8Array.of(1, 2, 3),
		identity: harness.authority,
		deferStore: true,
		meta: {
			next: parentCids.map((hash) => ({
				hash,
				meta: { gid: RESOURCE_GID, clock: parentClock },
			})) as never,
			gid: parentCids.length === 0 ? RESOURCE_GID : undefined,
		},
	})) as EntryV0<Uint8Array>;
	const stored = Entry.getPreparedStorageBytes(entry);
	if (stored === undefined) throw new Error("Fixture has no prepared bytes");
	const entryBytes = new Uint8Array(stored);
	const prepared = await calculateRawCid(entryBytes);
	harness.entriesByCid.set(prepared.cid, entryBytes);
	return { entryBytes, entryCid: prepared.cid };
};

const acceptedPolicyFor = (
	fixture: FenceFixture,
): AcceptedResourceFencePolicyV2 => ({
	sequence: fixture.body.policySequence,
	digest: fixture.body.policyDigest,
});

const admit = async (
	reducer: TrustedNetworkV2ResourceFenceReducer,
	fixture: FenceFixture,
) => {
	const preparation = await reducer.prepare(fixture.entryBytes);
	if (preparation.status !== "prepared") {
		throw new Error(preparation.reason);
	}
	return reducer.ingestPrepared(
		preparation.candidate,
		acceptedPolicyFor(fixture),
	);
};

describe("TrustedNetwork v2 resource-fence reducer", () => {
	it("accepts an initial fence with an explicit empty signed frontier", async () => {
		const harness = await createHarness();
		const initial = await createFence(
			harness,
			fenceBody(harness.descriptor, { sequence: 0n }),
		);
		const reducer = harness.reducer();

		const result = await admit(reducer, initial);

		expect(result.status).to.equal("accepted");
		expect(reducer.state).to.equal("ACTIVE");
		expect(reducer.head?.sequence).to.equal(0n);
		const firstRead = reducer.head;
		const secondRead = reducer.head;
		expect(firstRead?.causalFrontier).to.deep.equal([]);
		expect(firstRead?.causalFrontier).not.to.equal(secondRead?.causalFrontier);
		firstRead!.digest[0] = 0xff;
		expect(reducer.head?.digest).to.deep.equal(initial.digest);
	});

	it("advances only through an exact policy-bound causal successor", async () => {
		const harness = await createHarness();
		const initial = await createFence(
			harness,
			fenceBody(harness.descriptor, { sequence: 0n }),
		);
		const next = await createFence(
			harness,
			fenceBody(harness.descriptor, {
				sequence: 1n,
				previousDigest: initial.digest,
			}),
			[initial.entryCid],
		);
		const reducer = harness.reducer();
		expect((await admit(reducer, initial)).status).to.equal("accepted");

		const result = await admit(reducer, next);

		expect(result.status).to.equal("accepted");
		expect(reducer.head?.sequence).to.equal(1n);
		expect(reducer.head?.previousFenceDigest).to.deep.equal(initial.digest);
		expect(reducer.head?.causalFrontier.map(({ cid }) => cid)).to.deep.equal([
			initial.entryCid,
		]);
	});

	it("does not roll back when an accepted ancestor is replayed", async () => {
		const harness = await createHarness();
		const initial = await createFence(
			harness,
			fenceBody(harness.descriptor, { sequence: 0n }),
		);
		const next = await createFence(
			harness,
			fenceBody(harness.descriptor, {
				sequence: 1n,
				previousDigest: initial.digest,
			}),
			[initial.entryCid],
		);
		const reducer = harness.reducer();
		await admit(reducer, initial);
		await admit(reducer, next);

		const replay = await admit(reducer, initial);

		expect(replay.status).to.equal("duplicate");
		expect(reducer.head?.entryCid).to.equal(next.entryCid);
	});

	it("persists deterministic evidence for a late direct sibling", async () => {
		const harness = await createHarness();
		const initial = await createFence(
			harness,
			fenceBody(harness.descriptor, { sequence: 0n }),
		);
		const first = await createFence(
			harness,
			fenceBody(harness.descriptor, {
				sequence: 1n,
				previousDigest: initial.digest,
				manifest: 0x52,
			}),
			[initial.entryCid],
		);
		const sibling = await createFence(
			harness,
			fenceBody(harness.descriptor, {
				sequence: 1n,
				previousDigest: initial.digest,
				manifest: 0x53,
			}),
			[initial.entryCid],
		);
		const reducer = harness.reducer();
		await admit(reducer, initial);
		await admit(reducer, first);

		const result = await admit(reducer, sibling);

		expect(result.status).to.equal("forked");
		expect(reducer.state).to.equal("FORKED");
		expect(reducer.head?.entryCid).to.equal(initial.entryCid);
		expect(
			reducer.forkEvidence?.children.map(({ entryCid }) => entryCid),
		).to.have.members([first.entryCid, sibling.entryCid]);
		const durable = reducer.exportDurableState();
		expect(durable.state).to.equal("FORKED");
	});

	it("fails closed when the accepted predecessor is not reachable", async () => {
		const harness = await createHarness();
		const initial = await createFence(
			harness,
			fenceBody(harness.descriptor, { sequence: 0n }),
		);
		const missing = await calculateRawCid(Uint8Array.of(9, 8, 7));
		const next = await createFence(
			harness,
			fenceBody(harness.descriptor, {
				sequence: 1n,
				previousDigest: initial.digest,
			}),
			[missing.cid],
		);
		const reducer = harness.reducer();
		await admit(reducer, initial);

		const result = await admit(reducer, next);

		expect(result.status).to.equal("unavailable");
		expect(reducer.state).to.equal("UNAVAILABLE");
		expect(result.fetchHints).to.deep.include({
			kind: "causal-entry",
			cid: missing.cid,
		});
		expect(reducer.exportDurableState().state).to.equal("UNAVAILABLE");
	});

	it("bounds a transitive causal traversal before accepting a successor", async () => {
		const harness = await createHarness();
		const initial = await createFence(
			harness,
			fenceBody(harness.descriptor, { sequence: 0n }),
		);
		const intermediate = await createCausalEntry(harness, [initial.entryCid]);
		const next = await createFence(
			harness,
			fenceBody(harness.descriptor, {
				sequence: 1n,
				previousDigest: initial.digest,
			}),
			[intermediate.entryCid],
		);
		const reducer = new TrustedNetworkV2ResourceFenceReducer({
			descriptor: harness.descriptor,
			expectedResourceId: RESOURCE_ID,
			expectedGid: RESOURCE_GID,
			resolveFenceEntry: async (digest) =>
				harness.fencesByDigest.get(keyFor(digest)),
			resolveEntryV0: async (cids) =>
				new Map(
					cids.map((cid) => [cid, harness.entriesByCid.get(cid)] as const),
				),
			causalLimits: {
				maxEntryBytes: 8 * 1024,
				maxDirectParents: 64,
				maxVisitedEntries: 1,
				maxTotalBytes: 16 * 1024,
				maxParentLinks: 64,
				maxResolveBatchSize: 1,
			},
		});
		await admit(reducer, initial);

		const result = await admit(reducer, next);

		expect(result.status).to.equal("unavailable");
		expect(result.reason).to.contain("capacity");
	});

	it("bounds a hanging causal resolver with a timeout", async () => {
		const harness = await createHarness();
		const initial = await createFence(
			harness,
			fenceBody(harness.descriptor, { sequence: 0n }),
		);
		const missing = await calculateRawCid(Uint8Array.of(6, 5, 4));
		const next = await createFence(
			harness,
			fenceBody(harness.descriptor, {
				sequence: 1n,
				previousDigest: initial.digest,
			}),
			[missing.cid],
		);
		const reducer = new TrustedNetworkV2ResourceFenceReducer({
			descriptor: harness.descriptor,
			expectedResourceId: RESOURCE_ID,
			expectedGid: RESOURCE_GID,
			resolveFenceEntry: async (digest) =>
				harness.fencesByDigest.get(keyFor(digest)),
			resolveEntryV0: async (_cids, { signal }) =>
				new Promise((resolve) => {
					signal.addEventListener("abort", () => resolve(new Map()), {
						once: true,
					});
				}),
			causalTimeoutMs: 5,
		});
		await admit(reducer, initial);

		const result = await admit(reducer, next);

		expect(result.status).to.equal("unavailable");
		expect(result.reason).to.contain("timed out");
	});

	it("applies one operation deadline across a signal-ignoring causal resolver", async () => {
		const harness = await createHarness();
		const initial = await createFence(
			harness,
			fenceBody(harness.descriptor, { sequence: 0n }),
		);
		const missing = await calculateRawCid(Uint8Array.of(8, 7, 6));
		const next = await createFence(
			harness,
			fenceBody(harness.descriptor, {
				sequence: 1n,
				previousDigest: initial.digest,
			}),
			[missing.cid],
		);
		let resolverSignal: AbortSignal | undefined;
		let settleResolver: (
			value: ReadonlyMap<string, Uint8Array | undefined>,
		) => void = () => {};
		const delayedResolution = new Promise<
			ReadonlyMap<string, Uint8Array | undefined>
		>((resolve) => {
			settleResolver = resolve;
		});
		const reducer = new TrustedNetworkV2ResourceFenceReducer({
			descriptor: harness.descriptor,
			expectedResourceId: RESOURCE_ID,
			expectedGid: RESOURCE_GID,
			resolveFenceEntry: async (digest) =>
				harness.fencesByDigest.get(keyFor(digest)),
			resolveEntryV0: async (_cids, { signal }) => {
				resolverSignal = signal;
				return delayedResolution;
			},
			causalTimeoutMs: 1_000,
			operationTimeoutMs: 10,
		});
		await admit(reducer, initial);

		const started = Date.now();
		const result = await admit(reducer, next);

		expect(result.status).to.equal("unavailable");
		expect(Date.now() - started).to.be.lessThan(500);
		expect(resolverSignal?.aborted).to.equal(true);
		expect(reducer.head?.entryCid).to.equal(initial.entryCid);
		settleResolver(new Map());
		await Promise.resolve();
		await Promise.resolve();
		expect(reducer.head?.entryCid).to.equal(initial.entryCid);
	});

	it("cancels an in-flight causal check without publishing", async () => {
		const harness = await createHarness();
		const initial = await createFence(
			harness,
			fenceBody(harness.descriptor, { sequence: 0n }),
		);
		const missing = await calculateRawCid(Uint8Array.of(3, 4, 5));
		const next = await createFence(
			harness,
			fenceBody(harness.descriptor, {
				sequence: 1n,
				previousDigest: initial.digest,
			}),
			[missing.cid],
		);
		const lifecycle = new AbortController();
		const reducer = new TrustedNetworkV2ResourceFenceReducer({
			descriptor: harness.descriptor,
			expectedResourceId: RESOURCE_ID,
			expectedGid: RESOURCE_GID,
			resolveFenceEntry: async (digest) =>
				harness.fencesByDigest.get(keyFor(digest)),
			resolveEntryV0: async (_cids, { signal }) =>
				new Promise((resolve) => {
					signal.addEventListener("abort", () => resolve(new Map()), {
						once: true,
					});
				}),
			signal: lifecycle.signal,
		});
		await admit(reducer, initial);
		const preparation = await reducer.prepare(next.entryBytes);
		if (preparation.status !== "prepared") throw new Error(preparation.reason);

		const admission = reducer.ingestPrepared(
			preparation.candidate,
			acceptedPolicyFor(next),
		);
		queueMicrotask(() => lifecycle.abort());
		const result = await admission;

		expect(result.status).to.equal("halted");
		expect(reducer.state).to.equal("HALTED");
		expect(reducer.head?.entryCid).to.equal(initial.entryCid);
	});

	it("restores and reauthenticates an active head under accepted policies", async () => {
		const harness = await createHarness();
		const initial = await createFence(
			harness,
			fenceBody(harness.descriptor, { sequence: 0n }),
		);
		const next = await createFence(
			harness,
			fenceBody(harness.descriptor, {
				sequence: 1n,
				previousDigest: initial.digest,
			}),
			[initial.entryCid],
		);
		const reducer = harness.reducer();
		await admit(reducer, initial);
		await admit(reducer, next);
		const durableState = reducer.exportDurableState();

		const restored = await TrustedNetworkV2ResourceFenceReducer.restore({
			descriptor: harness.descriptor,
			expectedResourceId: RESOURCE_ID,
			expectedGid: RESOURCE_GID,
			resolveFenceEntry: async (digest) =>
				harness.fencesByDigest.get(keyFor(digest)),
			resolveEntryV0: async (cids) =>
				new Map(
					cids.map((cid) => [cid, harness.entriesByCid.get(cid)] as const),
				),
			durableState,
			isAcceptedPolicy: async (reference) =>
				reference.sequence === 0n &&
				keyFor(reference.digest) ===
					keyFor(harness.descriptor.genesisPolicyDigest),
		});

		expect(restored.state).to.equal("ACTIVE");
		expect(restored.head?.entryCid).to.equal(next.entryCid);
		expect(restored.head?.causalFrontier.map(({ cid }) => cid)).to.deep.equal([
			initial.entryCid,
		]);
	});

	it("rejects a durable head that is not causally linked to its predecessor", async () => {
		const harness = await createHarness();
		const initial = await createFence(
			harness,
			fenceBody(harness.descriptor, { sequence: 0n }),
		);
		const linked = await createFence(
			harness,
			fenceBody(harness.descriptor, {
				sequence: 1n,
				previousDigest: initial.digest,
				manifest: 0x71,
			}),
			[initial.entryCid],
		);
		const unrelated = await calculateRawCid(Uint8Array.of(0xfa, 0xfb));
		const noncausal = await createFence(
			harness,
			fenceBody(harness.descriptor, {
				sequence: 1n,
				previousDigest: initial.digest,
				manifest: 0x72,
			}),
			[unrelated.cid],
		);
		const reducer = harness.reducer();
		await admit(reducer, initial);
		await admit(reducer, linked);
		const durable = reducer.exportDurableState();
		if (durable.state !== "ACTIVE") throw new Error("Expected active state");

		let error: unknown;
		try {
			await TrustedNetworkV2ResourceFenceReducer.restore({
				descriptor: harness.descriptor,
				expectedResourceId: RESOURCE_ID,
				expectedGid: RESOURCE_GID,
				resolveFenceEntry: async (digest) =>
					harness.fencesByDigest.get(keyFor(digest)),
				resolveEntryV0: async (cids) =>
					new Map(
						cids.map((cid) => [cid, harness.entriesByCid.get(cid)] as const),
					),
				durableState: {
					...durable,
					acceptedHeadEntryBytes: noncausal.entryBytes,
				},
				isAcceptedPolicy: async () => true,
			});
		} catch (cause) {
			error = cause;
		}
		expect(error).to.be.instanceOf(Error);
		expect((error as Error).message).to.contain("not causally linked");
	});

	it("retains an out-of-order candidate for an exact targeted retry", async () => {
		const harness = await createHarness();
		const initial = await createFence(
			harness,
			fenceBody(harness.descriptor, { sequence: 0n }),
		);
		const next = await createFence(
			harness,
			fenceBody(harness.descriptor, {
				sequence: 1n,
				previousDigest: initial.digest,
			}),
			[initial.entryCid],
		);
		const reducer = harness.reducer();
		harness.fencesByDigest.delete(keyFor(initial.digest));

		expect((await admit(reducer, next)).status).to.equal("pending");
		expect((await admit(reducer, initial)).status).to.equal("accepted");
		const retried = await reducer.retryPending(
			next.entryCid,
			acceptedPolicyFor(next),
		);

		expect(retried.status).to.equal("accepted");
		expect(reducer.head?.entryCid).to.equal(next.entryCid);
		expect(reducer.pendingCount).to.equal(0);
	});

	it("never promotes an unleased resolver-loaded candidate ancestor", async () => {
		const harness = await createHarness();
		const acceptedInitial = await createFence(
			harness,
			fenceBody(harness.descriptor, { sequence: 0n, manifest: 0x61 }),
		);
		const acceptedNext = await createFence(
			harness,
			fenceBody(harness.descriptor, {
				sequence: 1n,
				previousDigest: acceptedInitial.digest,
				manifest: 0x62,
			}),
			[acceptedInitial.entryCid],
		);
		const alternateInitial = await createFence(
			harness,
			fenceBody(harness.descriptor, { sequence: 0n, manifest: 0x63 }),
		);
		const alternateNext = await createFence(
			harness,
			fenceBody(harness.descriptor, {
				sequence: 1n,
				previousDigest: alternateInitial.digest,
				manifest: 0x64,
			}),
			[alternateInitial.entryCid],
		);
		const reducer = harness.reducer();
		await admit(reducer, acceptedInitial);
		await admit(reducer, acceptedNext);

		const descendantOnly = await admit(reducer, alternateNext);

		expect(descendantOnly.status).to.equal("pending");
		expect(reducer.state).to.equal("ACTIVE");
		expect(reducer.forkEvidence).to.equal(undefined);
		expect((await admit(reducer, alternateInitial)).status).to.equal("forked");
	});

	it("rejects a policy mismatch and a token prepared by another reducer", async () => {
		const harness = await createHarness();
		const initial = await createFence(
			harness,
			fenceBody(harness.descriptor, { sequence: 0n }),
		);
		const first = harness.reducer();
		const second = harness.reducer();
		const preparation = await first.prepare(initial.entryBytes);
		if (preparation.status !== "prepared") throw new Error(preparation.reason);

		const wrongPolicy = await first.ingestPrepared(preparation.candidate, {
			sequence: 0n,
			digest: bytes32(0xee),
		});
		const wrongReducer = await second.ingestPrepared(
			preparation.candidate,
			acceptedPolicyFor(initial),
		);

		expect(wrongPolicy.status).to.equal("rejected");
		expect(first.state).to.equal("EMPTY");
		expect(wrongReducer.status).to.equal("rejected");
		expect(second.state).to.equal("EMPTY");
		expect(
			(
				await first.ingestPrepared(
					preparation.candidate,
					acceptedPolicyFor(initial),
				)
			).status,
		).to.equal("accepted");
	});
});
