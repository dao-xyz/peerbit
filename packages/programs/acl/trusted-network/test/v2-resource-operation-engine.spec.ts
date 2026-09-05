import { serialize } from "@dao-xyz/borsh";
import type { CrashSafeAtomicReplaceStore } from "@peerbit/any-store-interface";
import { calculateRawCid } from "@peerbit/blocks-interface";
import { Ed25519Keypair } from "@peerbit/crypto";
import {
	type BoundedEntryV0CausalReachabilityLimits,
	Entry,
	EntryV0,
	LamportClock,
	Timestamp,
} from "@peerbit/log";
import { expect } from "chai";
import { compare } from "uint8arrays";
import type {
	AcceptedPolicyLeaseV2,
	PolicyLeaseReferenceV2,
	PolicyLeaseResultV2,
} from "../src/v2-policy-anchor.js";
import { TrustedNetworkV2DurablePolicyReducer } from "../src/v2-policy-anchor.js";
import {
	type CrashSafeResourceFenceAnchorStoreV2,
	type ResourceFencePolicyAnchorV2,
	TrustedNetworkV2DurableResourceFenceReducer,
} from "../src/v2-resource-fence-anchor.js";
import {
	ResourceCausalWorkBudgetV2,
	type ResourceCausalWorkLimitsV2,
} from "../src/v2-resource-fence-engine.js";
import {
	TRUSTED_NETWORK_V2_MAX_RESOURCE_OPERATION_CAUSAL_BYTES,
	TRUSTED_NETWORK_V2_MAX_RESOURCE_OPERATION_CAUSAL_ENTRIES,
	TRUSTED_NETWORK_V2_MAX_RESOURCE_OPERATION_CAUSAL_LINKS,
	TrustedNetworkV2ResourceOperationEngine,
} from "../src/v2-resource-operation-engine.js";
import {
	ResourceOperationEnvelopeV2,
	TRUSTED_NETWORK_V2_RESOURCE_OPERATION_PROFILE,
} from "../src/v2-resource-operation-entry.js";
import {
	NetworkDescriptorV2,
	OperationPolicyProofV2,
	PolicySnapshotBodyV2,
	PolicySubjectBindingV2,
	ResourceFenceV2,
	TRUSTED_NETWORK_V2_ENTRY_V0_AUTHORITY_ONLY_SIGNATURE_PROFILE,
	TRUSTED_NETWORK_V2_POLICY_HASH_SHA256,
	TRUSTED_NETWORK_V2_PROTOCOL_VERSION,
	TrustedNetworkRole,
	deriveNetworkIdV2,
	digestPolicySnapshotBodyV2,
} from "../src/v2.js";

const ZERO = new Uint8Array(32);
const RESOURCE_ID = new Uint8Array(32).fill(0x71);
const RESOURCE_GID = "resource-operation-engine";
const bytes32 = (value: number): Uint8Array => new Uint8Array(32).fill(value);
const hex = (value: Uint8Array): string => Buffer.from(value).toString("hex");
const causalLimits: BoundedEntryV0CausalReachabilityLimits = {
	maxEntryBytes: 128 * 1024,
	maxDirectParents: 64,
	maxVisitedEntries: TRUSTED_NETWORK_V2_MAX_RESOURCE_OPERATION_CAUSAL_ENTRIES,
	maxTotalBytes: TRUSTED_NETWORK_V2_MAX_RESOURCE_OPERATION_CAUSAL_BYTES,
	maxParentLinks: TRUSTED_NETWORK_V2_MAX_RESOURCE_OPERATION_CAUSAL_LINKS,
	maxResolveBatchSize: 64,
};

const deferred = () => {
	let resolve!: () => void;
	const promise = new Promise<void>((settle) => {
		resolve = settle;
	});
	return { promise, resolve };
};

class MemoryAnchorStore implements CrashSafeAtomicReplaceStore {
	readonly values: Map<string, Uint8Array>;
	private opened = true;

	constructor(values?: Map<string, Uint8Array>) {
		this.values = new Map(
			[...(values ?? [])].map(([key, value]) => [key, Uint8Array.from(value)]),
		);
	}

	readonly crashSafeDurability = {
		crashSafe: true as const,
		barrier: async (): Promise<void> => {},
		atomicReplace: async (key: string, value: Uint8Array): Promise<void> => {
			this.values.set(key, Uint8Array.from(value));
		},
	};

	status(): "open" | "closed" {
		return this.opened ? "open" : "closed";
	}

	open(): void {
		this.opened = true;
	}

	close(): void {
		this.opened = false;
	}

	get(key: string): Uint8Array | undefined {
		const value = this.values.get(key);
		return value === undefined ? undefined : Uint8Array.from(value);
	}

	put(key: string, value: Uint8Array): void {
		this.values.set(key, Uint8Array.from(value));
	}

	del(key: string): void {
		this.values.delete(key);
	}

	sublevel(): MemoryAnchorStore {
		return this;
	}

	async *iterator(): AsyncGenerator<[string, Uint8Array], void, void> {
		for (const [key, value] of this.values) {
			yield [key, Uint8Array.from(value)];
		}
	}

	clear(): void {
		this.values.clear();
	}

	size(): number {
		let size = 0;
		for (const value of this.values.values()) size += value.byteLength;
		return size;
	}

	persisted(): boolean {
		return true;
	}

	clone(): MemoryAnchorStore {
		return new MemoryAnchorStore(this.values);
	}
}

class HistoricalPolicyLease {
	active = 0;
	constructor(readonly anchor: TrustedNetworkV2DurablePolicyReducer) {}

	async withAcceptedPolicyLease<T>(
		reference: PolicyLeaseReferenceV2,
		use: (lease: AcceptedPolicyLeaseV2) => T | Promise<T>,
	): Promise<PolicyLeaseResultV2<T>> {
		return this.anchor.withAcceptedPolicyLease(reference, async (lease) => {
			this.active += 1;
			try {
				return await use(lease);
			} finally {
				this.active -= 1;
			}
		});
	}

	isUsable(): boolean {
		return this.anchor.isUsable();
	}

	asAnchor(): ResourceFencePolicyAnchorV2 {
		return this as unknown as ResourceFencePolicyAnchorV2;
	}
}

type SignedEntry = {
	entry: EntryV0<Uint8Array>;
	bytes: Uint8Array;
	cid: string;
	digest: Uint8Array;
};

type TestContext = {
	authority: Ed25519Keypair;
	writer: Ed25519Keypair;
	descriptor: NetworkDescriptorV2;
	policyLease: HistoricalPolicyLease;
	policy0: AcceptedPolicyLeaseV2["policy"];
	policy1: AcceptedPolicyLeaseV2["policy"];
	policy2: AcceptedPolicyLeaseV2["policy"];
	fencesByDigest: Map<string, Uint8Array>;
	entriesByCid: Map<string, Uint8Array>;
	store: MemoryAnchorStore;
	policyStore: MemoryAnchorStore;
	policyEntries: Map<string, Uint8Array>;
};

const createContext = async (): Promise<TestContext> => {
	const authority = await Ed25519Keypair.create();
	const writer = await Ed25519Keypair.create();
	const descriptor = new NetworkDescriptorV2({
		protocolVersion: TRUSTED_NETWORK_V2_PROTOCOL_VERSION,
		networkNonce: bytes32(0x31),
		policyAuthority: authority.publicKey,
		genesisPolicyDigest: bytes32(0x41),
		policyHashProfile: TRUSTED_NETWORK_V2_POLICY_HASH_SHA256,
		entrySignatureProfile:
			TRUSTED_NETWORK_V2_ENTRY_V0_AUTHORITY_ONLY_SIGNATURE_PROFILE,
	});
	const writerBinding = new PolicySubjectBindingV2({
		signingKey: writer.publicKey,
		roles: TrustedNetworkRole.WRITER,
	});
	const authorityBinding = new PolicySubjectBindingV2({
		signingKey: authority.publicKey,
		roles: TrustedNetworkRole.ADMIN,
	});
	const bodies: PolicySnapshotBodyV2[] = [];
	const policies: AcceptedPolicyLeaseV2["policy"][] = [];
	for (let sequence = 0; sequence < 3; sequence++) {
		const bindings = (
			sequence === 1 ? [authorityBinding] : [authorityBinding, writerBinding]
		).sort((left, right) =>
			compare(serialize(left.signingKey), serialize(right.signingKey)),
		);
		const body = new PolicySnapshotBodyV2({
			networkId: deriveNetworkIdV2(descriptor),
			sequence: BigInt(sequence),
			previousPolicyDigest:
				sequence === 0 ? ZERO : policies[sequence - 1]!.digest,
			bindings,
		});
		bodies.push(body);
		policies.push({
			sequence: body.sequence,
			digest: digestPolicySnapshotBodyV2(body),
			bindings,
		});
	}
	const [policy0, policy1, policy2] = policies;
	descriptor.genesisPolicyDigest = policy0!.digest;
	const policyEntries = new Map<string, Uint8Array>();
	const policyStore = new MemoryAnchorStore();
	const durablePolicy = await TrustedNetworkV2DurablePolicyReducer.open({
		descriptor,
		store: policyStore,
		resolvePolicyEntry: (digest) => policyEntries.get(hex(digest)),
	});
	for (const body of bodies) {
		const entry = await EntryV0.create({
			store: {} as never,
			data: serialize(body),
			identity: authority,
			deferStore: true,
		});
		const bytes = Entry.getPreparedStorageBytes(entry)!;
		policyEntries.set(hex(digestPolicySnapshotBodyV2(body)), bytes);
		expect((await durablePolicy.ingest(bytes)).status).to.equal("accepted");
	}
	const policyLease = new HistoricalPolicyLease(durablePolicy);
	return {
		authority,
		writer,
		descriptor,
		policyLease,
		policy0,
		policy1,
		policy2,
		fencesByDigest: new Map(),
		entriesByCid: new Map(),
		store: new MemoryAnchorStore(),
		policyStore,
		policyEntries,
	};
};

const preparedEntry = async (
	context: TestContext,
	entry: EntryV0<Uint8Array>,
): Promise<SignedEntry> => {
	const stored = Entry.getPreparedStorageBytes(entry);
	if (stored === undefined) throw new Error("Fixture has no prepared bytes");
	const bytes = new Uint8Array(stored);
	const calculated = await calculateRawCid(bytes);
	const signed = {
		entry,
		bytes,
		cid: calculated.cid,
		digest: new Uint8Array(calculated.block.cid.multihash.digest),
	};
	context.entriesByCid.set(signed.cid, signed.bytes);
	return signed;
};

const createFence = async (
	context: TestContext,
	properties: {
		sequence: bigint;
		previousDigest?: Uint8Array;
		policy: AcceptedPolicyLeaseV2["policy"];
		parents?: SignedEntry[];
		manifestByte?: number;
	},
): Promise<SignedEntry> => {
	const parents = properties.parents ?? [];
	const parentClock = new LamportClock({
		id: context.authority.publicKey.bytes,
		timestamp: new Timestamp({ wallTime: 1n }),
	});
	const body = new ResourceFenceV2({
		networkId: deriveNetworkIdV2(context.descriptor),
		resourceId: RESOURCE_ID,
		fenceSequence: properties.sequence,
		previousFenceDigest: properties.previousDigest ?? ZERO,
		policySequence: properties.policy.sequence,
		policyDigest: properties.policy.digest,
		contentEpoch: properties.sequence,
		epochManifestDigest: bytes32(properties.manifestByte ?? 0x51),
	});
	const entry = (await EntryV0.create({
		store: {} as never,
		data: serialize(body),
		identity: context.authority,
		deferStore: true,
		meta: {
			gid: parents.length === 0 ? RESOURCE_GID : undefined,
			next: parents.map((parent) => ({
				hash: parent.cid,
				meta: { gid: RESOURCE_GID, clock: parentClock },
			})) as never,
		},
	})) as EntryV0<Uint8Array>;
	const signed = await preparedEntry(context, entry);
	context.fencesByDigest.set(hex(signed.digest), signed.bytes);
	return signed;
};

const createOperation = async (
	context: TestContext,
	properties: {
		fence: SignedEntry;
		policy: AcceptedPolicyLeaseV2["policy"];
		contentEpoch: bigint;
		manifestByte: number;
		parents: SignedEntry[];
		payloadByte?: number;
	},
): Promise<SignedEntry> => {
	const parentClock = new LamportClock({
		id: context.writer.publicKey.bytes,
		timestamp: new Timestamp({ wallTime: 2n }),
	});
	const envelope = new ResourceOperationEnvelopeV2({
		profile: TRUSTED_NETWORK_V2_RESOURCE_OPERATION_PROFILE,
		policy: new OperationPolicyProofV2({
			networkId: deriveNetworkIdV2(context.descriptor),
			resourceId: RESOURCE_ID,
			policySequence: properties.policy.sequence,
			policyDigest: properties.policy.digest,
			fenceDigest: properties.fence.digest,
			contentEpoch: properties.contentEpoch,
		}),
		epochManifestDigest: bytes32(properties.manifestByte),
		applicationPayload: Uint8Array.of(properties.payloadByte ?? 7),
	});
	const entry = (await EntryV0.create({
		store: {} as never,
		data: serialize(envelope),
		identity: context.writer,
		deferStore: true,
		meta: {
			next: properties.parents.map((parent) => ({
				hash: parent.cid,
				meta: { gid: RESOURCE_GID, clock: parentClock },
			})) as never,
		},
	})) as EntryV0<Uint8Array>;
	return preparedEntry(context, entry);
};

const openAnchor = (
	context: TestContext,
	store: CrashSafeResourceFenceAnchorStoreV2 = context.store,
	fenceCausalLimits?: BoundedEntryV0CausalReachabilityLimits,
): Promise<TrustedNetworkV2DurableResourceFenceReducer> =>
	TrustedNetworkV2DurableResourceFenceReducer.open({
		descriptor: context.descriptor,
		expectedResourceId: RESOURCE_ID,
		expectedGid: RESOURCE_GID,
		policyAnchor: context.policyLease.asAnchor(),
		store,
		causalLimits: fenceCausalLimits,
		resolveFenceEntry: async (digest) =>
			context.fencesByDigest.get(hex(digest)),
		resolveEntryV0: async (cids) =>
			new Map(cids.map((cid) => [cid, context.entriesByCid.get(cid)])),
	});

const operationEngine = (
	context: TestContext,
	anchor: TrustedNetworkV2DurableResourceFenceReducer,
	causalWorkLimits?: Partial<ResourceCausalWorkLimitsV2>,
): TrustedNetworkV2ResourceOperationEngine =>
	new TrustedNetworkV2ResourceOperationEngine({
		descriptor: context.descriptor,
		expectedResourceId: RESOURCE_ID,
		expectedGid: RESOURCE_GID,
		fenceAnchor: anchor,
		causalWorkLimits,
		resolveEntryV0: async (cids) =>
			new Map(cids.map((cid) => [cid, context.entriesByCid.get(cid)])),
	});

const chainFixture = async (context: TestContext) => {
	const fence0 = await createFence(context, {
		sequence: 0n,
		policy: context.policy0,
		manifestByte: 0x51,
	});
	const before = await createOperation(context, {
		fence: fence0,
		policy: context.policy0,
		contentEpoch: 0n,
		manifestByte: 0x51,
		parents: [fence0],
		payloadByte: 1,
	});
	const concurrent = await createOperation(context, {
		fence: fence0,
		policy: context.policy0,
		contentEpoch: 0n,
		manifestByte: 0x51,
		parents: [fence0],
		payloadByte: 2,
	});
	const fence1 = await createFence(context, {
		sequence: 1n,
		previousDigest: fence0.digest,
		policy: context.policy1,
		parents: [before],
		manifestByte: 0x52,
	});
	const after = await createOperation(context, {
		fence: fence0,
		policy: context.policy0,
		contentEpoch: 0n,
		manifestByte: 0x51,
		parents: [fence1],
		payloadByte: 3,
	});
	const fence2 = await createFence(context, {
		sequence: 2n,
		previousDigest: fence1.digest,
		policy: context.policy2,
		parents: [fence1],
		manifestByte: 0x53,
	});
	const regranted = await createOperation(context, {
		fence: fence2,
		policy: context.policy2,
		contentEpoch: 2n,
		manifestByte: 0x53,
		parents: [fence2],
		payloadByte: 4,
	});
	return { fence0, before, concurrent, fence1, after, fence2, regranted };
};

describe("TrustedNetwork v2 resource-operation authorization", () => {
	it("classifies ancestor, concurrent, post-fence, and regrant cases without resurrection", async () => {
		const context = await createContext();
		const chain = await chainFixture(context);
		const anchor = await openAnchor(context);
		expect((await anchor.ingest(chain.fence0.bytes)).status).to.equal(
			"accepted",
		);
		const engine = operationEngine(context, anchor);
		expect((await engine.authorize(chain.concurrent.bytes)).status).to.equal(
			"provisional",
		);
		expect((await anchor.ingest(chain.fence1.bytes)).status).to.equal(
			"accepted",
		);
		expect((await engine.authorize(chain.before.bytes)).status).to.equal(
			"policy-final",
		);
		expect((await engine.authorize(chain.concurrent.bytes)).status).to.equal(
			"rejected",
		);
		expect((await engine.authorize(chain.after.bytes)).status).to.equal(
			"rejected",
		);
		expect((await anchor.ingest(chain.fence2.bytes)).status).to.equal(
			"accepted",
		);
		expect((await engine.authorize(chain.regranted.bytes)).status).to.equal(
			"provisional",
		);
		const wrongManifest = await createOperation(context, {
			fence: chain.fence2,
			policy: context.policy2,
			contentEpoch: 2n,
			manifestByte: 0x54,
			parents: [chain.fence2],
		});
		expect((await engine.authorize(wrongManifest.bytes)).reason).to.contain(
			"another epoch manifest",
		);
		const revokedWriter = await createOperation(context, {
			fence: chain.fence1,
			policy: context.policy1,
			contentEpoch: 1n,
			manifestByte: 0x52,
			parents: [chain.fence1],
		});
		expect((await engine.authorize(revokedWriter.bytes)).reason).to.contain(
			"no verified signer with WRITER",
		);
		// The first closing fence remains the decision boundary after regrant.
		expect((await engine.authorize(chain.concurrent.bytes)).status).to.equal(
			"rejected",
		);
	});

	it("restores the same non-resurrection verdict from both committed checkpoints", async () => {
		const context = await createContext();
		const chain = await chainFixture(context);
		const anchor = await openAnchor(context);
		await anchor.ingest(chain.fence0.bytes);
		await anchor.ingest(chain.fence1.bytes);
		await anchor.ingest(chain.fence2.bytes);
		const persisted = context.store.clone();
		anchor.abort();
		context.policyLease.anchor.abort();
		context.policyLease = new HistoricalPolicyLease(
			await TrustedNetworkV2DurablePolicyReducer.open({
				descriptor: context.descriptor,
				store: context.policyStore.clone(),
				resolvePolicyEntry: (digest) => context.policyEntries.get(hex(digest)),
			}),
		);

		const reopened = await openAnchor(context, persisted);
		const result = await operationEngine(context, reopened).authorize(
			chain.concurrent.bytes,
		);
		expect(reopened.head?.entryCid).to.equal(chain.fence2.cid);
		expect(result.status).to.equal("rejected");
		expect(result.reason).to.contain("concurrent");
	});

	it("keeps an unseen same-policy fence retryable until its exact admission", async () => {
		const context = await createContext();
		const chain = await chainFixture(context);
		const anchor = await openAnchor(context);
		await anchor.ingest(chain.fence0.bytes);
		const fence = await createFence(context, {
			sequence: 1n,
			policy: context.policy0,
			previousDigest: chain.fence0.digest,
			parents: [chain.before],
			manifestByte: 0x51,
		});
		const operation = await createOperation(context, {
			fence,
			policy: context.policy0,
			contentEpoch: 1n,
			manifestByte: 0x51,
			parents: [fence],
		});
		const engine = operationEngine(context, anchor);
		expect((await engine.authorize(operation.bytes)).status).to.equal(
			"unavailable",
		);
		expect((await anchor.ingest(fence.bytes)).status).to.equal("accepted");
		expect((await engine.authorize(operation.bytes)).status).to.equal(
			"provisional",
		);
	});

	it("does not permanently reject an operation when historical policy is missing", async () => {
		const context = await createContext();
		const chain = await chainFixture(context);
		const anchor = await openAnchor(context);
		await anchor.ingest(chain.fence0.bytes);
		await anchor.ingest(chain.fence1.bytes);
		const policyBytes = context.policyEntries.get(hex(context.policy0.digest))!;
		context.policyEntries.delete(hex(context.policy0.digest));
		const engine = operationEngine(context, anchor);
		expect((await engine.authorize(chain.before.bytes)).status).to.equal(
			"unavailable",
		);
		context.policyEntries.set(hex(context.policy0.digest), policyBytes);
		expect((await engine.authorize(chain.before.bytes)).status).to.equal(
			"policy-final",
		);
	});

	it("bounds historical traversal and rejects mismatched policy or epoch commitments", async () => {
		const context = await createContext();
		const chain = await chainFixture(context);
		const anchor = await openAnchor(context);
		await anchor.ingest(chain.fence0.bytes);
		await anchor.ingest(chain.fence1.bytes);
		const engine = new TrustedNetworkV2ResourceOperationEngine({
			descriptor: context.descriptor,
			expectedResourceId: RESOURCE_ID,
			expectedGid: RESOURCE_GID,
			fenceAnchor: anchor,
			resolveEntryV0: (cids) =>
				new Map(cids.map((cid) => [cid, context.entriesByCid.get(cid)])),
			maxFenceSteps: 0,
		});
		expect((await engine.authorize(chain.before.bytes)).status).to.equal(
			"unavailable",
		);
		for (const properties of [
			{ policy: context.policy2, contentEpoch: 1n },
			{ policy: context.policy1, contentEpoch: 2n },
		]) {
			const operation = await createOperation(context, {
				fence: chain.fence1,
				...properties,
				manifestByte: 0x52,
				parents: [chain.fence1],
			});
			expect((await engine.authorize(operation.bytes)).status).to.equal(
				"rejected",
			);
		}
	});

	it("returns bounded dependency hints and succeeds after the exact block arrives", async () => {
		const context = await createContext();
		const chain = await chainFixture(context);
		const anchor = await openAnchor(context);
		await anchor.ingest(chain.fence0.bytes);
		await anchor.ingest(chain.fence1.bytes);
		await anchor.ingest(chain.fence2.bytes);
		const engine = operationEngine(context, anchor);
		context.fencesByDigest.delete(hex(chain.fence0.digest));
		const missingFence = await engine.authorize(chain.concurrent.bytes);
		expect(missingFence.status).to.equal("unavailable");
		expect(missingFence.fetchHints).to.deep.include({
			kind: "resource-fence-predecessor",
			digest: chain.fence0.digest,
		});
		context.fencesByDigest.set(hex(chain.fence0.digest), chain.fence0.bytes);
		const bridge = await createOperation(context, {
			fence: chain.fence2,
			policy: context.policy2,
			contentEpoch: 2n,
			manifestByte: 0x53,
			parents: [chain.fence2],
			payloadByte: 5,
		});
		const operation = await createOperation(context, {
			fence: chain.fence2,
			policy: context.policy2,
			contentEpoch: 2n,
			manifestByte: 0x53,
			parents: [bridge],
			payloadByte: 6,
		});
		context.entriesByCid.delete(bridge.cid);
		const unavailable = await engine.authorize(operation.bytes);
		expect(unavailable.status).to.equal("unavailable");
		expect(unavailable.fetchHints).to.deep.include({
			kind: "causal-entry",
			cid: bridge.cid,
		});

		context.entriesByCid.set(bridge.cid, bridge.bytes);
		expect((await engine.authorize(operation.bytes)).status).to.equal(
			"provisional",
		);
	});

	it("converges after opposite fence delivery order", async () => {
		const context = await createContext();
		const chain = await chainFixture(context);
		const first = await openAnchor(context);
		const second = await openAnchor(context, new MemoryAnchorStore());
		await first.ingest(chain.fence0.bytes);
		await first.ingest(chain.fence1.bytes);
		expect((await second.ingest(chain.fence1.bytes)).status).to.equal(
			"pending",
		);
		expect((await second.ingest(chain.fence0.bytes)).status).to.equal(
			"accepted",
		);
		expect(first.head?.entryCid).to.equal(chain.fence1.cid);
		expect(second.head?.entryCid).to.equal(chain.fence1.cid);
		const [left, right] = await Promise.all([
			operationEngine(context, first).authorize(chain.concurrent.bytes),
			operationEngine(context, second).authorize(chain.concurrent.bytes),
		]);
		expect(left.status).to.equal("rejected");
		expect(right.status).to.equal(left.status);
	});

	it("halts authorization after a durable resource-fence fork", async () => {
		const context = await createContext();
		const fence0 = await createFence(context, {
			sequence: 0n,
			policy: context.policy0,
		});
		const operation = await createOperation(context, {
			fence: fence0,
			policy: context.policy0,
			contentEpoch: 0n,
			manifestByte: 0x51,
			parents: [fence0],
		});
		const first = await createFence(context, {
			sequence: 1n,
			previousDigest: fence0.digest,
			policy: context.policy1,
			parents: [fence0],
			manifestByte: 0x52,
		});
		const sibling = await createFence(context, {
			sequence: 1n,
			previousDigest: fence0.digest,
			policy: context.policy1,
			parents: [fence0],
			manifestByte: 0x53,
		});
		const anchor = await openAnchor(context);
		await anchor.ingest(fence0.bytes);
		await anchor.ingest(first.bytes);
		expect((await anchor.ingest(sibling.bytes)).status).to.equal("forked");
		expect(
			(await operationEngine(context, anchor).authorize(operation.bytes))
				.status,
		).to.equal("halted");
	});

	it("holds policy then resource through callbacks and bounds queued cancellation", async () => {
		const context = await createContext();
		const chain = await chainFixture(context);
		const anchor = await openAnchor(context);
		await anchor.ingest(chain.fence0.bytes);
		const entered = deferred();
		const release = deferred();
		const controller = new AbortController();
		const first = anchor.withAcceptedFenceLease(
			{
				fenceDigest: chain.fence0.digest,
				policy: context.policy0,
				signal: controller.signal,
			},
			async () => {
				expect(context.policyLease.active).to.be.greaterThan(0);
				entered.resolve();
				await release.promise;
				return "held";
			},
		);
		await entered.promise;
		controller.abort();
		let secondCalled = false;
		const second = await anchor.withAcceptedFenceLease(
			{
				fenceDigest: chain.fence0.digest,
				policy: context.policy0,
				timeoutMs: 5,
			},
			() => {
				secondCalled = true;
			},
		);
		expect(second.status).to.equal("unavailable");
		expect(secondCalled).to.equal(false);
		release.resolve();
		expect(await first).to.deep.equal({ status: "completed", value: "held" });
		for (
			let attempts = 0;
			attempts < 20 && anchor.bufferedAdmissionCount > 0;
			attempts++
		) {
			await new Promise((resolve) => setTimeout(resolve, 0));
		}
		expect(anchor.bufferedAdmissionCount).to.equal(0);
		await expect(
			anchor.withAcceptedFenceLease(
				{ fenceDigest: chain.fence0.digest, policy: context.policy0 },
				() => {
					throw new Error("consumer failed");
				},
			),
		).to.be.rejectedWith("consumer failed");
		expect(
			(
				await anchor.withAcceptedFenceLease(
					{ fenceDigest: chain.fence0.digest, policy: context.policy0 },
					() => "usable",
				)
			).status,
		).to.equal("completed");
	});

	it("bounds abandoned resolver calls across repeated cancellation and recovers after settlement", async () => {
		const context = await createContext();
		const chain = await chainFixture(context);
		const anchor = await openAnchor(context);
		await anchor.ingest(chain.fence0.bytes);
		const operation = await createOperation(context, {
			fence: chain.fence0,
			policy: context.policy0,
			contentEpoch: 0n,
			manifestByte: 0x51,
			parents: [chain.before],
		});
		let controller = new AbortController();
		let calls = 0;
		let responsive = false;
		const releases: Array<() => void> = [];
		const engine = new TrustedNetworkV2ResourceOperationEngine({
			descriptor: context.descriptor,
			expectedResourceId: RESOURCE_ID,
			expectedGid: RESOURCE_GID,
			fenceAnchor: anchor,
			resolveEntryV0: (cids) => {
				calls += 1;
				const bytes = new Map(
					cids.map((cid) => [cid, context.entriesByCid.get(cid)]),
				);
				if (responsive) return bytes;
				controller.abort();
				return new Promise((resolve) => releases.push(() => resolve(bytes)));
			},
		});
		for (let attempt = 0; attempt < 65; attempt++) {
			controller = new AbortController();
			expect(
				(await engine.authorize(operation.bytes, { signal: controller.signal }))
					.status,
			).to.equal("unavailable");
		}
		expect(calls).to.equal(64);
		expect(engine.bufferedAuthorizationCount).to.equal(0);
		expect(engine.bufferedAuthorizationBytes).to.equal(0);
		for (const release of releases) release();
		await Promise.resolve();
		await Promise.resolve();
		responsive = true;
		expect((await engine.authorize(operation.bytes)).status).to.equal(
			"provisional",
		);
	});

	it("shares exact entry, byte, and link ceilings across historical prefix and classification walks", async () => {
		const context = await createContext();
		const chain = await chainFixture(context);
		const anchor = await openAnchor(context);
		for (const fence of [chain.fence0, chain.fence1, chain.fence2]) {
			expect((await anchor.ingest(fence.bytes)).status).to.equal("accepted");
		}
		// Prefix walks visit [fence2] and [fence1, before]. Classification
		// visits [before], [fence1], then [before, fence0], without deduplication.
		const exact: ResourceCausalWorkLimitsV2 = {
			maxVisitedEntries: 7,
			maxTotalBytes:
				chain.fence2.bytes.byteLength +
				2 * chain.fence1.bytes.byteLength +
				3 * chain.before.bytes.byteLength +
				chain.fence0.bytes.byteLength,
			maxParentLinks: 6,
		};
		expect(
			(
				await operationEngine(context, anchor, exact).authorize(
					chain.before.bytes,
				)
			).status,
		).to.equal("policy-final");
		for (const key of Object.keys(exact) as Array<keyof typeof exact>) {
			const engine = operationEngine(context, anchor, {
				...exact,
				[key]: exact[key] - 1,
			});
			const result = await engine.authorize(chain.before.bytes);
			expect(result.status, key).to.equal("unavailable");
			expect(result.reason, key).to.contain("causal work budget exhausted");
			expect(result.applicationPayload, key).to.equal(undefined);
			// An exhausted evaluation does not consume the next operation's budget.
			expect((await engine.authorize(chain.regranted.bytes)).status).to.equal(
				"provisional",
			);
		}
	});

	it("stops during historical-prefix exhaustion before calling the operation resolver", async () => {
		const context = await createContext();
		const chain = await chainFixture(context);
		const anchor = await openAnchor(context);
		for (const fence of [chain.fence0, chain.fence1, chain.fence2]) {
			await anchor.ingest(fence.bytes);
		}
		let operationResolverCalls = 0;
		const engine = new TrustedNetworkV2ResourceOperationEngine({
			descriptor: context.descriptor,
			expectedResourceId: RESOURCE_ID,
			expectedGid: RESOURCE_GID,
			fenceAnchor: anchor,
			causalWorkLimits: { maxVisitedEntries: 2 },
			resolveEntryV0: () => {
				operationResolverCalls += 1;
				return new Map();
			},
		});
		const result = await engine.authorize(chain.before.bytes);
		expect(result.status).to.equal("unavailable");
		expect(result.reason).to.contain("causal work budget exhausted");
		expect(operationResolverCalls).to.equal(0);
		expect(result.applicationPayload).to.equal(undefined);
		expect(
			(await operationEngine(context, anchor).authorize(chain.before.bytes))
				.status,
		).to.equal("policy-final");
	});

	it("does not raise a historical fence walk's lower configured limits", async () => {
		const context = await createContext();
		const chain = await chainFixture(context);
		const anchor = await openAnchor(context);
		for (const fence of [chain.fence0, chain.fence1, chain.fence2]) {
			await anchor.ingest(fence.bytes);
		}
		// The retained head-to-parent edge is direct and can reopen with one
		// entry. The older fence1-to-fence0 transition needs two entries.
		const reopened = await openAnchor(context, context.store.clone(), {
			...causalLimits,
			maxVisitedEntries: 1,
		});
		const result = await operationEngine(context, reopened).authorize(
			chain.before.bytes,
		);
		expect(result.status).to.equal("unavailable");
		expect(result.applicationPayload).to.equal(undefined);
	});

	it("debits capacity and cancelled walk receipts, including work before exhaustion", async () => {
		const context = await createContext();
		const chain = await chainFixture(context);
		const initial: ResourceCausalWorkLimitsV2 = {
			maxVisitedEntries: 8,
			maxTotalBytes: 10_000,
			maxParentLinks: 8,
		};
		for (const outcome of ["aggregate", "per-walk", "cancelled"] as const) {
			const allowance =
				outcome === "aggregate"
					? { ...initial, maxVisitedEntries: 1 }
					: initial;
			const budget = new ResourceCausalWorkBudgetV2(allowance);
			const controller = new AbortController();
			let calls = 0;
			const result = await ResourceCausalWorkBudgetV2.check(
				{
					ancestorCid: chain.fence0.cid,
					descendant: { cid: chain.fence1.cid, bytes: chain.fence1.bytes },
					limits:
						outcome === "per-walk"
							? { ...causalLimits, maxVisitedEntries: 1 }
							: causalLimits,
					signal: controller.signal,
					resolve: () => {
						calls += 1;
						controller.abort();
						return new Map();
					},
				},
				budget,
			);
			expect(result.status).to.equal(
				outcome === "cancelled" ? "incomplete" : "capacity",
			);
			expect(result.visited.entries).to.equal(outcome === "cancelled" ? 2 : 1);
			expect(result.visited.bytes).to.equal(chain.fence1.bytes.byteLength);
			expect(result.visited.parentLinks).to.equal(1);
			expect(calls).to.equal(outcome === "cancelled" ? 1 : 0);
			expect(budget.remaining).to.deep.equal({
				maxVisitedEntries: allowance.maxVisitedEntries - result.visited.entries,
				maxTotalBytes: allowance.maxTotalBytes - result.visited.bytes,
				maxParentLinks: allowance.maxParentLinks - result.visited.parentLinks,
			});
			expect(budget.exhausted).to.equal(outcome !== "cancelled");
		}
	});

	it("only permits lowering the aggregate causal ceilings", async () => {
		const context = await createContext();
		const anchor = await openAnchor(context);
		for (const key of [
			"maxVisitedEntries",
			"maxTotalBytes",
			"maxParentLinks",
		] as const) {
			for (const value of [0, -1, Number.NaN, causalLimits[key] + 1]) {
				expect(() =>
					operationEngine(context, anchor, { [key]: value }),
				).to.throw("positive bounded safe integer");
			}
		}
	});

	it("does not publish an ancestor verdict after cancellation during the reverse closing-fence walk", async () => {
		const context = await createContext();
		const chain = await chainFixture(context);
		const before = await createOperation(context, {
			fence: chain.fence0,
			policy: context.policy0,
			contentEpoch: 0n,
			manifestByte: 0x51,
			parents: [chain.fence0, chain.before],
			payloadByte: 9,
		});
		const closing = await createFence(context, {
			sequence: 1n,
			previousDigest: chain.fence0.digest,
			policy: context.policy1,
			parents: [before],
			manifestByte: 0x52,
		});
		const anchor = await openAnchor(context);
		await anchor.ingest(chain.fence0.bytes);
		await anchor.ingest(closing.bytes);
		for (const cancellation of ["caller", "engine"] as const) {
			const caller = new AbortController();
			let cancelledInReverseWalk = false;
			const engine = new TrustedNetworkV2ResourceOperationEngine({
				descriptor: context.descriptor,
				expectedResourceId: RESOURCE_ID,
				expectedGid: RESOURCE_GID,
				fenceAnchor: anchor,
				resolveEntryV0: (cids) => {
					// Descent from the named fence and descent of the closing fence
					// from this operation are direct edges; only the reverse walk
					// has to resolve the additional bridge.
					if (cids.includes(chain.before.cid)) {
						cancelledInReverseWalk = true;
						if (cancellation === "caller") caller.abort();
						else engine.abort();
					}
					return new Map(
						cids.map((cid) => [cid, context.entriesByCid.get(cid)]),
					);
				},
			});
			const result = await engine.authorize(before.bytes, {
				signal: caller.signal,
			});
			expect(cancelledInReverseWalk).to.equal(true);
			expect(result.status).to.equal(
				cancellation === "caller" ? "unavailable" : "halted",
			);
			expect(result.applicationPayload).to.equal(undefined);
		}
	});
});
