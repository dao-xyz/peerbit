import { serialize } from "@dao-xyz/borsh";
import type { CrashSafeAtomicReplaceStore } from "@peerbit/any-store-interface";
import { Ed25519Keypair } from "@peerbit/crypto";
import { Entry, EntryV0 } from "@peerbit/log";
import { expect } from "chai";
import { equals } from "uint8arrays";
import type {
	AcceptedPolicyLeaseV2,
	PolicyLeaseReferenceV2,
	PolicyLeaseResultV2,
} from "../src/v2-policy-anchor.js";
import {
	type CrashSafeResourceFenceAnchorStoreV2,
	type ResourceFencePolicyAnchorV2,
	TRUSTED_NETWORK_V2_MAX_QUEUED_RESOURCE_FENCE_INPUT_BYTES,
	TRUSTED_NETWORK_V2_RESOURCE_FENCE_ANCHOR_STORE_OWNER,
	TrustedNetworkV2DurableResourceFenceReducer,
} from "../src/v2-resource-fence-anchor.js";
import { authenticateResourceFenceEntryV2 } from "../src/v2-resource-fence-entry.js";
import {
	NetworkDescriptorV2,
	ResourceFenceV2,
	TRUSTED_NETWORK_V2_ENTRY_V0_AUTHORITY_ONLY_SIGNATURE_PROFILE,
	TRUSTED_NETWORK_V2_POLICY_HASH_SHA256,
	TRUSTED_NETWORK_V2_PROTOCOL_VERSION,
	deriveNetworkIdV2,
} from "../src/v2.js";

const ZERO = new Uint8Array(32);
const RESOURCE_ID = new Uint8Array(32).fill(0x71);
const RESOURCE_GID = "resource-fence-anchor-test";

const hex = (bytes: Uint8Array): string => Buffer.from(bytes).toString("hex");

const deferred = () => {
	let resolve!: () => void;
	const promise = new Promise<void>((resolvePromise) => {
		resolve = resolvePromise;
	});
	return { promise, resolve };
};

type Gate = { entered: Promise<void>; release: () => void };

type StoreAction =
	| {
			kind: "gate";
			entered: ReturnType<typeof deferred>;
			release: Promise<void>;
	  }
	| { kind: "throw-before"; error: Error }
	| { kind: "commit-then-throw"; error: Error };

class ControlledResourceFenceStore implements CrashSafeAtomicReplaceStore {
	readonly values: Map<string, Uint8Array>;
	atomicReplaceCalls = 0;
	barrierCalls = 0;
	iteratorCalls = 0;
	onAtomicReplace?: () => void;
	private opened = true;
	private readonly actions: StoreAction[] = [];

	constructor(values?: Map<string, Uint8Array>) {
		this.values = new Map(
			[...(values ?? [])].map(([key, value]) => [key, Uint8Array.from(value)]),
		);
	}

	readonly crashSafeDurability = {
		crashSafe: true as const,
		barrier: async (): Promise<void> => {
			this.barrierCalls += 1;
		},
		atomicReplace: async (key: string, value: Uint8Array): Promise<void> => {
			this.atomicReplaceCalls += 1;
			this.onAtomicReplace?.();
			const action = this.actions.shift();
			if (action?.kind === "gate") {
				action.entered.resolve();
				await action.release;
			}
			if (action?.kind === "throw-before") throw action.error;
			this.values.set(key, Uint8Array.from(value));
			if (action?.kind === "commit-then-throw") throw action.error;
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

	sublevel(): ControlledResourceFenceStore {
		return this;
	}

	async *iterator(): AsyncGenerator<[string, Uint8Array], void, void> {
		this.iteratorCalls += 1;
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

	gateNextAtomicReplace(): Gate {
		const entered = deferred();
		const release = deferred();
		this.actions.push({
			kind: "gate",
			entered,
			release: release.promise,
		});
		return { entered: entered.promise, release: release.resolve };
	}

	failNextAtomicReplace(error: Error, afterCommit = false): void {
		this.actions.push({
			kind: afterCommit ? "commit-then-throw" : "throw-before",
			error,
		});
	}

	clone(): ControlledResourceFenceStore {
		return new ControlledResourceFenceStore(this.values);
	}
}

const copyLease = (policy: AcceptedPolicyLeaseV2["policy"]) => ({
	sequence: policy.sequence,
	digest: Uint8Array.from(policy.digest),
	bindings: policy.bindings.map((binding) => binding),
});

class ControlledPolicyLease {
	active = 0;
	calls = 0;
	allow = true;
	canonicalReady = true;
	state: "ACTIVE" | "UNAVAILABLE" | "FORKED" | "HALTED" = "ACTIVE";
	readonly references: PolicyLeaseReferenceV2[] = [];
	private tail: Promise<void> = Promise.resolve();

	constructor(
		readonly policy: AcceptedPolicyLeaseV2["policy"],
		private readonly neverAcquire = false,
	) {}

	withAcceptedPolicyLease<T>(
		reference: PolicyLeaseReferenceV2,
		use: (lease: AcceptedPolicyLeaseV2) => T | Promise<T>,
	): Promise<PolicyLeaseResultV2<T>> {
		const digest = Uint8Array.from(reference.digest);
		const captured = { ...reference, digest };
		const result = this.tail.then(async (): Promise<PolicyLeaseResultV2<T>> => {
			this.calls += 1;
			this.references.push(captured);
			if (this.neverAcquire) {
				await new Promise<void>((resolve) => {
					if (reference.signal?.aborted) return resolve();
					reference.signal?.addEventListener("abort", () => resolve(), {
						once: true,
					});
				});
				return {
					status: "unavailable",
					reason: "test lease was cancelled",
				};
			}
			if (
				!this.allow ||
				reference.sequence !== this.policy.sequence ||
				!equals(reference.digest, this.policy.digest)
			) {
				return { status: "rejected", reason: "policy is not accepted" };
			}
			if (reference.signal?.aborted) {
				return { status: "unavailable", reason: "lease was cancelled" };
			}
			this.active += 1;
			try {
				return {
					status: "completed",
					value: await use({
						policy: copyLease(this.policy),
						acceptedHead: copyLease(this.policy),
					}),
				};
			} finally {
				this.active -= 1;
			}
		});
		this.tail = result.then(
			(): void => {},
			(): void => {},
		);
		return result;
	}

	asAnchor(): ResourceFencePolicyAnchorV2 {
		return this as unknown as ResourceFencePolicyAnchorV2;
	}

	isUsable(): boolean {
		return this.canonicalReady && this.state === "ACTIVE";
	}
}

class TrackedAbortSignal {
	readonly aborted = false;
	added = 0;
	removed = 0;
	private readonly listeners = new Set<EventListenerOrEventListenerObject>();

	addEventListener(
		type: string,
		listener: EventListenerOrEventListenerObject,
	): void {
		if (type !== "abort") return;
		this.added += 1;
		this.listeners.add(listener);
	}

	removeEventListener(
		type: string,
		listener: EventListenerOrEventListenerObject,
	): void {
		if (type !== "abort") return;
		this.removed += 1;
		this.listeners.delete(listener);
	}

	get activeListeners(): number {
		return this.listeners.size;
	}
}

class AdversarialUint8Array extends Uint8Array {
	iteratorCalls = 0;

	constructor(source: Uint8Array) {
		super(source.byteLength);
		Uint8Array.prototype.set.call(this, source);
	}

	[Symbol.iterator](): IterableIterator<number> {
		this.iteratorCalls += 1;
		throw new Error("caller iterator must not run");
	}
}

type FenceFixture = {
	entry: EntryV0<Uint8Array>;
	bytes: Uint8Array;
	entryCid: string;
	digest: Uint8Array;
};

type Fixture = {
	authority: Ed25519Keypair;
	descriptor: NetworkDescriptorV2;
	policy: AcceptedPolicyLeaseV2["policy"];
	fencesByDigest: Map<string, Uint8Array>;
	entriesByCid: Map<string, Uint8Array>;
	createFence: (properties: {
		sequence: bigint;
		previousDigest?: Uint8Array;
		parent?: EntryV0<Uint8Array>;
		parents?: EntryV0<Uint8Array>[];
		manifestByte?: number;
		metaDataBytes?: number;
	}) => Promise<FenceFixture>;
};

const createFixture = async (): Promise<Fixture> => {
	const authority = await Ed25519Keypair.create();
	const descriptor = new NetworkDescriptorV2({
		protocolVersion: TRUSTED_NETWORK_V2_PROTOCOL_VERSION,
		networkNonce: new Uint8Array(32).fill(0x31),
		policyAuthority: authority.publicKey,
		genesisPolicyDigest: new Uint8Array(32).fill(0x41),
		policyHashProfile: TRUSTED_NETWORK_V2_POLICY_HASH_SHA256,
		entrySignatureProfile:
			TRUSTED_NETWORK_V2_ENTRY_V0_AUTHORITY_ONLY_SIGNATURE_PROFILE,
	});
	const policy: AcceptedPolicyLeaseV2["policy"] = {
		sequence: 0n,
		digest: Uint8Array.from(descriptor.genesisPolicyDigest),
		bindings: [],
	};
	const fencesByDigest = new Map<string, Uint8Array>();
	const entriesByCid = new Map<string, Uint8Array>();
	const createFence: Fixture["createFence"] = async (properties) => {
		const body = new ResourceFenceV2({
			networkId: deriveNetworkIdV2(descriptor),
			resourceId: RESOURCE_ID,
			fenceSequence: properties.sequence,
			previousFenceDigest: properties.previousDigest ?? ZERO,
			policySequence: policy.sequence,
			policyDigest: policy.digest,
			contentEpoch: properties.sequence,
			epochManifestDigest: new Uint8Array(32).fill(
				properties.manifestByte ?? Number(properties.sequence + 1n),
			),
		});
		const parents =
			properties.parents ?? (properties.parent ? [properties.parent] : []);
		const entry = (await EntryV0.create({
			store: {} as never,
			data: serialize(body),
			identity: authority,
			deferStore: true,
			meta: {
				gid: parents.length === 0 ? RESOURCE_GID : undefined,
				next: parents,
				data:
					properties.metaDataBytes === undefined
						? undefined
						: new Uint8Array(properties.metaDataBytes).fill(0xaa),
			},
		})) as EntryV0<Uint8Array>;
		const prepared = Entry.getPreparedStorageBytes(entry);
		if (!prepared) throw new Error("fixture has no prepared entry bytes");
		const bytes = new Uint8Array(prepared);
		const authenticated = await authenticateResourceFenceEntryV2({
			entryBytes: bytes,
			descriptor,
			expectedResourceId: RESOURCE_ID,
			expectedGid: RESOURCE_GID,
		});
		const fixture = {
			entry,
			bytes,
			entryCid: authenticated.entryCid,
			digest: authenticated.digest,
		};
		fencesByDigest.set(hex(fixture.digest), fixture.bytes);
		entriesByCid.set(fixture.entryCid, fixture.bytes);
		return fixture;
	};
	return {
		authority,
		descriptor,
		policy,
		fencesByDigest,
		entriesByCid,
		createFence,
	};
};

const openAnchor = (
	fixture: Fixture,
	store: CrashSafeResourceFenceAnchorStoreV2,
	lease: ControlledPolicyLease,
	options?: {
		signal?: AbortSignal;
		resourceId?: Uint8Array;
		gid?: string;
		operationTimeoutMs?: number;
		policyLeaseMaxSteps?: number;
	},
) =>
	TrustedNetworkV2DurableResourceFenceReducer.open({
		descriptor: fixture.descriptor,
		expectedResourceId: options?.resourceId ?? RESOURCE_ID,
		expectedGid: options?.gid ?? RESOURCE_GID,
		policyAnchor: lease.asAnchor(),
		store,
		resolveFenceEntry: (digest) => fixture.fencesByDigest.get(hex(digest)),
		resolveEntryV0: (cids) =>
			new Map(cids.map((cid) => [cid, fixture.entriesByCid.get(cid)])),
		causalTimeoutMs: 500,
		policyLeaseMaxSteps: options?.policyLeaseMaxSteps ?? 7,
		policyLeaseTimeoutMs: 1_000,
		operationTimeoutMs: options?.operationTimeoutMs,
		signal: options?.signal,
	});

describe("TrustedNetwork v2 durable resource-fence anchor", function () {
	this.timeout(30_000);

	it("holds the accepted-policy lease through commit and publishes only afterward", async () => {
		const fixture = await createFixture();
		const store = new ControlledResourceFenceStore();
		const lease = new ControlledPolicyLease(fixture.policy);
		const anchor = await openAnchor(fixture, store, lease);
		store.onAtomicReplace = () => expect(lease.active).to.equal(1);

		const initial = await fixture.createFence({ sequence: 0n });
		const hostile = new AdversarialUint8Array(initial.bytes);
		const firstGate = store.gateNextAtomicReplace();
		const acceptingInitial = anchor.ingest(hostile);
		hostile.fill(0xff);
		await firstGate.entered;
		expect(hostile.iteratorCalls).to.equal(0);
		expect(anchor.state).to.equal("EMPTY");
		expect(anchor.head).to.equal(undefined);
		expect(anchor.isCommittedHeadStable()).to.equal(false);
		expect(anchor.bufferedAdmissionBytes).to.equal(initial.bytes.byteLength);
		firstGate.release();
		expect((await acceptingInitial).status).to.equal("accepted");
		expect(anchor.state).to.equal("ACTIVE");
		expect(anchor.isCommittedHeadStable()).to.equal(true);
		expect(anchor.head?.entryCid).to.equal(initial.entryCid);
		expect(anchor.head?.causalFrontier).to.deep.equal([]);
		expect(anchor.bufferedAdmissionCount).to.equal(0);
		expect(anchor.bufferedAdmissionBytes).to.equal(0);
		expect(lease.references[0]).to.include({
			sequence: 0n,
			maxSteps: 7,
			timeoutMs: 1_000,
		});
		expect(lease.references[0]?.signal).to.be.instanceOf(AbortSignal);

		const writesBeforeDuplicate = store.atomicReplaceCalls;
		expect((await anchor.ingest(initial.bytes)).status).to.equal("duplicate");
		expect(store.atomicReplaceCalls).to.equal(writesBeforeDuplicate);

		const successor = await fixture.createFence({
			sequence: 1n,
			previousDigest: initial.digest,
			parent: initial.entry,
		});
		const successorGate = store.gateNextAtomicReplace();
		const acceptingSuccessor = anchor.ingest(successor.bytes);
		await successorGate.entered;
		expect(anchor.head?.entryCid).to.equal(initial.entryCid);
		expect(anchor.isCommittedHeadStable()).to.equal(false);
		successorGate.release();
		expect((await acceptingSuccessor).status).to.equal("accepted");
		expect(anchor.head?.entryCid).to.equal(successor.entryCid);
		expect(anchor.head?.causalFrontier.map(({ cid }) => cid)).to.deep.equal([
			initial.entryCid,
		]);
	});

	it("restores exact ACTIVE and UNAVAILABLE bytes offline, then retries", async () => {
		const fixture = await createFixture();
		const store = new ControlledResourceFenceStore();
		const lease = new ControlledPolicyLease(fixture.policy);
		let anchor = await openAnchor(fixture, store, lease);
		const initial = await fixture.createFence({ sequence: 0n });
		const one = await fixture.createFence({
			sequence: 1n,
			previousDigest: initial.digest,
			parent: initial.entry,
		});
		expect((await anchor.ingest(initial.bytes)).status).to.equal("accepted");
		expect((await anchor.ingest(one.bytes)).status).to.equal("accepted");

		const intermediate = (await EntryV0.create({
			store: {} as never,
			data: Uint8Array.of(9),
			identity: fixture.authority,
			deferStore: true,
			meta: { next: [one.entry] },
		})) as EntryV0<Uint8Array>;
		const two = await fixture.createFence({
			sequence: 2n,
			previousDigest: one.digest,
			parent: intermediate,
		});
		const unavailable = await anchor.ingest(two.bytes);
		expect(unavailable.status).to.equal("unavailable");
		expect(anchor.state).to.equal("UNAVAILABLE");
		expect(
			unavailable.fetchHints.some(
				(hint) =>
					hint.kind === "causal-entry" && hint.cid === intermediate.hash,
			),
		).to.equal(true);

		anchor.abort();
		const offlineFixture = { ...fixture };
		offlineFixture.fencesByDigest = new Map();
		offlineFixture.entriesByCid = new Map();
		const reopenedLease = new ControlledPolicyLease(fixture.policy);
		anchor = await openAnchor(offlineFixture, store.clone(), reopenedLease);
		expect(anchor.state).to.equal("UNAVAILABLE");
		expect(anchor.head?.entryCid).to.equal(one.entryCid);
		expect(
			anchor.pendingPolicyReferences.map(({ entryCid }) => entryCid),
		).to.deep.equal([two.entryCid]);

		const intermediateBytes = Entry.getPreparedStorageBytes(intermediate);
		if (!intermediateBytes) throw new Error("missing intermediate bytes");
		offlineFixture.entriesByCid.set(
			intermediate.hash,
			new Uint8Array(intermediateBytes),
		);
		expect((await anchor.retryPending(two.entryCid)).status).to.equal(
			"accepted",
		);
		expect(anchor.state).to.equal("ACTIVE");
		expect(anchor.head?.entryCid).to.equal(two.entryCid);
	});

	it("does not mutate or write when the referenced policy lease fails", async () => {
		const fixture = await createFixture();
		const store = new ControlledResourceFenceStore();
		const lease = new ControlledPolicyLease(fixture.policy);
		lease.allow = false;
		const anchor = await openAnchor(fixture, store, lease);
		const initial = await fixture.createFence({ sequence: 0n });

		const result = await anchor.ingest(initial.bytes);
		expect(result.status).to.equal("rejected");
		expect(anchor.state).to.equal("EMPTY");
		expect(anchor.head).to.equal(undefined);
		expect(anchor.pendingCount).to.equal(0);
		expect(store.atomicReplaceCalls).to.equal(0);
	});

	it("reports the committed head unstable during policy or fence uncertainty", async () => {
		const fixture = await createFixture();
		const lease = new ControlledPolicyLease(fixture.policy);
		const anchor = await openAnchor(
			fixture,
			new ControlledResourceFenceStore(),
			lease,
		);
		const initial = await fixture.createFence({ sequence: 0n });
		expect((await anchor.ingest(initial.bytes)).status).to.equal("accepted");
		expect(anchor.isCommittedHeadStable()).to.equal(true);

		lease.canonicalReady = false;
		expect(anchor.isCommittedHeadStable()).to.equal(false);
		lease.canonicalReady = true;
		for (const state of ["UNAVAILABLE", "FORKED", "HALTED"] as const) {
			lease.state = state;
			expect(anchor.isCommittedHeadStable()).to.equal(false);
		}
		lease.state = "ACTIVE";
		expect(anchor.isCommittedHeadStable()).to.equal(true);

		const one = await fixture.createFence({
			sequence: 1n,
			previousDigest: initial.digest,
			parent: initial.entry,
		});
		const two = await fixture.createFence({
			sequence: 2n,
			previousDigest: one.digest,
			parent: one.entry,
		});
		expect((await anchor.ingest(two.bytes)).status).to.equal("pending");
		expect(anchor.state).to.equal("ACTIVE");
		expect(anchor.pendingCount).to.equal(1);
		expect(anchor.isCommittedHeadStable()).to.equal(false);
	});

	it("deterministically drains admitted children and detects a fork in every order", async () => {
		const orders = [
			["root", "left", "right"],
			["root", "right", "left"],
			["left", "root", "right"],
			["right", "root", "left"],
			["left", "right", "root"],
			["right", "left", "root"],
		] as const;
		for (const order of orders) {
			const fixture = await createFixture();
			const lease = new ControlledPolicyLease(fixture.policy);
			const anchor = await openAnchor(
				fixture,
				new ControlledResourceFenceStore(),
				lease,
			);
			const root = await fixture.createFence({ sequence: 0n });
			const left = await fixture.createFence({
				sequence: 1n,
				previousDigest: root.digest,
				parent: root.entry,
				manifestByte: 0x11,
			});
			const right = await fixture.createFence({
				sequence: 1n,
				previousDigest: root.digest,
				parent: root.entry,
				manifestByte: 0x22,
			});
			const candidates = { root, left, right };
			for (const name of order) {
				await anchor.ingest(candidates[name].bytes);
				expect(lease.active).to.equal(0);
			}
			expect(anchor.state, order.join(" -> ")).to.equal("FORKED");
			expect(anchor.pendingCount).to.equal(0);
			expect(
				anchor.forkEvidence?.children.map(({ entryCid }) => entryCid).sort(),
			).to.deep.equal([left.entryCid, right.entryCid].sort());
			expect(lease.calls).to.be.at.most(3 + 3);
		}
	});

	it("persists canonical authority-fork evidence and reopens fail closed", async () => {
		const fixture = await createFixture();
		const store = new ControlledResourceFenceStore();
		const lease = new ControlledPolicyLease(fixture.policy);
		let anchor = await openAnchor(fixture, store, lease);
		const initial = await fixture.createFence({ sequence: 0n });
		const left = await fixture.createFence({
			sequence: 1n,
			previousDigest: initial.digest,
			parent: initial.entry,
			manifestByte: 0x11,
		});
		const right = await fixture.createFence({
			sequence: 1n,
			previousDigest: initial.digest,
			parent: initial.entry,
			manifestByte: 0x22,
		});
		await anchor.ingest(initial.bytes);
		await anchor.ingest(left.bytes);
		const forked = await anchor.ingest(right.bytes);
		expect(forked.status).to.equal("forked");
		expect(anchor.state).to.equal("FORKED");
		expect(anchor.isCommittedHeadStable()).to.equal(false);
		expect(anchor.forkEvidence?.children).to.have.length(2);
		const canonicalCids = [left.entryCid, right.entryCid].sort();
		expect(
			anchor.forkEvidence?.children.map(({ entryCid }) => entryCid).sort(),
		).to.deep.equal(canonicalCids);

		const writes = store.atomicReplaceCalls;
		anchor = await openAnchor(
			fixture,
			store.clone(),
			new ControlledPolicyLease(fixture.policy),
		);
		expect(anchor.state).to.equal("FORKED");
		expect(anchor.forkEvidence?.children).to.have.length(2);
		expect((await anchor.ingest(initial.bytes)).status).to.equal("halted");
		expect(store.atomicReplaceCalls).to.equal(writes);
	});

	it("halts on ambiguous replacement and reopens whichever complete state exists", async () => {
		for (const afterCommit of [false, true]) {
			const fixture = await createFixture();
			const store = new ControlledResourceFenceStore();
			const lease = new ControlledPolicyLease(fixture.policy);
			const tracked = new TrackedAbortSignal();
			const anchor = await openAnchor(fixture, store, lease, {
				signal: tracked as unknown as AbortSignal,
			});
			const initial = await fixture.createFence({ sequence: 0n });
			const one = await fixture.createFence({
				sequence: 1n,
				previousDigest: initial.digest,
				parent: initial.entry,
			});
			await anchor.ingest(initial.bytes);
			store.failNextAtomicReplace(
				new Error("injected replacement failure"),
				afterCommit,
			);
			await expect(anchor.ingest(one.bytes)).to.be.rejectedWith(
				"publication is ambiguous and halted",
			);
			expect(anchor.state).to.equal("HALTED");
			expect(tracked.activeListeners).to.equal(0);
			expect(anchor.head?.entryCid).to.equal(initial.entryCid);
			const calls = store.atomicReplaceCalls;
			await expect(anchor.ingest(one.bytes)).to.be.rejectedWith(
				"publication is ambiguous and halted",
			);
			expect(store.atomicReplaceCalls).to.equal(calls);

			const reopened = await openAnchor(
				fixture,
				store.clone(),
				new ControlledPolicyLease(fixture.policy),
			);
			expect(reopened.head?.entryCid).to.equal(
				afterCommit ? one.entryCid : initial.entryCid,
			);
		}
	});

	it("bounds retained queue bytes and cancels policy acquisition on abort", async () => {
		const fixture = await createFixture();
		const store = new ControlledResourceFenceStore();
		const lease = new ControlledPolicyLease(fixture.policy);
		const anchor = await openAnchor(fixture, store, lease);
		const large = await fixture.createFence({
			sequence: 0n,
			metaDataBytes: 7_000,
		});
		const gate = store.gateNextAtomicReplace();
		const unsettled = [anchor.ingest(large.bytes)];
		await gate.entered;
		while (
			anchor.bufferedAdmissionBytes + large.bytes.byteLength <=
			TRUSTED_NETWORK_V2_MAX_QUEUED_RESOURCE_FENCE_INPUT_BYTES
		) {
			unsettled.push(anchor.ingest(large.bytes));
		}
		const overflow = await anchor.ingest(large.bytes);
		expect(overflow.status).to.equal("capacity");
		expect(anchor.bufferedAdmissionBytes).to.be.at.most(
			TRUSTED_NETWORK_V2_MAX_QUEUED_RESOURCE_FENCE_INPUT_BYTES,
		);
		expect(anchor.bufferedAdmissionCount).to.be.greaterThan(0);
		gate.release();
		expect((await Promise.all(unsettled))[0]?.status).to.equal("accepted");
		expect(anchor.bufferedAdmissionCount).to.equal(0);
		expect(anchor.bufferedAdmissionBytes).to.equal(0);

		const cancelledStore = new ControlledResourceFenceStore();
		const neverLease = new ControlledPolicyLease(fixture.policy, true);
		const cancelled = await openAnchor(fixture, cancelledStore, neverLease);
		const pending = cancelled.ingest(
			(await fixture.createFence({ sequence: 0n })).bytes,
		);
		while (neverLease.calls === 0)
			await new Promise((resolve) => setTimeout(resolve, 0));
		cancelled.abort();
		expect((await pending).status).to.equal("halted");
		expect(cancelled.state).to.equal("HALTED");
		expect(cancelled.bufferedAdmissionCount).to.equal(0);
		expect(cancelledStore.atomicReplaceCalls).to.equal(0);
	});

	it("bounds the complete lease operation and releases its queue reservation", async () => {
		const fixture = await createFixture();
		const store = new ControlledResourceFenceStore();
		const neverLease = new ControlledPolicyLease(fixture.policy, true);
		const anchor = await openAnchor(fixture, store, neverLease, {
			operationTimeoutMs: 25,
			policyLeaseMaxSteps: 0,
		});
		const initial = await fixture.createFence({ sequence: 0n });
		const started = Date.now();
		const result = await anchor.ingest(initial.bytes);
		expect(result.status).to.equal("unavailable");
		expect(Date.now() - started).to.be.lessThan(2_000);
		expect(anchor.state).to.equal("EMPTY");
		expect(anchor.bufferedAdmissionCount).to.equal(0);
		expect(anchor.bufferedAdmissionBytes).to.equal(0);
		expect(store.atomicReplaceCalls).to.equal(0);
		expect(neverLease.references[0]?.maxSteps).to.equal(0);
		expect(neverLease.references[0]?.deadline).to.be.a("number");
		expect(neverLease.references[0]?.signal?.aborted).to.equal(true);
	});

	it("removes an external abort listener when open fails after registration", async () => {
		const fixture = await createFixture();
		const store = new ControlledResourceFenceStore();
		store.put(
			`${TRUSTED_NETWORK_V2_RESOURCE_FENCE_ANCHOR_STORE_OWNER}/legacy`,
			Uint8Array.of(1),
		);
		const tracked = new TrackedAbortSignal();
		await expect(
			openAnchor(fixture, store, new ControlledPolicyLease(fixture.policy), {
				signal: tracked as unknown as AbortSignal,
			}),
		).to.be.rejectedWith("Legacy resource-fence anchor records");
		expect(tracked.added).to.equal(1);
		expect(tracked.removed).to.equal(1);
		expect(tracked.activeListeners).to.equal(0);
	});

	it("binds durable slots to the exact resource and gid scope", async () => {
		const fixture = await createFixture();
		const store = new ControlledResourceFenceStore();
		const lease = new ControlledPolicyLease(fixture.policy);
		const anchor = await openAnchor(fixture, store, lease);
		expect(
			(await anchor.ingest((await fixture.createFence({ sequence: 0n })).bytes))
				.status,
		).to.equal("accepted");
		anchor.abort();

		await expect(
			openAnchor(
				fixture,
				store.clone(),
				new ControlledPolicyLease(fixture.policy),
				{ resourceId: new Uint8Array(32).fill(0x72) },
			),
		).to.be.rejectedWith("different scope");
		await expect(
			openAnchor(
				fixture,
				store.clone(),
				new ControlledPolicyLease(fixture.policy),
				{ gid: `${RESOURCE_GID}-other` },
			),
		).to.be.rejectedWith("different scope");
	});
});
