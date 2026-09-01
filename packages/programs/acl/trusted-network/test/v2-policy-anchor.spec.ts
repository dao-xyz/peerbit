import { field, fixedArray, serialize, variant, vec } from "@dao-xyz/borsh";
import { CrashSafeTwoSlotCheckpoint } from "@peerbit/any-store/checkpoint";
import { Ed25519Keypair, PublicSignKey, sha256Sync } from "@peerbit/crypto";
import { EntryV0 } from "@peerbit/log";
import { expect } from "chai";
import { compare, concat } from "uint8arrays";
import {
	type CrashSafePolicyAnchorStoreV2,
	TRUSTED_NETWORK_V2_MAX_POLICY_ANCHOR_CHECKPOINT_PAYLOAD_BYTES,
	TrustedNetworkV2DurablePolicyReducer,
} from "../src/v2-policy-anchor.js";
import type { PolicyAdmissionResultV2 } from "../src/v2-policy-engine.js";
import {
	NetworkDescriptorV2,
	PolicySnapshotBodyV2,
	PolicySubjectBindingV2,
	TRUSTED_NETWORK_V2_ENTRY_V0_AUTHORITY_ONLY_SIGNATURE_PROFILE,
	TRUSTED_NETWORK_V2_MAX_POLICY_ENTRY_BYTES,
	TRUSTED_NETWORK_V2_POLICY_HASH_SHA256,
	TRUSTED_NETWORK_V2_PROTOCOL_VERSION,
	TrustedNetworkRole,
	deriveNetworkIdV2,
	digestPolicySnapshotBodyV2,
} from "../src/v2.js";

const ZERO_DIGEST = new Uint8Array(32);
const MAX_PENDING_POLICIES = 64;
const MAX_FORK_CHILDREN = MAX_PENDING_POLICIES + 2;
const CHECKPOINT_SCOPE_DOMAIN = new TextEncoder().encode(
	"peerbit/trusted-network/v2/policy-anchor/checkpoint/v1\0",
);

@variant([2, 16, 5])
class TestPolicyAnchorCheckpointPayloadV2 {
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

	@field({ type: vec(Uint8Array, "u8") })
	forkObservationEntryBytes: Uint8Array[];

	constructor(properties?: {
		state: number;
		acceptedHeadEntryBytes: Uint8Array;
		comparisonCandidateEntryBytes: Uint8Array;
		acceptedAncestorDigest: Uint8Array;
		unavailableReason: string;
		forkObservationEntryBytes: Uint8Array[];
	}) {
		if (properties) Object.assign(this, properties);
	}
}

const hex = (bytes: Uint8Array): string => Buffer.from(bytes).toString("hex");

const checkpointScope = (descriptor: NetworkDescriptorV2): Uint8Array => {
	const descriptorBytes = serialize(descriptor);
	const descriptorLength = new Uint8Array(4);
	new DataView(descriptorLength.buffer).setUint32(
		0,
		descriptorBytes.byteLength,
		false,
	);
	return concat([CHECKPOINT_SCOPE_DOMAIN, descriptorLength, descriptorBytes]);
};

const entryBytes = (entry: EntryV0<Uint8Array>): Uint8Array => serialize(entry);

const canonicalObservationBytes = (entries: PolicyFixture[]): Uint8Array[] =>
	entries
		.map(({ entry }) => {
			const bytes = entryBytes(entry);
			return { bytes, hash: sha256Sync(bytes) };
		})
		.sort((left, right) => {
			const hashOrder = compare(left.hash, right.hash);
			return hashOrder === 0 ? compare(left.bytes, right.bytes) : hashOrder;
		})
		.map(({ bytes }) => bytes);

class AdversarialUint8Array extends Uint8Array {
	iteratorCalls = 0;
	readonly iteratorLength: number;

	constructor(source: Uint8Array, iteratorLength: number) {
		super(source.byteLength);
		Uint8Array.prototype.set.call(this, source);
		this.iteratorLength = iteratorLength;
	}

	*[Symbol.iterator](): IterableIterator<number> {
		this.iteratorCalls += 1;
		for (let index = 0; index < this.iteratorLength; index++) yield 0;
	}
}

const installOwnIterator = (
	bytes: Uint8Array,
	iterator: () => IterableIterator<number>,
): (() => number) => {
	let calls = 0;
	Object.defineProperty(bytes, Symbol.iterator, {
		configurable: true,
		value: (): IterableIterator<number> => {
			calls += 1;
			return iterator();
		},
	});
	return () => calls;
};

const sortedBindings = (
	bindings: Array<[PublicSignKey, number]>,
): PolicySubjectBindingV2[] =>
	bindings
		.map(
			([signingKey, roles]) =>
				new PolicySubjectBindingV2({ signingKey, roles }),
		)
		.sort((a, b) => compare(serialize(a.signingKey), serialize(b.signingKey)));

type PolicyFixture = {
	body: PolicySnapshotBodyV2;
	digest: Uint8Array;
	entry: EntryV0<Uint8Array>;
};

type ChainFixture = {
	authority: Ed25519Keypair;
	alice: Ed25519Keypair;
	bob: Ed25519Keypair;
	descriptor: NetworkDescriptorV2;
	chain: [PolicyFixture, PolicyFixture, PolicyFixture, PolicyFixture];
};

const createEntry = async (
	bodyBytes: Uint8Array,
	identity: Ed25519Keypair,
	next?: EntryV0<Uint8Array>,
): Promise<EntryV0<Uint8Array>> =>
	(await EntryV0.create({
		store: {} as never,
		data: bodyBytes,
		identity,
		deferStore: true,
		meta: { next: next === undefined ? [] : [next] },
	})) as EntryV0<Uint8Array>;

const createPolicy = async (properties: {
	descriptor: NetworkDescriptorV2;
	sequence: bigint;
	previousPolicyDigest: Uint8Array;
	bindings: Array<[PublicSignKey, number]>;
	signer: Ed25519Keypair;
	next?: EntryV0<Uint8Array>;
}): Promise<PolicyFixture> => {
	const body = new PolicySnapshotBodyV2({
		networkId: deriveNetworkIdV2(properties.descriptor),
		sequence: properties.sequence,
		previousPolicyDigest: properties.previousPolicyDigest,
		bindings: sortedBindings(properties.bindings),
	});
	return {
		body,
		digest: digestPolicySnapshotBodyV2(body),
		entry: await createEntry(
			serialize(body),
			properties.signer,
			properties.next,
		),
	};
};

const createChain = async (): Promise<ChainFixture> => {
	const authority = await Ed25519Keypair.create();
	const alice = await Ed25519Keypair.create();
	const bob = await Ed25519Keypair.create();
	const descriptorWithoutGenesis = new NetworkDescriptorV2({
		protocolVersion: TRUSTED_NETWORK_V2_PROTOCOL_VERSION,
		networkNonce: Uint8Array.from({ length: 32 }, (_, index) => index + 1),
		policyAuthority: authority.publicKey,
		genesisPolicyDigest: ZERO_DIGEST,
		policyHashProfile: TRUSTED_NETWORK_V2_POLICY_HASH_SHA256,
		entrySignatureProfile:
			TRUSTED_NETWORK_V2_ENTRY_V0_AUTHORITY_ONLY_SIGNATURE_PROFILE,
	});
	const genesisBody = new PolicySnapshotBodyV2({
		networkId: deriveNetworkIdV2(descriptorWithoutGenesis),
		sequence: 0n,
		previousPolicyDigest: ZERO_DIGEST,
		bindings: sortedBindings([
			[authority.publicKey, TrustedNetworkRole.ADMIN],
			[alice.publicKey, TrustedNetworkRole.WRITER],
		]),
	});
	const descriptor = new NetworkDescriptorV2({
		...descriptorWithoutGenesis,
		genesisPolicyDigest: digestPolicySnapshotBodyV2(genesisBody),
	});
	const genesis: PolicyFixture = {
		body: genesisBody,
		digest: descriptor.genesisPolicyDigest,
		entry: await createEntry(serialize(genesisBody), authority),
	};
	const one = await createPolicy({
		descriptor,
		sequence: 1n,
		previousPolicyDigest: genesis.digest,
		bindings: [
			[authority.publicKey, TrustedNetworkRole.ADMIN],
			[alice.publicKey, TrustedNetworkRole.READER],
			[bob.publicKey, TrustedNetworkRole.WRITER],
		],
		signer: authority,
		next: genesis.entry,
	});
	const two = await createPolicy({
		descriptor,
		sequence: 2n,
		previousPolicyDigest: one.digest,
		bindings: [
			[authority.publicKey, TrustedNetworkRole.ADMIN],
			[alice.publicKey, TrustedNetworkRole.READER],
			[
				bob.publicKey,
				TrustedNetworkRole.WRITER | TrustedNetworkRole.REPLICATOR,
			],
		],
		signer: authority,
		next: one.entry,
	});
	const three = await createPolicy({
		descriptor,
		sequence: 3n,
		previousPolicyDigest: two.digest,
		bindings: [
			[authority.publicKey, TrustedNetworkRole.ADMIN],
			[alice.publicKey, TrustedNetworkRole.WRITER | TrustedNetworkRole.READER],
			[bob.publicKey, TrustedNetworkRole.REPLICATOR],
		],
		signer: authority,
		next: two.entry,
	});
	return {
		authority,
		alice,
		bob,
		descriptor,
		chain: [genesis, one, two, three],
	};
};

const createDirectChild = async (
	fixture: ChainFixture,
	roles: number,
	subject: PublicSignKey = fixture.alice.publicKey,
): Promise<PolicyFixture> =>
	createPolicy({
		descriptor: fixture.descriptor,
		sequence: 1n,
		previousPolicyDigest: fixture.chain[0].digest,
		bindings: [
			[fixture.authority.publicKey, TrustedNetworkRole.ADMIN],
			[subject, roles],
		],
		signer: fixture.authority,
		next: fixture.chain[0].entry,
	});

const deferred = () => {
	let resolve!: () => void;
	let reject!: (reason?: unknown) => void;
	const promise = new Promise<void>((resolvePromise, rejectPromise) => {
		resolve = () => resolvePromise();
		reject = rejectPromise;
	});
	return { promise, resolve, reject };
};

type Gate = {
	entered: Promise<void>;
	release: () => void;
};

type StoreAction =
	| {
			kind: "gate";
			entered: ReturnType<typeof deferred>;
			release: Promise<void>;
	  }
	| { kind: "throw-before"; error: Error }
	| { kind: "commit-then-throw"; error: Error };

class ControlledPolicyAnchorStore implements CrashSafePolicyAnchorStoreV2 {
	readonly values: Map<string, Uint8Array>;
	getCalls = 0;
	atomicReplaceCalls = 0;
	barrierCalls = 0;
	iteratorCalls = 0;
	private opened = true;
	private readonly atomicReplaceActions: StoreAction[] = [];

	constructor(values?: Map<string, Uint8Array>) {
		this.values = new Map(
			[...(values ?? [])].map(([key, value]) => [key, Uint8Array.from(value)]),
		);
	}

	readonly crashSafeDurability = {
		crashSafe: true as const,
		barrier: async (): Promise<void> => this.barrier(),
		atomicReplace: async (key: string, value: Uint8Array): Promise<void> => {
			this.atomicReplaceCalls += 1;
			const action = this.atomicReplaceActions.shift();
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
		this.getCalls += 1;
		const value = this.values.get(key);
		return value === undefined ? undefined : Uint8Array.from(value);
	}

	async put(key: string, value: Uint8Array): Promise<void> {
		this.values.set(key, Uint8Array.from(value));
	}

	del(key: string): void {
		this.values.delete(key);
	}

	sublevel(): ControlledPolicyAnchorStore {
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
		this.atomicReplaceActions.push({
			kind: "gate",
			entered,
			release: release.promise,
		});
		return { entered: entered.promise, release: () => release.resolve() };
	}

	failNextAtomicReplace(error: Error, afterCommit = false): void {
		this.atomicReplaceActions.push({
			kind: afterCommit ? "commit-then-throw" : "throw-before",
			error,
		});
	}

	clone(): ControlledPolicyAnchorStore {
		return new ControlledPolicyAnchorStore(this.values);
	}

	private async barrier(): Promise<void> {
		this.barrierCalls += 1;
	}
}

const createResolver = () => {
	const entries = new Map<string, Uint8Array>();
	return {
		add: (...fixtures: PolicyFixture[]): void => {
			for (const fixture of fixtures) {
				entries.set(hex(fixture.digest), entryBytes(fixture.entry));
			}
		},
		remove: (...fixtures: PolicyFixture[]): void => {
			for (const fixture of fixtures) entries.delete(hex(fixture.digest));
		},
		resolve: (digest: Uint8Array): Uint8Array | undefined =>
			entries.get(hex(digest)),
	};
};

const openAnchor = (
	fixture: ChainFixture,
	store: CrashSafePolicyAnchorStoreV2,
	resolvePolicyEntry: (
		digest: Uint8Array,
		options: { signal: AbortSignal },
	) => Uint8Array | undefined | Promise<Uint8Array | undefined> = () =>
		undefined,
): Promise<TrustedNetworkV2DurablePolicyReducer> =>
	TrustedNetworkV2DurablePolicyReducer.open({
		descriptor: fixture.descriptor,
		resolvePolicyEntry,
		store,
		resolveTimeoutMs: 500,
	});

const commitTestCheckpointPayload = async (
	fixture: ChainFixture,
	store: ControlledPolicyAnchorStore,
	payload: TestPolicyAnchorCheckpointPayloadV2,
): Promise<void> => {
	const checkpoint = await CrashSafeTwoSlotCheckpoint.open({
		store,
		scope: checkpointScope(fixture.descriptor),
		maxPayloadBytes:
			TRUSTED_NETWORK_V2_MAX_POLICY_ANCHOR_CHECKPOINT_PAYLOAD_BYTES,
	});
	await checkpoint.commit(serialize(payload));
};

const rejection = async (promise: Promise<unknown>): Promise<Error> => {
	try {
		await promise;
	} catch (error) {
		return error instanceof Error ? error : new Error(String(error));
	}
	throw new Error("Expected promise to reject");
};

const expectHead = (
	anchor: TrustedNetworkV2DurablePolicyReducer,
	policy: PolicyFixture,
): void => {
	expect(anchor.head?.sequence).to.equal(policy.body.sequence);
	expect(hex(anchor.head!.digest)).to.equal(hex(policy.digest));
};

const CHECKPOINT_KEY_PREFIX = "\0peerbit:two-slot-checkpoint:v1:";

const checkpointKeys = (store: ControlledPolicyAnchorStore): string[] =>
	[...store.values.keys()]
		.filter((key) => key.startsWith(CHECKPOINT_KEY_PREFIX))
		.sort();

const storeContainsBytes = (
	store: ControlledPolicyAnchorStore,
	bytes: Uint8Array,
): boolean =>
	[...store.values.values()].some(
		(value) => Buffer.from(value).indexOf(Buffer.from(bytes)) >= 0,
	);

describe("TrustedNetwork v2 durable policy anchor", () => {
	it("requires an explicit crash-safe durability capability", async () => {
		const fixture = await createChain();
		const base = new ControlledPolicyAnchorStore();
		const noCapability = {
			get: base.get.bind(base),
			put: base.put.bind(base),
			iterator: base.iterator.bind(base),
		};
		const falseCapability = {
			...noCapability,
			crashSafeDurability: {
				crashSafe: false,
				barrier: async (): Promise<void> => undefined,
			},
		};
		const barrierOnly = {
			...noCapability,
			crashSafeDurability: {
				crashSafe: true,
				barrier: async (): Promise<void> => undefined,
			},
		};

		expect(
			(
				await rejection(
					openAnchor(
						fixture,
						noCapability as unknown as CrashSafePolicyAnchorStoreV2,
					),
				)
			).message,
		).to.match(/crash.safe|durab/i);
		expect(
			(
				await rejection(
					openAnchor(
						fixture,
						falseCapability as unknown as CrashSafePolicyAnchorStoreV2,
					),
				)
			).message,
		).to.match(/crash.safe|durab/i);
		expect(
			(
				await rejection(
					openAnchor(
						fixture,
						barrierOnly as unknown as CrashSafePolicyAnchorStoreV2,
					),
				)
			).message,
		).to.match(/atomic.*replace/i);
	});

	it("fails authorization closed until an atomic checkpoint replacement completes", async () => {
		const fixture = await createChain();
		const store = new ControlledPolicyAnchorStore();
		const anchor = await openAnchor(fixture, store);
		expect(
			(await anchor.ingest(entryBytes(fixture.chain[0].entry))).status,
		).to.equal("accepted");
		expect(
			anchor.isAuthorized(fixture.alice.publicKey, TrustedNetworkRole.WRITER),
		).to.equal(true);

		const firstReplacementGate = store.gateNextAtomicReplace();
		const acceptingOne = anchor.ingest(entryBytes(fixture.chain[1].entry));
		await firstReplacementGate.entered;
		expect(
			anchor.isAuthorized(fixture.alice.publicKey, TrustedNetworkRole.WRITER),
		).to.equal(false);
		expect(
			anchor.isAuthorized(fixture.alice.publicKey, TrustedNetworkRole.READER),
		).to.equal(false);
		firstReplacementGate.release();
		expect((await acceptingOne).status).to.equal("accepted");
		expect(
			anchor.isAuthorized(fixture.alice.publicKey, TrustedNetworkRole.READER),
		).to.equal(true);

		const secondReplacementGate = store.gateNextAtomicReplace();
		const acceptingTwo = anchor.ingest(entryBytes(fixture.chain[2].entry));
		await secondReplacementGate.entered;
		expect(
			anchor.isAuthorized(fixture.bob.publicKey, TrustedNetworkRole.WRITER),
		).to.equal(false);
		expect(
			anchor.isAuthorized(fixture.bob.publicKey, TrustedNetworkRole.REPLICATOR),
		).to.equal(false);
		secondReplacementGate.release();
		expect((await acceptingTwo).status).to.equal("accepted");
		expect(
			anchor.isAuthorized(fixture.bob.publicKey, TrustedNetworkRole.REPLICATOR),
		).to.equal(true);
	});

	it("reopens an ACTIVE head without peers or historical resolution", async () => {
		const fixture = await createChain();
		const store = new ControlledPolicyAnchorStore();
		const anchor = await openAnchor(fixture, store);
		await anchor.ingest(entryBytes(fixture.chain[0].entry));
		await anchor.ingest(entryBytes(fixture.chain[1].entry));
		const exactHeadEntryBytes = entryBytes(fixture.chain[1].entry);
		expect(fixture.chain[1].entry.hash).to.be.a("string").and.not.be.empty;
		expect(storeContainsBytes(store, exactHeadEntryBytes)).to.equal(true);
		anchor.abort();
		const beforeReopen = [...store.values].map(([key, value]) => [
			key,
			hex(value),
		]);
		let resolverCalls = 0;
		const reopened = await openAnchor(fixture, store, () => {
			resolverCalls += 1;
			throw new Error("peerless reopen must not resolve history");
		});

		expect(reopened.state).to.equal("ACTIVE");
		expectHead(reopened, fixture.chain[1]);
		expect(resolverCalls).to.equal(0);
		expect(
			reopened.isAuthorized(fixture.bob.publicKey, TrustedNetworkRole.WRITER),
		).to.equal(true);
		const replacementsBeforeDuplicate = store.atomicReplaceCalls;
		expect((await reopened.ingest(exactHeadEntryBytes)).status).to.equal(
			"duplicate",
		);
		expect(store.atomicReplaceCalls).to.equal(replacementsBeforeDuplicate);
		expect(
			[...store.values].map(([key, value]) => [key, hex(value)]),
		).to.deep.equal(beforeReopen);
	});

	it("does not make a candidate-only PENDING working set durable", async () => {
		const fixture = await createChain();
		const store = new ControlledPolicyAnchorStore();
		const anchor = await openAnchor(fixture, store);
		await anchor.ingest(entryBytes(fixture.chain[0].entry));
		const replacementsBeforePending = store.atomicReplaceCalls;

		const pending = await anchor.ingest(entryBytes(fixture.chain[2].entry));
		expect(pending.status).to.equal("pending");
		expect(anchor.pendingCount).to.equal(1);
		expect(store.atomicReplaceCalls).to.equal(replacementsBeforePending);
		anchor.abort();

		const reopened = await openAnchor(fixture, store);
		expect(reopened.state).to.equal("ACTIVE");
		expectHead(reopened, fixture.chain[0]);
		expect(reopened.pendingCount).to.equal(0);
		expect(reopened.pendingBytes).to.equal(0);
	});

	it("restores the exact UNAVAILABLE comparison and can retry it", async () => {
		const fixture = await createChain();
		const forkChild = await createDirectChild(
			fixture,
			TrustedNetworkRole.WRITER | TrustedNetworkRole.READER,
		);
		const resolver = createResolver();
		const store = new ControlledPolicyAnchorStore();
		const anchor = await openAnchor(fixture, store, resolver.resolve);
		for (const policy of fixture.chain.slice(0, 3)) {
			expect((await anchor.ingest(entryBytes(policy.entry))).status).to.equal(
				"accepted",
			);
		}
		const unavailable = await anchor.ingest(entryBytes(forkChild.entry));
		expect(unavailable.status).to.equal("unavailable");
		expect(anchor.state).to.equal("UNAVAILABLE");
		const beforeReopen = [...store.values].map(([key, value]) => [
			key,
			hex(value),
		]);
		expect(
			storeContainsBytes(store, entryBytes(fixture.chain[2].entry)),
		).to.equal(true);
		expect(storeContainsBytes(store, entryBytes(forkChild.entry))).to.equal(
			true,
		);
		anchor.abort();

		const reopened = await openAnchor(fixture, store, resolver.resolve);
		expect(reopened.state).to.equal("UNAVAILABLE");
		expectHead(reopened, fixture.chain[2]);
		expect(reopened.pendingCount).to.equal(1);
		expect(reopened.pendingDigests.map(hex)).to.deep.equal([
			hex(forkChild.digest),
		]);
		expect(
			[...store.values].map(([key, value]) => [key, hex(value)]),
		).to.deep.equal(beforeReopen);
		expect(
			reopened.isAuthorized(fixture.bob.publicKey, TrustedNetworkRole.WRITER),
		).to.equal(false);

		resolver.add(fixture.chain[0], fixture.chain[1]);
		expect((await reopened.retryUnavailable()).status).to.equal("forked");
		expect(reopened.state).to.equal("FORKED");
	});

	it("halts on checkpoint replacement failures and reopens the physically committed state", async () => {
		const cases: Array<{
			name: string;
			arm: (store: ControlledPolicyAnchorStore) => void;
			expectedHead: 0 | 1;
		}> = [
			{
				name: "replacement before commit",
				arm: (store) =>
					store.failNextAtomicReplace(new Error("replacement failed")),
				expectedHead: 0,
			},
			{
				name: "replacement after an ambiguous commit",
				arm: (store) =>
					store.failNextAtomicReplace(
						new Error("replacement outcome ambiguous"),
						true,
					),
				expectedHead: 1,
			},
		];

		for (const scenario of cases) {
			const fixture = await createChain();
			const store = new ControlledPolicyAnchorStore();
			const anchor = await openAnchor(fixture, store);
			await anchor.ingest(entryBytes(fixture.chain[0].entry));
			scenario.arm(store);

			const error = await rejection(
				anchor.ingest(entryBytes(fixture.chain[1].entry)),
			);
			expect(error.message, scenario.name).to.match(/ambiguous.*halted/i);
			expect(anchor.state, scenario.name).to.equal("HALTED");
			expect(
				anchor.isAuthorized(fixture.alice.publicKey, TrustedNetworkRole.WRITER),
				scenario.name,
			).to.equal(false);
			expect(
				(await rejection(anchor.ingest(entryBytes(fixture.chain[2].entry))))
					.message,
				scenario.name,
			).to.match(/ambiguous.*halted/i);

			const reopened = await openAnchor(fixture, store);
			expect(reopened.state, scenario.name).to.equal("ACTIVE");
			expectHead(reopened, fixture.chain[scenario.expectedHead]);
		}
	});

	it("orders terminal and lifecycle failure before validation or capacity", async () => {
		const fixture = await createChain();
		const poisonedStore = new ControlledPolicyAnchorStore();
		const poisoned = await openAnchor(fixture, poisonedStore);
		poisonedStore.failNextAtomicReplace(new Error("poison publication"));
		const terminal = await rejection(
			poisoned.ingest(entryBytes(fixture.chain[0].entry)),
		);
		const oversized = new Uint8Array(
			TRUSTED_NETWORK_V2_MAX_POLICY_ENTRY_BYTES + 1,
		);
		expect(await rejection(poisoned.ingest(oversized))).to.equal(terminal);
		expect(await rejection(poisoned.retryUnavailable())).to.equal(terminal);
		const poisonedProxy = new Proxy(Uint8Array.of(0xff), {});
		expect(
			await rejection(poisoned.ingest(poisonedProxy as unknown as Uint8Array)),
		).to.equal(terminal);

		const aborted = await openAnchor(
			fixture,
			new ControlledPolicyAnchorStore(),
		);
		aborted.abort();
		expect((await aborted.ingest(oversized)).status).to.equal("halted");
		expect(
			(await aborted.ingest(poisonedProxy as unknown as Uint8Array)).status,
		).to.equal("halted");
		expect((await aborted.retryUnavailable()).status).to.equal("halted");
	});

	it("serializes concurrent admissions and makes exact duplicates write-free", async () => {
		const fixture = await createChain();
		const store = new ControlledPolicyAnchorStore();
		const anchor = await openAnchor(fixture, store);
		await anchor.ingest(entryBytes(fixture.chain[0].entry));
		const replacementsBeforeDuplicate = store.atomicReplaceCalls;

		expect(
			(await anchor.ingest(entryBytes(fixture.chain[0].entry))).status,
		).to.equal("duplicate");
		expect(store.atomicReplaceCalls).to.equal(replacementsBeforeDuplicate);

		const replacementGate = store.gateNextAtomicReplace();
		const first = anchor.ingest(entryBytes(fixture.chain[1].entry));
		const second = anchor.ingest(entryBytes(fixture.chain[2].entry));
		await replacementGate.entered;
		const replacementsWhileFirstIsBlocked = store.atomicReplaceCalls;
		await Promise.resolve();
		expect(store.atomicReplaceCalls).to.equal(replacementsWhileFirstIsBlocked);
		expect(
			anchor.isAuthorized(fixture.alice.publicKey, TrustedNetworkRole.READER),
		).to.equal(false);
		replacementGate.release();

		expect((await first).status).to.equal("accepted");
		expect((await second).status).to.equal("accepted");
		expect(anchor.state).to.equal("ACTIVE");
		expectHead(anchor, fixture.chain[2]);
	});

	it("rejects incompatible views without iterator, species, or length hooks", async () => {
		const fixture = await createChain();
		const valid = entryBytes(fixture.chain[0].entry);
		const subclass = new AdversarialUint8Array(
			valid,
			TRUSTED_NETWORK_V2_MAX_POLICY_ENTRY_BYTES + 200_000,
		);
		const subclassStore = new ControlledPolicyAnchorStore();
		const subclassAnchor = await openAnchor(fixture, subclassStore);
		expect((await subclassAnchor.ingest(subclass)).status).to.equal("accepted");
		expect(subclass.iteratorCalls).to.equal(0);
		expect(subclassAnchor.bufferedAdmissionCount).to.equal(0);
		expect(subclassAnchor.bufferedAdmissionBytes).to.equal(0);

		const reentrantStore = new ControlledPolicyAnchorStore();
		const reentrantAnchor = await openAnchor(fixture, reentrantStore);
		const reentrant = Uint8Array.from(valid);
		let iteratorCalls = 0;
		Object.defineProperty(reentrant, Symbol.iterator, {
			value: (): IterableIterator<number> => {
				iteratorCalls += 1;
				void reentrantAnchor.ingest(Uint8Array.of(0xff));
				return [][Symbol.iterator]();
			},
		});
		expect((await reentrantAnchor.ingest(reentrant)).status).to.equal(
			"accepted",
		);
		expect(iteratorCalls).to.equal(0);
		expect(reentrantAnchor.bufferedAdmissionCount).to.equal(0);
		expect(reentrantAnchor.bufferedAdmissionBytes).to.equal(0);

		const incompatibleAnchor = await openAnchor(
			fixture,
			new ControlledPolicyAnchorStore(),
		);
		const proxied = new Proxy(Uint8Array.from(valid), {});
		expect(
			(await incompatibleAnchor.ingest(proxied as unknown as Uint8Array))
				.status,
		).to.equal("rejected");
		const detached = new Uint8Array(new ArrayBuffer(8));
		structuredClone(detached.buffer, { transfer: [detached.buffer] });
		expect((await incompatibleAnchor.ingest(detached)).status).to.equal(
			"rejected",
		);
		expect(incompatibleAnchor.bufferedAdmissionCount).to.equal(0);
		expect(incompatibleAnchor.bufferedAdmissionBytes).to.equal(0);
	});

	it("bounds copied admissions while a checkpoint replacement is blocked", async () => {
		const fixture = await createChain();
		const store = new ControlledPolicyAnchorStore();
		const anchor = await openAnchor(fixture, store);
		const validEntry = entryBytes(fixture.chain[0].entry);
		const validEntryLength = validEntry.byteLength;
		Object.defineProperty(validEntry, "byteLength", { value: Number.NaN });
		const validIteratorCalls = installOwnIterator(
			validEntry,
			function* (): IterableIterator<number> {},
		);
		const callsBeforeOversized = {
			get: store.getCalls,
			atomicReplace: store.atomicReplaceCalls,
		};
		const oversized = new Uint8Array(
			TRUSTED_NETWORK_V2_MAX_POLICY_ENTRY_BYTES + 1,
		);
		Object.defineProperty(oversized, "byteLength", { value: 1 });
		const oversizedIteratorCalls = installOwnIterator(
			oversized,
			function* (): IterableIterator<number> {},
		);
		expect((await anchor.ingest(oversized)).status).to.equal("rejected");
		expect(oversizedIteratorCalls()).to.equal(0);
		expect(anchor.bufferedAdmissionCount).to.equal(0);
		expect(anchor.bufferedAdmissionBytes).to.equal(0);
		expect(store.getCalls).to.equal(callsBeforeOversized.get);
		expect(store.atomicReplaceCalls).to.equal(
			callsBeforeOversized.atomicReplace,
		);

		const replacementGate = store.gateNextAtomicReplace();
		const admitted: Array<Promise<PolicyAdmissionResultV2>> = [
			anchor.ingest(validEntry),
		];
		await replacementGate.entered;
		for (let index = 1; index < MAX_PENDING_POLICIES; index++) {
			admitted.push(anchor.ingest(validEntry));
		}
		expect(anchor.bufferedAdmissionCount).to.equal(MAX_PENDING_POLICIES);
		expect(anchor.bufferedAdmissionBytes).to.equal(
			MAX_PENDING_POLICIES * validEntryLength,
		);
		const callsAtCapacity = {
			get: store.getCalls,
			atomicReplace: store.atomicReplaceCalls,
		};
		const capacity = await anchor.ingest(validEntry);
		expect(capacity.status).to.equal("capacity");
		expect(capacity.pendingCount).to.equal(0);
		expect(capacity.pendingBytes).to.equal(0);
		const retryCapacity = await anchor.retryUnavailable();
		expect(retryCapacity.status).to.equal("capacity");
		expect(retryCapacity.pendingCount).to.equal(0);
		expect(retryCapacity.pendingBytes).to.equal(0);
		for (let index = 1; index < 10_000; index++) {
			expect((await anchor.ingest(validEntry)).status).to.equal("capacity");
		}
		expect(anchor.bufferedAdmissionCount).to.equal(MAX_PENDING_POLICIES);
		expect(anchor.bufferedAdmissionBytes).to.equal(
			MAX_PENDING_POLICIES * validEntryLength,
		);
		expect(store.getCalls).to.equal(callsAtCapacity.get);
		expect(store.atomicReplaceCalls).to.equal(callsAtCapacity.atomicReplace);

		replacementGate.release();
		const results = await Promise.all(admitted);
		expect(results[0]!.status).to.equal("accepted");
		expect(
			results.slice(1).every(({ status }) => status === "duplicate"),
		).to.equal(true);
		expect(anchor.bufferedAdmissionCount).to.equal(0);
		expect(anchor.bufferedAdmissionBytes).to.equal(0);
		expect(validIteratorCalls()).to.equal(0);

		const byteGate = store.gateNextAtomicReplace();
		const advancingEntry = entryBytes(fixture.chain[1].entry);
		const advancing = anchor.ingest(advancingEntry);
		await byteGate.entered;
		const maximumEntry = new Uint8Array(
			TRUSTED_NETWORK_V2_MAX_POLICY_ENTRY_BYTES,
		);
		Object.defineProperty(maximumEntry, "byteLength", { value: 0 });
		const maximumIteratorCalls = installOwnIterator(
			maximumEntry,
			function* (): IterableIterator<number> {
				for (
					let index = 0;
					index < TRUSTED_NETWORK_V2_MAX_POLICY_ENTRY_BYTES + 200_000;
					index++
				) {
					yield 0;
				}
			},
		);
		const maximumQueued = anchor.ingest(maximumEntry);
		expect(anchor.bufferedAdmissionCount).to.equal(2);
		expect(anchor.bufferedAdmissionBytes).to.equal(
			advancingEntry.byteLength + TRUSTED_NETWORK_V2_MAX_POLICY_ENTRY_BYTES,
		);
		const callsAtByteCapacity = {
			get: store.getCalls,
			atomicReplace: store.atomicReplaceCalls,
		};
		expect((await anchor.ingest(maximumEntry)).status).to.equal("capacity");
		expect(store.getCalls).to.equal(callsAtByteCapacity.get);
		expect(store.atomicReplaceCalls).to.equal(
			callsAtByteCapacity.atomicReplace,
		);
		expect(anchor.bufferedAdmissionBytes).to.be.lessThanOrEqual(
			TRUSTED_NETWORK_V2_MAX_POLICY_ENTRY_BYTES * 2,
		);
		byteGate.release();
		expect((await advancing).status).to.equal("accepted");
		expect((await maximumQueued).status).to.equal("rejected");
		expect(maximumIteratorCalls()).to.equal(0);
		expect(anchor.bufferedAdmissionCount).to.equal(0);
		expect(anchor.bufferedAdmissionBytes).to.equal(0);
	});

	it("fail-stops without authenticating or persisting anything after durable FORKED", async () => {
		const fixture = await createChain();
		const children = await Promise.all([
			createDirectChild(fixture, TrustedNetworkRole.WRITER),
			createDirectChild(fixture, TrustedNetworkRole.READER),
			createDirectChild(fixture, TrustedNetworkRole.REPLICATOR),
			createDirectChild(
				fixture,
				TrustedNetworkRole.WRITER | TrustedNetworkRole.READER,
			),
		]);
		const store = new ControlledPolicyAnchorStore();
		const resolver = createResolver();
		resolver.add(fixture.chain[0]);
		const anchor = await openAnchor(fixture, store, resolver.resolve);
		const deliveredChildren = [...children].sort((left, right) =>
			compare(right.digest, left.digest),
		);
		await anchor.ingest(entryBytes(fixture.chain[0].entry));
		expect(
			(await anchor.ingest(entryBytes(deliveredChildren[0].entry))).status,
		).to.equal("accepted");
		expect(
			(await anchor.ingest(entryBytes(deliveredChildren[1].entry))).status,
		).to.equal("forked");
		expect(checkpointKeys(store)).to.have.length(2);
		const durableCanonical = anchor.forkEvidence!.children.map(({ digest }) =>
			hex(digest),
		);
		const durableCanonicalEntryBytes = anchor.forkEvidence!.children.map(
			({ entryBytes }) => hex(entryBytes),
		);
		expect(
			storeContainsBytes(store, entryBytes(fixture.chain[0].entry)),
		).to.equal(true);
		for (const child of anchor.forkEvidence!.children) {
			expect(storeContainsBytes(store, child.entryBytes)).to.equal(true);
		}
		const valuesAtFork = [...store.values].map(([key, value]) => [
			key,
			hex(value),
		]);
		const getsAtFork = store.getCalls;
		const replacementsAtFork = store.atomicReplaceCalls;

		// This child would displace a member of the canonical pair if it reached
		// the core. Durable FORKED closes admission before capture/authentication,
		// core mutation, or any store operation.
		expect(
			(await anchor.ingest(entryBytes(deliveredChildren[2].entry))).status,
		).to.equal("halted");
		expect(checkpointKeys(store)).to.have.length(2);
		expect(
			anchor.forkEvidence!.children.map(({ digest }) => hex(digest)),
		).to.deep.equal(durableCanonical);

		expect(
			(await anchor.ingest(entryBytes(deliveredChildren[3].entry))).status,
		).to.equal("halted");
		expect((await anchor.ingest(Uint8Array.of(0xff))).status).to.equal(
			"halted",
		);
		expect((await anchor.retryUnavailable()).status).to.equal("halted");
		expect(checkpointKeys(store)).to.have.length(2);
		expect(anchor.state).to.equal("FORKED");
		expect(anchor.forkEvidence?.children).to.have.length(2);
		expect(
			anchor.forkEvidence!.children.map(({ digest }) => hex(digest)),
		).to.deep.equal(durableCanonical);
		expect(store.getCalls).to.equal(getsAtFork);
		expect(store.atomicReplaceCalls).to.equal(replacementsAtFork);
		expect(
			[...store.values].map(([key, value]) => [key, hex(value)]),
		).to.deep.equal(valuesAtFork);
		anchor.abort();

		const reopened = await openAnchor(fixture, store);
		expect(reopened.state).to.equal("FORKED");
		expect(reopened.forkEvidence?.children).to.have.length(2);
		expect(
			reopened.forkEvidence!.children.map(({ digest }) => hex(digest)),
		).to.deep.equal(durableCanonical);
		expect(
			reopened.forkEvidence!.children.map(({ entryBytes }) => hex(entryBytes)),
		).to.deep.equal(durableCanonicalEntryBytes);
	});

	it("releases the external lifecycle listener when a forked anchor is aborted", async () => {
		const fixture = await createChain();
		const children = await Promise.all([
			createDirectChild(fixture, TrustedNetworkRole.WRITER),
			createDirectChild(fixture, TrustedNetworkRole.READER),
		]);
		const resolver = createResolver();
		resolver.add(fixture.chain[0]);
		const controller = new AbortController();
		const originalRemove = controller.signal.removeEventListener;
		let removedAbortListeners = 0;
		Object.defineProperty(controller.signal, "removeEventListener", {
			configurable: true,
			value: (...args: unknown[]): unknown => {
				if (args[0] === "abort") removedAbortListeners += 1;
				return Reflect.apply(originalRemove, controller.signal, args);
			},
		});
		const anchor = await TrustedNetworkV2DurablePolicyReducer.open({
			descriptor: fixture.descriptor,
			resolvePolicyEntry: resolver.resolve,
			store: new ControlledPolicyAnchorStore(),
			signal: controller.signal,
		});
		await anchor.ingest(entryBytes(fixture.chain[0].entry));
		await anchor.ingest(entryBytes(children[0]!.entry));
		expect(
			(await anchor.ingest(entryBytes(children[1]!.entry))).status,
		).to.equal("forked");
		expect(removedAbortListeners).to.equal(0);

		anchor.abort();
		expect(removedAbortListeners).to.equal(1);
		expect(anchor.state).to.equal("FORKED");
		expect((await anchor.ingest(Uint8Array.of(0xff))).status).to.equal(
			"halted",
		);
	});

	it("closes admissions that were queued before the fork became durable", async () => {
		const fixture = await createChain();
		const children = await Promise.all([
			createDirectChild(fixture, TrustedNetworkRole.WRITER),
			createDirectChild(fixture, TrustedNetworkRole.READER),
			createDirectChild(fixture, TrustedNetworkRole.REPLICATOR),
		]);
		const store = new ControlledPolicyAnchorStore();
		const resolver = createResolver();
		resolver.add(fixture.chain[0]);
		const anchor = await openAnchor(fixture, store, resolver.resolve);
		await anchor.ingest(entryBytes(fixture.chain[0].entry));
		await anchor.ingest(entryBytes(children[0]!.entry));

		const replacementGate = store.gateNextAtomicReplace();
		const fork = anchor.ingest(entryBytes(children[1]!.entry));
		await replacementGate.entered;
		const queuedAfterFork = anchor.ingest(entryBytes(children[2]!.entry));
		replacementGate.release();
		expect((await fork).status).to.equal("forked");
		const callsAfterFork = {
			get: store.getCalls,
			atomicReplace: store.atomicReplaceCalls,
		};

		expect((await queuedAfterFork).status).to.equal("halted");
		expect(anchor.state).to.equal("FORKED");
		expect(store.getCalls).to.equal(callsAfterFork.get);
		expect(store.atomicReplaceCalls).to.equal(callsAfterFork.atomicReplace);
	});

	it("retains the complete hard-bounded fork transition", async () => {
		const fixture = await createChain();
		const subjects = await Promise.all(
			Array.from({ length: MAX_PENDING_POLICIES }, () =>
				Ed25519Keypair.create(),
			),
		);
		const children = await Promise.all(
			subjects.map((subject) =>
				createDirectChild(
					fixture,
					TrustedNetworkRole.WRITER,
					subject.publicKey,
				),
			),
		);
		const store = new ControlledPolicyAnchorStore();
		const resolver = createResolver();
		const anchor = await openAnchor(fixture, store, resolver.resolve);
		for (const child of children) {
			expect((await anchor.ingest(entryBytes(child.entry))).status).to.equal(
				"pending",
			);
		}
		expect(anchor.pendingCount).to.equal(MAX_PENDING_POLICIES);
		expect(store.atomicReplaceCalls).to.equal(0);

		resolver.add(fixture.chain[0]);
		const forked = await anchor.ingest(entryBytes(fixture.chain[0].entry));
		expect(forked.status).to.equal("forked");
		expect(forked.forkObservations).to.have.length(MAX_PENDING_POLICIES);
		expect(anchor.state).to.equal("FORKED");
		expect(anchor.pendingCount).to.equal(0);
		expect(store.atomicReplaceCalls).to.equal(1);
		expect(checkpointKeys(store)).to.have.length(1);
		expect([...store.values.keys()]).to.deep.equal(checkpointKeys(store));
		expect(
			TRUSTED_NETWORK_V2_MAX_POLICY_ANCHOR_CHECKPOINT_PAYLOAD_BYTES,
		).to.equal(
			(MAX_FORK_CHILDREN + 1) * TRUSTED_NETWORK_V2_MAX_POLICY_ENTRY_BYTES + 313,
		);
		expect(
			store.values.get(checkpointKeys(store)[0]!)!.byteLength,
		).to.be.at.most(
			TRUSTED_NETWORK_V2_MAX_POLICY_ANCHOR_CHECKPOINT_PAYLOAD_BYTES + 120,
		);
		const rawEvidenceBytes =
			entryBytes(fixture.chain[0].entry).byteLength +
			children.reduce(
				(total, child) => total + entryBytes(child.entry).byteLength,
				0,
			);
		expect(rawEvidenceBytes).to.be.at.most(
			(MAX_FORK_CHILDREN + 1) * TRUSTED_NETWORK_V2_MAX_POLICY_ENTRY_BYTES,
		);

		const expectedCanonical = children
			.map(({ digest }) => digest)
			.sort(compare)
			.slice(0, 2)
			.map(hex);
		expect(
			anchor.forkEvidence!.children.map(({ digest }) => hex(digest)),
		).to.deep.equal(expectedCanonical);
		anchor.abort();

		const reopened = await openAnchor(fixture, store);
		expect(reopened.state).to.equal("FORKED");
		expect(
			reopened.forkEvidence!.children.map(({ digest }) => hex(digest)),
		).to.deep.equal(expectedCanonical);
	});

	it("round trips the reachable maximum of 65 committed fork proofs", async () => {
		const fixture = await createChain();
		const subjects = await Promise.all(
			Array.from({ length: MAX_PENDING_POLICIES }, () =>
				Ed25519Keypair.create(),
			),
		);
		const siblings = await Promise.all(
			subjects.map((subject) =>
				createDirectChild(
					fixture,
					TrustedNetworkRole.REPLICATOR,
					subject.publicKey,
				),
			),
		);
		const store = new ControlledPolicyAnchorStore();
		const resolver = createResolver();
		resolver.add(fixture.chain[0]);
		const anchor = await openAnchor(fixture, store, resolver.resolve);
		expect(
			(await anchor.ingest(entryBytes(fixture.chain[0].entry))).status,
		).to.equal("accepted");
		expect(
			(await anchor.ingest(entryBytes(fixture.chain[1].entry))).status,
		).to.equal("accepted");

		// With the accepted branch's parent temporarily unavailable, one blocked
		// comparison plus 63 siblings fills the fixed pending set. Retry removes
		// the comparison candidate before combining it with the accepted child and
		// the remaining 63, yielding the reachable maximum of 65 exact proofs.
		resolver.remove(fixture.chain[0]);
		for (const sibling of siblings) {
			expect((await anchor.ingest(entryBytes(sibling.entry))).status).to.equal(
				"unavailable",
			);
		}
		expect(anchor.pendingCount).to.equal(MAX_PENDING_POLICIES);
		resolver.add(fixture.chain[0]);
		const forked = await anchor.retryUnavailable();
		expect(forked.status).to.equal("forked");
		expect(forked.forkObservations).to.have.length(MAX_PENDING_POLICIES + 1);
		expect(checkpointKeys(store)).to.have.length(2);
		anchor.abort();

		const reopened = await openAnchor(fixture, store, resolver.resolve);
		expect(reopened.state).to.equal("FORKED");
		expect(reopened.forkEvidence?.children).to.have.length(2);
	});

	it("performs no store work across 100,000 post-FORK admissions", async () => {
		const fixture = await createChain();
		const children = await Promise.all([
			createDirectChild(fixture, TrustedNetworkRole.WRITER),
			createDirectChild(fixture, TrustedNetworkRole.READER),
		]);
		const store = new ControlledPolicyAnchorStore();
		const resolver = createResolver();
		resolver.add(fixture.chain[0]);
		const anchor = await openAnchor(fixture, store, resolver.resolve);
		await anchor.ingest(entryBytes(fixture.chain[0].entry));
		await anchor.ingest(entryBytes(children[0]!.entry));
		expect(
			(await anchor.ingest(entryBytes(children[1]!.entry))).status,
		).to.equal("forked");
		const callsAtFork = {
			get: store.getCalls,
			atomicReplace: store.atomicReplaceCalls,
		};
		const logicalBytesAtFork = [...store.values.values()].reduce(
			(total, value) => total + value.byteLength,
			0,
		);

		const internal = anchor as unknown as {
			published: { head?: unknown };
			core: { state: unknown };
		};
		Object.defineProperty(internal.published, "head", {
			configurable: true,
			get: () => {
				throw new Error("post-fork rejection must not read or copy the head");
			},
		});
		Object.defineProperty(internal.core, "state", {
			configurable: true,
			get: () => {
				throw new Error("post-fork rejection must not inspect core state");
			},
		});
		let finalResult: PolicyAdmissionResultV2 | undefined;
		for (let index = 0; index < 100_000; index++) {
			finalResult = await anchor.ingest(Uint8Array.of(index & 0xff));
		}
		const retryResult = await anchor.retryUnavailable();

		expect(finalResult?.status).to.equal("halted");
		expect(finalResult?.head).to.equal(undefined);
		expect(retryResult.status).to.equal("halted");
		expect(retryResult.head).to.equal(undefined);
		expect(anchor.state).to.equal("FORKED");
		expect(store.getCalls).to.equal(callsAtFork.get);
		expect(store.atomicReplaceCalls).to.equal(callsAtFork.atomicReplace);
		expect(
			[...store.values.values()].reduce(
				(total, value) => total + value.byteLength,
				0,
			),
		).to.equal(logicalBytesAtFork);
	});

	it("binds every checkpoint to the exact canonical network descriptor", async () => {
		const fixture = await createChain();
		const store = new ControlledPolicyAnchorStore();
		const anchor = await openAnchor(fixture, store);
		for (const policy of fixture.chain.slice(0, 3)) {
			await anchor.ingest(entryBytes(policy.entry));
		}
		anchor.abort();

		const otherFixture = await createChain();
		expect(
			(await rejection(openAnchor(otherFixture, store.clone()))).message,
		).to.match(/different scope/i);
	});

	it("rejects semantically invalid application payloads inside a valid checkpoint envelope", async () => {
		const fixture = await createChain();
		const store = new ControlledPolicyAnchorStore();
		await commitTestCheckpointPayload(
			fixture,
			store,
			new TestPolicyAnchorCheckpointPayloadV2({
				state: 1,
				acceptedHeadEntryBytes: entryBytes(fixture.chain[0].entry),
				comparisonCandidateEntryBytes: entryBytes(fixture.chain[1].entry),
				acceptedAncestorDigest: ZERO_DIGEST,
				unavailableReason: "",
				forkObservationEntryBytes: [],
			}),
		);

		expect(checkpointKeys(store)).to.have.length(1);
		expect((await rejection(openAnchor(fixture, store))).message).to.match(
			/active comparison candidate.*empty/i,
		);
	});

	it("pins and commits the exact maximum checkpoint payload framing", async () => {
		const maximumParent = new Uint8Array(
			TRUSTED_NETWORK_V2_MAX_POLICY_ENTRY_BYTES,
		);
		const maximumObservations = Array.from(
			{ length: MAX_FORK_CHILDREN },
			(_, index) => {
				const entry = new Uint8Array(TRUSTED_NETWORK_V2_MAX_POLICY_ENTRY_BYTES);
				entry[0] = index;
				return entry;
			},
		);
		const encoded = serialize(
			new TestPolicyAnchorCheckpointPayloadV2({
				state: 3,
				acceptedHeadEntryBytes: maximumParent,
				comparisonCandidateEntryBytes: new Uint8Array(0),
				acceptedAncestorDigest: ZERO_DIGEST,
				unavailableReason: "",
				forkObservationEntryBytes: maximumObservations,
			}),
		);

		expect(encoded.byteLength).to.equal(
			TRUSTED_NETWORK_V2_MAX_POLICY_ANCHOR_CHECKPOINT_PAYLOAD_BYTES,
		);
		expect(encoded.byteLength).to.equal(
			(MAX_FORK_CHILDREN + 1) * TRUSTED_NETWORK_V2_MAX_POLICY_ENTRY_BYTES + 313,
		);

		const store = new ControlledPolicyAnchorStore();
		const checkpoint = await CrashSafeTwoSlotCheckpoint.open({
			store,
			scope: Uint8Array.of(1),
			maxPayloadBytes:
				TRUSTED_NETWORK_V2_MAX_POLICY_ANCHOR_CHECKPOINT_PAYLOAD_BYTES,
		});
		await checkpoint.commit(encoded);
		expect(store.values.get(checkpointKeys(store)[0]!)!.byteLength).to.equal(
			TRUSTED_NETWORK_V2_MAX_POLICY_ANCHOR_CHECKPOINT_PAYLOAD_BYTES + 120,
		);
	});

	it("restores the 66-observation ceiling and rejects a valid 67th observation", async () => {
		const fixture = await createChain();
		const subjects = await Promise.all(
			Array.from({ length: MAX_FORK_CHILDREN + 1 }, () =>
				Ed25519Keypair.create(),
			),
		);
		const children = await Promise.all(
			subjects.map((subject) =>
				createDirectChild(
					fixture,
					TrustedNetworkRole.READER,
					subject.publicKey,
				),
			),
		);
		const payload = (observations: PolicyFixture[]) =>
			new TestPolicyAnchorCheckpointPayloadV2({
				state: 3,
				acceptedHeadEntryBytes: entryBytes(fixture.chain[0].entry),
				comparisonCandidateEntryBytes: new Uint8Array(0),
				acceptedAncestorDigest: ZERO_DIGEST,
				unavailableReason: "",
				forkObservationEntryBytes: canonicalObservationBytes(observations),
			});

		const atCeiling = new ControlledPolicyAnchorStore();
		await commitTestCheckpointPayload(
			fixture,
			atCeiling,
			payload(children.slice(0, MAX_FORK_CHILDREN)),
		);
		const restored = await openAnchor(fixture, atCeiling);
		expect(restored.state).to.equal("FORKED");
		expect(restored.forkEvidence?.children).to.have.length(2);

		const overCeiling = new ControlledPolicyAnchorStore();
		await commitTestCheckpointPayload(fixture, overCeiling, payload(children));
		expect(
			(await rejection(openAnchor(fixture, overCeiling))).message,
		).to.match(/2-66 observations/i);
	});

	it("persists every sibling discovered when a pending set drains into a fork", async () => {
		const fixture = await createChain();
		const children = await Promise.all([
			createDirectChild(fixture, TrustedNetworkRole.WRITER),
			createDirectChild(fixture, TrustedNetworkRole.READER),
			createDirectChild(fixture, TrustedNetworkRole.REPLICATOR),
			createDirectChild(
				fixture,
				TrustedNetworkRole.WRITER | TrustedNetworkRole.READER,
			),
		]);
		const store = new ControlledPolicyAnchorStore();
		const resolver = createResolver();
		const anchor = await openAnchor(fixture, store, resolver.resolve);
		const deliveredChildren = [...children].sort((left, right) =>
			compare(right.digest, left.digest),
		);

		// Deliberately oppose delivery and digest order. Pending drain performs its
		// own deterministic ordering; this case verifies the complete plural seam.
		for (const child of deliveredChildren) {
			expect((await anchor.ingest(entryBytes(child.entry))).status).to.equal(
				"pending",
			);
		}
		expect(anchor.pendingCount).to.equal(4);
		expect(store.atomicReplaceCalls).to.equal(0);
		resolver.add(fixture.chain[0]);
		const drained = await anchor.ingest(entryBytes(fixture.chain[0].entry));
		expect(drained.status).to.equal("forked");
		expect(drained.forkObservations).to.have.length(4);
		expect(
			(drained.forkObservations ?? [])
				.map(({ digest }) => digest)
				.sort(compare)
				.map(hex),
		).to.deep.equal(
			children
				.map(({ digest }) => digest)
				.sort(compare)
				.map(hex),
		);
		expect(anchor.state).to.equal("FORKED");
		expect(anchor.pendingCount).to.equal(0);
		expect(store.atomicReplaceCalls).to.equal(1);
		expect(checkpointKeys(store)).to.have.length(1);
		expect([...store.values.keys()]).to.deep.equal(checkpointKeys(store));
		const expectedCanonical = children
			.map(({ digest }) => digest)
			.sort(compare)
			.slice(0, 2)
			.map(hex);
		expect(
			anchor.forkEvidence!.children.map(({ digest }) => hex(digest)),
		).to.deep.equal(expectedCanonical);
		anchor.abort();

		const reopened = await openAnchor(fixture, store, resolver.resolve);
		expect(reopened.state).to.equal("FORKED");
		expect(reopened.forkEvidence?.children).to.have.length(2);
		expect(
			reopened.forkEvidence!.children.map(({ digest }) => hex(digest)),
		).to.deep.equal(expectedCanonical);

		const reorderedStore = new ControlledPolicyAnchorStore();
		const reorderedResolver = createResolver();
		const reordered = await openAnchor(
			fixture,
			reorderedStore,
			reorderedResolver.resolve,
		);
		for (const child of [...deliveredChildren].reverse()) {
			expect((await reordered.ingest(entryBytes(child.entry))).status).to.equal(
				"pending",
			);
		}
		reorderedResolver.add(fixture.chain[0]);
		expect(
			(await reordered.ingest(entryBytes(fixture.chain[0].entry))).status,
		).to.equal("forked");
		expect(
			[...reorderedStore.values].map(([key, value]) => [key, hex(value)]),
		).to.deep.equal([...store.values].map(([key, value]) => [key, hex(value)]));
	});

	it("durably retains an evaluation child displaced by pending canonical selection", async () => {
		const fixture = await createChain();
		const siblings = (
			await Promise.all([
				createDirectChild(fixture, TrustedNetworkRole.WRITER),
				createDirectChild(fixture, TrustedNetworkRole.READER),
				createDirectChild(fixture, TrustedNetworkRole.REPLICATOR),
			])
		).sort((left, right) => compare(left.digest, right.digest));
		const [low, middle, high] = siblings as [
			PolicyFixture,
			PolicyFixture,
			PolicyFixture,
		];
		const store = new ControlledPolicyAnchorStore();
		const resolver = createResolver();
		resolver.add(fixture.chain[0]);
		const anchor = await openAnchor(fixture, store, resolver.resolve);

		expect(
			(await anchor.ingest(entryBytes(fixture.chain[0].entry))).status,
		).to.equal("accepted");
		expect((await anchor.ingest(entryBytes(high.entry))).status).to.equal(
			"accepted",
		);
		expectHead(anchor, high);

		resolver.remove(fixture.chain[0]);
		expect((await anchor.ingest(entryBytes(middle.entry))).status).to.equal(
			"unavailable",
		);
		expect(anchor.state).to.equal("UNAVAILABLE");
		expect(checkpointKeys(store)).to.have.length(2);
		const replacementsAtUnavailable = store.atomicReplaceCalls;
		expect((await anchor.ingest(entryBytes(low.entry))).status).to.equal(
			"unavailable",
		);
		expect(anchor.pendingCount).to.equal(2);
		// Candidate-only pending growth does not create another durable state.
		expect(checkpointKeys(store)).to.have.length(2);
		expect(store.atomicReplaceCalls).to.equal(replacementsAtUnavailable);

		resolver.add(fixture.chain[0]);
		const forked = await anchor.retryUnavailable();
		expect(forked.status).to.equal("forked");
		expect(forked.forkObservations).to.have.length(3);
		expect(
			(forked.forkObservations ?? [])
				.map(({ entryBytes: bytes }) => hex(bytes))
				.sort(),
		).to.deep.equal(siblings.map(({ entry }) => hex(entryBytes(entry))).sort());
		expect(anchor.state).to.equal("FORKED");
		expect(
			anchor.forkEvidence!.children.map(({ digest }) => hex(digest)),
		).to.deep.equal([hex(low.digest), hex(middle.digest)]);

		// One replacement publishes the complete fork, including the displaced
		// evaluation child, in the next fixed checkpoint slot.
		expect(store.atomicReplaceCalls).to.equal(replacementsAtUnavailable + 1);
		expect(checkpointKeys(store)).to.have.length(2);
		expect([...store.values.keys()].sort()).to.deep.equal(
			checkpointKeys(store),
		);
		const latestRecord = store.values.get(
			checkpointKeys(store).find((key) => key.endsWith(":b"))!,
		)!;
		for (const sibling of siblings) {
			expect(
				Buffer.from(latestRecord).indexOf(
					Buffer.from(entryBytes(sibling.entry)),
				),
			).to.be.greaterThan(-1);
		}
		anchor.abort();

		const reopened = await openAnchor(fixture, store);
		expect(reopened.state).to.equal("FORKED");
		expect(
			reopened.forkEvidence!.children.map(({ digest }) => hex(digest)),
		).to.deep.equal([hex(low.digest), hex(middle.digest)]);
	});

	it("rejects every legacy append namespace and a pre-aborted open before storage access", async () => {
		const fixture = await createChain();
		const legacyOwner = "peerbit/trusted-network/v2/policy-anchor/v1";
		for (const key of [
			legacyOwner,
			`${legacyOwner}/generation/00000000000000000001`,
		]) {
			const legacy = new ControlledPolicyAnchorStore();
			legacy.values.set(key, Uint8Array.of(1));
			expect((await rejection(openAnchor(fixture, legacy))).message).to.match(
				/legacy|append|unsupported/i,
			);
		}

		const mixed = new ControlledPolicyAnchorStore();
		const anchor = await openAnchor(fixture, mixed);
		await anchor.ingest(entryBytes(fixture.chain[0].entry));
		anchor.abort();
		mixed.values.set(
			`${legacyOwner}/generation/00000000000000000001`,
			Uint8Array.of(1),
		);
		expect((await rejection(openAnchor(fixture, mixed))).message).to.match(
			/legacy|append|unsupported/i,
		);

		const aborted = new ControlledPolicyAnchorStore();
		const controller = new AbortController();
		controller.abort();
		const error = await rejection(
			TrustedNetworkV2DurablePolicyReducer.open({
				descriptor: fixture.descriptor,
				resolvePolicyEntry: () => undefined,
				store: aborted,
				signal: controller.signal,
			}),
		);
		expect(error.message).to.match(/aborted/i);
		expect(aborted.barrierCalls).to.equal(0);
		expect(aborted.iteratorCalls).to.equal(0);
		expect(aborted.getCalls).to.equal(0);
	});
});
