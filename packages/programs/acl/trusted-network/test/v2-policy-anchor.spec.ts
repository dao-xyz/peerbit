import { serialize } from "@dao-xyz/borsh";
import { Ed25519Keypair, PublicSignKey } from "@peerbit/crypto";
import { EntryV0 } from "@peerbit/log";
import { expect } from "chai";
import { compare } from "uint8arrays";
import {
	type CrashSafePolicyAnchorStoreV2,
	TRUSTED_NETWORK_V2_POLICY_ANCHOR_STORE_OWNER,
	TrustedNetworkV2DurablePolicyReducer,
} from "../src/v2-policy-anchor.js";
import {
	NetworkDescriptorV2,
	PolicySnapshotBodyV2,
	PolicySubjectBindingV2,
	TRUSTED_NETWORK_V2_ENTRY_V0_AUTHORITY_ONLY_SIGNATURE_PROFILE,
	TRUSTED_NETWORK_V2_POLICY_HASH_SHA256,
	TRUSTED_NETWORK_V2_PROTOCOL_VERSION,
	TrustedNetworkRole,
	deriveNetworkIdV2,
	digestPolicySnapshotBodyV2,
} from "../src/v2.js";

const ZERO_DIGEST = new Uint8Array(32);
// Outer record variant (3), body variant (3), version (1), generation (8),
// descriptor (32), and prior checksum (32) precede the generation-kind byte.
const POLICY_ANCHOR_GENERATION_KIND_OFFSET = 3 + 3 + 1 + 8 + 32 + 32;
const POLICY_ANCHOR_STATE_GENERATION_KIND = 1;
const POLICY_ANCHOR_OBSERVATION_GENERATION_KIND = 2;

const hex = (bytes: Uint8Array): string => Buffer.from(bytes).toString("hex");

const entryBytes = (entry: EntryV0<Uint8Array>): Uint8Array => serialize(entry);

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
): Promise<PolicyFixture> =>
	createPolicy({
		descriptor: fixture.descriptor,
		sequence: 1n,
		previousPolicyDigest: fixture.chain[0].digest,
		bindings: [
			[fixture.authority.publicKey, TrustedNetworkRole.ADMIN],
			[fixture.alice.publicKey, roles],
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
	| { kind: "pass" }
	| {
			kind: "gate";
			entered: ReturnType<typeof deferred>;
			release: Promise<void>;
	  }
	| { kind: "throw-before"; error: Error }
	| { kind: "commit-then-throw"; error: Error };

class ControlledPolicyAnchorStore implements CrashSafePolicyAnchorStoreV2 {
	readonly values: Map<string, Uint8Array>;
	readonly successfulPutKeys: string[] = [];
	putCalls = 0;
	barrierCalls = 0;
	iteratorCalls = 0;
	private readonly putActions: StoreAction[] = [];
	private readonly barrierActions: StoreAction[] = [];

	constructor(values?: Map<string, Uint8Array>) {
		this.values = new Map(
			[...(values ?? [])].map(([key, value]) => [key, Uint8Array.from(value)]),
		);
	}

	readonly crashSafeDurability = {
		crashSafe: true as const,
		barrier: async (): Promise<void> => this.barrier(),
	};

	get(key: string): Uint8Array | undefined {
		const value = this.values.get(key);
		return value === undefined ? undefined : Uint8Array.from(value);
	}

	async put(key: string, value: Uint8Array): Promise<void> {
		this.putCalls += 1;
		const action = this.putActions.shift();
		if (action?.kind === "gate") {
			action.entered.resolve();
			await action.release;
		}
		if (action?.kind === "throw-before") throw action.error;
		this.values.set(key, Uint8Array.from(value));
		this.successfulPutKeys.push(key);
		if (action?.kind === "commit-then-throw") throw action.error;
	}

	async *iterator(): AsyncGenerator<[string, Uint8Array], void, void> {
		this.iteratorCalls += 1;
		for (const [key, value] of this.values) {
			yield [key, Uint8Array.from(value)];
		}
	}

	gateNextPut(): Gate {
		const entered = deferred();
		const release = deferred();
		this.putActions.push({ kind: "gate", entered, release: release.promise });
		return { entered: entered.promise, release: () => release.resolve() };
	}

	gateNextBarrier(): Gate {
		const entered = deferred();
		const release = deferred();
		this.barrierActions.push({
			kind: "gate",
			entered,
			release: release.promise,
		});
		return { entered: entered.promise, release: () => release.resolve() };
	}

	failNextPut(error: Error, afterCommit = false): void {
		this.putActions.push({
			kind: afterCommit ? "commit-then-throw" : "throw-before",
			error,
		});
	}

	passNextPut(): void {
		this.putActions.push({ kind: "pass" });
	}

	failNextBarrier(error: Error): void {
		this.barrierActions.push({ kind: "throw-before", error });
	}

	clone(): ControlledPolicyAnchorStore {
		return new ControlledPolicyAnchorStore(this.values);
	}

	private async barrier(): Promise<void> {
		this.barrierCalls += 1;
		const action = this.barrierActions.shift();
		if (action?.kind === "gate") {
			action.entered.resolve();
			await action.release;
		}
		if (action?.kind === "throw-before") throw action.error;
		if (action?.kind === "commit-then-throw") throw action.error;
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
	});

	it("fails authorization closed until put and barrier both complete", async () => {
		const fixture = await createChain();
		const store = new ControlledPolicyAnchorStore();
		const anchor = await openAnchor(fixture, store);
		expect(
			(await anchor.ingest(entryBytes(fixture.chain[0].entry))).status,
		).to.equal("accepted");
		expect(
			anchor.isAuthorized(fixture.alice.publicKey, TrustedNetworkRole.WRITER),
		).to.equal(true);

		const putGate = store.gateNextPut();
		const acceptingOne = anchor.ingest(entryBytes(fixture.chain[1].entry));
		await putGate.entered;
		expect(
			anchor.isAuthorized(fixture.alice.publicKey, TrustedNetworkRole.WRITER),
		).to.equal(false);
		expect(
			anchor.isAuthorized(fixture.alice.publicKey, TrustedNetworkRole.READER),
		).to.equal(false);
		putGate.release();
		expect((await acceptingOne).status).to.equal("accepted");
		expect(
			anchor.isAuthorized(fixture.alice.publicKey, TrustedNetworkRole.READER),
		).to.equal(true);

		const barrierGate = store.gateNextBarrier();
		const acceptingTwo = anchor.ingest(entryBytes(fixture.chain[2].entry));
		await barrierGate.entered;
		expect(
			anchor.isAuthorized(fixture.bob.publicKey, TrustedNetworkRole.WRITER),
		).to.equal(false);
		expect(
			anchor.isAuthorized(fixture.bob.publicKey, TrustedNetworkRole.REPLICATOR),
		).to.equal(false);
		barrierGate.release();
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
		anchor.abort();
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
	});

	it("does not make a candidate-only PENDING working set durable", async () => {
		const fixture = await createChain();
		const store = new ControlledPolicyAnchorStore();
		const anchor = await openAnchor(fixture, store);
		await anchor.ingest(entryBytes(fixture.chain[0].entry));
		const putsBeforePending = store.putCalls;
		const barriersBeforePending = store.barrierCalls;

		const pending = await anchor.ingest(entryBytes(fixture.chain[2].entry));
		expect(pending.status).to.equal("pending");
		expect(anchor.pendingCount).to.equal(1);
		expect(store.putCalls).to.equal(putsBeforePending);
		expect(store.barrierCalls).to.equal(barriersBeforePending);
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

	it("halts on definite and ambiguous persistence failures and reopens the valid prefix", async () => {
		const cases: Array<{
			name: string;
			arm: (store: ControlledPolicyAnchorStore) => void;
			expectedHead: 0 | 1;
		}> = [
			{
				name: "put before commit",
				arm: (store) => store.failNextPut(new Error("put failed")),
				expectedHead: 0,
			},
			{
				name: "put after an ambiguous commit",
				arm: (store) =>
					store.failNextPut(new Error("put outcome ambiguous"), true),
				expectedHead: 1,
			},
			{
				name: "barrier after the generation put",
				arm: (store) => store.failNextBarrier(new Error("barrier failed")),
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

	it("serializes concurrent admissions and makes exact duplicates write-free", async () => {
		const fixture = await createChain();
		const store = new ControlledPolicyAnchorStore();
		const anchor = await openAnchor(fixture, store);
		await anchor.ingest(entryBytes(fixture.chain[0].entry));
		const putsBeforeDuplicate = store.putCalls;
		const barriersBeforeDuplicate = store.barrierCalls;

		expect(
			(await anchor.ingest(entryBytes(fixture.chain[0].entry))).status,
		).to.equal("duplicate");
		expect(store.putCalls).to.equal(putsBeforeDuplicate);
		expect(store.barrierCalls).to.equal(barriersBeforeDuplicate);

		const putGate = store.gateNextPut();
		const first = anchor.ingest(entryBytes(fixture.chain[1].entry));
		const second = anchor.ingest(entryBytes(fixture.chain[2].entry));
		await putGate.entered;
		const putsWhileFirstIsBlocked = store.putCalls;
		await Promise.resolve();
		expect(store.putCalls).to.equal(putsWhileFirstIsBlocked);
		expect(
			anchor.isAuthorized(fixture.alice.publicKey, TrustedNetworkRole.READER),
		).to.equal(false);
		putGate.release();

		expect((await first).status).to.equal("accepted");
		expect((await second).status).to.equal("accepted");
		expect(anchor.state).to.equal("ACTIVE");
		expectHead(anchor, fixture.chain[2]);
	});

	it("durably retains every authenticated fork child while bounding the canonical pair", async () => {
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
		const canonicalDigests = (observed: PolicyFixture[]): string[] =>
			observed
				.map(({ digest }) => digest)
				.sort(compare)
				.slice(0, 2)
				.map(hex);
		const generationPrefix = `${TRUSTED_NETWORK_V2_POLICY_ANCHOR_STORE_OWNER}/generation/`;
		const generationKeys = (): string[] =>
			[...store.values.keys()]
				.filter((key) => key.startsWith(generationPrefix))
				.sort();
		await anchor.ingest(entryBytes(fixture.chain[0].entry));
		expect(
			(await anchor.ingest(entryBytes(deliveredChildren[0].entry))).status,
		).to.equal("accepted");
		expect(
			(await anchor.ingest(entryBytes(deliveredChildren[1].entry))).status,
		).to.equal("forked");
		expect(generationKeys()).to.have.length(3);
		const canonicalBeforeObservation = anchor.forkEvidence!.children.map(
			({ digest }) => hex(digest),
		);

		// This smaller, post-FORKED child displaces one member of the canonical
		// pair. It must append one observation delta, not repeat the FORKED state.
		expect(
			(await anchor.ingest(entryBytes(deliveredChildren[2].entry))).status,
		).to.equal("halted");
		expect(generationKeys()).to.have.length(4);
		expect(
			anchor.forkEvidence!.children.map(({ digest }) => hex(digest)),
		).to.deep.equal(canonicalDigests(deliveredChildren.slice(0, 3)));
		expect(
			anchor.forkEvidence!.children.map(({ digest }) => hex(digest)),
		).not.to.deep.equal(canonicalBeforeObservation);
		expect(
			store.values.get(generationKeys()[2]!)![
				POLICY_ANCHOR_GENERATION_KIND_OFFSET
			],
		).to.equal(POLICY_ANCHOR_STATE_GENERATION_KIND);
		expect(
			store.values.get(generationKeys()[3]!)![
				POLICY_ANCHOR_GENERATION_KIND_OFFSET
			],
		).to.equal(POLICY_ANCHOR_OBSERVATION_GENERATION_KIND);

		expect(
			(await anchor.ingest(entryBytes(deliveredChildren[3].entry))).status,
		).to.equal("halted");
		// Genesis, first child, fork state, and one immutable observation
		// generation for each later unique child.
		expect(generationKeys()).to.have.length(5);
		expect(anchor.state).to.equal("FORKED");
		expect(anchor.forkEvidence?.children).to.have.length(2);
		const expectedCanonical = canonicalDigests(children);
		expect(
			anchor.forkEvidence!.children.map(({ digest }) => hex(digest)),
		).to.deep.equal(expectedCanonical);

		const putsBeforeDuplicate = store.putCalls;
		const barriersBeforeDuplicate = store.barrierCalls;
		expect(
			(await anchor.ingest(entryBytes(deliveredChildren[3].entry))).status,
		).to.equal("halted");
		expect(store.putCalls).to.equal(putsBeforeDuplicate);
		expect(store.barrierCalls).to.equal(barriersBeforeDuplicate);
		anchor.abort();

		const reopened = await openAnchor(fixture, store);
		expect(reopened.state).to.equal("FORKED");
		expect(reopened.forkEvidence?.children).to.have.length(2);
		expect(
			reopened.forkEvidence!.children.map(({ digest }) => hex(digest)),
		).to.deep.equal(expectedCanonical);
	});

	it("fails closed on truncated, corrupt, wrong-version, wrong-descriptor, and gapped state", async () => {
		const fixture = await createChain();
		const store = new ControlledPolicyAnchorStore();
		const anchor = await openAnchor(fixture, store);
		for (const policy of fixture.chain.slice(0, 3)) {
			await anchor.ingest(entryBytes(policy.entry));
		}
		anchor.abort();

		const generationPrefix = `${TRUSTED_NETWORK_V2_POLICY_ANCHOR_STORE_OWNER}/generation/`;
		const generationKeys = [...store.values.keys()]
			.filter((key) => key.startsWith(generationPrefix))
			.sort();
		expect(generationKeys).to.have.length(3);
		const latestKey = generationKeys[2]!;

		const truncated = store.clone();
		const fullRecord = truncated.values.get(latestKey)!;
		truncated.values.set(latestKey, fullRecord.slice(0, -1));
		expect((await rejection(openAnchor(fixture, truncated))).message).to.match(
			/decode|deserialize|buffer|range|record/i,
		);

		const corrupt = store.clone();
		const corruptRecord = Uint8Array.from(corrupt.values.get(latestKey)!);
		corruptRecord[corruptRecord.byteLength - 1] ^= 1;
		corrupt.values.set(latestKey, corruptRecord);
		expect((await rejection(openAnchor(fixture, corrupt))).message).to.match(
			/checksum|canonical|signature|decode/i,
		);

		const unsupported = store.clone();
		const unsupportedRecord = Uint8Array.from(
			unsupported.values.get(latestKey)!,
		);
		expect([...unsupportedRecord.slice(0, 7)]).to.deep.equal([
			2, 16, 2, 2, 16, 1, 1,
		]);
		unsupportedRecord[6] = 2;
		unsupported.values.set(latestKey, unsupportedRecord);
		expect(
			(await rejection(openAnchor(fixture, unsupported))).message,
		).to.match(/unsupported.*format/i);

		const gapped = store.clone();
		gapped.values.delete(generationKeys[1]!);
		expect((await rejection(openAnchor(fixture, gapped))).message).to.match(
			/gapped/i,
		);

		const otherFixture = await createChain();
		expect(
			(await rejection(openAnchor(otherFixture, store.clone()))).message,
		).to.match(/another descriptor/i);
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
		expect(store.putCalls).to.equal(0);
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

		const generationPrefix = `${TRUSTED_NETWORK_V2_POLICY_ANCHOR_STORE_OWNER}/generation/`;
		const generationKeys = [...store.values.keys()].filter((key) =>
			key.startsWith(generationPrefix),
		);
		// One FORKED state generation, followed by one self-contained generation
		// for each of the two non-canonical observations.
		expect(generationKeys).to.have.length(3);
		expect([...store.values.keys()]).to.deep.equal(generationKeys);
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
		const generationPrefix = `${TRUSTED_NETWORK_V2_POLICY_ANCHOR_STORE_OWNER}/generation/`;
		const generationKeys = (): string[] =>
			[...store.values.keys()]
				.filter((key) => key.startsWith(generationPrefix))
				.sort();

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
		expect(generationKeys()).to.have.length(3);
		expect((await anchor.ingest(entryBytes(low.entry))).status).to.equal(
			"unavailable",
		);
		expect(anchor.pendingCount).to.equal(2);
		// Candidate-only pending growth does not create another durable state.
		expect(generationKeys()).to.have.length(3);

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

		// The transition appends a FORKED state carrying low+middle, then one
		// observation delta retaining the displaced high evaluation child.
		expect(generationKeys()).to.have.length(5);
		expect([...store.values.keys()]).to.deep.equal(generationKeys());
		expect(
			store.values.get(generationKeys()[3]!)![
				POLICY_ANCHOR_GENERATION_KIND_OFFSET
			],
		).to.equal(POLICY_ANCHOR_STATE_GENERATION_KIND);
		expect(
			store.values.get(generationKeys()[4]!)![
				POLICY_ANCHOR_GENERATION_KIND_OFFSET
			],
		).to.equal(POLICY_ANCHOR_OBSERVATION_GENERATION_KIND);
		expect(
			Buffer.from(store.values.get(generationKeys()[4]!)!).indexOf(
				Buffer.from(entryBytes(high.entry)),
			),
		).to.be.greaterThan(-1);
		anchor.abort();

		const reopened = await openAnchor(fixture, store);
		expect(reopened.state).to.equal("FORKED");
		expect(
			reopened.forkEvidence!.children.map(({ digest }) => hex(digest)),
		).to.deep.equal([hex(low.digest), hex(middle.digest)]);
	});

	it("reopens the last complete FORKED prefix after a later plural generation fails", async () => {
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
		for (const child of children) {
			await anchor.ingest(entryBytes(child.entry));
		}
		resolver.add(fixture.chain[0]);
		store.passNextPut();
		store.passNextPut();
		store.failNextPut(new Error("second plural generation failed"));

		expect(
			(await rejection(anchor.ingest(entryBytes(fixture.chain[0].entry))))
				.message,
		).to.match(/ambiguous.*halted/i);
		expect(anchor.state).to.equal("HALTED");
		const generationPrefix = `${TRUSTED_NETWORK_V2_POLICY_ANCHOR_STORE_OWNER}/generation/`;
		const generationKeys = [...store.values.keys()].filter((key) =>
			key.startsWith(generationPrefix),
		);
		// The FORKED state and first extra observation were both fenced before
		// the second extra observation failed.
		expect(generationKeys).to.have.length(2);
		expect([...store.values.keys()]).to.deep.equal(generationKeys);

		const reopened = await openAnchor(fixture, store, resolver.resolve);
		expect(reopened.state).to.equal("FORKED");
		expect(reopened.forkEvidence?.children).to.have.length(2);
	});

	it("rejects unknown owned keys and a pre-aborted open before storage access", async () => {
		const fixture = await createChain();
		const unknown = new ControlledPolicyAnchorStore();
		unknown.values.set(
			`${TRUSTED_NETWORK_V2_POLICY_ANCHOR_STORE_OWNER}/unknown`,
			Uint8Array.of(1),
		);
		expect((await rejection(openAnchor(fixture, unknown))).message).to.match(
			/unknown|unrecognized|owned namespace/i,
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
	});
});
