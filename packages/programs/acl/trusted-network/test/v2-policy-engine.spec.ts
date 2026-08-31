import { serialize } from "@dao-xyz/borsh";
import { Ed25519Keypair, PublicSignKey, X25519Keypair } from "@peerbit/crypto";
import { EntryV0 } from "@peerbit/log";
import { expect } from "chai";
import { compare, concat } from "uint8arrays";
import { TrustedNetworkV2PolicyReducer } from "../src/v2-policy-engine.js";
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

const hex = (bytes: Uint8Array): string => Buffer.from(bytes).toString("hex");

const entryBytes = (entry: EntryV0<Uint8Array>): Uint8Array => serialize(entry);

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
	chain: PolicyFixture[];
};

const createEntry = async (
	bodyBytes: Uint8Array,
	identity: Ed25519Keypair,
	properties?: {
		signers?: Ed25519Keypair[];
		next?: EntryV0<Uint8Array>;
		metaData?: Uint8Array;
		encryption?: Parameters<typeof EntryV0.create>[0]["encryption"];
	},
): Promise<EntryV0<Uint8Array>> =>
	(await EntryV0.create({
		store: {} as never,
		data: bodyBytes,
		identity,
		deferStore: true,
		meta: {
			next: properties?.next ? [properties.next] : [],
			data: properties?.metaData,
		},
		signers: properties?.signers?.map((signer) => signer.sign.bind(signer)),
		encryption: properties?.encryption,
	})) as EntryV0<Uint8Array>;

const createPolicy = async (properties: {
	descriptor: NetworkDescriptorV2;
	sequence: bigint;
	previousPolicyDigest: Uint8Array;
	bindings: Array<[PublicSignKey, number]>;
	signer: Ed25519Keypair;
	signers?: Ed25519Keypair[];
	next?: EntryV0<Uint8Array>;
	bodyBytes?: (body: PolicySnapshotBodyV2) => Uint8Array;
	metaData?: Uint8Array;
	encryption?: Parameters<typeof EntryV0.create>[0]["encryption"];
}): Promise<PolicyFixture> => {
	const body = new PolicySnapshotBodyV2({
		networkId: deriveNetworkIdV2(properties.descriptor),
		sequence: properties.sequence,
		previousPolicyDigest: properties.previousPolicyDigest,
		bindings: sortedBindings(properties.bindings),
	});
	const digest = digestPolicySnapshotBodyV2(body);
	const entry = await createEntry(
		properties.bodyBytes?.(body) ?? serialize(body),
		properties.signer,
		{
			signers: properties.signers,
			next: properties.next,
			metaData: properties.metaData,
			encryption: properties.encryption,
		},
	);
	return { body, digest, entry };
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

const permutations = <T>(values: T[]): T[][] => {
	if (values.length < 2) return [values];
	return values.flatMap((value, index) =>
		permutations(values.filter((_, candidate) => candidate !== index)).map(
			(rest) => [value, ...rest],
		),
	);
};

const createResolver = () => {
	const entries = new Map<string, Uint8Array>();
	return {
		add: (fixture: PolicyFixture) =>
			entries.set(hex(fixture.digest), entryBytes(fixture.entry)),
		resolve: (digest: Uint8Array) => entries.get(hex(digest)),
	};
};

const createPolicyEntryBytesWithSize = async (
	fixture: ChainFixture,
	policy: PolicyFixture,
	targetBytes: number,
	next?: EntryV0<Uint8Array>,
): Promise<Uint8Array> => {
	const bodyBytes = serialize(policy.body);
	const baseline = entryBytes(
		await createEntry(bodyBytes, fixture.authority, {
			metaData: new Uint8Array(),
			next,
		}),
	);
	const paddingBytes = targetBytes - baseline.byteLength;
	if (paddingBytes < 0) throw new Error("Target entry size is below baseline");
	const bytes = entryBytes(
		await createEntry(bodyBytes, fixture.authority, {
			metaData: new Uint8Array(paddingBytes),
			next,
		}),
	);
	expect(bytes.byteLength).to.equal(targetBytes);
	return bytes;
};

describe("TrustedNetwork v2 policy reducer", () => {
	it("converges across every bounded delivery permutation and projects roles", async () => {
		const fixture = await createChain();
		for (const order of permutations(fixture.chain)) {
			const resolver = createResolver();
			const reducer = new TrustedNetworkV2PolicyReducer({
				descriptor: fixture.descriptor,
				resolvePolicyEntry: resolver.resolve,
				maxPending: fixture.chain.length,
			});
			for (const policy of order) {
				resolver.add(policy);
				await reducer.ingest(entryBytes(policy.entry));
			}

			expect(reducer.state).to.equal("ACTIVE");
			expect(reducer.head?.sequence).to.equal(3n);
			expect(hex(reducer.head!.digest)).to.equal(hex(fixture.chain[3]!.digest));
			expect(reducer.pendingCount).to.equal(0);
			expect(
				reducer.isAuthorized(
					fixture.alice.publicKey,
					TrustedNetworkRole.WRITER | TrustedNetworkRole.READER,
				),
			).to.be.true;
			expect(
				reducer.isAuthorized(fixture.alice.publicKey, TrustedNetworkRole.ADMIN),
			).to.be.false;
			expect(
				reducer.isAuthorized(
					fixture.bob.publicKey,
					TrustedNetworkRole.REPLICATOR,
				),
			).to.be.true;
			expect(
				reducer.isAuthorized(fixture.bob.publicKey, TrustedNetworkRole.WRITER),
			).to.be.false;
		}
	});

	it("advances directly from its retained head without resolver persistence", async () => {
		const fixture = await createChain();
		const reducer = new TrustedNetworkV2PolicyReducer({
			descriptor: fixture.descriptor,
			resolvePolicyEntry: () => undefined,
		});
		for (const policy of fixture.chain) {
			expect((await reducer.ingest(entryBytes(policy.entry))).status).to.equal(
				"accepted",
			);
		}
		expect(reducer.head?.sequence).to.equal(3n);
	});

	it("enforces the protocol entry ceiling before decoding direct input", async () => {
		const fixture = await createChain();
		for (const targetBytes of [
			TRUSTED_NETWORK_V2_MAX_POLICY_ENTRY_BYTES - 1,
			TRUSTED_NETWORK_V2_MAX_POLICY_ENTRY_BYTES,
		]) {
			const reducer = new TrustedNetworkV2PolicyReducer({
				descriptor: fixture.descriptor,
				resolvePolicyEntry: () => undefined,
			});
			const bytes = await createPolicyEntryBytesWithSize(
				fixture,
				fixture.chain[0]!,
				targetBytes,
			);
			expect((await reducer.ingest(bytes)).status).to.equal("accepted");
		}

		const atLimit = await createPolicyEntryBytesWithSize(
			fixture,
			fixture.chain[0]!,
			TRUSTED_NETWORK_V2_MAX_POLICY_ENTRY_BYTES,
		);
		const reducer = new TrustedNetworkV2PolicyReducer({
			descriptor: fixture.descriptor,
			resolvePolicyEntry: () => undefined,
		});
		const oversized = await reducer.ingest(
			concat([atLimit, new Uint8Array([0])]),
		);
		expect(oversized.status).to.equal("rejected");
		expect(oversized.reason).to.contain(
			String(TRUSTED_NETWORK_V2_MAX_POLICY_ENTRY_BYTES),
		);
		expect(oversized.reason).not.to.contain("deserial");
		expect(reducer.state).to.equal("EMPTY");

		const maxSizedChild = await createPolicyEntryBytesWithSize(
			fixture,
			fixture.chain[2]!,
			TRUSTED_NETWORK_V2_MAX_POLICY_ENTRY_BYTES,
			fixture.chain[1]!.entry,
		);
		const pendingReducer = new TrustedNetworkV2PolicyReducer({
			descriptor: fixture.descriptor,
			resolvePolicyEntry: () => undefined,
		});
		const pending = await pendingReducer.ingest(maxSizedChild);
		expect(pending.status).to.equal("pending");
		expect(pendingReducer.pendingCount).to.equal(1);
	});

	it("captures the intrinsic Uint8Array extent without iterator or length hooks", async () => {
		const fixture = await createChain();
		const valid = entryBytes(fixture.chain[0]!.entry);
		const freshReducer = (): TrustedNetworkV2PolicyReducer =>
			new TrustedNetworkV2PolicyReducer({
				descriptor: fixture.descriptor,
				resolvePolicyEntry: () => undefined,
			});

		for (const shadowedLength of [0, Number.NaN]) {
			const shadowed = Uint8Array.from(valid);
			Object.defineProperty(shadowed, "byteLength", {
				value: shadowedLength,
			});
			const iteratorCalls = installOwnIterator(
				shadowed,
				function* (): IterableIterator<number> {},
			);
			expect((await freshReducer().ingest(shadowed)).status).to.equal(
				"accepted",
			);
			expect(iteratorCalls()).to.equal(0);
		}

		const subclass = new AdversarialUint8Array(
			valid,
			TRUSTED_NETWORK_V2_MAX_POLICY_ENTRY_BYTES + 200_000,
		);
		expect((await freshReducer().ingest(subclass)).status).to.equal("accepted");
		expect(subclass.iteratorCalls).to.equal(0);

		const expanding = Uint8Array.of(0xff);
		const expandingIteratorCalls = installOwnIterator(
			expanding,
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
		expect((await freshReducer().ingest(expanding)).status).to.equal(
			"rejected",
		);
		expect(expandingIteratorCalls()).to.equal(0);

		const oversized = new Uint8Array(
			TRUSTED_NETWORK_V2_MAX_POLICY_ENTRY_BYTES + 1,
		);
		Object.defineProperty(oversized, "byteLength", { value: 1 });
		const oversizedIteratorCalls = installOwnIterator(
			oversized,
			function* (): IterableIterator<number> {},
		);
		const oversizedResult = await freshReducer().ingest(oversized);
		expect(oversizedResult.status).to.equal("rejected");
		expect(oversizedResult.reason).to.contain(
			String(TRUSTED_NETWORK_V2_MAX_POLICY_ENTRY_BYTES),
		);
		expect(oversizedIteratorCalls()).to.equal(0);

		const proxied = new Proxy(Uint8Array.from(valid), {});
		expect(
			(await freshReducer().ingest(proxied as unknown as Uint8Array)).status,
		).to.equal("rejected");

		const detached = new Uint8Array(new ArrayBuffer(8));
		structuredClone(detached.buffer, { transfer: [detached.buffer] });
		expect((await freshReducer().ingest(detached)).status).to.equal("rejected");
	});

	it("bounds signal-ignoring resolver attempts and keeps candidate-only ancestry pending", async () => {
		const fixture = await createChain();
		let attemptSignal: AbortSignal | undefined;
		let rejectLate: ((error: Error) => void) | undefined;
		const reducer = new TrustedNetworkV2PolicyReducer({
			descriptor: fixture.descriptor,
			resolveTimeoutMs: 10,
			resolvePolicyEntry: (_digest, { signal }) => {
				attemptSignal = signal;
				return new Promise<Uint8Array | undefined>((_resolve, reject) => {
					rejectLate = reject;
				});
			},
		});

		const started = Date.now();
		const result = await reducer.ingest(entryBytes(fixture.chain[2]!.entry));
		expect(Date.now() - started).to.be.lessThan(1_000);
		expect(result.status).to.equal("pending");
		expect(result.reason).to.contain("timed out");
		expect(attemptSignal?.aborted).to.be.true;
		expect(result.fetchHints.map((hint) => hex(hint.digest))).to.deep.equal([
			hex(fixture.chain[1]!.digest),
		]);
		expect(reducer.state).to.equal("EMPTY");

		rejectLate?.(new Error("late resolver rejection"));
		await new Promise<void>((resolve) => setTimeout(resolve, 0));
	});

	it("captures raw entry bytes before an admission waits in the queue", async () => {
		const fixture = await createChain();
		let releaseFirstResolution:
			| ((value: Uint8Array | undefined) => void)
			| undefined;
		let markResolutionStarted: (() => void) | undefined;
		const resolutionStarted = new Promise<void>((resolve) => {
			markResolutionStarted = resolve;
		});
		let firstResolution = true;
		const reducer = new TrustedNetworkV2PolicyReducer({
			descriptor: fixture.descriptor,
			resolvePolicyEntry: () => {
				if (!firstResolution) return undefined;
				firstResolution = false;
				markResolutionStarted?.();
				return new Promise<Uint8Array | undefined>((resolve) => {
					releaseFirstResolution = resolve;
				});
			},
		});

		const blockingAdmission = reducer.ingest(
			entryBytes(fixture.chain[2]!.entry),
		);
		await resolutionStarted;
		const queuedBytes = entryBytes(fixture.chain[0]!.entry);
		const queuedAdmission = reducer.ingest(queuedBytes);
		queuedBytes.fill(0xff);
		releaseFirstResolution?.(undefined);

		expect((await blockingAdmission).status).to.equal("pending");
		expect((await queuedAdmission).status).to.equal("accepted");
		expect(reducer.head?.sequence).to.equal(0n);
	});

	it("treats bad resolver bytes as unavailable and retries accepted ancestry with a fresh attempt", async () => {
		const fixture = await createChain();
		let mode: "oversized" | "stalled" | "available" = "oversized";
		let calls = 0;
		const seenSignals: AbortSignal[] = [];
		const reducer = new TrustedNetworkV2PolicyReducer({
			descriptor: fixture.descriptor,
			resolveTimeoutMs: 100,
			resolvePolicyEntry: (digest, { signal }) => {
				calls += 1;
				seenSignals.push(signal);
				if (mode === "oversized") {
					return new Uint8Array(TRUSTED_NETWORK_V2_MAX_POLICY_ENTRY_BYTES + 1);
				}
				if (mode === "stalled") {
					return new Promise<Uint8Array | undefined>(() => {});
				}
				return hex(digest) === hex(fixture.chain[1]!.digest)
					? entryBytes(fixture.chain[1]!.entry)
					: undefined;
			},
		});

		const pending = await reducer.ingest(entryBytes(fixture.chain[2]!.entry));
		expect(pending.status).to.equal("pending");
		expect(pending.reason).to.contain("unavailable");
		expect(reducer.pendingCount).to.equal(1);

		mode = "available";
		for (const policy of fixture.chain.slice(0, 3)) {
			await reducer.ingest(entryBytes(policy.entry));
		}
		expect(reducer.state).to.equal("ACTIVE");
		expect(reducer.head?.sequence).to.equal(2n);

		mode = "stalled";
		const unavailable = await reducer.ingest(
			entryBytes(fixture.chain[0]!.entry),
		);
		expect(unavailable.status).to.equal("unavailable");
		expect(reducer.state).to.equal("UNAVAILABLE");
		expect(
			reducer.isAuthorized(fixture.alice.publicKey, TrustedNetworkRole.READER),
		).to.be.false;

		mode = "available";
		const retried = await reducer.retryUnavailable();
		expect(retried.status).to.equal("duplicate");
		expect(reducer.state).to.equal("ACTIVE");
		expect(calls).to.be.greaterThan(2);
		expect(seenSignals.some((signal) => signal.aborted)).to.be.true;
	});

	it("halts authorization on lifecycle abort and bounds pending accounting", async () => {
		const fixture = await createChain();
		const lifecycle = new AbortController();
		const reducer = new TrustedNetworkV2PolicyReducer({
			descriptor: fixture.descriptor,
			resolvePolicyEntry: () => undefined,
			signal: lifecycle.signal,
		});
		expect(
			(await reducer.ingest(entryBytes(fixture.chain[0]!.entry))).status,
		).to.equal("accepted");
		expect(
			reducer.isAuthorized(
				fixture.authority.publicKey,
				TrustedNetworkRole.ADMIN,
			),
		).to.be.true;

		lifecycle.abort();
		expect(reducer.state).to.equal("HALTED");
		expect(
			reducer.isAuthorized(
				fixture.authority.publicKey,
				TrustedNetworkRole.ADMIN,
			),
		).to.be.false;
		expect(
			(await reducer.ingest(entryBytes(fixture.chain[1]!.entry))).status,
		).to.equal("halted");

		expect(
			() =>
				new TrustedNetworkV2PolicyReducer({
					descriptor: fixture.descriptor,
					resolvePolicyEntry: () => undefined,
					maxPendingPolicyBytes:
						TRUSTED_NETWORK_V2_MAX_POLICY_ENTRY_BYTES * 2 + 64 + 1,
				}),
		).to.throw("no greater than");
		expect(
			() =>
				new TrustedNetworkV2PolicyReducer({
					descriptor: fixture.descriptor,
					resolvePolicyEntry: () => undefined,
					maxPending: 65,
				}),
		).to.throw("no greater than 64");
	});

	it("aborts an in-flight resolver without allowing late state mutation", async () => {
		const fixture = await createChain();
		let attemptSignal: AbortSignal | undefined;
		let resolveLate: ((bytes: Uint8Array | undefined) => void) | undefined;
		let markStarted: (() => void) | undefined;
		const started = new Promise<void>((resolve) => {
			markStarted = resolve;
		});
		const reducer = new TrustedNetworkV2PolicyReducer({
			descriptor: fixture.descriptor,
			resolveTimeoutMs: 10_000,
			resolvePolicyEntry: (_digest, { signal }) => {
				attemptSignal = signal;
				markStarted?.();
				return new Promise<Uint8Array | undefined>((resolve) => {
					resolveLate = resolve;
				});
			},
		});

		const admission = reducer.ingest(entryBytes(fixture.chain[2]!.entry));
		await started;
		reducer.abort();
		expect((await admission).status).to.equal("halted");
		expect(attemptSignal?.aborted).to.be.true;
		expect(reducer.state).to.equal("HALTED");
		expect(reducer.head).to.be.undefined;
		expect(reducer.pendingCount).to.equal(0);

		let validationReads = 0;
		const lateBytes = new Proxy(entryBytes(fixture.chain[1]!.entry), {
			get: (target, property) => {
				if (property === "byteLength") validationReads += 1;
				const value = Reflect.get(target, property, target) as unknown;
				return typeof value === "function" ? value.bind(target) : value;
			},
		});
		resolveLate?.(lateBytes);
		await new Promise<void>((resolve) => setTimeout(resolve, 0));
		expect(validationReads).to.equal(0);
		expect(reducer.state).to.equal("HALTED");
		expect(reducer.head).to.be.undefined;
		expect(reducer.pendingCount).to.equal(0);
	});

	it("fails closed on missing accepted ancestry and retries the exact candidate to a fork", async () => {
		const fixture = await createChain();
		const competingChild = await createPolicy({
			descriptor: fixture.descriptor,
			sequence: 1n,
			previousPolicyDigest: fixture.chain[0]!.digest,
			bindings: [
				[fixture.authority.publicKey, TrustedNetworkRole.ADMIN],
				[fixture.bob.publicKey, TrustedNetworkRole.READER],
			],
			signer: fixture.authority,
		});
		const resolver = createResolver();
		const reducer = new TrustedNetworkV2PolicyReducer({
			descriptor: fixture.descriptor,
			resolvePolicyEntry: resolver.resolve,
		});
		for (const policy of fixture.chain.slice(0, 3)) {
			expect((await reducer.ingest(entryBytes(policy.entry))).status).to.equal(
				"accepted",
			);
		}
		expect(
			reducer.isAuthorized(
				fixture.bob.publicKey,
				TrustedNetworkRole.WRITER | TrustedNetworkRole.REPLICATOR,
			),
		).to.be.true;

		const unavailable = await reducer.ingest(entryBytes(competingChild.entry));
		expect(unavailable.status).to.equal("unavailable");
		expect(reducer.state).to.equal("UNAVAILABLE");
		expect(reducer.head?.sequence).to.equal(2n);
		expect(reducer.pendingCount).to.equal(1);
		expect(
			unavailable.fetchHints.map((hint) => hex(hint.digest)),
		).to.deep.equal([hex(fixture.chain[1]!.digest)]);
		expect(
			reducer.isAuthorized(
				fixture.bob.publicKey,
				TrustedNetworkRole.WRITER | TrustedNetworkRole.REPLICATOR,
			),
		).to.be.false;

		unavailable.fetchHints[0]!.digest.fill(0xff);
		const blockedAdvance = await reducer.ingest(
			entryBytes(fixture.chain[3]!.entry),
		);
		expect(blockedAdvance.status).to.equal("unavailable");
		expect(
			blockedAdvance.fetchHints.map((hint) => hex(hint.digest)),
		).to.include(hex(fixture.chain[1]!.digest));
		expect(reducer.head?.sequence).to.equal(2n);
		expect(reducer.state).to.equal("UNAVAILABLE");

		resolver.add(fixture.chain[0]!);
		resolver.add(fixture.chain[1]!);
		const retried = await reducer.retryUnavailable();
		expect(retried.status).to.equal("forked");
		expect(reducer.state).to.equal("FORKED");
		expect(reducer.head?.sequence).to.equal(0n);
		expect(reducer.pendingCount).to.equal(0);
		expect(
			reducer.isAuthorized(fixture.alice.publicKey, TrustedNetworkRole.WRITER),
		).to.be.false;
	});

	it("restores ACTIVE only after the exact unavailable comparison becomes definitive", async () => {
		const fixture = await createChain();
		const resolver = createResolver();
		const reducer = new TrustedNetworkV2PolicyReducer({
			descriptor: fixture.descriptor,
			resolvePolicyEntry: resolver.resolve,
		});
		for (const policy of fixture.chain.slice(0, 3)) {
			await reducer.ingest(entryBytes(policy.entry));
		}

		expect(
			(await reducer.ingest(entryBytes(fixture.chain[0]!.entry))).status,
		).to.equal("unavailable");
		expect(reducer.state).to.equal("UNAVAILABLE");
		expect(
			reducer.isAuthorized(fixture.alice.publicKey, TrustedNetworkRole.READER),
		).to.be.false;

		resolver.add(fixture.chain[1]!);
		const retried = await reducer.retryUnavailable();
		expect(retried.status).to.equal("duplicate");
		expect(reducer.state).to.equal("ACTIVE");
		expect(reducer.head?.sequence).to.equal(2n);
		expect(
			reducer.isAuthorized(fixture.alice.publicKey, TrustedNetworkRole.READER),
		).to.be.true;
	});

	it("fails closed for every accepted-ancestor resolver failure mode", async () => {
		const fixture = await createChain();
		const competingChild = await createPolicy({
			descriptor: fixture.descriptor,
			sequence: 1n,
			previousPolicyDigest: fixture.chain[0]!.digest,
			bindings: [
				[fixture.authority.publicKey, TrustedNetworkRole.ADMIN],
				[fixture.bob.publicKey, TrustedNetworkRole.READER],
			],
			signer: fixture.authority,
		});
		const failureModes: Array<{
			name: string;
			resolve: () => Uint8Array | undefined;
		}> = [
			{ name: "missing", resolve: () => undefined },
			{
				name: "throws",
				resolve: () => {
					throw new Error("resolver unavailable");
				},
			},
			{
				name: "wrong digest",
				resolve: () => entryBytes(fixture.chain[0]!.entry),
			},
			{
				name: "malformed",
				resolve: () => new Uint8Array([0xff]),
			},
		];

		for (const failure of failureModes) {
			const reducer = new TrustedNetworkV2PolicyReducer({
				descriptor: fixture.descriptor,
				resolvePolicyEntry: failure.resolve,
			});
			for (const policy of fixture.chain.slice(0, 3)) {
				expect(
					(await reducer.ingest(entryBytes(policy.entry))).status,
				).to.equal("accepted", failure.name);
			}

			const result = await reducer.ingest(entryBytes(competingChild.entry));
			expect(result.status, failure.name).to.equal("unavailable");
			expect(reducer.state, failure.name).to.equal("UNAVAILABLE");
			expect(reducer.head?.sequence, failure.name).to.equal(2n);
			expect(result.fetchHints.map((hint) => hex(hint.digest))).to.deep.equal([
				hex(fixture.chain[1]!.digest),
			]);
			expect(
				reducer.isAuthorized(
					fixture.bob.publicKey,
					TrustedNetworkRole.REPLICATOR,
				),
				failure.name,
			).to.be.false;
		}
	});

	it("keeps accepted-ancestry capacity overflow terminal and fail closed", async () => {
		const fixture = await createChain();
		const competingChild = await createPolicy({
			descriptor: fixture.descriptor,
			sequence: 1n,
			previousPolicyDigest: fixture.chain[0]!.digest,
			bindings: [[fixture.authority.publicKey, TrustedNetworkRole.ADMIN]],
			signer: fixture.authority,
		});
		const accountedBytes =
			serialize(competingChild.entry).byteLength +
			serialize(competingChild.body).byteLength +
			64;
		const reducer = new TrustedNetworkV2PolicyReducer({
			descriptor: fixture.descriptor,
			resolvePolicyEntry: () => undefined,
			maxPendingPolicyBytes: accountedBytes - 1,
		});
		for (const policy of fixture.chain.slice(0, 3)) {
			await reducer.ingest(entryBytes(policy.entry));
		}

		const result = await reducer.ingest(entryBytes(competingChild.entry));
		expect(result.status).to.equal("capacity");
		expect(reducer.state).to.equal("UNAVAILABLE");
		expect(reducer.pendingCount).to.equal(0);
		expect(result.fetchHints.map((hint) => hex(hint.digest))).to.deep.equal([
			hex(fixture.chain[1]!.digest),
		]);
		expect(
			reducer.isAuthorized(fixture.alice.publicKey, TrustedNetworkRole.READER),
		).to.be.false;
		expect((await reducer.retryUnavailable()).status).to.equal("capacity");
		expect(reducer.state).to.equal("UNAVAILABLE");
	});

	it("stays fail closed when an unrelated candidate evicts the blocked candidate", async () => {
		const fixture = await createChain();
		const alternatives = [
			await createPolicy({
				descriptor: fixture.descriptor,
				sequence: 1n,
				previousPolicyDigest: fixture.chain[0]!.digest,
				bindings: [[fixture.authority.publicKey, TrustedNetworkRole.ADMIN]],
				signer: fixture.authority,
			}),
			await createPolicy({
				descriptor: fixture.descriptor,
				sequence: 1n,
				previousPolicyDigest: fixture.chain[0]!.digest,
				bindings: [
					[fixture.authority.publicKey, TrustedNetworkRole.ADMIN],
					[fixture.bob.publicKey, TrustedNetworkRole.READER],
				],
				signer: fixture.authority,
			}),
		].sort((left, right) => compare(left.digest, right.digest));
		const lower = alternatives[0]!;
		const higher = alternatives[1]!;
		const reducer = new TrustedNetworkV2PolicyReducer({
			descriptor: fixture.descriptor,
			resolvePolicyEntry: () => undefined,
			maxPending: 1,
		});
		for (const policy of fixture.chain.slice(0, 3)) {
			await reducer.ingest(entryBytes(policy.entry));
		}

		expect((await reducer.ingest(entryBytes(higher.entry))).status).to.equal(
			"unavailable",
		);
		const overflow = await reducer.ingest(entryBytes(lower.entry));
		expect(overflow.status).to.equal("capacity");
		expect(overflow.evictedPolicyDigests?.map(hex)).to.include(
			hex(higher.digest),
		);
		expect(reducer.pendingDigests.map(hex)).to.deep.equal([hex(lower.digest)]);
		expect(reducer.state).to.equal("UNAVAILABLE");
		expect(
			reducer.isAuthorized(fixture.alice.publicKey, TrustedNetworkRole.READER),
		).to.be.false;
		expect((await reducer.retryUnavailable()).status).to.equal("capacity");
		expect(reducer.state).to.equal("UNAVAILABLE");
	});

	it("keeps missing parents pending with explicit, copy-safe fetch hints", async () => {
		const fixture = await createChain();
		const reducer = new TrustedNetworkV2PolicyReducer({
			descriptor: fixture.descriptor,
			resolvePolicyEntry: () => undefined,
			maxPending: 4,
		});
		const childResult = await reducer.ingest(
			entryBytes(fixture.chain[2]!.entry),
		);
		expect(childResult.status).to.equal("pending");
		expect(childResult.fetchHints).to.deep.equal([
			{ kind: "policy-parent", digest: fixture.chain[1]!.digest },
		]);

		childResult.fetchHints[0]!.digest.fill(0xff);
		const pendingDigest = reducer.pendingDigests[0]!;
		pendingDigest.fill(0xff);
		expect(hex(reducer.pendingDigests[0]!)).to.equal(
			hex(fixture.chain[2]!.digest),
		);

		await reducer.ingest(entryBytes(fixture.chain[1]!.entry));
		expect(reducer.pendingCount).to.equal(2);
		await reducer.ingest(entryBytes(fixture.chain[0]!.entry));
		expect(reducer.pendingCount).to.equal(0);
		expect(reducer.head?.sequence).to.equal(2n);
	});

	it("uses a deterministic hard pending bound independent of delivery order", async () => {
		const fixture = await createChain();
		const disconnected = await Promise.all(
			[0x31, 0x32, 0x33].map((marker) =>
				createPolicy({
					descriptor: fixture.descriptor,
					sequence: 1n,
					previousPolicyDigest: Uint8Array.from({ length: 32 }, () => marker),
					bindings: [[fixture.authority.publicKey, TrustedNetworkRole.ADMIN]],
					signer: fixture.authority,
				}),
			),
		);
		const retained: string[][] = [];
		for (const order of [disconnected, [...disconnected].reverse()]) {
			const reducer = new TrustedNetworkV2PolicyReducer({
				descriptor: fixture.descriptor,
				resolvePolicyEntry: () => undefined,
				maxPending: 2,
			});
			const results: PolicyAdmissionResultV2[] = [];
			for (const policy of order) {
				results.push(await reducer.ingest(entryBytes(policy.entry)));
			}
			expect(reducer.pendingCount).to.equal(2);
			expect(results.some((result) => result.evictedPolicyDigests)).to.be.true;
			retained.push(reducer.pendingDigests.map(hex));
		}
		expect(retained[0]).to.deep.equal(retained[1]);
		expect(retained[0]).to.deep.equal(
			disconnected
				.map((policy) => hex(policy.digest))
				.sort()
				.slice(0, 2),
		);
	});

	it("enforces per-candidate bytes and mixed-size count eviction deterministically", async () => {
		const fixture = await createChain();
		const candidates = await Promise.all(
			[0x41, 0x42, 0x43].map((marker, index) =>
				createPolicy({
					descriptor: fixture.descriptor,
					sequence: 1n,
					previousPolicyDigest: Uint8Array.from({ length: 32 }, () => marker),
					bindings: [[fixture.authority.publicKey, TrustedNetworkRole.ADMIN]],
					signer: fixture.authority,
					metaData: new Uint8Array([0, 257, 37][index]),
				}),
			),
		);
		const accounted = candidates.map(
			(candidate) =>
				serialize(candidate.entry).byteLength +
				serialize(candidate.body).byteLength +
				64,
		);
		const retained: string[][] = [];
		for (const order of [candidates, [...candidates].reverse()]) {
			const reducer = new TrustedNetworkV2PolicyReducer({
				descriptor: fixture.descriptor,
				resolvePolicyEntry: () => undefined,
				maxPending: 2,
				maxPendingPolicyBytes: Math.max(...accounted),
			});
			for (const policy of order) {
				await reducer.ingest(entryBytes(policy.entry));
			}
			expect(reducer.pendingCount).to.equal(2);
			expect(reducer.pendingBytes).to.be.at.most(2 * Math.max(...accounted));
			retained.push(reducer.pendingDigests.map(hex));
		}
		expect(retained[0]).to.deep.equal(retained[1]);
		expect(retained[0]).to.deep.equal(
			candidates
				.map((candidate) => hex(candidate.digest))
				.sort()
				.slice(0, 2),
		);

		const noCapacity = new TrustedNetworkV2PolicyReducer({
			descriptor: fixture.descriptor,
			resolvePolicyEntry: () => undefined,
			maxPendingPolicyBytes: accounted[0]! - 1,
		});
		const result = await noCapacity.ingest(entryBytes(candidates[0]!.entry));
		expect(result.status).to.equal("capacity");
		expect(result.pendingCount).to.equal(0);
		expect(result.pendingBytes).to.equal(0);
	});

	it("rejects non-contiguous sequence once the named parent is known", async () => {
		const fixture = await createChain();
		const invalid = await createPolicy({
			descriptor: fixture.descriptor,
			sequence: 9n,
			previousPolicyDigest: fixture.chain[0]!.digest,
			bindings: [[fixture.authority.publicKey, TrustedNetworkRole.ADMIN]],
			signer: fixture.authority,
		});
		const reducer = new TrustedNetworkV2PolicyReducer({
			descriptor: fixture.descriptor,
			resolvePolicyEntry: (digest) =>
				hex(digest) === hex(fixture.chain[0]!.digest)
					? entryBytes(fixture.chain[0]!.entry)
					: undefined,
		});
		const result = await reducer.ingest(entryBytes(invalid.entry));
		expect(result.status).to.equal("rejected");
		expect(result.reason).to.contain("not contiguous");
	});

	it("rejects wrong, additional, invalid, and encrypted signatures", async () => {
		const fixture = await createChain();
		const body = fixture.chain[0]!.body;
		const attacker = await Ed25519Keypair.create();
		const wrongSigner = await createEntry(serialize(body), attacker);
		const additionalSigner = await createEntry(
			serialize(body),
			fixture.authority,
			{ signers: [fixture.authority, attacker] },
		);
		const invalidSignature = await createEntry(
			serialize(body),
			fixture.authority,
		);
		invalidSignature._signatures!.signatures[0]!.decrypted._data![10] ^= 0xff;

		const sender = await X25519Keypair.create();
		const receiver = await X25519Keypair.create();
		const encryptedSignature = await createEntry(
			serialize(body),
			fixture.authority,
			{
				encryption: {
					keypair: sender,
					receiver: {
						meta: undefined,
						payload: undefined,
						signatures: receiver.publicKey,
					},
				},
			},
		);

		for (const [entry, message] of [
			[wrongSigner, "not the policy authority"],
			[additionalSigner, "exactly one signature"],
			[invalidSignature, "signature is invalid"],
			[encryptedSignature, "signature must be public"],
		] as const) {
			const reducer = new TrustedNetworkV2PolicyReducer({
				descriptor: fixture.descriptor,
				resolvePolicyEntry: () => undefined,
			});
			const result = await reducer.ingest(entryBytes(entry));
			expect(result.status).to.equal("rejected");
			expect(result.reason).to.contain(message);
		}
	});

	it("rejects encrypted metadata and payload, non-EntryV0, and tampering", async () => {
		const fixture = await createChain();
		const sender = await X25519Keypair.create();
		const receiver = await X25519Keypair.create();
		const encryptedMeta = await createEntry(
			serialize(fixture.chain[0]!.body),
			fixture.authority,
			{
				encryption: {
					keypair: sender,
					receiver: {
						meta: receiver.publicKey,
						payload: undefined,
						signatures: undefined,
					},
				},
			},
		);
		const encryptedPayload = await createEntry(
			serialize(fixture.chain[0]!.body),
			fixture.authority,
			{
				encryption: {
					keypair: sender,
					receiver: {
						meta: undefined,
						payload: receiver.publicKey,
						signatures: undefined,
					},
				},
			},
		);
		const trailingBody = await createEntry(
			concat([serialize(fixture.chain[0]!.body), new Uint8Array([0])]),
			fixture.authority,
		);
		const tamperedPayload = await createEntry(
			serialize(fixture.chain[0]!.body),
			fixture.authority,
		);
		tamperedPayload._payload.decrypted._data![20] ^= 0xff;

		for (const [bytes, message] of [
			[entryBytes(encryptedMeta), "metadata must be public"],
			[entryBytes(encryptedPayload), "payload must be public"],
			[entryBytes(trailingBody), "after deserialized"],
			[entryBytes(tamperedPayload), "signature is invalid"],
			[new Uint8Array(), "must use EntryV0"],
		] as const) {
			const reducer = new TrustedNetworkV2PolicyReducer({
				descriptor: fixture.descriptor,
				resolvePolicyEntry: () => undefined,
			});
			const result = await reducer.ingest(bytes);
			expect(result.status).to.equal("rejected");
			expect(result.reason).to.contain(message);
		}
	});

	it("detects a late same-parent fork, rolls back, and halts authorization", async () => {
		const fixture = await createChain();
		const fork = await createPolicy({
			descriptor: fixture.descriptor,
			sequence: 1n,
			previousPolicyDigest: fixture.chain[0]!.digest,
			bindings: [
				[fixture.authority.publicKey, TrustedNetworkRole.ADMIN],
				[fixture.bob.publicKey, TrustedNetworkRole.READER],
			],
			signer: fixture.authority,
			next: fixture.chain[0]!.entry,
		});

		for (const forkFirst of [false, true]) {
			const resolver = createResolver();
			fixture.chain.forEach(resolver.add);
			resolver.add(fork);
			const reducer = new TrustedNetworkV2PolicyReducer({
				descriptor: fixture.descriptor,
				resolvePolicyEntry: resolver.resolve,
			});
			await reducer.ingest(entryBytes(fixture.chain[0]!.entry));
			if (forkFirst) {
				await reducer.ingest(entryBytes(fork.entry));
				expect(
					(await reducer.ingest(entryBytes(fixture.chain[1]!.entry))).status,
				).to.equal("forked");
			} else {
				await reducer.ingest(entryBytes(fixture.chain[1]!.entry));
				await reducer.ingest(entryBytes(fixture.chain[2]!.entry));
				expect((await reducer.ingest(entryBytes(fork.entry))).status).to.equal(
					"forked",
				);
			}

			expect(reducer.state).to.equal("FORKED");
			expect(reducer.head?.sequence).to.equal(0n);
			expect(hex(reducer.head!.digest)).to.equal(hex(fixture.chain[0]!.digest));
			expect(reducer.rolesFor(fixture.alice.publicKey)).to.equal(
				TrustedNetworkRole.WRITER,
			);
			expect(
				reducer.isAuthorized(
					fixture.alice.publicKey,
					TrustedNetworkRole.WRITER,
				),
			).to.be.false;
			expect(
				reducer.forkEvidence?.children.map((child) => hex(child.digest)),
			).to.deep.equal([hex(fixture.chain[1]!.digest), hex(fork.digest)].sort());
			expect(
				(await reducer.ingest(entryBytes(fixture.chain[3]!.entry))).status,
			).to.equal("halted");
		}
	});

	it("retains the same canonical fork pair across all three-child permutations", async () => {
		const fixture = await createChain();
		const children = [
			fixture.chain[1]!,
			await createPolicy({
				descriptor: fixture.descriptor,
				sequence: 1n,
				previousPolicyDigest: fixture.chain[0]!.digest,
				bindings: [
					[fixture.authority.publicKey, TrustedNetworkRole.ADMIN],
					[fixture.bob.publicKey, TrustedNetworkRole.READER],
				],
				signer: fixture.authority,
			}),
			await createPolicy({
				descriptor: fixture.descriptor,
				sequence: 1n,
				previousPolicyDigest: fixture.chain[0]!.digest,
				bindings: [
					[fixture.authority.publicKey, TrustedNetworkRole.ADMIN],
					[fixture.alice.publicKey, TrustedNetworkRole.REPLICATOR],
				],
				signer: fixture.authority,
			}),
		];
		const expected = [...children]
			.sort((left, right) => compare(left.digest, right.digest))
			.slice(0, 2);

		for (const order of permutations(children)) {
			const resolver = createResolver();
			resolver.add(fixture.chain[0]!);
			const reducer = new TrustedNetworkV2PolicyReducer({
				descriptor: fixture.descriptor,
				resolvePolicyEntry: resolver.resolve,
			});
			await reducer.ingest(entryBytes(fixture.chain[0]!.entry));
			const statuses: string[] = [];
			for (const child of order) {
				statuses.push((await reducer.ingest(entryBytes(child.entry))).status);
			}

			expect(statuses).to.deep.equal(["accepted", "forked", "halted"]);
			expect(reducer.state).to.equal("FORKED");
			expect(reducer.head?.sequence).to.equal(0n);
			expect(reducer.pendingCount).to.equal(0);
			expect(
				reducer.isAuthorized(
					fixture.alice.publicKey,
					TrustedNetworkRole.WRITER,
				),
			).to.be.false;
			expect(
				reducer.forkEvidence?.children.map((child) => hex(child.digest)),
			).to.deep.equal(expected.map((child) => hex(child.digest)));
			expect(
				reducer.forkEvidence?.children.map((child) => hex(child.entryBytes)),
			).to.deep.equal(expected.map((child) => hex(serialize(child.entry))));
		}
	});

	it("uses entry bytes as the canonical tie-break for duplicate fork bodies", async () => {
		const fixture = await createChain();
		const competingChild = await createPolicy({
			descriptor: fixture.descriptor,
			sequence: 1n,
			previousPolicyDigest: fixture.chain[0]!.digest,
			bindings: [
				[fixture.authority.publicKey, TrustedNetworkRole.ADMIN],
				[fixture.bob.publicKey, TrustedNetworkRole.READER],
			],
			signer: fixture.authority,
		});
		const alternateWrapper = await createPolicy({
			descriptor: fixture.descriptor,
			sequence: fixture.chain[1]!.body.sequence,
			previousPolicyDigest: fixture.chain[1]!.body.previousPolicyDigest,
			bindings: fixture.chain[1]!.body.bindings.map((binding) => [
				binding.signingKey,
				binding.roles,
			]),
			signer: fixture.authority,
			metaData: new Uint8Array([0x7f]),
		});
		expect(hex(alternateWrapper.digest)).to.equal(
			hex(fixture.chain[1]!.digest),
		);
		const wrappers = [
			serialize(fixture.chain[1]!.entry),
			serialize(alternateWrapper.entry),
		].sort(compare);
		for (const order of permutations([
			fixture.chain[1]!,
			alternateWrapper,
			competingChild,
		])) {
			const resolver = createResolver();
			resolver.add(fixture.chain[0]!);
			const reducer = new TrustedNetworkV2PolicyReducer({
				descriptor: fixture.descriptor,
				resolvePolicyEntry: resolver.resolve,
			});
			await reducer.ingest(entryBytes(fixture.chain[0]!.entry));
			for (const child of order) {
				await reducer.ingest(entryBytes(child.entry));
			}

			const retained = reducer.forkEvidence!.children.find(
				(child) => hex(child.digest) === hex(fixture.chain[1]!.digest),
			)!;
			expect(hex(retained.entryBytes)).to.equal(hex(wrappers[0]!));
			const evidenceBeforeInvalid = reducer.forkEvidence!.children.map(
				(child) => hex(child.entryBytes),
			);
			expect((await reducer.ingest(new Uint8Array())).status).to.equal(
				"rejected",
			);
			expect(
				reducer.forkEvidence!.children.map((child) => hex(child.entryBytes)),
			).to.deep.equal(evidenceBeforeInvalid);
			expect(reducer.state).to.equal("FORKED");
		}
	});

	it("serializes concurrent admissions so same-parent forks cannot race past", async () => {
		const fixture = await createChain();
		const fork = await createPolicy({
			descriptor: fixture.descriptor,
			sequence: 1n,
			previousPolicyDigest: fixture.chain[0]!.digest,
			bindings: [
				[fixture.authority.publicKey, TrustedNetworkRole.ADMIN],
				[fixture.bob.publicKey, TrustedNetworkRole.READER],
			],
			signer: fixture.authority,
		});
		const resolver = createResolver();
		fixture.chain.forEach(resolver.add);
		resolver.add(fork);
		const reducer = new TrustedNetworkV2PolicyReducer({
			descriptor: fixture.descriptor,
			resolvePolicyEntry: resolver.resolve,
		});
		await reducer.ingest(entryBytes(fixture.chain[0]!.entry));
		const results = await Promise.all([
			reducer.ingest(entryBytes(fixture.chain[1]!.entry)),
			reducer.ingest(entryBytes(fork.entry)),
		]);
		expect(results.map((result) => result.status)).to.deep.equal([
			"accepted",
			"forked",
		]);
		expect(reducer.state).to.equal("FORKED");
		expect(reducer.head?.sequence).to.equal(0n);
	});

	it("does not mutate entries and returns copy-safe head and fork evidence", async () => {
		const fixture = await createChain();
		const resolver = createResolver();
		fixture.chain.forEach(resolver.add);
		const reducer = new TrustedNetworkV2PolicyReducer({
			descriptor: fixture.descriptor,
			resolvePolicyEntry: resolver.resolve,
		});
		const before = serialize(fixture.chain[0]!.entry);
		await reducer.ingest(entryBytes(fixture.chain[0]!.entry));
		expect(serialize(fixture.chain[0]!.entry)).to.deep.equal(before);

		const head = reducer.head!;
		head.digest.fill(0xff);
		head.bindings[0]!.roles = 0;
		fixture.chain[0]!.entry._payload.decrypted._data!.fill(0xff);
		expect(hex(reducer.head!.digest)).to.equal(hex(fixture.chain[0]!.digest));
		expect(reducer.rolesFor(fixture.authority.publicKey)).to.equal(
			TrustedNetworkRole.ADMIN,
		);

		const forkFixture = await createChain();
		const fork = await createPolicy({
			descriptor: forkFixture.descriptor,
			sequence: 1n,
			previousPolicyDigest: forkFixture.chain[0]!.digest,
			bindings: [
				[forkFixture.authority.publicKey, TrustedNetworkRole.ADMIN],
				[forkFixture.bob.publicKey, TrustedNetworkRole.READER],
			],
			signer: forkFixture.authority,
		});
		const forkResolver = createResolver();
		forkFixture.chain.forEach(forkResolver.add);
		forkResolver.add(fork);
		const forkReducer = new TrustedNetworkV2PolicyReducer({
			descriptor: forkFixture.descriptor,
			resolvePolicyEntry: forkResolver.resolve,
		});
		await forkReducer.ingest(entryBytes(forkFixture.chain[0]!.entry));
		await forkReducer.ingest(entryBytes(forkFixture.chain[1]!.entry));
		await forkReducer.ingest(entryBytes(fork.entry));
		const evidence = forkReducer.forkEvidence!;
		const expectedCommonParentDigest = hex(evidence.commonParent.digest);
		const expectedChildDigests = evidence.children.map((child) =>
			hex(child.digest),
		);
		const expectedChildEntryBytes = evidence.children.map((child) =>
			hex(child.entryBytes),
		);
		evidence.commonParent.digest.fill(0xff);
		evidence.commonParent.bindings[0]!.roles = 0;
		for (const child of evidence.children) {
			child.digest.fill(0xff);
			child.entryBytes.fill(0xff);
		}
		expect(hex(forkReducer.forkEvidence!.commonParent.digest)).to.equal(
			expectedCommonParentDigest,
		);
		expect(
			forkReducer.forkEvidence!.children.map((child) => hex(child.digest)),
		).to.deep.equal(expectedChildDigests);
		expect(
			forkReducer.forkEvidence!.children.map((child) => hex(child.entryBytes)),
		).to.deep.equal(expectedChildEntryBytes);
	});
});
