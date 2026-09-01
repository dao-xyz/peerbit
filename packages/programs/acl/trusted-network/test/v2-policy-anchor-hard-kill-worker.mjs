// @ts-nocheck -- this is a plain-Node crash worker, not a package API module.
import { deserialize, serialize } from "@dao-xyz/borsh";
import { createStore } from "@peerbit/any-store";
import { Ed25519Keypair, PublicSignKey } from "@peerbit/crypto";
import { EntryV0 } from "@peerbit/log";
import { compare } from "uint8arrays";

// This plain Node worker exercises the exact compiled artifacts under test.
// The parent process deliberately SIGKILLs writer modes without closing Level.
const distModule = (relativePath) =>
	new URL(`../dist/${relativePath}`, import.meta.url).href;
const {
	TRUSTED_NETWORK_V2_POLICY_ANCHOR_STORE_OWNER,
	TrustedNetworkV2DurablePolicyReducer,
} = await import(distModule("src/v2-policy-anchor.js"));
const {
	NetworkDescriptorV2,
	PolicySnapshotBodyV2,
	PolicySubjectBindingV2,
	TRUSTED_NETWORK_V2_ENTRY_V0_AUTHORITY_ONLY_SIGNATURE_PROFILE,
	TRUSTED_NETWORK_V2_POLICY_HASH_SHA256,
	TRUSTED_NETWORK_V2_PROTOCOL_VERSION,
	TrustedNetworkRole,
	deriveNetworkIdV2,
	digestPolicySnapshotBodyV2,
} = await import(distModule("src/v2.js"));

const ZERO_DIGEST = new Uint8Array(32);
const GENERATION_PREFIX = `${TRUSTED_NETWORK_V2_POLICY_ANCHOR_STORE_OWNER}/generation/`;
const expectedGenerationKey = (generation) =>
	`${GENERATION_PREFIX}${generation.toString().padStart(20, "0")}`;

const base64 = (bytes) => Buffer.from(bytes).toString("base64");
const fromBase64 = (value) => Uint8Array.from(Buffer.from(value, "base64"));
const hex = (bytes) => Buffer.from(bytes).toString("hex");

const assert = (condition, message) => {
	if (!condition) throw new Error(message);
};

const sortedBindings = (bindings) =>
	bindings
		.map(
			([signingKey, roles]) =>
				new PolicySubjectBindingV2({ signingKey, roles }),
		)
		.sort((left, right) =>
			compare(serialize(left.signingKey), serialize(right.signingKey)),
		);

const createEntry = async (bodyBytes, identity, next) =>
	EntryV0.create({
		store: {},
		data: bodyBytes,
		identity,
		deferStore: true,
		meta: { next: next === undefined ? [] : [next] },
	});

const createPolicy = async ({
	descriptor,
	sequence,
	previousPolicyDigest,
	bindings,
	signer,
	next,
}) => {
	const body = new PolicySnapshotBodyV2({
		networkId: deriveNetworkIdV2(descriptor),
		sequence,
		previousPolicyDigest,
		bindings: sortedBindings(bindings),
	});
	return {
		body,
		digest: digestPolicySnapshotBodyV2(body),
		entry: await createEntry(serialize(body), signer, next),
	};
};

const createFixture = async () => {
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
	const genesis = {
		body: genesisBody,
		digest: descriptor.genesisPolicyDigest,
		entry: await createEntry(serialize(genesisBody), authority),
	};
	const active = await createPolicy({
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
	const childRoles = [
		TrustedNetworkRole.WRITER,
		TrustedNetworkRole.READER,
		TrustedNetworkRole.REPLICATOR,
		TrustedNetworkRole.WRITER | TrustedNetworkRole.READER,
	];
	const children = await Promise.all(
		childRoles.map((roles) =>
			createPolicy({
				descriptor,
				sequence: 1n,
				previousPolicyDigest: genesis.digest,
				bindings: [
					[authority.publicKey, TrustedNetworkRole.ADMIN],
					[alice.publicKey, roles],
				],
				signer: authority,
				next: genesis.entry,
			}),
		),
	);
	const canonicalChildDigests = children
		.map(({ digest }) => digest)
		.sort(compare)
		.map(hex);

	return {
		descriptor,
		genesis,
		active,
		children,
		wire: {
			descriptor: base64(serialize(descriptor)),
			authority: base64(serialize(authority.publicKey)),
			alice: base64(serialize(alice.publicKey)),
			bob: base64(serialize(bob.publicKey)),
			activeDigest: hex(active.digest),
			childEntries: children.map(({ entry }) => base64(serialize(entry))),
			canonicalChildDigests,
			roles: {
				authority: TrustedNetworkRole.ADMIN,
				alice: TrustedNetworkRole.READER,
				bob: TrustedNetworkRole.WRITER,
			},
		},
	};
};

const decodeFixture = (wire) => ({
	wire,
	descriptor: deserialize(fromBase64(wire.descriptor), NetworkDescriptorV2),
	authority: deserialize(fromBase64(wire.authority), PublicSignKey),
	alice: deserialize(fromBase64(wire.alice), PublicSignKey),
	bob: deserialize(fromBase64(wire.bob), PublicSignKey),
	childEntries: wire.childEntries.map(fromBase64),
});

const emit = (message) => {
	process.stdout.write(`${JSON.stringify(message)}\n`);
};

const holdForKill = () => {
	setInterval(() => undefined, 60_000);
	return new Promise(() => undefined);
};

const generationKeys = async (store) => {
	const keys = [];
	for await (const [key] of store.iterator()) {
		if (key.startsWith(GENERATION_PREFIX)) keys.push(key);
	}
	return keys.sort();
};

const openStore = async (directory) => {
	const store = createStore(directory);
	await store.open();
	assert(
		store.crashSafeDurability?.crashSafe === true,
		"ClassicLevel did not expose crash-safe durability",
	);
	return store;
};

const writeAcknowledgedActive = async (directory) => {
	const fixture = await createFixture();
	const store = await openStore(directory);
	const anchor = await TrustedNetworkV2DurablePolicyReducer.open({
		descriptor: fixture.descriptor,
		resolvePolicyEntry: () => undefined,
		store,
		resolveTimeoutMs: 500,
	});
	assert(
		(await anchor.ingest(serialize(fixture.genesis.entry))).status ===
			"accepted",
		"Genesis was not accepted",
	);
	assert(
		(await anchor.ingest(serialize(fixture.active.entry))).status ===
			"accepted",
		"Active update was not accepted",
	);
	emit({ event: "acknowledged-active", fixture: fixture.wire });
	await holdForKill();
};

const writeFork = async (directory, stopAfterFirstObservation) => {
	const fixture = await createFixture();
	const store = await openStore(directory);
	const lowerDurability = store.crashSafeDurability;
	const intendedPrefix = [
		expectedGenerationKey(1),
		expectedGenerationKey(2),
		expectedGenerationKey(3),
	];
	let generationWrittenSinceBarrier;
	let fencedGenerationCount = 0;
	const probedStore = {
		get: (key) => store.get(key),
		put: async (key, value) => {
			await store.put(key, value);
			if (!key.startsWith(GENERATION_PREFIX)) return;
			assert(
				generationWrittenSinceBarrier === undefined,
				"Multiple policy generations were written before one durability barrier",
			);
			generationWrittenSinceBarrier = key;
		},
		iterator: () => store.iterator(),
		crashSafeDurability: {
			crashSafe: true,
			barrier: async () => {
				await lowerDurability.barrier();
				const fencedGeneration = generationWrittenSinceBarrier;
				generationWrittenSinceBarrier = undefined;
				if (fencedGeneration === undefined) return;
				assert(
					fencedGeneration === intendedPrefix[fencedGenerationCount],
					`Unexpected fenced policy generation: ${fencedGeneration}`,
				);
				fencedGenerationCount += 1;
				if (stopAfterFirstObservation && fencedGenerationCount === 2) {
					assert(
						JSON.stringify(await generationKeys(store)) ===
							JSON.stringify(intendedPrefix.slice(0, 2)),
						"Incomplete fork is not exactly one state and one observation",
					);
					emit({ event: "fenced-incomplete-fork", fixture: fixture.wire });
					await holdForKill();
				}
			},
		},
	};
	const resolvedEntries = new Map();
	const anchor = await TrustedNetworkV2DurablePolicyReducer.open({
		descriptor: fixture.descriptor,
		resolvePolicyEntry: (digest) => resolvedEntries.get(hex(digest)),
		store: probedStore,
		resolveTimeoutMs: 500,
	});
	for (const child of fixture.children) {
		assert(
			(await anchor.ingest(serialize(child.entry))).status === "pending",
			"Direct child was not retained as pending",
		);
	}
	assert(anchor.pendingCount === 4, "Expected four pending fork children");
	assert(
		(await generationKeys(store)).length === 0,
		"Pending-only state must remain ephemeral",
	);
	resolvedEntries.set(
		hex(fixture.genesis.digest),
		serialize(fixture.genesis.entry),
	);
	const forked = await anchor.ingest(serialize(fixture.genesis.entry));
	assert(forked.status === "forked", "Plural fork was not published");
	assert(
		JSON.stringify(await generationKeys(store)) ===
			JSON.stringify(intendedPrefix),
		"Complete fork is not exactly one state and two observation generations",
	);
	emit({ event: "acknowledged-complete-fork", fixture: fixture.wire });
	await holdForKill();
	throw new Error(
		"Plural fork publication passed the generation hard-kill fence",
	);
};

const writeIncompleteFork = (directory) => writeFork(directory, true);
const writeCompleteFork = (directory) => writeFork(directory, false);

const reopenActive = async (directory, wire) => {
	const fixture = decodeFixture(wire);
	const store = await openStore(directory);
	let resolverCalls = 0;
	const anchor = await TrustedNetworkV2DurablePolicyReducer.open({
		descriptor: fixture.descriptor,
		resolvePolicyEntry: () => {
			resolverCalls += 1;
			throw new Error("ACTIVE reopen must not resolve policy history");
		},
		store,
		resolveTimeoutMs: 500,
	});
	const head = anchor.head;
	const result = {
		event: "reopened-active",
		state: anchor.state,
		sequence: head?.sequence.toString(),
		digest: head === undefined ? undefined : hex(head.digest),
		resolverCalls,
		generationCount: (await generationKeys(store)).length,
		authorityRoles: anchor.rolesFor(fixture.authority),
		aliceRoles: anchor.rolesFor(fixture.alice),
		bobRoles: anchor.rolesFor(fixture.bob),
		authorityAdmin: anchor.isAuthorized(
			fixture.authority,
			TrustedNetworkRole.ADMIN,
		),
		aliceReader: anchor.isAuthorized(fixture.alice, TrustedNetworkRole.READER),
		aliceWriter: anchor.isAuthorized(fixture.alice, TrustedNetworkRole.WRITER),
		bobWriter: anchor.isAuthorized(fixture.bob, TrustedNetworkRole.WRITER),
	};
	anchor.abort();
	await store.close();
	emit(result);
};

const reopenIncompleteFork = async (directory, wire) => {
	const fixture = decodeFixture(wire);
	const store = await openStore(directory);
	let resolverCalls = 0;
	try {
		await TrustedNetworkV2DurablePolicyReducer.open({
			descriptor: fixture.descriptor,
			resolvePolicyEntry: () => {
				resolverCalls += 1;
				throw new Error("Incomplete FORKED reopen must not resolve history");
			},
			store,
			resolveTimeoutMs: 500,
		});
		throw new Error("Incomplete FORKED history returned a reducer");
	} catch (error) {
		await store.close();
		emit({
			event: "rejected-incomplete-fork",
			message: error instanceof Error ? error.message : String(error),
			resolverCalls,
		});
	}
};

const reopenCompleteFork = async (directory, wire) => {
	const fixture = decodeFixture(wire);
	const store = await openStore(directory);
	let resolverCalls = 0;
	const anchor = await TrustedNetworkV2DurablePolicyReducer.open({
		descriptor: fixture.descriptor,
		resolvePolicyEntry: () => {
			resolverCalls += 1;
			throw new Error("FORKED reopen must not resolve policy history");
		},
		store,
		resolveTimeoutMs: 500,
	});
	const initialEvidence = anchor.forkEvidence;
	const result = {
		event: "reopened-complete-fork",
		state: anchor.state,
		commonParentSequence: initialEvidence?.commonParent.sequence.toString(),
		canonicalDigests: initialEvidence?.children.map(({ digest }) =>
			hex(digest),
		),
		resolverCalls,
		generationCount: (await generationKeys(store)).length,
	};
	anchor.abort();
	await store.close();
	emit(result);
};

const [mode, directory, wireJson] = process.argv.slice(2);
if (!mode || !directory) throw new Error("Expected worker mode and directory");

if (mode === "write-active") {
	await writeAcknowledgedActive(directory);
} else if (mode === "write-incomplete-fork") {
	await writeIncompleteFork(directory);
} else if (mode === "write-complete-fork") {
	await writeCompleteFork(directory);
} else if (mode === "read-active") {
	if (!wireJson) throw new Error("Expected serialized ACTIVE fixture");
	await reopenActive(directory, JSON.parse(wireJson));
} else if (mode === "read-incomplete-fork") {
	if (!wireJson) throw new Error("Expected serialized FORKED fixture");
	await reopenIncompleteFork(directory, JSON.parse(wireJson));
} else if (mode === "read-complete-fork") {
	if (!wireJson) throw new Error("Expected serialized FORKED fixture");
	await reopenCompleteFork(directory, JSON.parse(wireJson));
} else {
	throw new Error(`Unknown worker mode: ${mode}`);
}
