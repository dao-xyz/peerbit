/* eslint-disable no-console */
import { serialize } from "@dao-xyz/borsh";
import { Ed25519Keypair } from "@peerbit/crypto";
import { EntryV0 } from "@peerbit/log";
import assert from "node:assert/strict";
import { performance } from "node:perf_hooks";
import { compare } from "uint8arrays";
import { TrustedNetworkV2PolicyReducer } from "../dist/src/v2-policy-engine.js";
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
} from "../dist/src/v2.js";

const parsePositiveInteger = (name, fallback) => {
	const raw = process.env[name];
	const value = raw == null ? fallback : Number.parseInt(raw, 10);
	if (!Number.isSafeInteger(value) || value < 1) {
		throw new Error(`${name} must be a positive safe integer, got ${raw}`);
	}
	return value;
};

const depth = parsePositiveInteger("PEERBIT_POLICY_PREFIX_BENCH_DEPTH", 256);
const label = process.env.PEERBIT_POLICY_PREFIX_BENCH_LABEL;
const ZERO_DIGEST = new Uint8Array(32);
const hex = (bytes) => Buffer.from(bytes).toString("hex");
const sortedBindings = (bindings) =>
	bindings
		.map(
			([signingKey, roles]) =>
				new PolicySubjectBindingV2({ signingKey, roles }),
		)
		.sort((left, right) =>
			compare(serialize(left.signingKey), serialize(right.signingKey)),
		);

const authority = await Ed25519Keypair.create();
const descriptorWithoutGenesis = new NetworkDescriptorV2({
	protocolVersion: TRUSTED_NETWORK_V2_PROTOCOL_VERSION,
	networkNonce: Uint8Array.from({ length: 32 }, (_, index) => index + 1),
	policyAuthority: authority.publicKey,
	genesisPolicyDigest: ZERO_DIGEST,
	policyHashProfile: TRUSTED_NETWORK_V2_POLICY_HASH_SHA256,
	entrySignatureProfile:
		TRUSTED_NETWORK_V2_ENTRY_V0_AUTHORITY_ONLY_SIGNATURE_PROFILE,
});
const bindings = sortedBindings([
	[authority.publicKey, TrustedNetworkRole.ADMIN],
]);
const genesisBody = new PolicySnapshotBodyV2({
	networkId: deriveNetworkIdV2(descriptorWithoutGenesis),
	sequence: 0n,
	previousPolicyDigest: ZERO_DIGEST,
	bindings,
});
const descriptor = new NetworkDescriptorV2({
	...descriptorWithoutGenesis,
	genesisPolicyDigest: digestPolicySnapshotBodyV2(genesisBody),
});
const networkId = deriveNetworkIdV2(descriptor);
const entriesByDigest = new Map();

const createEntry = async (body, next) => {
	const entry = await EntryV0.create({
		store: {},
		data: serialize(body),
		identity: authority,
		deferStore: true,
		meta: { next: next === undefined ? [] : [next] },
	});
	const bytes = serialize(entry);
	entriesByDigest.set(hex(digestPolicySnapshotBodyV2(body)), bytes);
	return { body, entry, bytes };
};

const setupStartedAt = performance.now();
let current = await createEntry(genesisBody);
for (let sequence = 1; sequence <= depth; sequence++) {
	const body = new PolicySnapshotBodyV2({
		networkId,
		sequence: BigInt(sequence),
		previousPolicyDigest: digestPolicySnapshotBodyV2(current.body),
		bindings,
	});
	current = await createEntry(body, current.entry);
}

let resolverCalls = 0;
const reducer = await TrustedNetworkV2PolicyReducer.restore({
	descriptor,
	resolvePolicyEntry: async (digest) => {
		resolverCalls += 1;
		return entriesByDigest.get(hex(digest));
	},
	durableState: {
		formatVersion: 1,
		state: "ACTIVE",
		acceptedHeadEntryBytes: current.bytes,
	},
});
const setupMs = performance.now() - setupStartedAt;

let sampler;
try {
	globalThis.gc?.();
	const rssBeforeBytes = process.memoryUsage().rss;
	let peakRssBytes = rssBeforeBytes;
	sampler = setInterval(() => {
		peakRssBytes = Math.max(peakRssBytes, process.memoryUsage().rss);
	}, 5);
	const cpuBefore = process.cpuUsage();
	const startedAt = performance.now();
	const result = await reducer.resolveAcceptedPolicyPrefix({
		sequence: 0n,
		digest: descriptor.genesisPolicyDigest,
	});
	const traversalMs = performance.now() - startedAt;
	const cpu = process.cpuUsage(cpuBefore);
	peakRssBytes = Math.max(peakRssBytes, process.memoryUsage().rss);
	assert.equal(result.status, "resolved");
	assert.equal(resolverCalls, depth);

	console.log(
		JSON.stringify({
			label,
			depth,
			setupMs,
			traversalMs,
			policiesPerSecond: depth / (traversalMs / 1_000),
			resolverCalls,
			cpuUserMs: cpu.user / 1_000,
			cpuSystemMs: cpu.system / 1_000,
			rssBeforeBytes,
			peakRssBytes,
			peakRssDeltaBytes: peakRssBytes - rssBeforeBytes,
			node: process.version,
			platform: `${process.platform}-${process.arch}`,
		}),
	);
} finally {
	clearInterval(sampler);
	reducer.abort();
}
