import { deserialize, serialize } from "@dao-xyz/borsh";
import type { Libp2p } from "@libp2p/interface";
import { createStore } from "@peerbit/any-store";
import type { CrashSafeAtomicReplaceStore } from "@peerbit/any-store-interface";
import { isCrashSafeAtomicReplaceStore } from "@peerbit/any-store/checkpoint";
import { calculateRawCid } from "@peerbit/blocks-interface";
import type { Identity } from "@peerbit/crypto";
import { Entry, EntryV0 } from "@peerbit/log";
import { TestSession } from "@peerbit/test-utils";
import { join } from "node:path";
import { compare } from "uint8arrays";
import { TrustedNetworkV2DurablePolicyReducer } from "../../src/v2-policy-anchor.js";
import { authenticatePolicySnapshotEntryV2 } from "../../src/v2-policy-engine.js";
import {
	ImmutableResourceDocumentV2,
	TrustedNetworkV2ResourceDocumentProjection,
} from "../../src/v2-resource-document-projection.js";
import { TrustedNetworkV2DurableResourceFenceReducer } from "../../src/v2-resource-fence-anchor.js";
import { authenticateResourceFenceEntryV2 } from "../../src/v2-resource-fence-entry.js";
import {
	ResourceOperationEnvelopeV2,
	TRUSTED_NETWORK_V2_RESOURCE_OPERATION_PROFILE,
} from "../../src/v2-resource-operation-entry.js";
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
} from "../../src/v2.js";

const bytes32 = (value: number) => new Uint8Array(32).fill(value);
const hex = (bytes: Uint8Array) => Buffer.from(bytes).toString("hex");
const ZERO = bytes32(0);
export const RESOURCE_ID = bytes32(0x71);
export const RESOURCE_GID = "protected-recovery-resource";
export const DOCUMENT_PROFILE = bytes32(0x81);

export type RecoveryEntry = {
	entry: EntryV0<Uint8Array>;
	bytes: Uint8Array;
	cid: string;
	/** Policy body digest for policies; signed raw-entry digest otherwise. */
	digest: Uint8Array;
};

const signedEntry = async (
	identity: Identity,
	data: Uint8Array,
	parents?: RecoveryEntry[],
): Promise<RecoveryEntry> => {
	const entry = (await EntryV0.create({
		store: {} as never,
		data,
		identity,
		deferStore: true,
		meta:
			parents === undefined
				? undefined
				: {
						gid: parents.length === 0 ? RESOURCE_GID : undefined,
						next: parents.map((parent) => parent.entry),
					},
	})) as EntryV0<Uint8Array>;
	const bytes = new Uint8Array(Entry.getPreparedStorageBytes(entry)!);
	const calculated = await calculateRawCid(bytes);
	return {
		entry,
		bytes,
		cid: calculated.cid,
		digest: new Uint8Array(calculated.block.cid.multihash.digest),
	};
};

/** Signed immutable history only: no peer, resolver, or accepted state is shared. */
export const createRecoveryHistory = async (
	authority: Identity,
	writer: Identity,
) => {
	const descriptor = new NetworkDescriptorV2({
		protocolVersion: TRUSTED_NETWORK_V2_PROTOCOL_VERSION,
		networkNonce: bytes32(0x31),
		policyAuthority: authority.publicKey,
		genesisPolicyDigest: bytes32(0x41),
		policyHashProfile: TRUSTED_NETWORK_V2_POLICY_HASH_SHA256,
		entrySignatureProfile:
			TRUSTED_NETWORK_V2_ENTRY_V0_AUTHORITY_ONLY_SIGNATURE_PROFILE,
	});
	const policies: RecoveryEntry[] = [];
	for (let sequence = 0; sequence < 3; sequence++) {
		const bindings = [
			new PolicySubjectBindingV2({
				signingKey: authority.publicKey,
				roles: TrustedNetworkRole.ADMIN,
			}),
			...(sequence === 1
				? []
				: [
						new PolicySubjectBindingV2({
							signingKey: writer.publicKey,
							roles: TrustedNetworkRole.WRITER,
						}),
					]),
		].sort((a, b) => compare(serialize(a.signingKey), serialize(b.signingKey)));
		const body = new PolicySnapshotBodyV2({
			networkId: deriveNetworkIdV2(descriptor),
			sequence: BigInt(sequence),
			previousPolicyDigest:
				sequence === 0 ? ZERO : policies[sequence - 1]!.digest,
			bindings,
		});
		policies.push({
			...(await signedEntry(authority, serialize(body))),
			digest: digestPolicySnapshotBodyV2(body),
		});
	}
	descriptor.genesisPolicyDigest = policies[0]!.digest;
	const fences: RecoveryEntry[] = [];
	const fence = async (sequence: number, parents: RecoveryEntry[]) => {
		const entry = await signedEntry(
			authority,
			serialize(
				new ResourceFenceV2({
					networkId: deriveNetworkIdV2(descriptor),
					resourceId: RESOURCE_ID,
					fenceSequence: BigInt(sequence),
					previousFenceDigest:
						sequence === 0 ? ZERO : fences[sequence - 1]!.digest,
					policySequence: BigInt(sequence),
					policyDigest: policies[sequence]!.digest,
					contentEpoch: BigInt(sequence),
					epochManifestDigest: bytes32(0x51 + sequence),
				}),
			),
			parents,
		);
		fences.push(entry);
		return entry;
	};
	const operation = (epoch: number, parents: RecoveryEntry[], value: number) =>
		signedEntry(
			writer,
			serialize(
				new ResourceOperationEnvelopeV2({
					profile: TRUSTED_NETWORK_V2_RESOURCE_OPERATION_PROFILE,
					policy: new OperationPolicyProofV2({
						networkId: deriveNetworkIdV2(descriptor),
						resourceId: RESOURCE_ID,
						policySequence: BigInt(epoch),
						policyDigest: policies[epoch]!.digest,
						fenceDigest: fences[epoch]!.digest,
						contentEpoch: BigInt(epoch),
					}),
					epochManifestDigest: bytes32(0x51 + epoch),
					applicationPayload: serialize(
						new ImmutableResourceDocumentV2({
							key: "document",
							value: Uint8Array.of(value),
						}),
					),
				}),
			),
			parents,
		);
	const fence0 = await fence(0, []);
	const before = await operation(0, [fence0], 1);
	const concurrent = await operation(0, [fence0], 2);
	const fence1 = await fence(1, [before]);
	const after = await operation(0, [fence1], 3);
	const fence2 = await fence(2, [fence1]);
	const regranted = await operation(2, [fence2], 4);
	return {
		descriptor,
		policies,
		fences,
		operations: { before, concurrent, after, regranted },
	};
};

/** Each open creates every mutable owner and resolver afresh from this directory. */
export const openRecoveryReplica = async (
	directory: string,
	suppliedDescriptor: NetworkDescriptorV2,
) => {
	const descriptor = deserialize(
		serialize(suppliedDescriptor),
		NetworkDescriptorV2,
	);
	const lifecycle = new AbortController();
	const stores: ReturnType<typeof createStore>[] = [];
	let session: TestSession | undefined;
	let projection: TrustedNetworkV2ResourceDocumentProjection | undefined;
	let closed = false;
	const close = async () => {
		if (closed) return;
		closed = true;
		const errors: unknown[] = [];
		try {
			await projection?.close();
		} catch (error) {
			errors.push(error);
		}
		lifecycle.abort();
		const results = await Promise.allSettled([
			...stores.map((store) => store.close()),
			...(session ? [session.stop()] : []),
		]);
		for (const result of results)
			if (result.status === "rejected") errors.push(result.reason);
		if (errors.length)
			throw new AggregateError(errors, "Recovery replica cleanup failed");
	};
	const openStore = async (
		name: string,
	): Promise<CrashSafeAtomicReplaceStore> => {
		const store = createStore(join(directory, name));
		stores.push(store);
		await store.open();
		if (!isCrashSafeAtomicReplaceStore(store))
			throw new Error("Recovery requires crash-safe disk stores");
		return store;
	};
	try {
		session = await TestSession.disconnected(1, {
			directory: join(directory, "peer"),
		});
		const peer = session.peers[0]!;
		const node = (peer as typeof peer & { libp2p: Libp2p }).libp2p;
		const policyStore = await openStore("policy");
		const fenceStore = await openStore("fence");
		const journalStore = await openStore("journal");
		const projectionStore = await openStore("projection");
		const catalogue = await openStore("dependencies");
		const local = (cid: string) =>
			peer.services.blocks.get(cid, { remote: false });
		const resolveDigest = async (kind: string, digest: Uint8Array) => {
			const encoded = await catalogue.get(`${kind}/${hex(digest)}`);
			return encoded === undefined
				? undefined
				: local(new TextDecoder().decode(encoded));
		};
		const resolveEntryV0 = async (cids: readonly string[]) =>
			new Map(
				await Promise.all(
					cids.map(async (cid) => [cid, await local(cid)] as const),
				),
			);
		const policy = await TrustedNetworkV2DurablePolicyReducer.open({
			descriptor,
			store: policyStore,
			signal: lifecycle.signal,
			resolvePolicyEntry: (digest) => resolveDigest("policy", digest),
			resolvePolicyEntryByCid: local,
		});
		const fence = await TrustedNetworkV2DurableResourceFenceReducer.open({
			descriptor,
			expectedResourceId: RESOURCE_ID,
			expectedGid: RESOURCE_GID,
			policyAnchor: policy,
			store: fenceStore,
			signal: lifecycle.signal,
			resolveFenceEntry: (digest) => resolveDigest("fence", digest),
			resolveEntryV0,
		});
		projection = await TrustedNetworkV2ResourceDocumentProjection.open({
			descriptor,
			expectedResourceId: RESOURCE_ID,
			expectedGid: RESOURCE_GID,
			fenceAnchor: fence,
			store: journalStore,
			projectionStore,
			documentProfileId: DOCUMENT_PROFILE,
			signal: lifecycle.signal,
			resolveEntryV0,
		});
		const remember = async (
			suppliedBytes: Uint8Array,
			kind: "policy" | "fence" | "operation",
		) => {
			const bytes = new Uint8Array(suppliedBytes);
			const digest =
				kind === "policy"
					? (await authenticatePolicySnapshotEntryV2(bytes, descriptor)).digest
					: kind === "fence"
						? (
								await authenticateResourceFenceEntryV2({
									entryBytes: bytes,
									descriptor,
									expectedResourceId: RESOURCE_ID,
									expectedGid: RESOURCE_GID,
								})
							).digest
						: undefined;
			const cid = await peer.services.blocks.put(bytes);
			await peer.services.blocks.crashSafeDurability?.barrier();
			if (digest !== undefined)
				await catalogue.crashSafeDurability.atomicReplace(
					`${kind}/${hex(digest)}`,
					new TextEncoder().encode(cid),
				);
			return cid;
		};
		return { peer, node, policy, fence, projection, remember, close };
	} catch (error) {
		try {
			await close();
		} catch (cleanupError) {
			throw new AggregateError(
				[error, cleanupError],
				"Recovery replica open failed",
			);
		}
		throw error;
	}
};
