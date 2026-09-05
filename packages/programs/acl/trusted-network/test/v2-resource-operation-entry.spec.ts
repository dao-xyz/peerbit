import { serialize } from "@dao-xyz/borsh";
import { Ed25519Keypair, Secp256k1Keypair } from "@peerbit/crypto";
import { Entry, EntryV0, LamportClock, Timestamp } from "@peerbit/log";
import { expect } from "chai";
import * as publicApi from "../src/index.js";
import {
	ResourceOperationEnvelopeV2,
	TRUSTED_NETWORK_V2_MAX_RESOURCE_OPERATION_APPLICATION_BYTES,
	TRUSTED_NETWORK_V2_MAX_RESOURCE_OPERATION_METADATA_BYTES,
	TRUSTED_NETWORK_V2_RESOURCE_OPERATION_ENVELOPE_OVERHEAD_BYTES,
	TRUSTED_NETWORK_V2_RESOURCE_OPERATION_PROFILE,
	authenticateResourceOperationEntryV2,
	decodeResourceOperationEnvelopeV2,
	readAuthenticatedResourceOperationEntryV2,
} from "../src/v2-resource-operation-entry.js";
import {
	NetworkDescriptorV2,
	OperationPolicyProofV2,
	TRUSTED_NETWORK_V2_ENTRY_V0_AUTHORITY_ONLY_SIGNATURE_PROFILE,
	TRUSTED_NETWORK_V2_POLICY_HASH_SHA256,
	TRUSTED_NETWORK_V2_PROTOCOL_VERSION,
	deriveNetworkIdV2,
} from "../src/v2.js";

const bytes32 = (value: number): Uint8Array => new Uint8Array(32).fill(value);
const RESOURCE_ID = bytes32(0x71);
const RESOURCE_GID = "resource-operation-entry";

const descriptorFor = (authority: Ed25519Keypair): NetworkDescriptorV2 =>
	new NetworkDescriptorV2({
		protocolVersion: TRUSTED_NETWORK_V2_PROTOCOL_VERSION,
		networkNonce: bytes32(0x31),
		policyAuthority: authority.publicKey,
		genesisPolicyDigest: bytes32(0x41),
		policyHashProfile: TRUSTED_NETWORK_V2_POLICY_HASH_SHA256,
		entrySignatureProfile:
			TRUSTED_NETWORK_V2_ENTRY_V0_AUTHORITY_ONLY_SIGNATURE_PROFILE,
	});

const envelopeFor = (
	descriptor: NetworkDescriptorV2,
	properties?: Partial<{
		profile: number;
		resourceId: Uint8Array;
		applicationPayload: Uint8Array;
	}>,
): ResourceOperationEnvelopeV2 =>
	new ResourceOperationEnvelopeV2({
		profile:
			properties?.profile ?? TRUSTED_NETWORK_V2_RESOURCE_OPERATION_PROFILE,
		policy: new OperationPolicyProofV2({
			networkId: deriveNetworkIdV2(descriptor),
			resourceId: properties?.resourceId ?? RESOURCE_ID,
			policySequence: 0n,
			policyDigest: descriptor.genesisPolicyDigest,
			fenceDigest: bytes32(0x51),
			contentEpoch: 0n,
		}),
		epochManifestDigest: bytes32(0x61),
		applicationPayload:
			properties?.applicationPayload ?? Uint8Array.of(1, 2, 3),
	});

const createOperation = async (properties: {
	descriptor: NetworkDescriptorV2;
	identity: Ed25519Keypair | Secp256k1Keypair;
	envelope?: ResourceOperationEnvelopeV2;
	gid?: string;
	metaData?: Uint8Array;
	signers?: Array<Ed25519Keypair | Secp256k1Keypair>;
}): Promise<Uint8Array> => {
	const entry = (await EntryV0.create({
		store: {} as never,
		data: serialize(properties.envelope ?? envelopeFor(properties.descriptor)),
		identity: properties.identity,
		deferStore: true,
		meta: {
			gid: properties.gid ?? RESOURCE_GID,
			clock: new LamportClock({
				id: properties.identity.publicKey.bytes,
				timestamp: new Timestamp({ wallTime: 2n }),
			}),
			data: properties.metaData,
		},
		signers: properties.signers?.map((signer) => signer.sign.bind(signer)),
	})) as EntryV0<Uint8Array>;
	const bytes = Entry.getPreparedStorageBytes(entry);
	if (bytes === undefined) throw new Error("Fixture has no prepared bytes");
	return new Uint8Array(bytes);
};

describe("TrustedNetwork v2 resource-operation EntryV0 authentication", () => {
	it("authenticates the application bytes, metadata, and signatures as one immutable entry", async () => {
		const authority = await Ed25519Keypair.create();
		const writer = await Ed25519Keypair.create();
		const descriptor = descriptorFor(authority);
		const bytes = await createOperation({
			descriptor,
			identity: writer,
			metaData: Uint8Array.of(9, 8),
		});
		const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
		const metaEnd = 7 + view.getUint32(3, true);
		const payloadStart = metaEnd + 6;
		const payloadEnd = payloadStart + view.getUint32(metaEnd + 2, true);
		const firstSignatureData = payloadEnd + 4 + 1 + 1 + 4 + 6 + 5;
		for (const offset of [metaEnd - 1, payloadEnd - 1, firstSignatureData]) {
			const tampered = new Uint8Array(bytes);
			tampered[offset] ^= 1;
			await expect(
				authenticateResourceOperationEntryV2({
					entryBytes: tampered,
					descriptor,
					expectedResourceId: RESOURCE_ID,
					expectedGid: RESOURCE_GID,
				}),
			).to.be.rejectedWith("signature is invalid");
		}
		const trailing = new Uint8Array(bytes.length + 1);
		trailing.set(bytes);
		await expect(
			authenticateResourceOperationEntryV2({
				entryBytes: trailing,
				descriptor,
				expectedResourceId: RESOURCE_ID,
				expectedGid: RESOURCE_GID,
			}),
		).to.be.rejectedWith("trailing storage");
	});

	it("rejects forged framing and oversized metadata before generic decoding", async () => {
		const authority = await Ed25519Keypair.create();
		const descriptor = descriptorFor(authority);
		const oversized = await createOperation({
			descriptor,
			identity: authority,
			metaData: new Uint8Array(
				TRUSTED_NETWORK_V2_MAX_RESOURCE_OPERATION_METADATA_BYTES + 1,
			),
		});
		await expect(
			authenticateResourceOperationEntryV2({
				entryBytes: oversized,
				descriptor,
				expectedResourceId: RESOURCE_ID,
				expectedGid: RESOURCE_GID,
			}),
		).to.be.rejectedWith("metadata may contain at most");
		const bytes = await createOperation({ descriptor, identity: authority });
		const corrupted = new Uint8Array(bytes);
		new DataView(
			corrupted.buffer,
			corrupted.byteOffset,
			corrupted.byteLength,
		).setUint32(3, 0xffffffff, true);
		await expect(
			authenticateResourceOperationEntryV2({
				entryBytes: corrupted,
				descriptor,
				expectedResourceId: RESOURCE_ID,
				expectedGid: RESOURCE_GID,
			}),
		).to.be.rejectedWith("truncated metadata");
		const envelope = serialize(envelopeFor(descriptor));
		new DataView(
			envelope.buffer,
			envelope.byteOffset,
			envelope.byteLength,
		).setUint32(
			TRUSTED_NETWORK_V2_RESOURCE_OPERATION_ENVELOPE_OVERHEAD_BYTES - 4,
			0xffffffff,
			true,
		);
		expect(() =>
			decodeResourceOperationEnvelopeV2(envelope, descriptor),
		).to.throw("framing is invalid");
	});

	it("pins the envelope variant, nested proof order, and golden bytes", () => {
		const envelope = new ResourceOperationEnvelopeV2({
			profile: TRUSTED_NETWORK_V2_RESOURCE_OPERATION_PROFILE,
			policy: new OperationPolicyProofV2({
				networkId: bytes32(0x91),
				resourceId: bytes32(0xa2),
				policySequence: 0x0102030405060708n,
				policyDigest: bytes32(0xb3),
				fenceDigest: bytes32(0xc4),
				contentEpoch: 0x1112131415161718n,
			}),
			epochManifestDigest: bytes32(0xd5),
			applicationPayload: Uint8Array.of(0xe6, 0xf7),
		});
		const bytes = serialize(envelope);
		expect(bytes).to.have.length(
			TRUSTED_NETWORK_V2_RESOURCE_OPERATION_ENVELOPE_OVERHEAD_BYTES + 2,
		);
		expect(Buffer.from(bytes).toString("hex")).to.equal(
			"02040102039191919191919191919191919191919191919191919191919191919191919191a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a20807060504030201b3b3b3b3b3b3b3b3b3b3b3b3b3b3b3b3b3b3b3b3b3b3b3b3b3b3b3b3b3b3b3b3c4c4c4c4c4c4c4c4c4c4c4c4c4c4c4c4c4c4c4c4c4c4c4c4c4c4c4c4c4c4c4c41817161514131211d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d502000000e6f7",
		);
	});

	it("remains internal and authenticates a manifest-bound multi-signer envelope", async () => {
		expect("ResourceOperationEnvelopeV2" in publicApi).to.equal(false);
		expect("authenticateResourceOperationEntryV2" in publicApi).to.equal(false);
		expect("TrustedNetworkV2ResourceOperationEngine" in publicApi).to.equal(
			false,
		);
		const authority = await Ed25519Keypair.create();
		const writer = await Ed25519Keypair.create();
		const applicationSigner = await Secp256k1Keypair.create();
		const descriptor = descriptorFor(authority);
		const envelope = envelopeFor(descriptor);
		expect(serialize(envelope).byteLength).to.equal(
			TRUSTED_NETWORK_V2_RESOURCE_OPERATION_ENVELOPE_OVERHEAD_BYTES + 3,
		);
		const entryBytes = await createOperation({
			descriptor,
			identity: writer,
			envelope,
			metaData: Uint8Array.of(9, 8),
			signers: [writer, applicationSigner],
		});

		const token = await authenticateResourceOperationEntryV2({
			entryBytes,
			descriptor,
			expectedResourceId: RESOURCE_ID,
			expectedGid: RESOURCE_GID,
		});
		const first = readAuthenticatedResourceOperationEntryV2(token)!;
		const second = readAuthenticatedResourceOperationEntryV2(token)!;
		expect(first.envelope.applicationPayload).to.deep.equal(
			Uint8Array.of(1, 2, 3),
		);
		expect(first.verifiedSignerKeyBytes).to.have.length(2);
		expect(first.metaData).to.deep.equal(Uint8Array.of(9, 8));
		expect(first.entryBytes).not.to.equal(entryBytes);
		expect(first.entryBytes).not.to.equal(second.entryBytes);
		first.entryBytes[0] ^= 0xff;
		expect(second.entryBytes[0]).to.equal(entryBytes[0]);
		expect(
			readAuthenticatedResourceOperationEntryV2({
				entryCid: token.entryCid,
			}),
		).to.equal(undefined);
	});

	it("captures caller bytes before signature verification yields", async () => {
		const authority = await Ed25519Keypair.create();
		const writer = await Ed25519Keypair.create();
		const descriptor = descriptorFor(authority);
		const entryBytes = await createOperation({ descriptor, identity: writer });
		const original = new Uint8Array(entryBytes);
		const authenticating = authenticateResourceOperationEntryV2({
			entryBytes,
			descriptor,
			expectedResourceId: RESOURCE_ID,
			expectedGid: RESOURCE_GID,
		});
		entryBytes.fill(0xff);
		const token = await authenticating;
		expect(
			readAuthenticatedResourceOperationEntryV2(token)?.entryBytes,
		).to.deep.equal(original);
	});

	it("rejects wrong context, unknown profiles, and oversized payloads", async () => {
		const authority = await Ed25519Keypair.create();
		const writer = await Ed25519Keypair.create();
		const descriptor = descriptorFor(authority);
		const wrongResource = await createOperation({
			descriptor,
			identity: writer,
			envelope: envelopeFor(descriptor, { resourceId: bytes32(0x72) }),
		});
		await expect(
			authenticateResourceOperationEntryV2({
				entryBytes: wrongResource,
				descriptor,
				expectedResourceId: RESOURCE_ID,
				expectedGid: RESOURCE_GID,
			}),
		).to.be.rejectedWith("another resource");

		const unknown = envelopeFor(descriptor, { profile: 2 });
		expect(() =>
			decodeResourceOperationEnvelopeV2(serialize(unknown), descriptor),
		).to.throw("Unsupported resource operation profile");
		const oversized = envelopeFor(descriptor, {
			applicationPayload: new Uint8Array(
				TRUSTED_NETWORK_V2_MAX_RESOURCE_OPERATION_APPLICATION_BYTES + 1,
			),
		});
		expect(() =>
			decodeResourceOperationEnvelopeV2(serialize(oversized), descriptor),
		).to.throw("at most");

		const wrongGid = await createOperation({
			descriptor,
			identity: writer,
			gid: "another-log",
		});
		await expect(
			authenticateResourceOperationEntryV2({
				entryBytes: wrongGid,
				descriptor,
				expectedResourceId: RESOURCE_ID,
				expectedGid: RESOURCE_GID,
			}),
		).to.be.rejectedWith("another resource log");
	});

	it("bounds signature cardinality and requires the clock principal to sign", async () => {
		const authority = await Ed25519Keypair.create();
		const writer = await Ed25519Keypair.create();
		const descriptor = descriptorFor(authority);
		const tooMany = await Promise.all(
			Array.from({ length: 9 }, () => Ed25519Keypair.create()),
		);
		const tooManyBytes = await createOperation({
			descriptor,
			identity: writer,
			signers: tooMany,
		});
		await expect(
			authenticateResourceOperationEntryV2({
				entryBytes: tooManyBytes,
				descriptor,
				expectedResourceId: RESOURCE_ID,
				expectedGid: RESOURCE_GID,
			}),
		).to.be.rejectedWith("1-8 signatures");

		const another = await Ed25519Keypair.create();
		const missingClockSigner = await createOperation({
			descriptor,
			identity: writer,
			signers: [another],
		});
		await expect(
			authenticateResourceOperationEntryV2({
				entryBytes: missingClockSigner,
				descriptor,
				expectedResourceId: RESOURCE_ID,
				expectedGid: RESOURCE_GID,
			}),
		).to.be.rejectedWith("clock id must identify one verified signer");
	});
});
