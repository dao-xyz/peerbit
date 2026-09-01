import { deserialize, serialize } from "@dao-xyz/borsh";
import {
	calculateRawCid,
	cidifyString,
	createBlock,
	stringifyCid,
} from "@peerbit/blocks-interface";
import {
	Ed25519Keypair,
	Secp256k1Keypair,
	X25519Keypair,
	verify,
} from "@peerbit/crypto";
import {
	Entry,
	EntryType,
	EntryV0,
	LamportClock,
	NO_ENCODING,
	Timestamp,
} from "@peerbit/log";
import { expect } from "chai";
import { runInNewContext } from "node:vm";
import { concat } from "uint8arrays";
import * as publicApi from "../src/index.js";
import { authenticateResourceFenceEntryV2 } from "../src/v2-resource-fence-entry.js";
import {
	NetworkDescriptorV2,
	ResourceFenceV2,
	TRUSTED_NETWORK_V2_ENTRY_V0_AUTHORITY_ONLY_SIGNATURE_PROFILE,
	TRUSTED_NETWORK_V2_MAX_RESOURCE_FENCE_DIRECT_PARENTS,
	TRUSTED_NETWORK_V2_MAX_RESOURCE_FENCE_ENTRY_BYTES,
	TRUSTED_NETWORK_V2_POLICY_HASH_SHA256,
	TRUSTED_NETWORK_V2_PROTOCOL_VERSION,
	deriveNetworkIdV2,
} from "../src/v2.js";

type Authority = Ed25519Keypair | Secp256k1Keypair;

const bytes32 = (value: number): Uint8Array => new Uint8Array(32).fill(value);
const ZERO = bytes32(0);
const RESOURCE_ID = bytes32(0x71);
const RESOURCE_GID = "resource-gid";

const descriptorFor = (authority: Authority): NetworkDescriptorV2 =>
	new NetworkDescriptorV2({
		protocolVersion: TRUSTED_NETWORK_V2_PROTOCOL_VERSION,
		networkNonce: bytes32(0x31),
		policyAuthority: authority.publicKey,
		genesisPolicyDigest: bytes32(0x41),
		policyHashProfile: TRUSTED_NETWORK_V2_POLICY_HASH_SHA256,
		entrySignatureProfile:
			TRUSTED_NETWORK_V2_ENTRY_V0_AUTHORITY_ONLY_SIGNATURE_PROFILE,
	});

const fenceFor = (
	descriptor: NetworkDescriptorV2,
	properties?: Partial<{
		networkId: Uint8Array;
		resourceId: Uint8Array;
		fenceSequence: bigint;
		previousFenceDigest: Uint8Array;
		policySequence: bigint;
		policyDigest: Uint8Array;
		contentEpoch: bigint;
		epochManifestDigest: Uint8Array;
	}>,
): ResourceFenceV2 =>
	new ResourceFenceV2({
		networkId: properties?.networkId ?? deriveNetworkIdV2(descriptor),
		resourceId: properties?.resourceId ?? RESOURCE_ID,
		fenceSequence: properties?.fenceSequence ?? 0n,
		previousFenceDigest: properties?.previousFenceDigest ?? ZERO,
		policySequence: properties?.policySequence ?? 0n,
		policyDigest: properties?.policyDigest ?? descriptor.genesisPolicyDigest,
		contentEpoch: properties?.contentEpoch ?? 0n,
		epochManifestDigest: properties?.epochManifestDigest ?? bytes32(0x51),
	});

type FenceEntryFixture = {
	entry: EntryV0<Uint8Array>;
	entryBytes: Uint8Array;
};

const createFenceEntry = async (properties: {
	authority: Authority;
	descriptor: NetworkDescriptorV2;
	body?: ResourceFenceV2;
	payloadBytes?: Uint8Array;
	gid?: string;
	parentCids?: string[];
	metaData?: Uint8Array;
	type?: EntryType;
	clockId?: Uint8Array;
	signers?: Authority[];
	encryption?: Parameters<typeof EntryV0.create>[0]["encryption"];
}): Promise<FenceEntryFixture> => {
	const gid = properties.gid ?? RESOURCE_GID;
	const parentClock = new LamportClock({
		id: properties.authority.publicKey.bytes,
		timestamp: new Timestamp({ wallTime: 1n }),
	});
	const next = (properties.parentCids ?? []).map((hash) => ({
		hash,
		meta: { gid, clock: parentClock },
	}));
	const clock = new LamportClock({
		id: properties.clockId ?? properties.authority.publicKey.bytes,
		timestamp: new Timestamp({ wallTime: 2n }),
	});
	const entry = (await EntryV0.create({
		store: {} as never,
		data:
			properties.payloadBytes ??
			serialize(properties.body ?? fenceFor(properties.descriptor)),
		identity: properties.authority,
		deferStore: true,
		meta: {
			clock,
			gid: next.length === 0 ? gid : undefined,
			next: next as never,
			data: properties.metaData,
			type: properties.type,
		},
		signers: properties.signers?.map((signer) => signer.sign.bind(signer)),
		encryption: properties.encryption,
	})) as EntryV0<Uint8Array>;
	const prepared = Entry.getPreparedStorageBytes(entry);
	if (!prepared)
		throw new Error("Fixture did not retain prepared storage bytes");
	return { entry, entryBytes: new Uint8Array(prepared) };
};

const authenticate = (
	fixture: FenceEntryFixture,
	descriptor: NetworkDescriptorV2,
	properties?: { resourceId?: Uint8Array; gid?: string },
) =>
	authenticateResourceFenceEntryV2({
		entryBytes: fixture.entryBytes,
		descriptor,
		expectedResourceId: properties?.resourceId ?? RESOURCE_ID,
		expectedGid: properties?.gid ?? RESOURCE_GID,
	});

const readU32 = (bytes: Uint8Array, offset: number): number =>
	new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength).getUint32(
		offset,
		true,
	);

const writeU32 = (bytes: Uint8Array, offset: number, value: number): void => {
	new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength).setUint32(
		offset,
		value,
		true,
	);
};

const locateEntryFields = (bytes: Uint8Array) => {
	let offset = 1 + 2;
	const metaLength = offset;
	const metaStart = metaLength + 4;
	offset = metaStart + readU32(bytes, metaLength);
	const payloadLength = offset + 2;
	const payloadStart = payloadLength + 4;
	offset = payloadStart + readU32(bytes, payloadLength);
	const reserved = offset;
	offset += 4 + 1 + 1;
	const signatureCount = offset;
	offset += 4 + 2;
	const signatureLength = offset;
	const signatureStart = signatureLength + 4;
	const hashOption = signatureStart + readU32(bytes, signatureLength);

	let metaOffset = metaStart + 2;
	const clockIdLength = metaOffset;
	metaOffset += 4 + readU32(bytes, clockIdLength) + 1;
	const wallTime = metaOffset;
	metaOffset += 8 + 4;
	const gidLength = metaOffset;
	const gidStart = gidLength + 4;
	metaOffset = gidStart + readU32(bytes, gidLength);
	const nextCount = metaOffset;
	metaOffset += 4;
	const parentLengths: number[] = [];
	for (let i = 0; i < readU32(bytes, nextCount); i++) {
		parentLengths.push(metaOffset);
		metaOffset += 4 + readU32(bytes, metaOffset);
	}
	const entryType = metaOffset;
	metaOffset += 1;
	const metaDataOption = metaOffset;
	const metaDataLength =
		bytes[metaDataOption] === 1 ? metaDataOption + 1 : undefined;

	return {
		clockIdLength,
		entryType,
		gidLength,
		gidStart,
		hashOption,
		metaDataLength,
		metaLength,
		nextCount,
		parentLengths,
		payloadDataLength: payloadStart + 1,
		payloadLength,
		reserved,
		signatureCount,
		signatureDataLength: signatureStart + 1,
		signatureLength,
		wallTime,
	};
};

const replaceSignatureText = (
	entryBytes: Uint8Array,
	replacement: string,
): Uint8Array => {
	const bytes = new Uint8Array(entryBytes);
	const signatureDataLength = locateEntryFields(bytes).signatureDataLength;
	const encoded = new TextEncoder().encode(replacement);
	if (encoded.byteLength !== readU32(bytes, signatureDataLength)) {
		throw new Error("Replacement signature length does not match fixture");
	}
	bytes.set(encoded, signatureDataLength + 4);
	return bytes;
};

const canonicalParentCids = async (count: number): Promise<string[]> =>
	Promise.all(
		Array.from({ length: count }, (_, index) =>
			calculateRawCid(Uint8Array.of(index & 0xff, (index >>> 8) & 0xff)).then(
				(value) => value.cid,
			),
		),
	);

class AdversarialUint8Array extends Uint8Array {
	iteratorCalls = 0;

	constructor(source: Uint8Array) {
		super(source.byteLength);
		Uint8Array.prototype.set.call(this, source);
	}

	[Symbol.iterator](): IterableIterator<number> {
		this.iteratorCalls++;
		throw new Error("Caller iterator must not run");
	}
}

describe("TrustedNetwork v2 resource-fence EntryV0 authentication", () => {
	it("stays internal and authenticates canonical Ed25519 and secp256k1 storage", async () => {
		expect("authenticateResourceFenceEntryV2" in publicApi).to.equal(false);
		for (const [kind, authority, expectedBytes] of [
			["ed25519", await Ed25519Keypair.create(), 398],
			["secp256k1", await Secp256k1Keypair.create(), 468],
		] as const) {
			const descriptor = descriptorFor(authority);
			const metaData = Uint8Array.of(7, 8, 9);
			const fixture = await createFenceEntry({
				authority,
				descriptor,
				metaData,
			});
			const signableEnd = locateEntryFields(fixture.entryBytes).reserved + 4;
			const scannedSignable = new Uint8Array(signableEnd + 2);
			scannedSignable.set(fixture.entryBytes.subarray(0, signableEnd));
			expect(scannedSignable, `${kind} raw signable framing`).to.deep.equal(
				fixture.entry.getSignableBytes(),
			);
			expect(fixture.entryBytes.byteLength, kind).to.equal(
				expectedBytes + metaData.byteLength + 4,
			);
			const token = await authenticate(fixture, descriptor);
			const prepared = await calculateRawCid(fixture.entryBytes);
			expect(token.entryCid).to.equal(prepared.cid);
			expect(token.digest).to.deep.equal(prepared.block.cid.multihash.digest);
			expect(token.entryBytes).to.deep.equal(fixture.entryBytes);
			expect(token.entryBytes).not.to.equal(fixture.entryBytes);
			expect(token.body.resourceId).to.deep.equal(RESOURCE_ID);
			expect(token.gid).to.equal(RESOURCE_GID);
			expect(token.metaData).to.deep.equal(metaData);
			expect(token.metaData).not.to.equal(metaData);
			expect(token.directParents).to.deep.equal([]);
		}
	});

	it("rejects malleable secp256k1 signature representations", async () => {
		const authority = await Secp256k1Keypair.create();
		const descriptor = descriptorFor(authority);
		const fixture = await createFenceEntry({ authority, descriptor });
		await authenticate(fixture, descriptor);
		const signatureDataLength = locateEntryFields(
			fixture.entryBytes,
		).signatureDataLength;
		const signatureStart = signatureDataLength + 4;
		const canonical = new TextDecoder("utf-8", { fatal: true }).decode(
			fixture.entryBytes.subarray(
				signatureStart,
				signatureStart + readU32(fixture.entryBytes, signatureDataLength),
			),
		);
		expect(canonical).to.match(/^0x[0-9a-f]{128}(1b|1c)$/);

		const uppercase = canonical.replace(/[a-f]/g, (value) =>
			value.toUpperCase(),
		);
		const recoveryAlias =
			canonical.slice(0, -2) + (canonical.endsWith("1b") ? "00" : "01");
		const curveOrder = BigInt(
			"0xfffffffffffffffffffffffffffffffebaaedce6af48a03bbfd25e8cd0364141",
		);
		const highS = (curveOrder - BigInt(`0x${canonical.slice(66, 130)}`))
			.toString(16)
			.padStart(64, "0");
		const highSWithToggledRecovery =
			canonical.slice(0, 66) + highS + (canonical.endsWith("1b") ? "1c" : "1b");
		const canonicalCid = (await calculateRawCid(fixture.entryBytes)).cid;

		for (const [label, representation] of [
			["uppercase", uppercase],
			["recovery alias", recoveryAlias],
			["high-S", highSWithToggledRecovery],
		] as const) {
			expect(representation, label).not.to.equal(canonical);
			const malleableBytes = replaceSignatureText(
				fixture.entryBytes,
				representation,
			);
			const decoded = deserialize(malleableBytes, Entry);
			expect(decoded).to.be.instanceOf(EntryV0);
			(decoded as EntryV0<Uint8Array>).init({ encoding: NO_ENCODING });
			expect(
				await verify(
					(decoded as EntryV0<Uint8Array>).signatures[0]!,
					(decoded as EntryV0<Uint8Array>).getSignableBytes(),
				),
				label,
			).to.equal(true);
			expect((await calculateRawCid(malleableBytes)).cid, label).not.to.equal(
				canonicalCid,
			);
			await expect(
				authenticateResourceFenceEntryV2({
					entryBytes: malleableBytes,
					descriptor,
					expectedResourceId: RESOURCE_ID,
					expectedGid: RESOURCE_GID,
				}),
			).to.be.rejectedWith("secp256k1 signature is not canonical");
		}
	});

	it("accepts 64 ordered canonical parents and rejects 65, duplicates, and noncanonical CIDs", async () => {
		const authority = await Ed25519Keypair.create();
		const descriptor = descriptorFor(authority);
		const parentCids = await canonicalParentCids(
			TRUSTED_NETWORK_V2_MAX_RESOURCE_FENCE_DIRECT_PARENTS + 1,
		);
		const maximum = await createFenceEntry({
			authority,
			descriptor,
			parentCids: parentCids.slice(0, 64),
		});
		const token = await authenticate(maximum, descriptor);
		expect(token.directParents.map((parent) => parent.cid)).to.deep.equal(
			parentCids.slice(0, 64),
		);
		for (let i = 0; i < token.directParents.length; i++) {
			expect(token.directParents[i]!.digest).to.deep.equal(
				cidifyString(parentCids[i]!).multihash.digest,
			);
		}

		const tooMany = await createFenceEntry({
			authority,
			descriptor,
			parentCids,
		});
		await expect(authenticate(tooMany, descriptor)).to.be.rejectedWith(
			"at most 64",
		);
		const duplicate = await createFenceEntry({
			authority,
			descriptor,
			parentCids: [parentCids[0]!, parentCids[0]!],
		});
		await expect(authenticate(duplicate, descriptor)).to.be.rejectedWith(
			"must be unique",
		);

		const noncanonicalCid = cidifyString(parentCids[0]!).toString();
		expect(noncanonicalCid).not.to.equal(parentCids[0]);
		const noncanonical = await createFenceEntry({
			authority,
			descriptor,
			parentCids: [noncanonicalCid],
		});
		await expect(authenticate(noncanonical, descriptor)).to.be.rejectedWith(
			"canonical CIDv1/raw/sha2-256",
		);

		const dagCbor = await createBlock({ value: 1 }, "dag-cbor");
		const wrongCodec = await createFenceEntry({
			authority,
			descriptor,
			parentCids: [stringifyCid(dagCbor.cid)],
		});
		await expect(authenticate(wrongCodec, descriptor)).to.be.rejectedWith(
			"canonical CIDv1/raw/sha2-256",
		);
	});

	it("enforces the exact byte ceiling before generic decoding", async () => {
		const authority = await Ed25519Keypair.create();
		const descriptor = descriptorFor(authority);
		const baseline = await createFenceEntry({
			authority,
			descriptor,
			metaData: new Uint8Array(),
		});
		const padding =
			TRUSTED_NETWORK_V2_MAX_RESOURCE_FENCE_ENTRY_BYTES -
			baseline.entryBytes.byteLength;
		expect(padding).to.be.greaterThan(0);
		const exact = await createFenceEntry({
			authority,
			descriptor,
			metaData: new Uint8Array(padding),
		});
		expect(exact.entryBytes).to.have.length(
			TRUSTED_NETWORK_V2_MAX_RESOURCE_FENCE_ENTRY_BYTES,
		);
		await authenticate(exact, descriptor);

		const oversized = await createFenceEntry({
			authority,
			descriptor,
			metaData: new Uint8Array(padding + 1),
		});
		await expect(authenticate(oversized, descriptor)).to.be.rejectedWith(
			"1-8192 bytes",
		);
		await expect(
			authenticateResourceFenceEntryV2({
				entryBytes: new Uint8Array(),
				descriptor,
				expectedResourceId: RESOURCE_ID,
				expectedGid: RESOURCE_GID,
			}),
		).to.be.rejectedWith("1-8192 bytes");
	});

	it("captures hostile and cross-realm byte views without caller hooks or aliases", async () => {
		const authority = await Ed25519Keypair.create();
		const descriptor = descriptorFor(authority);
		const fixture = await createFenceEntry({ authority, descriptor });

		const adversarial = new AdversarialUint8Array(fixture.entryBytes);
		await authenticateResourceFenceEntryV2({
			entryBytes: adversarial,
			descriptor,
			expectedResourceId: RESOURCE_ID,
			expectedGid: RESOURCE_GID,
		});
		expect(adversarial.iteratorCalls).to.equal(0);

		const shadowed = new Uint8Array(fixture.entryBytes);
		Object.defineProperty(shadowed, "byteLength", { value: 1 });
		await authenticateResourceFenceEntryV2({
			entryBytes: shadowed,
			descriptor,
			expectedResourceId: RESOURCE_ID,
			expectedGid: RESOURCE_GID,
		});

		const crossRealm = runInNewContext("Uint8Array.from(values)", {
			values: Array.from(fixture.entryBytes),
		}) as Uint8Array;
		await authenticateResourceFenceEntryV2({
			entryBytes: crossRealm,
			descriptor,
			expectedResourceId: RESOURCE_ID,
			expectedGid: RESOURCE_GID,
		});

		const backing = new Uint8Array(fixture.entryBytes.byteLength + 9);
		backing.set(fixture.entryBytes, 5);
		const view = backing.subarray(5, 5 + fixture.entryBytes.byteLength);
		const resourceId = new Uint8Array(RESOURCE_ID);
		const promise = authenticateResourceFenceEntryV2({
			entryBytes: view,
			descriptor,
			expectedResourceId: resourceId,
			expectedGid: RESOURCE_GID,
		});
		view.fill(0xff);
		resourceId.fill(0xff);
		descriptor.networkNonce.fill(0xff);
		const token = await promise;
		expect(token.body.resourceId).to.deep.equal(RESOURCE_ID);
		expect(token.entryBytes).to.deep.equal(fixture.entryBytes);

		await expect(
			authenticateResourceFenceEntryV2({
				entryBytes: new Proxy(fixture.entryBytes, {}),
				descriptor: descriptorFor(authority),
				expectedResourceId: RESOURCE_ID,
				expectedGid: RESOURCE_GID,
			}),
		).to.be.rejectedWith("canonical EntryV0 bytes");
		await expect(
			authenticateResourceFenceEntryV2({
				entryBytes: new DataView(fixture.entryBytes.buffer) as never,
				descriptor: descriptorFor(authority),
				expectedResourceId: RESOURCE_ID,
				expectedGid: RESOURCE_GID,
			}),
		).to.be.rejectedWith("canonical EntryV0 bytes");

		const detached = new Uint8Array(fixture.entryBytes);
		structuredClone(detached.buffer, { transfer: [detached.buffer] });
		await expect(
			authenticateResourceFenceEntryV2({
				entryBytes: detached,
				descriptor: descriptorFor(authority),
				expectedResourceId: RESOURCE_ID,
				expectedGid: RESOURCE_GID,
			}),
		).to.be.rejectedWith("canonical EntryV0 bytes");
	});

	it("bounds every nested length and rejects every truncation", async () => {
		const authority = await Ed25519Keypair.create();
		const descriptor = descriptorFor(authority);
		const parentCids = await canonicalParentCids(1);
		const fixture = await createFenceEntry({
			authority,
			descriptor,
			parentCids,
			metaData: new Uint8Array(),
		});
		const locations = locateEntryFields(fixture.entryBytes);
		for (const offset of [
			locations.metaLength,
			locations.payloadLength,
			locations.clockIdLength,
			locations.gidLength,
			locations.parentLengths[0]!,
			locations.metaDataLength!,
			locations.payloadDataLength,
			locations.signatureLength,
			locations.signatureDataLength,
		]) {
			const malformed = new Uint8Array(fixture.entryBytes);
			writeU32(malformed, offset, 0xffffffff);
			await expect(
				authenticateResourceFenceEntryV2({
					entryBytes: malformed,
					descriptor,
					expectedResourceId: RESOURCE_ID,
					expectedGid: RESOURCE_GID,
				}),
			).to.be.rejected;
		}
		const hostileCount = new Uint8Array(fixture.entryBytes);
		writeU32(hostileCount, locations.nextCount, 0xffffffff);
		await expect(
			authenticateResourceFenceEntryV2({
				entryBytes: hostileCount,
				descriptor,
				expectedResourceId: RESOURCE_ID,
				expectedGid: RESOURCE_GID,
			}),
		).to.be.rejectedWith(/at most 64|impossible direct-parent count/);

		for (let length = 0; length < fixture.entryBytes.byteLength; length++) {
			await expect(
				authenticateResourceFenceEntryV2({
					entryBytes: fixture.entryBytes.subarray(0, length),
					descriptor,
					expectedResourceId: RESOURCE_ID,
					expectedGid: RESOURCE_GID,
				}),
			).to.be.rejected;
		}
	});

	it("rejects non-public wrappers, malformed envelope fields, and signature failures", async () => {
		const authority = await Ed25519Keypair.create();
		const attacker = await Ed25519Keypair.create();
		const descriptor = descriptorFor(authority);
		const fixture = await createFenceEntry({ authority, descriptor });
		const locations = locateEntryFields(fixture.entryBytes);

		const reserved = new Uint8Array(fixture.entryBytes);
		reserved[locations.reserved] = 1;
		await expect(
			authenticateResourceFenceEntryV2({
				entryBytes: reserved,
				descriptor,
				expectedResourceId: RESOURCE_ID,
				expectedGid: RESOURCE_GID,
			}),
		).to.be.rejectedWith("reserved bytes must be zero");

		await expect(
			authenticateResourceFenceEntryV2({
				entryBytes: serialize(fixture.entry),
				descriptor,
				expectedResourceId: RESOURCE_ID,
				expectedGid: RESOURCE_GID,
			}),
		).to.be.rejectedWith("hash option");
		await expect(
			authenticateResourceFenceEntryV2({
				entryBytes: concat([fixture.entryBytes, Uint8Array.of(0)]),
				descriptor,
				expectedResourceId: RESOURCE_ID,
				expectedGid: RESOURCE_GID,
			}),
		).to.be.rejectedWith("trailing storage bytes");

		const noSignatures = await createFenceEntry({
			authority,
			descriptor,
			signers: [],
		});
		await expect(authenticate(noSignatures, descriptor)).to.be.rejectedWith(
			"exactly one signature",
		);
		const twoSignatures = await createFenceEntry({
			authority,
			descriptor,
			signers: [authority, attacker],
		});
		await expect(authenticate(twoSignatures, descriptor)).to.be.rejectedWith(
			"exactly one signature",
		);

		const cut = await createFenceEntry({
			authority,
			descriptor,
			type: EntryType.CUT,
		});
		await expect(authenticate(cut, descriptor)).to.be.rejectedWith(
			"must be an APPEND",
		);
		const wrongClock = await createFenceEntry({
			authority,
			descriptor,
			clockId: attacker.publicKey.bytes,
		});
		await expect(authenticate(wrongClock, descriptor)).to.be.rejectedWith(
			"clock id is not the policy authority",
		);
		const wrongSigner = await createFenceEntry({
			authority: attacker,
			descriptor,
			clockId: authority.publicKey.bytes,
		});
		await expect(authenticate(wrongSigner, descriptor)).to.be.rejectedWith(
			"signer is not the policy authority",
		);

		const tampered = new Uint8Array(fixture.entryBytes);
		tampered[locations.wallTime] ^= 1;
		await expect(
			authenticateResourceFenceEntryV2({
				entryBytes: tampered,
				descriptor,
				expectedResourceId: RESOURCE_ID,
				expectedGid: RESOURCE_GID,
			}),
		).to.be.rejectedWith("signature is invalid");

		const invalidUtf8 = new Uint8Array(fixture.entryBytes);
		invalidUtf8[locations.gidStart] = 0xff;
		await expect(
			authenticateResourceFenceEntryV2({
				entryBytes: invalidUtf8,
				descriptor,
				expectedResourceId: RESOURCE_ID,
				expectedGid: RESOURCE_GID,
			}),
		).to.be.rejected;
	});

	it("rejects genuine encrypted wrappers", async () => {
		const authority = await Ed25519Keypair.create();
		const descriptor = descriptorFor(authority);
		const sender = await X25519Keypair.create();
		const receiver = await X25519Keypair.create();
		const receiverProfiles: Array<
			NonNullable<
				Parameters<typeof EntryV0.create>[0]["encryption"]
			>["receiver"]
		> = [
			{
				meta: receiver.publicKey,
				payload: undefined,
				signatures: undefined,
			},
			{
				meta: undefined,
				payload: receiver.publicKey,
				signatures: undefined,
			},
			{
				meta: undefined,
				payload: undefined,
				signatures: receiver.publicKey,
			},
		];
		for (const receiverFields of receiverProfiles) {
			const encrypted = await createFenceEntry({
				authority,
				descriptor,
				encryption: { keypair: sender, receiver: receiverFields },
			});
			await expect(authenticate(encrypted, descriptor)).to.be.rejected;
		}
	});

	it("binds the signed body to network, resource id, gid, and exact body length", async () => {
		const authority = await Ed25519Keypair.create();
		const descriptor = descriptorFor(authority);
		const fixture = await createFenceEntry({ authority, descriptor });
		await expect(
			authenticate(fixture, descriptor, { gid: "another-gid" }),
		).to.be.rejectedWith("another resource log");
		await expect(
			authenticate(fixture, descriptor, { resourceId: bytes32(0x99) }),
		).to.be.rejectedWith("another resource");

		const wrongNetwork = await createFenceEntry({
			authority,
			descriptor,
			body: fenceFor(descriptor, { networkId: bytes32(0x81) }),
		});
		await expect(authenticate(wrongNetwork, descriptor)).to.be.rejectedWith(
			"another network",
		);
		const wrongResource = await createFenceEntry({
			authority,
			descriptor,
			body: fenceFor(descriptor, { resourceId: bytes32(0x91) }),
		});
		await expect(authenticate(wrongResource, descriptor)).to.be.rejectedWith(
			"another resource",
		);
		const wrongLength = await createFenceEntry({
			authority,
			descriptor,
			payloadBytes: concat([serialize(fenceFor(descriptor)), Uint8Array.of(0)]),
		});
		await expect(authenticate(wrongLength, descriptor)).to.be.rejectedWith(
			"exactly 186 bytes",
		);
	});
});
