import { serialize } from "@dao-xyz/borsh";
import { calculateRawCid, cidifyString } from "@peerbit/blocks-interface";
import {
	Ed25519Keypair,
	Ed25519PrivateKey,
	Ed25519PublicKey,
	PreHash,
	Secp256k1Keypair,
	SignatureWithKey,
	ready,
} from "@peerbit/crypto";
import { expect } from "chai";
import {
	CUSTODY_HANDOFF_PROFILE_ID,
	CUSTODY_HANDOFF_PROFILE_MASK,
	type CanonicalCustodyHandoffManifest,
	type CustodyHandoffManifestInput,
	DEFAULT_CUSTODY_HANDOFF_CODEC_LIMITS,
	MAX_CUSTODY_HANDOFF_CODEC_LIMITS,
	createCustodyHandoffManifestV1,
	createCustodyHandoffReceiptV1,
	decodeCustodyHandoffManifestV1,
	decodeCustodyHandoffReceiptV1,
} from "../src/custody-handoff-codec.js";

const source = new Ed25519Keypair({
	publicKey: new Ed25519PublicKey({
		publicKey: new Uint8Array([
			5, 149, 176, 13, 66, 29, 75, 143, 214, 180, 65, 225, 86, 4, 119, 164, 133,
			242, 216, 14, 93, 209, 61, 169, 189, 187, 11, 119, 123, 38, 85, 62,
		]),
	}),
	privateKey: new Ed25519PrivateKey({
		privateKey: new Uint8Array([
			33, 26, 237, 82, 39, 39, 253, 88, 140, 102, 107, 38, 88, 61, 94, 198, 153,
			191, 15, 237, 202, 199, 19, 143, 26, 80, 99, 66, 102, 99, 63, 205,
		]),
	}),
});

const destination = new Ed25519Keypair({
	publicKey: new Ed25519PublicKey({
		publicKey: new Uint8Array([
			0, 83, 117, 223, 84, 41, 239, 99, 197, 171, 102, 198, 110, 4, 225, 6, 135,
			52, 107, 232, 107, 134, 115, 112, 98, 202, 24, 88, 110, 2, 122, 236,
		]),
	}),
	privateKey: new Ed25519PrivateKey({
		privateKey: new Uint8Array([
			158, 191, 201, 210, 111, 174, 133, 245, 76, 53, 91, 75, 19, 154, 85, 113,
			119, 56, 13, 46, 211, 62, 233, 195, 142, 131, 12, 75, 176, 41, 177, 222,
		]),
	}),
});

const third = new Ed25519Keypair({
	publicKey: new Ed25519PublicKey({
		publicKey: new Uint8Array([
			38, 88, 36, 255, 43, 10, 168, 50, 178, 240, 103, 216, 196, 143, 196, 17,
			254, 112, 106, 68, 144, 157, 34, 9, 233, 209, 102, 16, 192, 20, 66, 139,
		]),
	}),
	privateKey: new Ed25519PrivateKey({
		privateKey: new Uint8Array([
			42, 241, 139, 40, 85, 71, 39, 66, 187, 79, 12, 209, 106, 137, 118, 102,
			142, 115, 6, 206, 129, 169, 246, 211, 52, 250, 216, 90, 66, 224, 36, 17,
		]),
	}),
});

const digest = (byte: number) => byte.toString(16).padStart(2, "0").repeat(32);
const attemptGeneration = Uint8Array.from(
	{ length: 32 },
	(_, index) => index + 1,
);
const custodyEpoch = Uint8Array.from(
	{ length: 32 },
	(_, index) => 0x80 + index,
);
const signWith = (key: Ed25519Keypair) => (bytes: Uint8Array) =>
	key.sign(bytes, PreHash.SHA_256);
const hex = (bytes: Uint8Array) => Buffer.from(bytes).toString("hex");

const GOLDEN_PROFILE_ID =
	"804bdc32693f2733d3eb7e86c0161a5358d37abbb89acf32d9d216d422a8e68f";
const GOLDEN_MOVE_KEY =
	"c3ec7dafa467732161e69a6a878e3eb68fe98df9c9024d4498343b3883ab4ff9";
const GOLDEN_HANDOFF_ID =
	"db0e412725998a4f05932e13911489861f7132a5ef44329e5f7d0b41a87296c4";
const GOLDEN_RECEIPT_ID =
	"8b18304ff914480389485dba716fb7d19aff3cccf904163b1bfee3f09c173dbe";
const GOLDEN_MANIFEST_HEX =
	"760100002b000000706565726269742d7368617265642d6c6f672d637573746f64792d68616e646f66662d6d616e696665737401000000804bdc32693f2733d3eb7e86c0161a5358d37abbb89acf32d9d216d422a8e68f0f00000003000000010203310000007a62327268684e5633323344586237445157546938674d5036586f3974767076467468576137544b33437a66726e715050060504030201000021000000000595b00d421d4b8fd6b441e1560477a485f2d80e5dd13da9bdbb0b777b26553e2100000000005375df5429ef63c5ab66c66e04e10687346be86b86737062ca18586e027aec1111111111111111111111111111111111111111111111111111111111111111222222222222222222222222222222222222222222222222222222222222222208070605040302010c0b0a0900100f0e0d33333333333333333333333333333333333333333333333333333333333333330102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f2040000000288134ae111ca306769bead5dffdacd71e64b99f496e7eedf1d9d9c647df130f413f60f643a964a1a721eed373bd29c46b380dc9aacbb11de021f269f88dba02";
const GOLDEN_RECEIPT_HEX =
	"e30000002a000000706565726269742d7368617265642d6c6f672d637573746f64792d68616e646f66662d7265636569707401000000804bdc32693f2733d3eb7e86c0161a5358d37abbb89acf32d9d216d422a8e68f0f000000c3ec7dafa467732161e69a6a878e3eb68fe98df9c9024d4498343b3883ab4ff9db0e412725998a4f05932e13911489861f7132a5ef44329e5f7d0b41a87296c42100000000005375df5429ef63c5ab66c66e04e10687346be86b86737062ca18586e027aec808182838485868788898a8b8c8d8e8f909192939495969798999a9b9c9d9e9f181716151413121140000000d109800fad228a5d6a8c5c1226e91f9035d9d79c6ff3b9eaee6152a4c56c5a2339b1c57441bad79ba244ac1790254b54249eed986cd0babbd97cf8f7b023f306";

describe("custody handoff codec", () => {
	let entryHash: string;
	let secpSource: Secp256k1Keypair;
	let secpDestination: Secp256k1Keypair;

	before(async () => {
		await ready;
		entryHash = (await calculateRawCid(Uint8Array.from([1, 2, 3, 4]))).cid;
		[secpSource, secpDestination] = await Promise.all([
			Secp256k1Keypair.create(),
			Secp256k1Keypair.create(),
		]);
	});

	const input = (
		overrides: Partial<CustodyHandoffManifestInput<"u32">> = {},
	): CustodyHandoffManifestInput<"u32"> => ({
		logId: Uint8Array.from([1, 2, 3]),
		entryHash,
		entryByteLength: 0x010203040506n,
		source: source.publicKey,
		destination: destination.publicKey,
		visit: {
			viewId: digest(0x11),
			planDigest: digest(0x22),
			installSequence: 0x0102030405060708n,
			taskOrdinal: 0x090a0b0c,
			resolution: "u32",
			hashNumber: 0x0d0e0f10,
		},
		ownerPlanId: digest(0x33),
		attemptGeneration,
		...overrides,
	});

	const manifest = () =>
		createCustodyHandoffManifestV1(input(), signWith(source));

	it("pins the V1 profile, canonical manifest/receipt bytes and body IDs", async () => {
		const created = await manifest();
		const receipt = await createCustodyHandoffReceiptV1(
			{
				manifest: created,
				custodyEpoch,
				pinSequence: 0x1112131415161718n,
			},
			signWith(destination),
		);

		expect(entryHash).to.equal(
			"zb2rhhNV323DXb7DQWTi8gMP6Xo9tvpvFthWa7TK3CzfrnqPP",
		);
		expect(CUSTODY_HANDOFF_PROFILE_ID).to.equal(GOLDEN_PROFILE_ID);
		expect(CUSTODY_HANDOFF_PROFILE_MASK).to.equal(15);
		expect(created.moveKey).to.equal(GOLDEN_MOVE_KEY);
		expect(created.handoffId).to.equal(GOLDEN_HANDOFF_ID);
		expect(receipt.receiptId).to.equal(GOLDEN_RECEIPT_ID);
		expect(hex(created.bytes)).to.equal(GOLDEN_MANIFEST_HEX);
		expect(hex(receipt.bytes)).to.equal(GOLDEN_RECEIPT_HEX);

		const decoded = await decodeCustodyHandoffManifestV1(created.bytes);
		const decodedReceipt = await decodeCustodyHandoffReceiptV1(
			receipt.bytes,
			decoded,
		);
		expect(decoded).to.deep.equal(created);
		expect(decodedReceipt).to.deep.equal(receipt);
		expect(decoded.source.hash).to.equal(source.publicKey.hashcode());
		expect(decoded.destination.hash).to.equal(destination.publicKey.hashcode());
		expect(decoded.source.publicKey).to.equal(hex(serialize(source.publicKey)));
		expect(decodedReceipt.custodyEpoch).to.equal(hex(custodyEpoch));
		expect(decodedReceipt.pinSequence).to.equal(0x1112131415161718n);
	});

	it("keeps retries exact while separating a new attempt from its move key", async () => {
		const first = await manifest();
		const retry = await manifest();
		const next = await createCustodyHandoffManifestV1(
			input({ attemptGeneration: new Uint8Array(32).fill(0x44) }),
			signWith(source),
		);

		expect(retry.bytes).to.deep.equal(first.bytes);
		expect(retry.moveKey).to.equal(first.moveKey);
		expect(retry.handoffId).to.equal(first.handoffId);
		expect(next.moveKey).to.equal(first.moveKey);
		expect(next.handoffId).to.not.equal(first.handoffId);
		expect(next.bytes).to.not.deep.equal(first.bytes);
	});

	it("domain-separates otherwise equal u32/u64 visits", async () => {
		const u32 = await createCustodyHandoffManifestV1(
			input({
				visit: {
					...input().visit,
					taskOrdinal: 1,
					hashNumber: 7,
				},
			}),
			signWith(source),
		);
		const u64 = await createCustodyHandoffManifestV1(
			{
				...input(),
				visit: {
					...input().visit,
					taskOrdinal: 1,
					resolution: "u64",
					hashNumber: 7n,
				},
			},
			signWith(source),
		);

		expect(u32.moveKey).to.not.equal(u64.moveKey);
		expect(u32.handoffId).to.not.equal(u64.handoffId);
		const decoded = await decodeCustodyHandoffManifestV1(u64.bytes);
		expect(decoded.visit.resolution).to.equal("u64");
		expect(decoded.visit.hashNumber).to.equal(7n);

		const maxU32 = await createCustodyHandoffManifestV1(
			input({
				visit: {
					...input().visit,
					taskOrdinal: 0xffff_ffff,
					hashNumber: 0xffff_ffff,
				},
			}),
			signWith(source),
		);
		const maxU64 = await createCustodyHandoffManifestV1(
			{
				...input(),
				visit: {
					...input().visit,
					installSequence: 0xffff_ffff_ffff_ffffn,
					resolution: "u64",
					hashNumber: 0xffff_ffff_ffff_ffffn,
				},
			},
			signWith(source),
		);
		expect(
			(await decodeCustodyHandoffManifestV1(maxU32.bytes)).visit,
		).to.deep.include({ taskOrdinal: 0xffff_ffff, hashNumber: 0xffff_ffff });
		expect(
			(await decodeCustodyHandoffManifestV1(maxU64.bytes)).visit,
		).to.deep.include({
			installSequence: 0xffff_ffff_ffff_ffffn,
			hashNumber: 0xffff_ffff_ffff_ffffn,
		});

		await expect(
			createCustodyHandoffManifestV1(
				input({
					visit: { ...input().visit, hashNumber: 0x1_0000_0000 },
				}),
				signWith(source),
			),
		).to.be.rejectedWith("u32 hash number");
		await expect(
			createCustodyHandoffManifestV1(
				{
					...input(),
					visit: {
						...input().visit,
						resolution: "u64",
						hashNumber: 0x1_0000_0000_0000_0000n,
					},
				},
				signWith(source),
			),
		).to.be.rejectedWith("u64 hash number");
	});

	it("round-trips stable secp256k1 u64 artifacts with SHA-256 prehash", async () => {
		const secpInput: CustodyHandoffManifestInput<"u64"> = {
			...input(),
			source: secpSource.publicKey,
			destination: secpDestination.publicKey,
			visit: {
				...input().visit,
				resolution: "u64",
				hashNumber: 0x0102_0304_0506_0708n,
			},
		};
		const signSource = async (bytes: Uint8Array) => {
			const signature = await secpSource.sign(bytes, PreHash.SHA_256);
			expect(signature.prehash).to.equal(PreHash.SHA_256);
			return signature;
		};
		const signDestination = async (bytes: Uint8Array) => {
			const signature = await secpDestination.sign(bytes, PreHash.SHA_256);
			expect(signature.prehash).to.equal(PreHash.SHA_256);
			return signature;
		};
		const first = await createCustodyHandoffManifestV1(secpInput, signSource);
		const retry = await createCustodyHandoffManifestV1(secpInput, signSource);
		const receipt = await createCustodyHandoffReceiptV1(
			{ manifest: first, custodyEpoch, pinSequence: 9n },
			signDestination,
		);
		const receiptRetry = await createCustodyHandoffReceiptV1(
			{ manifest: retry, custodyEpoch, pinSequence: 9n },
			signDestination,
		);

		expect(retry.moveKey).to.equal(first.moveKey);
		expect(retry.handoffId).to.equal(first.handoffId);
		expect(retry.bytes).to.deep.equal(first.bytes);
		expect(receiptRetry.receiptId).to.equal(receipt.receiptId);
		expect(receiptRetry.bytes).to.deep.equal(receipt.bytes);

		const decoded = await decodeCustodyHandoffManifestV1(first.bytes);
		const decodedReceipt = await decodeCustodyHandoffReceiptV1(
			receipt.bytes,
			decoded,
		);
		expect(decoded.visit).to.deep.include({
			resolution: "u64",
			hashNumber: 0x0102_0304_0506_0708n,
		});
		expect(decoded.source.hash).to.equal(secpSource.publicKey.hashcode());
		expect(decoded.destination.hash).to.equal(
			secpDestination.publicKey.hashcode(),
		);
		expect(decodedReceipt).to.deep.equal(receipt);
	});

	it("requires canonical CIDs, keys, digests, distinct parties and safe scalars", async () => {
		const nonCanonicalCid = cidifyString(entryHash).toString();
		expect(nonCanonicalCid).to.not.equal(entryHash);
		await expect(
			createCustodyHandoffManifestV1(
				input({ entryHash: nonCanonicalCid }),
				signWith(source),
			),
		).to.be.rejectedWith("entry CID");
		await expect(
			createCustodyHandoffManifestV1(
				input({ destination: source.publicKey }),
				signWith(source),
			),
		).to.be.rejectedWith("must be distinct");
		await expect(
			createCustodyHandoffManifestV1(
				input({ entryByteLength: 0n }),
				signWith(source),
			),
		).to.be.rejectedWith("entry byte length");
		await expect(
			createCustodyHandoffManifestV1(
				input({ entryByteLength: BigInt(Number.MAX_SAFE_INTEGER) + 1n }),
				signWith(source),
			),
		).to.be.rejectedWith("entry byte length");
		await expect(
			createCustodyHandoffManifestV1(
				input({ ownerPlanId: digest(0xaa).toUpperCase() }),
				signWith(source),
			),
		).to.be.rejectedWith("owner plan id");
		await expect(
			createCustodyHandoffManifestV1(
				input({
					visit: { ...input().visit, taskOrdinal: -0 },
				}),
				signWith(source),
			),
		).to.be.rejectedWith("task ordinal");

		const staleHash = new Ed25519PublicKey({
			publicKey: new Uint8Array(source.publicKey.publicKey),
		});
		staleHash.hashcode();
		staleHash.publicKey[0] ^= 0xff;
		await expect(
			createCustodyHandoffManifestV1(
				input({ source: staleHash }),
				signWith(source),
			),
		).to.be.rejectedWith("public key hash");
	});

	it("rejects zero or malformed attempt and custody generations", async () => {
		for (const value of [new Uint8Array(32), new Uint8Array(31)]) {
			await expect(
				createCustodyHandoffManifestV1(
					input({ attemptGeneration: value }),
					signWith(source),
				),
			).to.be.rejectedWith("attempt generation");
		}
		const created = await manifest();
		for (const value of [new Uint8Array(32), new Uint8Array(33)]) {
			await expect(
				createCustodyHandoffReceiptV1(
					{ manifest: created, custodyEpoch: value, pinSequence: 1n },
					signWith(destination),
				),
			).to.be.rejectedWith("custody epoch");
		}
	});

	it("requires the exact source/destination signer and SHA-256 prehash", async () => {
		await expect(
			createCustodyHandoffManifestV1(input(), signWith(destination)),
		).to.be.rejectedWith("does not match");
		await expect(
			createCustodyHandoffManifestV1(input(), (bytes) =>
				source.sign(bytes, PreHash.NONE),
			),
		).to.be.rejectedWith("requires SHA-256 prehash");

		const created = await manifest();
		await expect(
			createCustodyHandoffReceiptV1(
				{ manifest: created, custodyEpoch, pinSequence: 1n },
				signWith(third),
			),
		).to.be.rejectedWith("does not match");
		await expect(
			createCustodyHandoffReceiptV1(
				{ manifest: created, custodyEpoch, pinSequence: 1n },
				(bytes) => destination.sign(bytes, PreHash.NONE),
			),
		).to.be.rejectedWith("requires SHA-256 prehash");
	});

	it("rejects corrupted signatures, truncation, trailing bytes and oversized lengths", async () => {
		const created = await manifest();
		const corrupt = new Uint8Array(created.bytes);
		corrupt[corrupt.length - 1] ^= 1;
		await expect(decodeCustodyHandoffManifestV1(corrupt)).to.be.rejectedWith(
			"manifest signature",
		);
		for (let cut = 1; cut <= 4; cut++) {
			await expect(decodeCustodyHandoffManifestV1(created.bytes.slice(0, -cut)))
				.to.be.rejected;
		}
		await expect(
			decodeCustodyHandoffManifestV1(Uint8Array.from([...created.bytes, 0])),
		).to.be.rejectedWith("trailing bytes");
		await expect(
			decodeCustodyHandoffManifestV1(
				Uint8Array.from([0xff, 0xff, 0xff, 0xff, 0]),
			),
		).to.be.rejectedWith("body exceeds its byte limit");
		await expect(
			decodeCustodyHandoffManifestV1(
				new Uint8Array(MAX_CUSTODY_HANDOFF_CODEC_LIMITS.maxManifestBytes + 1),
			),
		).to.be.rejectedWith("Invalid custody handoff custody handoff manifest");

		const receipt = await createCustodyHandoffReceiptV1(
			{ manifest: created, custodyEpoch, pinSequence: 1n },
			signWith(destination),
		);
		const corruptReceipt = new Uint8Array(receipt.bytes);
		corruptReceipt[corruptReceipt.length - 1] ^= 1;
		await expect(
			decodeCustodyHandoffReceiptV1(corruptReceipt, created),
		).to.be.rejectedWith("receipt signature");
		await expect(
			decodeCustodyHandoffReceiptV1(
				Uint8Array.from([...receipt.bytes, 0]),
				created,
			),
		).to.be.rejectedWith("trailing bytes");
	});

	it("rejects zero generations in otherwise authenticated decoded bodies", async () => {
		const created = await manifest();
		const zeroAttempt = new Uint8Array(created.bytes);
		const manifestBodyLength = new DataView(
			zeroAttempt.buffer,
			zeroAttempt.byteOffset,
			4,
		).getUint32(0, true);
		zeroAttempt.fill(0, 4 + manifestBodyLength - 32, 4 + manifestBodyLength);
		await expect(
			decodeCustodyHandoffManifestV1(zeroAttempt),
		).to.be.rejectedWith("attempt generation");

		const receipt = await createCustodyHandoffReceiptV1(
			{ manifest: created, custodyEpoch, pinSequence: 1n },
			signWith(destination),
		);
		const zeroEpoch = new Uint8Array(receipt.bytes);
		const receiptBodyLength = new DataView(
			zeroEpoch.buffer,
			zeroEpoch.byteOffset,
			4,
		).getUint32(0, true);
		zeroEpoch.fill(0, 4 + receiptBodyLength - 40, 4 + receiptBodyLength - 8);
		await expect(
			decodeCustodyHandoffReceiptV1(zeroEpoch, created),
		).to.be.rejectedWith("custody epoch");
	});

	it("binds receipts to the exact manifest, profile, epoch and pin sequence", async () => {
		const first = await manifest();
		const second = await createCustodyHandoffManifestV1(
			input({ attemptGeneration: new Uint8Array(32).fill(0x55) }),
			signWith(source),
		);
		const receipt = await createCustodyHandoffReceiptV1(
			{ manifest: first, custodyEpoch, pinSequence: 7n },
			signWith(destination),
		);

		await expect(
			decodeCustodyHandoffReceiptV1(receipt.bytes, second),
		).to.be.rejectedWith("does not match its manifest");
		await expect(
			createCustodyHandoffReceiptV1(
				{ manifest: first, custodyEpoch, pinSequence: 0n },
				signWith(destination),
			),
		).to.be.rejectedWith("pin sequence");

		const nextEpoch = await createCustodyHandoffReceiptV1(
			{
				manifest: first,
				custodyEpoch: new Uint8Array(32).fill(0x66),
				pinSequence: 7n,
			},
			signWith(destination),
		);
		const nextPin = await createCustodyHandoffReceiptV1(
			{ manifest: first, custodyEpoch, pinSequence: 8n },
			signWith(destination),
		);
		expect(nextEpoch.receiptId).to.not.equal(receipt.receiptId);
		expect(nextPin.receiptId).to.not.equal(receipt.receiptId);
		expect(nextEpoch.custodyProfileId).to.equal(GOLDEN_PROFILE_ID);
	});

	it("enforces exact configurable byte boundaries", async () => {
		const created = await manifest();
		const keyBytes = serialize(source.publicKey).byteLength;
		const identifierBytes = new TextEncoder().encode(entryHash).byteLength;
		const exactManifest = await createCustodyHandoffManifestV1(
			input(),
			signWith(source),
			{
				limits: {
					maxManifestBytes: created.bytes.byteLength,
					maxIdentifierBytes: identifierBytes,
					maxPublicKeyBytes: keyBytes,
					maxSignatureBytes: 64,
				},
			},
		);
		expect(exactManifest.bytes).to.deep.equal(created.bytes);
		await expect(
			createCustodyHandoffManifestV1(input(), signWith(source), {
				limits: { maxManifestBytes: created.bytes.byteLength - 1 },
			}),
		).to.be.rejectedWith("encoded byte limit");
		await expect(
			createCustodyHandoffManifestV1(input(), signWith(source), {
				limits: { maxPublicKeyBytes: keyBytes - 1 },
			}),
		).to.be.rejectedWith("public key");
		await expect(
			createCustodyHandoffManifestV1(input(), signWith(source), {
				limits: { maxSignatureBytes: 63 },
			}),
		).to.be.rejectedWith("signature");

		const receipt = await createCustodyHandoffReceiptV1(
			{ manifest: created, custodyEpoch, pinSequence: 1n },
			signWith(destination),
		);
		const exactReceipt = await createCustodyHandoffReceiptV1(
			{ manifest: created, custodyEpoch, pinSequence: 1n },
			signWith(destination),
			{ limits: { maxReceiptBytes: receipt.bytes.byteLength } },
		);
		expect(exactReceipt.bytes).to.deep.equal(receipt.bytes);
		await expect(
			createCustodyHandoffReceiptV1(
				{ manifest: created, custodyEpoch, pinSequence: 1n },
				signWith(destination),
				{ limits: { maxReceiptBytes: receipt.bytes.byteLength - 1 } },
			),
		).to.be.rejectedWith("encoded byte limit");
	});

	it("reads only known limit fields and captures caller bytes before await", async () => {
		const reads = new Map<string | symbol, number>();
		const limits = new Proxy(
			{},
			{
				ownKeys() {
					throw new Error("must not enumerate limits");
				},
				get(_target, key) {
					reads.set(key, (reads.get(key) ?? 0) + 1);
					return undefined;
				},
			},
		) as Partial<typeof DEFAULT_CUSTODY_HANDOFF_CODEC_LIMITS>;
		const mutable = input({
			logId: Uint8Array.from([1, 2, 3]),
			attemptGeneration: new Uint8Array(attemptGeneration),
		});
		let release!: () => void;
		const gate = new Promise<void>((resolve) => {
			release = resolve;
		});
		const creating = createCustodyHandoffManifestV1(
			mutable,
			async (bytes) => {
				await gate;
				return source.sign(bytes, PreHash.SHA_256);
			},
			{ limits },
		);
		mutable.logId[0] = 0xff;
		mutable.attemptGeneration[0] = 0xee;
		release();
		const created = await creating;
		expect(created.logId).to.equal("010203");
		expect(created.attemptGeneration.startsWith("0102")).to.be.true;
		expect(reads.size).to.equal(5);
		for (const count of reads.values()) expect(count).to.equal(1);
	});

	it("reads a manifest bytes getter once before receipt validation", async () => {
		const created = await manifest();
		let reads = 0;
		const manifestLike = Object.defineProperty({}, "bytes", {
			enumerable: true,
			get() {
				reads += 1;
				return reads === 1
					? created.bytes
					: new Uint8Array(
							MAX_CUSTODY_HANDOFF_CODEC_LIMITS.maxManifestBytes + 1,
						);
			},
		});
		const receipt = await createCustodyHandoffReceiptV1(
			{
				manifest: manifestLike as CanonicalCustodyHandoffManifest,
				custodyEpoch,
				pinSequence: 1n,
			},
			signWith(destination),
		);
		expect(reads).to.equal(1);
		expect(receipt.handoffId).to.equal(created.handoffId);
	});

	it("rejects an oversized signature before envelope allocation", async () => {
		const oversized = new SignatureWithKey({
			publicKey: source.publicKey,
			prehash: PreHash.SHA_256,
			signature: new Uint8Array(
				MAX_CUSTODY_HANDOFF_CODEC_LIMITS.maxSignatureBytes + 1,
			),
		});
		await expect(
			createCustodyHandoffManifestV1(input(), () => oversized),
		).to.be.rejectedWith("signature");
	});
});
