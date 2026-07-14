import { deserialize, serialize } from "@dao-xyz/borsh";
import { keys } from "@libp2p/crypto";
import { expect } from "chai";
import { Wallet, getBytes } from "ethers";
import sodium from "libsodium-wrappers";
import {
	sign as signEd25519Browser,
	verifySignatureEd25519 as verifySignatureEd25519Browser,
} from "../src/ed25519-sign.browser.js";
import {
	sign as signEd25519,
	verifySignatureEd25519,
} from "../src/ed25519-sign.js";
import {
	Ed25519Keypair,
	Ed25519PublicKey,
	Secp256k1Keypair,
	Secp256k1PublicKey,
	SignatureWithKey,
	X25519Keypair,
	getKeypairFromPrivateKey,
	recoverPublicKeyFromSignature,
	verify,
	verifySignatureSecp256k1,
} from "../src/index.js";
import * as cryptoRoot from "../src/index.js";
import { PreHash } from "../src/prehash.js";

describe("Ed25519", () => {
	it("ser/der", async () => {
		const keypair = await Ed25519Keypair.create();
		const derser = deserialize(serialize(keypair), Ed25519Keypair);
		expect(new Uint8Array(derser.publicKey.publicKey)).to.deep.equal(
			keypair.publicKey.publicKey,
		);
	});

	it("PeerId", async () => {});

	describe("PeerId", () => {
		it("keypair", async () => {
			const kp = await Ed25519Keypair.create();
			const peerId = await kp.toPeerId();
			expect(peerId.publicKey!.raw).to.deep.eq(kp.publicKey.publicKey);
		});

		it("publickey", async () => {
			const kp = await Ed25519Keypair.create();
			const peerId = await kp.publicKey.toPeerId();
			const kpFrom = Ed25519PublicKey.fromPeerId(peerId);
			expect(kp.publicKey.equals(kpFrom)).to.be.true;
		});
	});

	describe("native", () => {
		it("verify native", async () => {
			const keypair = sodium.crypto_sign_keypair();
			const data = new Uint8Array([1, 2, 3]);
			const signature = sodium.crypto_sign_detached(data, keypair.privateKey);
			const isVerified = await verifySignatureEd25519(
				new SignatureWithKey({
					prehash: PreHash.NONE,
					publicKey: new Ed25519PublicKey({ publicKey: keypair.publicKey }),
					signature,
				}),
				data,
			);
			expect(isVerified).to.be.true;
		});

		it("verify", async () => {
			const keypair = await Ed25519Keypair.create();
			const data = new Uint8Array([1, 2, 3]);
			const signature = await keypair.sign(data);
			const isVerified = await verifySignatureEd25519(signature, data);
			expect(isVerified).to.be.true;

			const isNotVerified = await verify(signature, data.reverse());
			expect(isNotVerified).to.be.false;
		});

		it("verify hashed", async () => {
			const keypair = await Ed25519Keypair.create();
			const data = new Uint8Array([1, 2, 3]);
			const signature = await keypair.sign(data, PreHash.SHA_256);
			const isVerified = await verify(signature, data);
			expect(isVerified).to.be.true;

			const isNotVerified = await verifySignatureEd25519(
				signature,
				data.reverse(),
			);
			expect(isNotVerified).to.be.false;
		});

		it("exports verifyPrepared from the root entrypoint", async () => {
			const keypair = await Ed25519Keypair.create();
			const data = new Uint8Array([1, 2, 3]);
			const signature = await keypair.sign(data, PreHash.SHA_256);
			expect(cryptoRoot.verifyPrepared).to.be.a("function");
			const prepared = await cryptoRoot.prehashFn(data, signature.prehash);
			expect(await cryptoRoot.verifyPrepared(signature, prepared)).to.be.true;
		});
	});

	describe("browser", () => {
		it("verify", async () => {
			const keypair = await Ed25519Keypair.create();
			const data = new Uint8Array([1, 2, 3]);
			const signature = await signEd25519Browser(data, keypair, PreHash.NONE);
			const isVerified = await verifySignatureEd25519Browser(signature, data);
			expect(isVerified).to.be.true;

			const isNotVerified = await verifySignatureEd25519Browser(
				signature,
				data.reverse(),
			);
			expect(isNotVerified).to.be.false;
		});

		it("verify hashed", async () => {
			const keypair = await Ed25519Keypair.create();
			const data = new Uint8Array([1, 2, 3]);
			const signature = await signEd25519Browser(
				data,
				keypair,
				PreHash.SHA_256,
			);
			const isVerified = await verifySignatureEd25519Browser(signature, data);
			expect(isVerified).to.be.true;

			const isNotVerified = await verifySignatureEd25519Browser(
				signature,
				data.reverse(),
			);
			expect(isNotVerified).to.be.false;
		});
	});

	describe("mixed api", () => {
		const signFns = [signEd25519Browser, signEd25519];
		const verifyFns = [verifySignatureEd25519Browser, verifySignatureEd25519];

		it("sign", async () => {
			const keypair = await Ed25519Keypair.create();
			const data = new Uint8Array([1, 2, 3]);
			const signatures: SignatureWithKey[] = [];
			for (const signFn of signFns) {
				const signature = await signFn(data, keypair, PreHash.NONE);
				for (const verifyFn of verifyFns) {
					expect(await verifyFn(signature, data)).to.be.true;
				}

				signatures.push(signature);
				if (signatures.length > 1) {
					expect(
						signatures[signatures.length - 2].equals(
							signatures[signatures.length - 1],
						),
					).to.be.true;
				}
			}
		});

		it("sign hashed", async () => {
			const keypair = await Ed25519Keypair.create();
			const data = new Uint8Array([1, 2, 3]);
			const signatures: SignatureWithKey[] = [];
			for (const fn of signFns) {
				const signature = await fn(data, keypair, PreHash.SHA_256);
				signatures.push(signature);

				for (const verifyFn of verifyFns) {
					expect(await verifyFn(signature, data)).to.be.true;
				}

				if (signatures.length > 1) {
					expect(
						signatures[signatures.length - 2].equals(
							signatures[signatures.length - 1],
						),
					).to.be.true;
				}
			}
		});
	});
});

describe("X25519", () => {
	it("ser/der", async () => {
		const keypair = await X25519Keypair.create();
		const derser = deserialize(serialize(keypair), X25519Keypair);
		expect(new Uint8Array(derser.publicKey.publicKey)).to.deep.equal(
			keypair.publicKey.publicKey,
		);
	});
});

describe("Sepck2561k1", () => {
	const data = new Uint8Array([1, 2, 3]);
	const decoder = new TextDecoder();
	const oldWireVectors = [
		{
			privateKey:
				"0x0000000000000000000000000000000000000000000000000000000000000001",
			digest:
				"0x1d2e3f5061728394a5b6c7d8e9fa0b1c2d3e4f60718293a4b5c6d7e8f90a1b2c",
			signature:
				"0xe0cadda129bbfe422d073abe89a24aadd151b3aad0d9af765d70eb325206385844001498a5524c889cbf9012d5052a458ea8494fe8b1803a53de3da2c1db0bec1c",
			recoveryParam: 1,
		},
		{
			privateKey:
				"0x0000000000000000000000000000000000000000000000000000000000000002",
			digest:
				"0x3a4b5c6d7e8fa0b1c2d3e4f5061728394a5b6c7d8e9fb0c1d2e3f40516273849",
			signature:
				"0xc7db01f7356bd08214e54ea2bcd04bcad58eec823a3ab839f7a95450b9f3e45d09ae730270d61da6a7d9e8b08c8d93683e89d83e490d67fa128e55226fee77291b",
			recoveryParam: 0,
		},
	] as const;
	const fixedKeypair = (privateKey: string) =>
		getKeypairFromPrivateKey(
			keys.privateKeyFromRaw(getBytes(privateKey)),
		) as Secp256k1Keypair;
	const expectDigestRejection = async (operation: Promise<unknown>) => {
		let rejection: unknown;
		try {
			await operation;
		} catch (error) {
			rejection = error;
		}
		expect(rejection).to.be.instanceOf(Error);
		expect((rejection as Error).message).to.contain(
			"exactly 32-byte prepared digest",
		);
	};

	it("wallet sign", async () => {
		const wallet = Wallet.createRandom();
		const pk = await Secp256k1PublicKey.recover(wallet);
		const signature = await wallet.signMessage(data);
		const textEncoder = new TextEncoder();
		const signatureBytes = textEncoder.encode(signature);

		const signatureWithKey = new SignatureWithKey({
			prehash: PreHash.ETH_KECCAK_256,
			publicKey: pk,
			signature: signatureBytes,
		});

		const isVerified = await verifySignatureSecp256k1(signatureWithKey, data);
		expect(isVerified).to.be.true;

		const isNotVerified = await verifySignatureSecp256k1(
			signatureWithKey,
			new Uint8Array(data).reverse(),
		);
		expect(isNotVerified).to.be.false;
	});
	it("keypair sign", async () => {
		const privateKeyGenerated = await keys.generateKeyPair("secp256k1");
		const keypair = getKeypairFromPrivateKey(privateKeyGenerated);
		const signature = await keypair.sign(data, PreHash.ETH_KECCAK_256);
		expect(await verifySignatureSecp256k1(signature, data)).to.be.true;
	});

	it("preserves Ethers v5 wire signatures for both recovery parities", async () => {
		for (const vector of oldWireVectors) {
			const keypair = fixedKeypair(vector.privateKey);
			const digest = getBytes(vector.digest);
			const signature = await keypair.sign(digest, PreHash.NONE);

			expect(decoder.decode(signature.signature)).to.equal(vector.signature);
			expect(Number.parseInt(vector.signature.slice(-2), 16) - 27).to.equal(
				vector.recoveryParam,
			);
			expect(await verifySignatureSecp256k1(signature, digest)).to.be.true;
			expect(
				recoverPublicKeyFromSignature(digest, vector.signature),
			).to.deep.equal(keypair.publicKey.publicKey);
		}
	});

	it("requires exactly 32 prepared digest bytes", async () => {
		const keypair = fixedKeypair(oldWireVectors[0].privateKey);
		const validDigest = getBytes(oldWireVectors[0].digest);
		const validSignature = await keypair.sign(validDigest, PreHash.NONE);
		expect(await verifySignatureSecp256k1(validSignature, validDigest)).to.be
			.true;

		for (const length of [0, 1, 31, 33, 64]) {
			const invalidDigest = new Uint8Array(length);
			await expectDigestRejection(keypair.sign(invalidDigest, PreHash.NONE));
			expect(await verifySignatureSecp256k1(validSignature, invalidDigest)).to
				.be.false;
			expect(() =>
				recoverPublicKeyFromSignature(
					invalidDigest,
					oldWireVectors[0].signature,
				),
			).to.throw("exactly 32-byte prepared digest");
		}
	});

	it("rejects scalar-equivalent raw-message collisions", async () => {
		const keypair = fixedKeypair(oldWireVectors[0].privateKey);
		const scalarOne = new Uint8Array(32);
		scalarOne[31] = 1;
		const scalarOneSignature = await keypair.sign(scalarOne, PreHash.NONE);
		expect(
			await verifySignatureSecp256k1(scalarOneSignature, new Uint8Array([1])),
		).to.be.false;
		expect(
			await verifySignatureSecp256k1(
				scalarOneSignature,
				new Uint8Array([0, 1]),
			),
		).to.be.false;

		const prefix = new Uint8Array(32).fill(9);
		const prefixSignature = await keypair.sign(prefix, PreHash.NONE);
		for (const tail of [8, 9]) {
			const extended = new Uint8Array(33);
			extended.set(prefix);
			extended[32] = tail;
			expect(await verifySignatureSecp256k1(prefixSignature, extended)).to.be
				.false;
			await expectDigestRejection(keypair.sign(extended, PreHash.NONE));
		}
	});

	describe("PeerId", () => {
		it("publickey", async () => {
			const kp = await Secp256k1Keypair.create();
			const peerId = await kp.publicKey.toPeerId();
			const pk = Secp256k1PublicKey.fromPeerId(peerId);
			expect(pk.equals(kp.publicKey));
		});
	});
	it("ser/der", async () => {
		const wallet = Wallet.createRandom();
		const pk = await Secp256k1PublicKey.recover(wallet);
		const derser = deserialize(serialize(pk), Secp256k1PublicKey);
		expect(derser.equals(pk)).to.be.true;
	});
});
