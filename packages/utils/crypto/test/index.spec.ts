import { deserialize, serialize } from "@dao-xyz/borsh";
import { Wallet } from "@ethersproject/wallet";
import { generateKeyPair } from "@libp2p/crypto/keys";
import { expect } from "chai";
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
	verify,
	verifySignatureSecp256k1,
} from "../src/index.js";
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
		let signFns = [signEd25519Browser, signEd25519];
		let verifyFns = [verifySignatureEd25519Browser, verifySignatureEd25519];

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
			let signatures: SignatureWithKey[] = [];
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

	it("wallet sign", async () => {
		const wallet = Wallet.createRandom();
		const pk = await Secp256k1PublicKey.recover(wallet);
		const signature = await wallet.signMessage(data);
		const textEncoder = new TextEncoder();
		let signatureBytes = textEncoder.encode(signature);

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
		const privateKeyGenerated = await generateKeyPair("secp256k1");
		const keypair = getKeypairFromPrivateKey(privateKeyGenerated);
		const signature = await keypair.sign(data, PreHash.ETH_KECCAK_256);
		expect(await verifySignatureSecp256k1(signature, data)).to.be.true;
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
