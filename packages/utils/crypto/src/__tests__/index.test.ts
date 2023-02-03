import {
	Ed25519Keypair,
	X25519Keypair,
	verifySignatureSecp256k1,
	Secp256k1Keccak256PublicKey,
	Sec256k1Keccak256Keypair,
	verify,
	Ed25519PublicKey,
	SignatureWithKey,
} from "../index.js";
import sodium from "libsodium-wrappers";
import { deserialize, serialize } from "@dao-xyz/borsh";
import { Wallet } from "@ethersproject/wallet";
import { createSecp256k1PeerId } from "@libp2p/peer-id-factory";
import { supportedKeys } from "@libp2p/crypto/keys";
import {
	verifySignatureEd25519,
	sign as signEd25519,
} from "../ed25519-sign.js";
import {
	verifySignatureEd25519 as verifySignatureEd25519Browser,
	sign as signEd25519Browser,
} from "../ed25519-sign-browser";
import { PreHash } from "../prehash.js";

describe("Ed25519", () => {
	it("ser/der", async () => {
		const keypair = await Ed25519Keypair.create();
		const derser = deserialize(serialize(keypair), Ed25519Keypair);
		expect(derser.publicKey.publicKey).toEqual(keypair.publicKey.publicKey);
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
					signature: signature,
				}),
				data
			);
			expect(isVerified).toBeTrue();
		});

		it("verify", async () => {
			const keypair = await Ed25519Keypair.create();
			const data = new Uint8Array([1, 2, 3]);
			const signature = await keypair.sign(data);
			const isVerified = await verifySignatureEd25519(signature, data);
			expect(isVerified).toBeTrue();

			const isNotVerified = await verify(signature, data.reverse());
			expect(isNotVerified).toBeFalse();
		});

		it("verify hashed", async () => {
			const keypair = await Ed25519Keypair.create();
			const data = new Uint8Array([1, 2, 3]);
			const signature = await keypair.sign(data, PreHash.SHA_256);
			const isVerified = await verify(signature, data);
			expect(isVerified).toBeTrue();

			const isNotVerified = await verifySignatureEd25519(
				signature,
				data.reverse()
			);
			expect(isNotVerified).toBeFalse();
		});
	});

	describe("browser", () => {
		it("verify", async () => {
			const keypair = await Ed25519Keypair.create();
			const data = new Uint8Array([1, 2, 3]);
			const signature = await signEd25519Browser(data, keypair, PreHash.NONE);
			const isVerified = await verifySignatureEd25519Browser(signature, data);
			expect(isVerified).toBeTrue();

			const isNotVerified = await verifySignatureEd25519Browser(
				signature,
				data.reverse()
			);
			expect(isNotVerified).toBeFalse();
		});

		it("verify hashed", async () => {
			const keypair = await Ed25519Keypair.create();
			const data = new Uint8Array([1, 2, 3]);
			const signature = await signEd25519Browser(
				data,
				keypair,
				PreHash.SHA_256
			);
			const isVerified = await verifySignatureEd25519Browser(signature, data);
			expect(isVerified).toBeTrue();

			const isNotVerified = await verifySignatureEd25519Browser(
				signature,
				data.reverse()
			);
			expect(isNotVerified).toBeFalse();
		});
	});

	describe("mixed api", () => {
		let signFns = [signEd25519Browser, signEd25519];
		let verifyFns = [verifySignatureEd25519Browser, verifySignatureEd25519];

		it("sign", async () => {
			const keypair = await Ed25519Keypair.create();
			const data = new Uint8Array([1, 2, 3]);
			let signatures: SignatureWithKey[] = [];
			for (const signFn of signFns) {
				const signature = await signFn(data, keypair, PreHash.NONE);
				for (const verifyFn of verifyFns) {
					expect(await verifyFn(signature, data)).toBeTrue();
				}

				signatures.push(signature);
				if (signatures.length > 1) {
					expect(
						signatures[signatures.length - 2].equals(
							signatures[signatures.length - 1]
						)
					).toBeTrue();
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
					expect(await verifyFn(signature, data)).toBeTrue();
				}

				if (signatures.length > 1) {
					expect(
						signatures[signatures.length - 2].equals(
							signatures[signatures.length - 1]
						)
					).toBeTrue();
				}
			}
		});
	});
});

describe("X25519", () => {
	it("ser/der", async () => {
		const keypair = await X25519Keypair.create();
		const derser = deserialize(serialize(keypair), X25519Keypair);
		expect(derser.publicKey.publicKey).toEqual(keypair.publicKey.publicKey);
	});
});

describe("Sepck2561k1Keccak256", () => {
	const data = new Uint8Array([1, 2, 3]);

	it("verify", async () => {
		const wallet = Wallet.createRandom();
		const pk = new Secp256k1Keccak256PublicKey({
			address: wallet.address,
		});
		const signature = await wallet.signMessage(data);
		let signatureBytes = Buffer.from(signature);
		const signatureWithKey = new SignatureWithKey({
			prehash: PreHash.NONE,
			publicKey: pk,
			signature: signatureBytes,
		});

		const isVerified = await verifySignatureSecp256k1(signatureWithKey, data);
		expect(isVerified).toBeTrue();

		const isNotVerified = await verifySignatureSecp256k1(
			signatureWithKey,
			data.reverse()
		);
		expect(isNotVerified).toBeFalse();
	});
	it("from PeerId", async () => {
		const peerId = await createSecp256k1PeerId();
		const keypair = Sec256k1Keccak256Keypair.from(peerId);
		const privateKey = new supportedKeys["secp256k1"].Secp256k1PrivateKey(
			peerId.privateKey!.slice(4)
		);
		const publicKeyComputed = privateKey.public;
		expect(publicKeyComputed.bytes).toEqual(peerId.publicKey);
		const wallet = new Wallet(peerId.privateKey!.slice(4));
		const signature = await keypair.sign(data);
		expect(await verifySignatureSecp256k1(signature, data)).toBeTrue();
	});

	it("ser/der", async () => {
		const wallet = await Wallet.createRandom();
		const pk = new Secp256k1Keccak256PublicKey({
			address: wallet.address,
		});
		const derser = deserialize(serialize(pk), Secp256k1Keccak256PublicKey);
		expect(derser.address).toEqual(pk.address);
	});
});
