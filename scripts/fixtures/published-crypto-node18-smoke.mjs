import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

assert.match(
	process.version,
	/^v18\./,
	"this contract must execute on Node 18",
);

const {
	PreHash,
	Secp256k1Keypair,
	Secp256k1PrivateKey,
	Secp256k1PublicKey,
	recoverPublicKeyFromSignature,
	verify,
} = await import("@peerbit/crypto");

const cryptoEntry = fileURLToPath(import.meta.resolve("@peerbit/crypto"));
const requireFromCrypto = createRequire(cryptoEntry);
const nobleEntry = requireFromCrypto.resolve("@noble/curves/secp256k1");
let nobleDirectory = dirname(nobleEntry);
let nobleManifest;
for (let depth = 0; depth < 8; depth++) {
	try {
		const candidate = JSON.parse(
			await readFile(join(nobleDirectory, "package.json"), "utf8"),
		);
		if (candidate.name === "@noble/curves") {
			nobleManifest = candidate;
			break;
		}
	} catch (error) {
		if (error?.code !== "ENOENT") {
			throw error;
		}
	}
	nobleDirectory = dirname(nobleDirectory);
}
assert(nobleManifest, "could not resolve @peerbit/crypto's direct noble edge");
assert.equal(nobleManifest.version, "1.9.7");
assert.equal(nobleManifest.engines?.node, "^14.21.3 || >=16");

const vectors = [
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
];
const bytes = (hex) => Uint8Array.from(Buffer.from(hex.slice(2), "hex"));
const decoder = new TextDecoder();

for (const vector of vectors) {
	const digest = bytes(vector.digest);
	const recoveredPublicKey = recoverPublicKeyFromSignature(
		digest,
		vector.signature,
	);
	assert.equal(recoveredPublicKey.length, 33, "recovery must stay compressed");
	const keypair = new Secp256k1Keypair({
		privateKey: new Secp256k1PrivateKey({
			privateKey: bytes(vector.privateKey),
		}),
		publicKey: new Secp256k1PublicKey({ publicKey: recoveredPublicKey }),
	});
	const signature = await keypair.sign(digest, PreHash.NONE);
	assert.equal(decoder.decode(signature.signature), vector.signature);
	assert.equal(
		Number.parseInt(vector.signature.slice(-2), 16) - 27,
		vector.recoveryParam,
	);
	assert.equal(await verify(signature, digest), true);
	assert.deepEqual(
		recoverPublicKeyFromSignature(digest, vector.signature),
		recoveredPublicKey,
	);
}

const guardedKeypair = new Secp256k1Keypair({
	privateKey: new Secp256k1PrivateKey({
		privateKey: bytes(vectors[0].privateKey),
	}),
	publicKey: new Secp256k1PublicKey({
		publicKey: recoverPublicKeyFromSignature(
			bytes(vectors[0].digest),
			vectors[0].signature,
		),
	}),
});
await assert.rejects(
	guardedKeypair.sign(new Uint8Array(31), PreHash.NONE),
	/exactly 32-byte/,
);

console.log(
	`Published @peerbit/crypto passed fixed wire vectors on Node ${process.versions.node} with direct @noble/curves ${nobleManifest.version}.`,
);
