import B from "benchmark";
import crypto from "crypto";
import { Ed25519Keypair } from "../src/ed25519.js";
import { PreHash } from "../src/prehash.js";
import { SignatureWithKey, verify } from "../src/signature.js";

//node --loader ts-node/esm ./benchmark/index.ts

const keypair = await Ed25519Keypair.create();
const signatures: [Uint8Array, SignatureWithKey][] = [];
const size = 1e5;
for (let i = 0; i < 10000; i++) {
	const data = crypto.randomBytes(size);
	signatures.push([data, await keypair.sign(data, PreHash.NONE)]);
}

const signaturesHash: [Uint8Array, SignatureWithKey][] = [];
for (let i = 0; i < 10000; i++) {
	const data = crypto.randomBytes(size);
	signaturesHash.push([data, await keypair.sign(data, PreHash.SHA_256)]);
}

const suite = new B.Suite("ed25519");
suite
	.add("sign", {
		fn: async (deferred: any) => {
			const data = crypto.randomBytes(size); // 1kb
			await keypair.sign(data, PreHash.NONE);
			deferred.resolve();
		},
		defer: true,
	})
	.add("hash+sign", {
		fn: async (deferred: any) => {
			const data = crypto.randomBytes(size); // 1kb
			await keypair.sign(data, PreHash.SHA_256);
			deferred.resolve();
		},
		defer: true,
	})
	.add("verify", {
		fn: async (deferred: any) => {
			const [data, signature] =
				signatures[Math.floor(Math.random() * signatures.length)];
			if (!(await verify(signature, data))) {
				throw new Error("Unverified");
			}
			deferred.resolve();
		},
		defer: true,
	})
	.add("hash+verify", {
		fn: async (deferred: any) => {
			const [data, signature] =
				signaturesHash[Math.floor(Math.random() * signatures.length)];
			if (!(await verify(signature, data))) {
				throw new Error("Unexpected");
			}
			deferred.resolve();
		},
		defer: true,
	})
	.on("error", (error: any) => {
		throw error;
	})
	.on("cycle", (event: any) => {
		console.log(String(event.target));
	})
	.run({ async: true });
