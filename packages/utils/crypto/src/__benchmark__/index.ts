import B from "benchmark";
import crypto from "crypto";
import { Ed25519Keypair } from "../ed25519.js";
import { verify } from "../signature.js";
//node --loader ts-node/esm ./src/__benchmark__/index.ts
const large = crypto.randomBytes(1e6); //  1mb

const keypair = Ed25519Keypair.create();
const signatures: [Uint8Array, Uint8Array][] = [];
for (let i = 0; i < 10000; i++) {
	const data = crypto.randomBytes(1e3);
	signatures.push([data, await keypair.sign(data)]);
}

const signaturesHash: [Uint8Array, Uint8Array][] = [];
for (let i = 0; i < 10000; i++) {
	const data = crypto.randomBytes(1e3);
	signaturesHash.push([data, await keypair.sign(data, true)]);
}

const suite = new B.Suite("ed25519");
suite
	.add("sign", {
		fn: async (deferred) => {
			const data = crypto.randomBytes(1e3); // 1kb
			await keypair.sign(data);
			deferred.resolve();
		},
		defer: true,
	})
	.add("hash+sign", {
		fn: async (deferred) => {
			const data = crypto.randomBytes(1e3); // 1kb
			await keypair.sign(data, true);
			deferred.resolve();
		},
		defer: true,
	})
	.add("verify", {
		fn: async (deferred) => {
			const [data, signature] =
				signatures[Math.floor(Math.random() * signatures.length)];
			if (!(await verify(signature, keypair.publicKey, data))) {
				throw new Error("Unverified");
			}
			deferred.resolve();
		},
		defer: true,
	})
	.add("hash+verify", {
		fn: async (deferred) => {
			const [data, signature] =
				signaturesHash[Math.floor(Math.random() * signatures.length)];
			if (!(await verify(signature, keypair.publicKey, data, true))) {
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
