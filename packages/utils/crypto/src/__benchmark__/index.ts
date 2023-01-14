import B from 'benchmark'
import crypto from 'crypto';
import { Ed25519Keypair } from '../ed25519.js'
import { verify } from '../signature.js';

const large = crypto.randomBytes(1e6); //  1mb

const keypair = Ed25519Keypair.create();
const signatures: [Uint8Array, Uint8Array][] = []
for (let i = 0; i < 10000; i++) {
	const data = crypto.randomBytes(1e3);
	signatures.push([data, keypair.sign(data)])
}

const signaturesHash: [Uint8Array, Uint8Array][] = []
for (let i = 0; i < 10000; i++) {
	const data = crypto.randomBytes(1e3);
	signaturesHash.push([data, keypair.sign(data, true)])
}

const suite = new B.Suite('ed25519')
suite.add("sign", () => {
	const data = crypto.randomBytes(1e3); // 1kb
	keypair.sign(data);
}).add("hash+sign", () => {
	const data = crypto.randomBytes(1e3);  // 1kb
	keypair.sign(data, true);
}).add('verify', () => {
	const [data, signature] = signatures[Math.floor(Math.random() * signatures.length)];
	if (!verify(signature, keypair.publicKey, data)) {
		throw new Error("Unverified")
	}
}).add('hash+verify', () => {
	const [data, signature] = signaturesHash[Math.floor(Math.random() * signatures.length)];
	if (!verify(signature, keypair.publicKey, data, true)) {
		throw new Error("Unexpected")
	}
}).on('error', (error: any) => {
	throw error;
}).on('cycle', (event: any) => {
	console.log(String(event.target));
}).run(({ async: true }))

