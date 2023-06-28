import B from "benchmark";
import crypto from "crypto";
import { sha256, sha256Base64 } from "../hash.js";
import { deserialize, field, serialize } from "@dao-xyz/borsh";

//node --loader ts-node/esm ./src/__benchmark__/hash.ts

/**
 * Benchmark for hashing + serializing
 */

const size = 1e3;
const data: Uint8Array[] = [];
for (let i = 0; i < 100; i++) {
	data.push(crypto.randomBytes(size));
}

class MSstring {
	@field({ type: "string" })
	string: string;

	constructor(string: string) {
		this.string = string;
	}
}

class MBytes {
	@field({ type: Uint8Array })
	bytes: Uint8Array;

	constructor(bytes: Uint8Array) {
		this.bytes = bytes;
	}
}

const getSample = () => {
	return data[Math.floor(Math.round(Math.random() * (data.length - 1)))];
};
const suite = new B.Suite("ed25519");
suite
	.add("hash-to-bytes", {
		fn: async (deferred) => {
			deserialize(serialize(new MBytes(await sha256(getSample()))), MBytes);
			deferred.resolve();
		},
		defer: true,
	})
	.add("hash-to-string", {
		fn: async (deferred) => {
			deserialize(
				serialize(new MSstring(await sha256Base64(getSample()))),
				MSstring
			);
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
