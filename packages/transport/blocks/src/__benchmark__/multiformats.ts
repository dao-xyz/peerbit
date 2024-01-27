import { deserialize, field, serialize, variant } from "@dao-xyz/borsh";
import B from "benchmark";
import crypto from "crypto";
import { sha256 } from "multiformats/hashes/sha2";
import { encode } from "multiformats/block";
import {
	checkDecodeBlock,
	cidifyString,
	codecMap,
	stringifyCid
} from "../block.js";
import { equals } from "uint8arrays";

// Run with "node --loader ts-node/esm ./src/__benchmark__/multiformats.ts"
// size: 1kb x 785 ops/sec ±2.66% (86 runs sampled)
// size: 1000kb x 38.14 ops/sec ±1.43% (40 runs sampled)

abstract class DummyCid {
	static decode(bytes: Uint8Array) {
		return deserialize(bytes, DummyCid);
	}
	abstract equals(other: DummyCid);
}

@variant(0)
class CIDSha256 extends DummyCid {
	@field({ type: Uint8Array })
	hash: Uint8Array;

	constructor(hash: Uint8Array) {
		super();
		this.hash = hash;
	}

	static encode(bytes: Uint8Array): CIDSha256 {
		const digest = crypto.createHash("sha256").update(bytes).digest();
		return new CIDSha256(digest);
	}

	equals(other: DummyCid) {
		if (other instanceof CIDSha256) {
			return equals(this.hash, other.hash);
		}
		return false;
	}
	get bytes() {
		return serialize(this);
	}
}

const codec = codecMap["raw"];
const sizes = [1e3, 1e6];
const suite = new B.Suite("_", { minSamples: 1, initCount: 1, maxTime: 5 });

const SAMPLE_SIZE = 1e3;
const sample = {};
for (const size of sizes) {
	sample[size] = [];
	for (let i = 0; i < SAMPLE_SIZE; i++) {
		sample[size].push(crypto.randomBytes(size));
	}
}

const getSample = (size: number): Uint8Array => {
	return sample[size][Math.floor(Math.random() * SAMPLE_SIZE)];
};

for (const size of sizes) {
	suite.add("multiformats, size: " + size / 1e3 + "kb", {
		defer: true,
		async: true,
		fn: async (deferred) => {
			{
				const rng = getSample(size);
				const cid = await encode({
					value: rng,
					codec,
					hasher: sha256
				});
				const cidString = stringifyCid(cid.cid);
				const cidObject = cidifyString(cidString);
				const checked = await checkDecodeBlock(cidObject, rng, {
					hasher: sha256,
					codec
				});
				if (!checked) {
					throw new Error("Not verified");
				}
				deferred.resolve();
			}
		}
	});

	suite.add("dummy, size: " + size / 1e3 + "kb", {
		defer: true,
		fn: (deferred) => {
			{
				const rng = getSample(size);
				const cid = CIDSha256.encode(rng);
				const cidBytes = cid.bytes;
				const cidObject = DummyCid.decode(cidBytes);
				const cid2 = CIDSha256.encode(rng);
				if (!cidObject.equals(cid2)) {
					throw new Error("Not verified");
				}
				deferred.resolve();
			}
		}
	});

	suite.add("sha256 digest hex, size: " + size / 1e3 + "kb", {
		defer: true,
		fn: (deferred) => {
			{
				const rng = getSample(size);

				const cid = crypto.createHash("sha256").update(rng).digest("hex");

				const cid2 = crypto.createHash("sha256").update(rng).digest("hex");

				if (cid !== cid2) {
					throw new Error("Not verified");
				}
				deferred.resolve();
			}
		}
	});
}
suite
	.on("cycle", (event: any) => {
		console.log(String(event.target));
	})
	.run({ async: true });
