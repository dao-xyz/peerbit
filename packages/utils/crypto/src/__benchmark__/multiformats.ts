import { deserialize, field, serialize, variant } from "@dao-xyz/borsh";
import B from "benchmark";
import crypto from "crypto";
import { equals } from "uint8arrays";
/* import { hash } from 'blake3' */
import { hasher, bytes } from "multiformats";
import { sha256 } from "multiformats/hashes/sha2";
import init, {
	hash as bhash,
	hash_mut,
	hash_unsafe,
	alloc,
} from "./pkg/blake3_bindgen.js";

// Allocate a shared buffer with enough space for the input and output
const wasmModule = await init();
export const blak3mf = hasher.from({
	name: "blake3",
	code: 0xf2,
	encode: (input) => bhash(input) as any,
});

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
@variant(3)
class CIDWebSha256 extends DummyCid {
	@field({ type: Uint8Array })
	hash: Uint8Array;

	constructor(hash: Uint8Array) {
		super();
		this.hash = hash;
	}

	static encode(bytes: Uint8Array): CIDWebSha256 {
		const digest = crypto.createHash("sha256").update(bytes).digest();
		return new CIDSha256(digest);
	}

	equals(other: DummyCid) {
		if (other instanceof CIDWebSha256) {
			return equals(this.hash, other.hash);
		}
		return false;
	}
	get bytes() {
		return serialize(this);
	}
}

@variant(1)
class CIDBlake3 extends DummyCid {
	@field({ type: Uint8Array })
	hash: Uint8Array;

	constructor(hash: Uint8Array) {
		super();
		this.hash = hash;
	}

	static encode(bytes: Uint8Array): CIDBlake3 {
		const digest = bhash(bytes);
		return new CIDBlake3(digest);
	}

	equals(other: DummyCid) {
		if (other instanceof CIDBlake3) {
			return equals(this.hash, other.hash);
		}
		return false;
	}
	get bytes() {
		return serialize(this);
	}
}

@variant(1)
class CIDBlake3Wasm extends DummyCid {
	@field({ type: Uint8Array })
	hash: Uint8Array;

	constructor(hash: Uint8Array) {
		super();
		this.hash = hash;
	}

	static encode(ptr: number, length: number): CIDBlake3Wasm {
		return new CIDBlake3(hash_unsafe(ptr, length));
	}

	equals(other: DummyCid) {
		if (other instanceof CIDBlake3Wasm) {
			return equals(this.hash, other.hash);
		}
		return false;
	}
	get bytes() {
		return serialize(this);
	}
}

const sizes = [1e3, 10 * 1e3, 1e6];
const suite = new B.Suite("_", { minSamples: 1, initCount: 1, maxTime: 5 });
const SAMPLE_SIZE = 1e3;
const sample = {};
const sharedAllocs: { [size: number]: number } = {};

for (const size of sizes) {
	sample[size] = [];
	for (let i = 0; i < SAMPLE_SIZE; i++) {
		sample[size].push(crypto.randomBytes(size));
	}
	sharedAllocs[size] = 0; // alloc(size);
}

const getSample = (size: number): Uint8Array => {
	return sample[size][Math.floor(Math.random() * SAMPLE_SIZE)];
};

/* const getSampleShared = (size: number): Uint8Array => {
	return new Uint8Array(wasmModule.memory.buffer, sharedAllocs[size], size);
};
 */

for (const size of sizes) {
	/* suite.add("multiformats sha256, size: " + size / 1e3 + "kb", {
		defer: true,
		async: true,
		fn: async (deferred) => {
			{
				const rng = getSample(size);
				const cid = await Block.encode({
					value: rng,
					codec,
					hasher: sha256,
				});
				const cidString = stringifyCid(cid.cid);
				const cidObject = cidifyString(cidString);
				const checked = await checkDecodeBlock(cidObject, rng, {
					hasher: sha256,
					codec,
				});
				if (!checked) {
					throw new Error("Not verified");
				}
				deferred.resolve();
			}
		},
	});

	suite.add("multiformats blake3, size: " + size / 1e3 + "kb", {
		defer: true,
		async: true,
		fn: async (deferred) => {
			{
				const rng = getSample(size);
				const cid = await Block.encode({
					value: rng,
					codec,
					hasher: blak3mf,
				});
				const cidString = stringifyCid(cid.cid);
				const cidObject = cidifyString(cidString);
				const checked = await checkDecodeBlock(cidObject, rng, {
					hasher: blak3mf,
					codec,
				});
				if (!checked) {
					throw new Error("Not verified");
				}
				deferred.resolve();
			}
		},
	});
*/
	suite.add("u8 sha256 discriminator, size: " + size / 1e3 + "kb", {
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
		},
	});

	suite.add("u8 sha256 web discriminator, size: " + size / 1e3 + "kb", {
		defer: true,
		fn: (deferred) => {
			{
				const rng = getSample(size);
				const cid = CIDWebSha256.encode(rng);
				const cidBytes = cid.bytes;
				const cidObject = DummyCid.decode(cidBytes);
				const cid2 = CIDWebSha256.encode(rng);
				if (!cidObject.equals(cid2)) {
					throw new Error("Not verified");
				}
				deferred.resolve();
			}
		},
	});

	suite.add("u8 blake wasm discriminator, size: " + size / 1e3 + "kb", {
		defer: true,
		fn: async (deferred) => {
			{
				//const rng = getSampleShared(size);
				const cid = await CIDBlake3Wasm.encode(sharedAllocs[size], size);
				const cidBytes = cid.bytes;
				const cidObject = DummyCid.decode(cidBytes);
				const cid2 = await CIDBlake3Wasm.encode(sharedAllocs[size], size);
				if (!cidObject.equals(cid2)) {
					throw new Error("Not verified");
				}
				deferred.resolve();
			}
		},
	});

	suite.add("u8 blake discriminator, size: " + size / 1e3 + "kb", {
		defer: true,
		fn: async (deferred) => {
			{
				const rng = getSample(size);
				const cid = await CIDBlake3.encode(rng);
				const cidBytes = cid.bytes;
				const cidObject = DummyCid.decode(cidBytes);
				const cid2 = await CIDBlake3.encode(rng);
				if (!cidObject.equals(cid2)) {
					throw new Error("Not verified");
				}
				deferred.resolve();
			}
		},
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
		},
	});
}
suite
	.on("cycle", (event: any) => {
		console.log(String(event.target));
	})
	.run({ async: true });
