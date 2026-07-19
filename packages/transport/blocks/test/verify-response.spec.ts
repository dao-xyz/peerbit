import * as dagCbor from "@ipld/dag-cbor";
import { createStore } from "@peerbit/any-store";
import {
	calculateRawCid,
	checkDecodeBlock,
	codecCodes,
	createBlock,
	stringifyCid,
	verifyBlockBytes,
} from "@peerbit/blocks-interface";
import { Ed25519Keypair } from "@peerbit/crypto";
import { expect } from "chai";
import { sha256 } from "multiformats/hashes/sha2";
import pDefer from "p-defer";
import { AnyBlockStore } from "../src/any-blockstore.js";
import { BlockRequest, BlockResponse, RemoteBlocks } from "../src/remote.js";

type Codec = {
	code: number;
	decode(bytes: Uint8Array): unknown;
};

type ProviderCache = {
	get(cid: string): string[] | undefined;
};

describe("opaque remote block verification", () => {
	const remotes: RemoteBlocks[] = [];

	afterEach(async () => {
		for (const remote of remotes.splice(0).reverse()) {
			if (remote.status !== "closed") {
				await remote.stop();
			}
		}
	});

	const createRemote = async (requestSeen: ReturnType<typeof pDefer<void>>) => {
		const key = await Ed25519Keypair.create();
		const remote = new RemoteBlocks({
			local: new AnyBlockStore(createStore()),
			publicKey: key.publicKey,
			publish: async (message) => {
				if (message instanceof BlockRequest) {
					requestSeen.resolve();
				}
			},
			waitFor: async (): Promise<string[]> => [],
		});
		remotes.push(remote);
		await remote.start();
		return remote;
	};

	const cachedProviders = (remote: RemoteBlocks, cid: string) =>
		(
			remote as unknown as {
				_providerCache: ProviderCache;
			}
		)._providerCache.get(cid);

	const replaceDagDecoder = (onDecode: () => void) => {
		const codecs = codecCodes as unknown as Record<number, Codec>;
		const original = codecs[dagCbor.code]!;
		codecs[dagCbor.code] = {
			...original,
			decode: (bytes) => {
				onDecode();
				return original.decode(bytes);
			},
		};
		return () => {
			codecs[dagCbor.code] = original;
		};
	};

	it("verifies CID semantics with a custom hasher without decoding", async () => {
		const block = await createBlock({ hello: "world" }, "dag-cbor");
		const operations: string[] = [];
		const hasher = {
			...sha256,
			digest: async (bytes: Uint8Array) => {
				operations.push("hash");
				return sha256.digest(bytes);
			},
		};
		const codec = {
			code: dagCbor.code,
			decode: () => {
				operations.push("decode");
			},
		};

		const verified = await verifyBlockBytes(block.cid, block.bytes, {
			codec,
			hasher,
		});

		expect(verified.equals(block.cid)).to.equal(true);
		expect(verified.version).to.equal(block.cid.version);
		expect(verified.code).to.equal(block.cid.code);
		expect(operations).to.deep.equal(["hash"]);
	});

	it("checks a DAG-CBOR digest before invoking its decoder", async () => {
		const expected = await createBlock({ expected: true }, "dag-cbor");
		const mismatching = await createBlock({ expected: false }, "dag-cbor");
		const operations: string[] = [];
		const hasher = {
			...sha256,
			digest: async (bytes: Uint8Array) => {
				operations.push("hash");
				return sha256.digest(bytes);
			},
		};
		const codec = {
			...dagCbor,
			decode: (bytes: Uint8Array) => {
				operations.push("decode");
				return dagCbor.decode(bytes);
			},
		};

		let rejected: unknown;
		try {
			await checkDecodeBlock(expected.cid, mismatching.bytes, {
				codec,
				hasher,
			});
		} catch (error) {
			rejected = error;
		}
		expect((rejected as Error)?.message).to.equal("CID does not match");
		expect(operations).to.deep.equal(["hash"]);

		operations.length = 0;
		const decoded = await checkDecodeBlock(expected.cid, expected.bytes, {
			codec,
			hasher,
		});
		expect(decoded.value).to.deep.equal({ expected: true });
		expect(operations).to.deep.equal(["hash", "decode"]);
	});

	it("preserves raw block bytes and CID semantics", async () => {
		const bytes = new Uint8Array([5, 4, 3]);
		const { block } = await calculateRawCid(bytes);

		const checked = await checkDecodeBlock(block.cid, bytes, {});

		expect(checked.cid.equals(block.cid)).to.equal(true);
		expect(checked.cid.version).to.equal(block.cid.version);
		expect(checked.cid.code).to.equal(block.cid.code);
		expect(checked.bytes).to.equal(bytes);
		expect(checked.value).to.equal(bytes);
	});

	it("rejects mismatching DAG-CBOR before decode or provider learning", async () => {
		const expected = await createBlock({ expected: true }, "dag-cbor");
		const mismatching = await createBlock(
			{
				untrusted: Array.from({ length: 256 }, (_, index) => ({
					index,
					value: `value-${index}`,
				})),
			},
			"dag-cbor",
		);
		const cid = stringifyCid(expected.cid);
		const requestSeen = pDefer<void>();
		const remote = await createRemote(requestSeen);
		let decodeCalls = 0;
		const restore = replaceDagDecoder(() => decodeCalls++);

		try {
			const read = remote.get(cid, {
				remote: { from: ["requested-provider"], timeout: 100 },
			});
			await requestSeen.promise;
			await remote.onMessage(new BlockResponse(cid, mismatching.bytes), {
				from: "invalid-responder",
			});

			expect(await read).to.equal(undefined);
			expect(decodeCalls).to.equal(0);
			expect(cachedProviders(remote, cid)).to.deep.equal([
				"requested-provider",
			]);
		} finally {
			restore();
		}
	});

	it("returns valid requested DAG-CBOR bytes without transport decoding", async () => {
		const operations: string[] = [];
		const hasher = {
			...sha256,
			digest: async (bytes: Uint8Array) => {
				operations.push("hash");
				return sha256.digest(bytes);
			},
		};
		const block = await createBlock({ hello: "remote" }, "dag-cbor", {
			hasher,
		});
		operations.length = 0;
		const cid = stringifyCid(block.cid);
		const requestSeen = pDefer<void>();
		const remote = await createRemote(requestSeen);
		const restore = replaceDagDecoder(() => operations.push("decode"));

		try {
			const read = remote.get(cid, {
				remote: {
					from: ["requested-provider"],
					timeout: 1_000,
					hasher,
				} as any,
			});
			await requestSeen.promise;
			await remote.onMessage(new BlockResponse(cid, block.bytes), {
				from: "valid-responder",
			});

			expect(await read).to.deep.equal(block.bytes);
			expect(operations).to.deep.equal(["hash"]);
			expect(cachedProviders(remote, cid)).to.deep.equal([
				"valid-responder",
				"requested-provider",
			]);
		} finally {
			restore();
		}
	});
});
