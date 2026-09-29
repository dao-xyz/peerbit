import { type Blocks, calculateRawCid } from "@peerbit/blocks-interface";
import { Ed25519Keypair, Ed25519PublicKey, PreHash } from "@peerbit/crypto";
import assert from "node:assert/strict";
import { toString } from "uint8arrays";
import {
	CHECKPOINT_MAX_ROOT_BYTES,
	type CheckpointFrontierRecord,
	createCheckpointFreeze,
	createCheckpointSnapshot,
	readCheckpointFreeze,
	readCheckpointSnapshot,
} from "../src/checkpoint-snapshot.js";

const encoder = new TextEncoder();
const encode = (value: unknown) => encoder.encode(JSON.stringify(value));
const decode = (bytes: Uint8Array) =>
	JSON.parse(new TextDecoder().decode(bytes));

const memory = () => {
	const bytes = new Map<string, Uint8Array>();
	const gets: string[] = [];
	const puts: string[] = [];
	const blocks = {
		get: (cid: string) => {
			gets.push(cid);
			return bytes.get(cid);
		},
		put: async (value: Uint8Array) => {
			const { cid } = await calculateRawCid(value);
			bytes.set(cid, new Uint8Array(value));
			puts.push(cid);
			return cid;
		},
		putKnown: (cid: string, value: Uint8Array) => {
			bytes.set(cid, new Uint8Array(value));
			puts.push(cid);
			return cid;
		},
	} as Blocks;
	return { bytes, blocks, gets, puts };
};

describe("checkpoint snapshot", function () {
	this.timeout(30_000);
	let owner: Ed25519Keypair;
	const resource = new Uint8Array(32).fill(7);
	let frontier: CheckpointFrontierRecord[];
	before(async () => {
		owner = await Ed25519Keypair.create();
		frontier = await Promise.all(
			Array.from({ length: 130 }, async (_, index) => ({
				cid: (await calculateRawCid(encoder.encode(`entry-${index}`))).cid,
				created: (1n << 64n) - 1n - BigInt(index),
			})),
		);
		frontier.sort((a, b) => (a.cid < b.cid ? -1 : 1));
	});

	const fixture = async () => {
		const store = memory();
		const genesis = await createCheckpointSnapshot({
			blocks: store.blocks,
			resource,
			owner,
			epoch: 0n,
			previous: null,
			frontier: [],
		});
		const sealed = await createCheckpointSnapshot({
			blocks: store.blocks,
			resource,
			owner,
			epoch: 1n,
			previous: genesis.cid,
			frontier,
		});
		return { ...store, genesis, sealed };
	};

	const collect = async (records: AsyncIterable<CheckpointFrontierRecord>) => {
		const result: CheckpointFrontierRecord[] = [];
		for await (const record of records) result.push(record);
		return result;
	};

	const signedRoot = async (
		store: ReturnType<typeof memory>,
		root: unknown,
	) => {
		const signature = await owner.sign(encode(root), PreHash.NONE);
		return store.blocks.put(
			encode({ root, signature: toString(signature.signature, "base16") }),
		);
	};

	it("round-trips genesis and a chunked frontier without rounding u64 timestamps", async () => {
		const { blocks, genesis, sealed, gets, puts, bytes } = await fixture();
		assert.equal(genesis.epoch, 0n);
		assert.equal(genesis.count, 0);
		assert.deepEqual(await collect(genesis.frontier()), []);
		assert.equal(sealed.previous, genesis.cid);
		assert.equal(sealed.count, 130);
		const root = decode(bytes.get(sealed.cid)!);
		assert.equal(root.root.chunks.length, 2);
		const putCount = puts.length;
		const reopened = await readCheckpointSnapshot({
			blocks,
			cid: sealed.cid,
			resource,
			owner: owner.publicKey,
		});
		assert.deepEqual(await collect(reopened.frontier()), frontier);
		assert.equal(
			puts.length,
			putCount,
			"read validation must not publish blocks",
		);
		assert.equal(
			gets.filter((cid) => frontier.some((record) => record.cid === cid))
				.length,
			0,
			"snapshot codec must not fetch parents or claim entry authentication",
		);
	});

	it("authenticates chunked freeze manifests without accepting them as checkpoint proposals", async () => {
		const store = await fixture();
		const freeze = await createCheckpointFreeze({
			blocks: store.blocks,
			resource,
			owner,
			epoch: 1n,
			previous: store.genesis.cid,
			frontier,
		});
		assert.notEqual(freeze.cid, store.sealed.cid);
		const other = await Ed25519Keypair.create();
		const reopened = await readCheckpointFreeze({
			blocks: store.blocks,
			cid: freeze.cid,
			resource,
			writers: [other.publicKey, owner.publicKey],
		});
		assert(reopened.writer.equals(owner.publicKey));
		reopened.writer.publicKey.fill(0);
		assert(reopened.writer.equals(owner.publicKey));
		assert.deepEqual(reopened.freezes, []);
		assert.deepEqual(await collect(reopened.frontier()), frontier);
		await assert.rejects(
			readCheckpointSnapshot({
				blocks: store.blocks,
				cid: freeze.cid,
				resource,
				owner: owner.publicKey,
			}),
			/Invalid checkpoint root/,
		);
		await assert.rejects(
			readCheckpointFreeze({
				blocks: store.blocks,
				cid: store.sealed.cid,
				resource,
				writers: [owner.publicKey],
			}),
			/Invalid checkpoint root/,
		);
	});

	it("binds an immutable sorted manifest set in checkpoint proposals", async () => {
		const store = await fixture();
		const manifests = [frontier[1]!.cid, frontier[0]!.cid];
		const sealed = await createCheckpointSnapshot({
			blocks: store.blocks,
			resource,
			owner,
			epoch: 1n,
			previous: store.genesis.cid,
			frontier: [],
			freezes: manifests,
		});
		manifests.length = 0;
		assert.deepEqual(
			sealed.freezes,
			frontier.slice(0, 2).map((item) => item.cid),
		);
		assert(Object.isFrozen(sealed.freezes));
		const reopened = await readCheckpointSnapshot({
			blocks: store.blocks,
			cid: sealed.cid,
			resource,
			owner: owner.publicKey,
		});
		assert.deepEqual(reopened.freezes, sealed.freezes);
		const wire = decode(store.bytes.get(sealed.cid)!);
		wire.root.freezes.reverse();
		const noncanonical = await signedRoot(store, wire.root);
		await assert.rejects(
			readCheckpointSnapshot({
				blocks: store.blocks,
				cid: noncanonical,
				resource,
				owner: owner.publicKey,
			}),
			/Noncanonical checkpoint root/,
		);
		wire.root.freezes.sort();
		wire.root.freezes.pop();
		const changed = await store.blocks.put(encode(wire));
		await assert.rejects(
			readCheckpointSnapshot({
				blocks: store.blocks,
				cid: changed,
				resource,
				owner: owner.publicKey,
			}),
			/signature/,
		);
	});

	it("rejects duplicate, excessive and forbidden freeze bindings before signing", async () => {
		const store = await fixture();
		const base = {
			blocks: store.blocks,
			resource,
			owner,
			epoch: 1n,
			previous: store.genesis.cid,
			frontier: [],
		};
		const putCount = store.puts.length;
		for (const freezes of [
			[frontier[0]!.cid, frontier[0]!.cid],
			frontier.slice(0, 33).map((item) => item.cid),
		]) {
			await assert.rejects(
				createCheckpointSnapshot({ ...base, freezes }),
				/Duplicate|manifest count/,
			);
		}
		await assert.rejects(
			createCheckpointSnapshot({
				...base,
				epoch: 0n,
				previous: null,
				freezes: [frontier[0]!.cid],
			}),
			/manifest binding/,
		);
		const extra = { ...base, freezes: [frontier[0]!.cid] };
		await assert.rejects(createCheckpointFreeze(extra), /manifest binding/);
		await assert.rejects(
			createCheckpointFreeze({ ...base, epoch: 0n, previous: null }),
			/manifest binding/,
		);
		assert.equal(store.puts.length, putCount);
	});

	it("rejects foreign or unauthorized freeze signers and malformed writer rosters", async () => {
		const store = await fixture();
		const freeze = await createCheckpointFreeze({
			blocks: store.blocks,
			resource,
			owner,
			epoch: 1n,
			previous: store.genesis.cid,
			frontier,
		});
		const other = await Ed25519Keypair.create();
		for (const trust of [
			{ resource: new Uint8Array(32), writers: [owner.publicKey] },
			{ resource, writers: [other.publicKey] },
		]) {
			const getCount = store.gets.length;
			await assert.rejects(
				readCheckpointFreeze({
					blocks: store.blocks,
					cid: freeze.cid,
					...trust,
				}),
				/Invalid checkpoint root/,
			);
			assert.equal(store.gets.length, getCount + 1);
		}
		for (const writers of [
			[],
			[owner.publicKey, owner.publicKey],
			Array.from({ length: 33 }, () => owner.publicKey),
		]) {
			const getCount = store.gets.length;
			await assert.rejects(
				readCheckpointFreeze({
					blocks: store.blocks,
					cid: freeze.cid,
					resource,
					writers,
				}),
				/roster|Duplicate checkpoint freeze writer/,
			);
			assert.equal(store.gets.length, getCount);
		}
		const wire = decode(store.bytes.get(freeze.cid)!);
		wire.signature = "00".repeat(64);
		const tampered = await store.blocks.put(encode(wire));
		await assert.rejects(
			readCheckpointFreeze({
				blocks: store.blocks,
				cid: tampered,
				resource,
				writers: [owner.publicKey],
			}),
			/signature/,
		);
	});

	it("captures the freeze resource and writer roster before asynchronous reads", async () => {
		const store = await fixture();
		const freeze = await createCheckpointFreeze({
			blocks: store.blocks,
			resource,
			owner,
			epoch: 1n,
			previous: store.genesis.cid,
			frontier: [],
		});
		const copiedResource = new Uint8Array(resource);
		const copiedKey = new Ed25519PublicKey({
			publicKey: new Uint8Array(owner.publicKey.publicKey),
		});
		const writers = [copiedKey];
		let release!: () => void;
		const gate = new Promise<void>((resolve) => (release = resolve));
		const opened = readCheckpointFreeze({
			blocks: {
				...store.blocks,
				get: async (cid: string) => {
					await gate;
					return store.bytes.get(cid);
				},
			} as Blocks,
			cid: freeze.cid,
			resource: copiedResource,
			writers,
		});
		copiedResource.fill(0);
		copiedKey.publicKey.fill(0);
		writers.length = 0;
		release();
		assert((await opened).writer.equals(owner.publicKey));
	});

	it("rejects signed invalid freeze roots and enforces the shared root byte bound", async () => {
		const store = await fixture();
		const freeze = await createCheckpointFreeze({
			blocks: store.blocks,
			resource,
			owner,
			epoch: 1n,
			previous: store.genesis.cid,
			frontier: [],
		});
		const root = decode(store.bytes.get(freeze.cid)!).root;
		for (const fields of [
			{ freezes: [frontier[0]!.cid] },
			{ epoch: "0", previous: null },
		]) {
			const cid = await signedRoot(store, { ...root, ...fields });
			await assert.rejects(
				readCheckpointFreeze({
					blocks: store.blocks,
					cid,
					resource,
					writers: [owner.publicKey],
				}),
				/manifest binding/,
			);
		}
		const cid = await store.blocks.put(
			new Uint8Array(CHECKPOINT_MAX_ROOT_BYTES + 1),
		);
		await assert.rejects(
			readCheckpointFreeze({
				blocks: store.blocks,
				cid,
				resource,
				writers: [owner.publicKey],
			}),
			/byte capacity/,
		);
	});

	it("retains exact verified chunks and root for a later offline open", async () => {
		const source = await fixture();
		const local = memory();
		const requests: unknown[] = [];
		const remoteBlocks = {
			...local.blocks,
			get: (cid: string, options: unknown) => {
				requests.push(options);
				return source.bytes.get(cid);
			},
		} as Blocks;
		const remote = await readCheckpointSnapshot({
			blocks: remoteBlocks,
			cid: source.sealed.cid,
			resource,
			owner: owner.publicKey,
			remote: true,
		});
		assert.equal(local.puts.length, 0);
		await remote.retain();
		assert.equal(local.puts.at(-1), source.sealed.cid);
		assert.equal(local.puts.length, 3);
		assert.deepEqual(
			requests,
			Array.from({ length: 3 }, () => ({ remote: { replicate: false } })),
		);
		const reopened = await readCheckpointSnapshot({
			blocks: local.blocks,
			cid: source.sealed.cid,
			resource,
			owner: owner.publicKey,
		});
		assert.deepEqual(await collect(reopened.frontier()), frontier);
	});

	it("rejects wrong resources, owners and signed-root mutations before chunk fetch", async () => {
		const store = await fixture();
		const otherOwner = await Ed25519Keypair.create();
		for (const trust of [
			{ resource: new Uint8Array(32), owner: owner.publicKey },
			{ resource, owner: otherOwner.publicKey },
		]) {
			const count = store.gets.length;
			await assert.rejects(
				readCheckpointSnapshot({
					blocks: store.blocks,
					cid: store.sealed.cid,
					...trust,
				}),
				/Invalid checkpoint root/,
			);
			assert.equal(store.gets.length, count + 1);
		}
		const wire = decode(store.bytes.get(store.sealed.cid)!);
		wire.root.epoch = "2";
		const cid = await store.blocks.put(encode(wire));
		await assert.rejects(
			readCheckpointSnapshot({
				blocks: store.blocks,
				cid,
				resource,
				owner: owner.publicKey,
			}),
			/signature/,
		);
	});

	it("rejects noncanonical encoding even with an otherwise valid owner signature", async () => {
		const store = await fixture();
		const wire = decode(store.bytes.get(store.sealed.cid)!);
		const cid = await store.blocks.put(
			encoder.encode(JSON.stringify(wire, null, 1)),
		);
		await assert.rejects(
			readCheckpointSnapshot({
				blocks: store.blocks,
				cid,
				resource,
				owner: owner.publicKey,
			}),
			/Noncanonical checkpoint root/,
		);
		wire.root.epoch = "01";
		const alternate = await signedRoot(store, wire.root);
		await assert.rejects(
			readCheckpointSnapshot({
				blocks: store.blocks,
				cid: alternate,
				resource,
				owner: owner.publicKey,
			}),
			/canonical checkpoint u64/,
		);
	});

	it("rejects missing and mismatched chunk bytes without retaining a root", async () => {
		for (const mode of ["missing", "mismatch"] as const) {
			const store = await fixture();
			const root = decode(store.bytes.get(store.sealed.cid)!).root;
			if (mode === "missing") store.bytes.delete(root.chunks[1]);
			else store.bytes.set(root.chunks[1], encode([]));
			const opened = await readCheckpointSnapshot({
				blocks: store.blocks,
				cid: store.sealed.cid,
				resource,
				owner: owner.publicKey,
			});
			store.puts.length = 0;
			await assert.rejects(
				opened.retain(),
				mode === "missing" ? /Missing checkpoint block/ : /CID does not match/,
			);
			assert.equal(store.puts.includes(store.sealed.cid), false);
		}
	});

	it("checks duplicate/order constraints across chunk boundaries", async () => {
		const store = await fixture();
		const root = decode(store.bytes.get(store.sealed.cid)!).root;
		const tail = decode(store.bytes.get(root.chunks[1])!);
		tail[0][0] = frontier[127]!.cid;
		root.chunks[1] = await store.blocks.put(encode(tail));
		const cid = await signedRoot(store, root);
		const opened = await readCheckpointSnapshot({
			blocks: store.blocks,
			cid,
			resource,
			owner: owner.publicKey,
		});
		await assert.rejects(collect(opened.frontier()), /strictly CID ordered/);
	});

	it("bounds the root before decoding and verifies its CID", async () => {
		const store = await fixture();
		const excessive = new Uint8Array(CHECKPOINT_MAX_ROOT_BYTES + 1);
		const excessiveCid = await store.blocks.put(excessive);
		await assert.rejects(
			readCheckpointSnapshot({
				blocks: store.blocks,
				cid: excessiveCid,
				resource,
				owner: owner.publicKey,
			}),
			/byte capacity/,
		);
		store.bytes.set(store.sealed.cid, encode({}));
		await assert.rejects(
			readCheckpointSnapshot({
				blocks: store.blocks,
				cid: store.sealed.cid,
				resource,
				owner: owner.publicKey,
			}),
			/CID does not match/,
		);
	});

	it("bounds root and chunk structure before parsing CID-valid deeply nested JSON", async () => {
		const text = `${"[".repeat(64)}0${"]".repeat(64)}`;
		for (const target of ["root", "chunk"] as const) {
			const store = await fixture();
			const deepCid = await store.blocks.put(encoder.encode(text));
			let cid = deepCid;
			if (target === "chunk") {
				const root = decode(store.bytes.get(store.sealed.cid)!).root;
				root.chunks[0] = deepCid;
				cid = await signedRoot(store, root);
			}
			const parse = JSON.parse;
			let parsedDeepInput = 0;
			JSON.parse = (input, reviver) => {
				if (input === text) parsedDeepInput++;
				return parse(input, reviver);
			};
			try {
				await assert.rejects(async () => {
					const opened = await readCheckpointSnapshot({
						blocks: store.blocks,
						cid,
						resource,
						owner: owner.publicKey,
					});
					await collect(opened.frontier());
				}, /too deep/);
				assert.equal(parsedDeepInput, 0);
			} finally {
				JSON.parse = parse;
			}
		}
	});

	it("does not sign/store a root for invalid late creator input", async () => {
		const store = memory();
		const previous = (await calculateRawCid(encoder.encode("previous"))).cid;
		let signatures = 0;
		await assert.rejects(
			createCheckpointSnapshot({
				blocks: store.blocks,
				resource,
				owner: {
					publicKey: owner.publicKey,
					sign: async (bytes, prehash) => {
						signatures++;
						return owner.sign(bytes, prehash);
					},
				},
				epoch: 1n,
				previous,
				frontier: [...frontier.slice(0, 128), frontier[127]!],
			}),
			/strictly CID ordered/,
		);
		assert.equal(signatures, 0);
		assert.equal(
			store.puts.length,
			1,
			"only the already validated immutable chunk may remain",
		);
	});

	it("enforces genesis and predecessor constraints", async () => {
		const store = memory();
		await assert.rejects(
			createCheckpointSnapshot({
				blocks: store.blocks,
				resource,
				owner,
				epoch: 0n,
				previous: null,
				frontier: [frontier[0]!],
			}),
			/genesis constraint/,
		);
		await assert.rejects(
			createCheckpointSnapshot({
				blocks: store.blocks,
				resource,
				owner,
				epoch: 1n,
				previous: null,
				frontier: [],
			}),
			/predecessor/,
		);
		assert.equal(store.puts.length, 0);
	});
});
