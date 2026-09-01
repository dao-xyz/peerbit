import { serialize } from "@dao-xyz/borsh";
import { AnyBlockStore, type BlockStore } from "@peerbit/blocks";
import { calculateRawCid } from "@peerbit/blocks-interface";
import { DecryptedThing, X25519Keypair } from "@peerbit/crypto";
import assert from "assert";
import { expect } from "chai";
import sodium from "libsodium-wrappers";
import { createEntry } from "../src/entry-create.js";
import { EntryType } from "../src/entry-type.js";
import { type EntryEncryption, EntryV0 } from "../src/entry-v0.js";
import { Entry } from "../src/entry.js";
import {
	type BoundedEntryV0CausalReachabilityInput,
	type BoundedEntryV0CausalReachabilityLimits,
	checkBoundedEntryV0CausalReachability,
} from "../src/index.js";
import type { SortableEntry } from "../src/log-sorting.js";
import { signKey } from "./fixtures/privateKey.js";

type RawEntry = {
	entry: EntryV0<Uint8Array>;
	cid: string;
	bytes: Uint8Array;
};

const limits: BoundedEntryV0CausalReachabilityLimits = {
	maxEntryBytes: 1 << 20,
	maxDirectParents: 64,
	maxVisitedEntries: 256,
	maxTotalBytes: 16 << 20,
	maxParentLinks: 1024,
	maxResolveBatchSize: 16,
};

const readU32 = (bytes: Uint8Array, offset: number): number =>
	new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength).getUint32(
		offset,
		true,
	);

const writeU32 = (bytes: Uint8Array, offset: number, value: number): void =>
	new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength).setUint32(
		offset,
		value,
		true,
	);

const nextCountOffset = (metaBytes: Uint8Array): number => {
	let offset = 0;
	offset += 2; // Meta + Clock variants.
	offset += 4 + readU32(metaBytes, offset); // Clock id.
	offset += 1 + 8 + 4; // Timestamp variant + fields.
	offset += 4 + readU32(metaBytes, offset); // Gid.
	return offset;
};

const signatureCountOffset = (bytes: Uint8Array): number => {
	let offset = 1; // EntryV0 variant.
	assert.equal(bytes[offset++], 0);
	assert.equal(bytes[offset++], 0);
	offset += 4 + readU32(bytes, offset); // Public metadata.
	assert.equal(bytes[offset++], 0);
	assert.equal(bytes[offset++], 0);
	offset += 4 + readU32(bytes, offset); // Public payload.
	offset += 4; // Reserved bytes.
	assert.equal(bytes[offset++], 1);
	assert.equal(bytes[offset++], 0);
	return offset;
};

describe("bounded raw EntryV0 causal reachability", function () {
	let store: BlockStore;
	let sequence = 0;

	before(async () => {
		await sodium.ready;
		store = new AnyBlockStore();
		await store.start();
	});

	after(async () => {
		await store.stop();
	});

	const createRawEntry = async (
		parents: SortableEntry[] = [],
		options: {
			type?: EntryType;
			encryption?: EntryEncryption;
		} = {},
	): Promise<RawEntry> => {
		const entry = (await createEntry({
			store,
			identity: signKey,
			data: new Uint8Array([sequence++ & 0xff]),
			meta: { next: parents, type: options.type },
			deferStore: true,
			encryption: options.encryption,
		})) as EntryV0<Uint8Array>;
		const prepared = Entry.takePreparedBlock(entry);
		assert(prepared, "expected prepared EntryV0 storage");
		return {
			entry,
			cid: prepared.cid,
			bytes: new Uint8Array(prepared.block.bytes),
		};
	};

	const rawCid = async (seed: number): Promise<string> =>
		(await calculateRawCid(new Uint8Array([0xff, seed]))).cid;

	const mapResolver = (entries: Iterable<RawEntry>, calls: string[][] = []) => {
		const values = new Map(
			[...entries].map((entry) => [entry.cid, entry.bytes] as const),
		);
		return async (cids: readonly string[]) => {
			calls.push([...cids]);
			return new Map(
				[...cids].reverse().map((cid) => [cid, values.get(cid)] as const),
			);
		};
	};

	it("proves a direct parent without resolving the target block", async () => {
		const ancestor = await createRawEntry();
		const descendant = await createRawEntry([ancestor.entry]);
		let resolverCalled = false;

		const result = await checkBoundedEntryV0CausalReachability({
			ancestorCid: ancestor.cid,
			descendant,
			limits,
			resolve: () => {
				resolverCalled = true;
				throw new Error("the direct target must not be fetched");
			},
		});

		expect(result.status).to.equal("ancestor");
		expect(resolverCalled).to.equal(false);
		expect(result.visited.entries).to.equal(1);
	});

	it("accepts canonical hash-bearing EntryV0 bytes as a generic log form", async () => {
		const ancestor = await createRawEntry();
		const descendant = await createRawEntry([ancestor.entry]);
		const hashBearingBytes = serialize(descendant.entry);
		const hashBearingCid = (await calculateRawCid(hashBearingBytes)).cid;

		const result = await checkBoundedEntryV0CausalReachability({
			ancestorCid: ancestor.cid,
			descendant: { cid: hashBearingCid, bytes: hashBearingBytes },
			limits,
			resolve: async () => new Map(),
		});

		expect(result.status).to.equal("ancestor");
	});

	it("proves indirect and CUT ancestry while preserving proof across a missing merge branch", async () => {
		const ancestor = await createRawEntry();
		const cut = await createRawEntry([ancestor.entry], { type: EntryType.CUT });
		const missing = await createRawEntry();
		const descendant = await createRawEntry([missing.entry, cut.entry]);
		const calls: string[][] = [];

		const result = await checkBoundedEntryV0CausalReachability({
			ancestorCid: ancestor.cid,
			descendant,
			limits,
			resolve: mapResolver([cut], calls),
		});

		expect(result.status).to.equal("ancestor");
		expect(calls).to.deep.equal([[...calls[0]!].sort()]);
		expect(calls[0]).to.include.members([cut.cid, missing.cid]);
	});

	it("returns not-ancestor only after all reachable branches resolve", async () => {
		const root = await createRawEntry();
		const left = await createRawEntry([root.entry]);
		const right = await createRawEntry([root.entry]);
		const descendant = await createRawEntry([
			right.entry,
			left.entry,
			right.entry,
		]);
		const unrelated = await rawCid(1);
		const calls: string[][] = [];

		const result = await checkBoundedEntryV0CausalReachability({
			ancestorCid: unrelated,
			descendant,
			limits: { ...limits, maxResolveBatchSize: 2 },
			resolve: mapResolver([root, left, right], calls),
		});

		expect(result.status).to.equal("not-ancestor");
		expect(calls[0]).to.deep.equal([left.cid, right.cid].sort());
		expect(result.visited.entries).to.equal(4);
		expect(result.visited.resolverCalls).to.equal(2);
	});

	it("keeps a wide single-entry resolver frontier globally sorted", async () => {
		const width = 512;
		const parents = await Promise.all(
			Array.from({ length: width }, () => createRawEntry()),
		);
		const descendant = await createRawEntry(parents.map(({ entry }) => entry));
		const unrelated = await rawCid(15);
		const calls: string[][] = [];

		const result = await checkBoundedEntryV0CausalReachability({
			ancestorCid: unrelated,
			descendant,
			limits: {
				...limits,
				maxDirectParents: width,
				maxVisitedEntries: width + 1,
				maxParentLinks: width,
				maxResolveBatchSize: 1,
			},
			resolve: mapResolver(parents, calls),
		});

		expect(result.status).to.equal("not-ancestor");
		expect(calls).to.have.length(width);
		expect(calls.flat()).to.deep.equal(parents.map(({ cid }) => cid).sort());
		expect(result.visited).to.deep.equal({
			entries: width + 1,
			bytes:
				descendant.bytes.byteLength +
				parents.reduce((sum, parent) => sum + parent.bytes.byteLength, 0),
			parentLinks: width,
			resolverCalls: width,
		});
	});

	it("reprioritizes newly discovered parents against the pending frontier", async () => {
		let parent: RawEntry | undefined;
		let branch: RawEntry | undefined;
		let pending: RawEntry | undefined;
		for (let attempt = 0; attempt < 64; attempt++) {
			const candidateParent = await createRawEntry();
			const candidateBranch = await createRawEntry([candidateParent.entry]);
			const candidatePending = await createRawEntry();
			if (
				candidateBranch.cid < candidatePending.cid &&
				candidateParent.cid < candidatePending.cid
			) {
				parent = candidateParent;
				branch = candidateBranch;
				pending = candidatePending;
				break;
			}
		}
		assert(parent && branch && pending, "expected a sortable CID fixture");
		const descendant = await createRawEntry([branch.entry, pending.entry]);
		const unrelated = await rawCid(16);
		const calls: string[][] = [];

		const result = await checkBoundedEntryV0CausalReachability({
			ancestorCid: unrelated,
			descendant,
			limits: { ...limits, maxResolveBatchSize: 1 },
			resolve: mapResolver([parent, branch, pending], calls),
		});

		expect(result.status).to.equal("not-ancestor");
		expect(calls).to.deep.equal([[branch.cid], [parent.cid], [pending.cid]]);
	});

	it("reports every pending heap member after resolver failure or abort", async () => {
		const width = 32;
		const parents = await Promise.all(
			Array.from({ length: width }, () => createRawEntry()),
		);
		const descendant = await createRawEntry(parents.map(({ entry }) => entry));
		const unrelated = await rawCid(17);
		const traversalLimits = {
			...limits,
			maxDirectParents: width,
			maxVisitedEntries: width + 1,
			maxParentLinks: width,
			maxResolveBatchSize: 3,
		};
		const expectedMissing = parents.map(({ cid }) => cid).sort();

		const failed = await checkBoundedEntryV0CausalReachability({
			ancestorCid: unrelated,
			descendant,
			limits: traversalLimits,
			resolve: async () => {
				throw new Error("offline");
			},
		});
		expect(failed.status).to.equal("incomplete");
		if (failed.status === "incomplete") {
			expect(failed.missingCids).to.deep.equal(expectedMissing);
		}

		const controller = new AbortController();
		const aborted = await checkBoundedEntryV0CausalReachability({
			ancestorCid: unrelated,
			descendant,
			limits: traversalLimits,
			signal: controller.signal,
			resolve: async () => {
				controller.abort();
				return new Map();
			},
		});
		expect(aborted.status).to.equal("incomplete");
		if (aborted.status === "incomplete") {
			expect(aborted.missingCids).to.deep.equal(expectedMissing);
		}
	});

	it("does not let a resolver expand the requested CID authority", async () => {
		const target = await createRawEntry();
		const requested = await createRawEntry();
		const injected = await createRawEntry([target.entry]);
		const descendant = await createRawEntry([requested.entry]);

		const result = await checkBoundedEntryV0CausalReachability({
			ancestorCid: target.cid,
			descendant,
			limits,
			resolve: async (cids) => {
				expect(Object.isFrozen(cids)).to.equal(true);
				try {
					(cids as string[]).push(injected.cid);
				} catch {
					// A hostile resolver must not mutate the traversal's own batch.
				}
				return new Map([
					[requested.cid, requested.bytes],
					[injected.cid, injected.bytes],
				]);
			},
		});

		expect(result.status).to.equal("not-ancestor");
		expect(result.visited.entries).to.equal(2);
	});

	it("accepts structural ReadonlyMap results and contains throwing get methods", async () => {
		const requested = await createRawEntry();
		const descendant = await createRawEntry([requested.entry]);
		const unrelated = await rawCid(14);
		const values = new Map([[requested.cid, requested.bytes]]);
		const structuralResult = await checkBoundedEntryV0CausalReachability({
			ancestorCid: unrelated,
			descendant,
			limits,
			resolve: async () =>
				({
					get: (cid: string) => values.get(cid),
				}) as ReadonlyMap<string, Uint8Array | undefined>,
		});
		expect(structuralResult.status).to.equal("not-ancestor");

		const throwingResult = await checkBoundedEntryV0CausalReachability({
			ancestorCid: unrelated,
			descendant,
			limits,
			resolve: async () =>
				({
					get: () => {
						throw new Error("hostile get");
					},
				}) as unknown as ReadonlyMap<string, Uint8Array | undefined>,
		});
		expect(throwingResult.status).to.equal("incomplete");
	});

	it("snapshots caller-owned query fields and limits before its first await", async () => {
		const ancestor = await createRawEntry();
		const replacement = await createRawEntry();
		const descendant = await createRawEntry([ancestor.entry]);
		const mutableLimits = { ...limits };
		const input: BoundedEntryV0CausalReachabilityInput = {
			ancestorCid: ancestor.cid,
			descendant,
			limits: mutableLimits,
			resolve: async () => new Map(),
		};

		const pending = checkBoundedEntryV0CausalReachability(input);
		input.ancestorCid = replacement.cid;
		input.descendant = replacement;
		input.resolve = async () => {
			throw new Error("mutated resolver must not be observed");
		};
		input.signal = AbortSignal.abort();
		mutableLimits.maxParentLinks = 0;

		const result = await pending;
		expect(result.status).to.equal("ancestor");
	});

	it("preserves the validated target and visit fence across resolver awaits", async () => {
		const reachable = await createRawEntry();
		const middle = await createRawEntry([reachable.entry]);
		const descendant = await createRawEntry([middle.entry]);
		const unrelated = await rawCid(10);
		const mutableLimits = { ...limits, maxVisitedEntries: 2 };
		let resolverStarted!: () => void;
		let resolverRelease!: () => void;
		const started = new Promise<void>((resolve) => {
			resolverStarted = resolve;
		});
		const release = new Promise<void>((resolve) => {
			resolverRelease = resolve;
		});
		const input: BoundedEntryV0CausalReachabilityInput = {
			ancestorCid: unrelated,
			descendant,
			limits: mutableLimits,
			resolve: async (cids) => {
				resolverStarted();
				await release;
				return new Map(
					cids.map((cid) => [
						cid,
						cid === middle.cid ? middle.bytes : undefined,
					]),
				);
			},
		};

		const pending = checkBoundedEntryV0CausalReachability(input);
		await started;
		input.ancestorCid = reachable.cid;
		mutableLimits.maxVisitedEntries = limits.maxVisitedEntries;
		resolverRelease();

		const result = await pending;
		expect(result.status).to.equal("capacity");
		expect(result.visited.entries).to.equal(2);
	});

	it("keeps the original resolver across traversal awaits", async () => {
		const root = await createRawEntry();
		const middle = await createRawEntry([root.entry]);
		const descendant = await createRawEntry([middle.entry]);
		const unrelated = await rawCid(11);
		const entries = new Map([
			[middle.cid, middle.bytes],
			[root.cid, root.bytes],
		]);
		let calls = 0;
		let resolverStarted!: () => void;
		let resolverRelease!: () => void;
		const started = new Promise<void>((resolve) => {
			resolverStarted = resolve;
		});
		const release = new Promise<void>((resolve) => {
			resolverRelease = resolve;
		});
		const input: BoundedEntryV0CausalReachabilityInput = {
			ancestorCid: unrelated,
			descendant,
			limits: { ...limits, maxResolveBatchSize: 1 },
			resolve: async (cids) => {
				calls++;
				if (calls === 1) {
					resolverStarted();
					await release;
				}
				return new Map(cids.map((cid) => [cid, entries.get(cid)]));
			},
		};

		const pending = checkBoundedEntryV0CausalReachability(input);
		await started;
		input.resolve = async () => new Map();
		resolverRelease();

		const result = await pending;
		expect(result.status).to.equal("not-ancestor");
		expect(calls).to.equal(2);
	});

	it("stops a forged causal cycle at the content-address boundary", async () => {
		const claimedParent = await createRawEntry();
		const forgedSelfCycle = await createRawEntry([claimedParent.entry]);
		const descendant = await createRawEntry([claimedParent.entry]);
		const unrelated = await rawCid(9);

		const result = await checkBoundedEntryV0CausalReachability({
			ancestorCid: unrelated,
			descendant,
			limits,
			resolve: async () =>
				new Map([[claimedParent.cid, forgedSelfCycle.bytes]]),
		});

		expect(result.status).to.equal("incomplete");
		expect(result.visited.entries).to.equal(2);
		expect(result.visited.resolverCalls).to.equal(1);
	});

	it("reports sorted missing CIDs for missing blocks, wrong bytes, and resolver errors", async () => {
		const firstMissing = await createRawEntry();
		const secondMissing = await createRawEntry();
		const descendant = await createRawEntry([
			secondMissing.entry,
			firstMissing.entry,
		]);
		const unrelated = await rawCid(2);

		const missingResult = await checkBoundedEntryV0CausalReachability({
			ancestorCid: unrelated,
			descendant,
			limits,
			resolve: async () => new Map(),
		});
		expect(missingResult).to.deep.include({ status: "incomplete" });
		if (missingResult.status === "incomplete") {
			expect(missingResult.missingCids).to.deep.equal(
				[firstMissing.cid, secondMissing.cid].sort(),
			);
		}

		const wrongResult = await checkBoundedEntryV0CausalReachability({
			ancestorCid: unrelated,
			descendant,
			limits,
			resolve: async (cids) =>
				new Map(cids.map((cid) => [cid, descendant.bytes])),
		});
		expect(wrongResult.status).to.equal("incomplete");

		const errorResult = await checkBoundedEntryV0CausalReachability({
			ancestorCid: unrelated,
			descendant,
			limits,
			resolve: async () => {
				throw new Error("offline");
			},
		});
		expect(errorResult.status).to.equal("incomplete");
	});

	it("accepts encrypted payloads and signatures but not encrypted metadata", async () => {
		const ancestor = await createRawEntry();
		const sender = await X25519Keypair.create();
		const receiver = await X25519Keypair.create();
		const encryptedBody = await createRawEntry([ancestor.entry], {
			encryption: {
				keypair: sender,
				receiver: {
					meta: undefined,
					payload: receiver.publicKey,
					signatures: receiver.publicKey,
				},
			},
		});
		const encryptedBodyResult = await checkBoundedEntryV0CausalReachability({
			ancestorCid: ancestor.cid,
			descendant: encryptedBody,
			limits,
			resolve: async () => new Map(),
		});
		expect(encryptedBodyResult.status).to.equal("ancestor");

		const encryptedMeta = await createRawEntry([ancestor.entry], {
			encryption: {
				keypair: sender,
				receiver: {
					meta: receiver.publicKey,
					payload: undefined,
					signatures: undefined,
				},
			},
		});
		const encryptedMetaResult = await checkBoundedEntryV0CausalReachability({
			ancestorCid: ancestor.cid,
			descendant: encryptedMeta,
			limits,
			resolve: async () => new Map(),
		});
		expect(encryptedMetaResult).to.deep.include({ status: "incomplete" });
	});

	it("rejects unbacked framing before Borsh allocation", async () => {
		const source = await createRawEntry();
		const unrelated = await rawCid(3);

		const malformedMeta = serialize(source.entry.meta);
		writeU32(malformedMeta, nextCountOffset(malformedMeta), 0xffffffff);
		const malformedEntry = new EntryV0<Uint8Array>({
			meta: new DecryptedThing({ data: malformedMeta }),
			payload: source.entry._payload,
			reserved: source.entry._reserved,
		});
		const malformedBytes = serialize(malformedEntry);
		const malformedCid = (await calculateRawCid(malformedBytes)).cid;
		const descendant = await createRawEntry([
			{
				hash: malformedCid,
				meta: source.entry.meta,
			} as SortableEntry,
		]);

		const malformedResult = await checkBoundedEntryV0CausalReachability({
			ancestorCid: unrelated,
			descendant,
			limits,
			resolve: async () => new Map([[malformedCid, malformedBytes]]),
		});
		expect(malformedResult.status).to.equal("incomplete");

		const malformedStorage = new Uint8Array(source.bytes);
		writeU32(
			malformedStorage,
			signatureCountOffset(malformedStorage),
			0xffffffff,
		);
		const malformedStorageCid = (await calculateRawCid(malformedStorage)).cid;
		const storageDescendant = await createRawEntry([
			{
				hash: malformedStorageCid,
				meta: source.entry.meta,
			} as SortableEntry,
		]);
		const storageResult = await checkBoundedEntryV0CausalReachability({
			ancestorCid: unrelated,
			descendant: storageDescendant,
			limits,
			resolve: async () => new Map([[malformedStorageCid, malformedStorage]]),
		});
		expect(storageResult.status).to.equal("incomplete");
	});

	it("charges framed parent links before later canonical decoding fails", async () => {
		const parent = await createRawEntry();
		const firstSource = await createRawEntry([parent.entry]);
		const secondSource = await createRawEntry([parent.entry]);
		const corruptParentUtf8 = async (source: RawEntry) => {
			const metaBytes = serialize(source.entry.meta);
			const countOffset = nextCountOffset(metaBytes);
			expect(readU32(metaBytes, countOffset)).to.equal(1);
			const parentLengthOffset = countOffset + 4;
			const parentLength = readU32(metaBytes, parentLengthOffset);
			expect(parentLength).to.be.greaterThan(0);
			metaBytes[parentLengthOffset + 4] = 0xff;
			const malformedEntry = new EntryV0<Uint8Array>({
				meta: new DecryptedThing({ data: metaBytes }),
				payload: source.entry._payload,
				reserved: source.entry._reserved,
			});
			const bytes = serialize(malformedEntry);
			return { cid: (await calculateRawCid(bytes)).cid, bytes };
		};
		const first = await corruptParentUtf8(firstSource);
		const second = await corruptParentUtf8(secondSource);
		const descendant = await createRawEntry([
			{ hash: first.cid, meta: firstSource.entry.meta } as SortableEntry,
			{ hash: second.cid, meta: secondSource.entry.meta } as SortableEntry,
		]);
		const unrelated = await rawCid(12);

		const result = await checkBoundedEntryV0CausalReachability({
			ancestorCid: unrelated,
			descendant,
			limits: { ...limits, maxParentLinks: 3 },
			resolve: async () =>
				new Map([
					[first.cid, first.bytes],
					[second.cid, second.bytes],
				]),
		});

		expect(result.status).to.equal("capacity");
		expect(result.visited.parentLinks).to.equal(3);
	});

	it("returns capacity for every caller-selected work fence", async () => {
		const root = await createRawEntry();
		const other = await createRawEntry();
		const descendant = await createRawEntry([root.entry, other.entry]);
		const target = await rawCid(4);

		for (const constrained of [
			{ ...limits, maxEntryBytes: descendant.bytes.byteLength - 1 },
			{ ...limits, maxTotalBytes: descendant.bytes.byteLength },
			{ ...limits, maxDirectParents: 1 },
			{ ...limits, maxParentLinks: 1 },
			{ ...limits, maxVisitedEntries: 1 },
		]) {
			const result = await checkBoundedEntryV0CausalReachability({
				ancestorCid: target,
				descendant,
				limits: constrained,
				resolve: mapResolver([root, other]),
			});
			expect(result.status).to.equal("capacity");
		}
	});

	it("keeps admitted traversal state within the visited-entry fence", async () => {
		const parents = await Promise.all(
			Array.from({ length: 64 }, () => createRawEntry()),
		);
		const descendant = await createRawEntry(parents.map(({ entry }) => entry));
		const unrelated = await rawCid(13);
		let resolverCalled = false;

		const result = await checkBoundedEntryV0CausalReachability({
			ancestorCid: unrelated,
			descendant,
			limits: { ...limits, maxVisitedEntries: 1 },
			resolve: async () => {
				resolverCalled = true;
				return new Map();
			},
		});

		expect(result.status).to.equal("capacity");
		expect(result.visited.entries).to.equal(1);
		expect(result.visited.parentLinks).to.equal(64);
		expect(resolverCalled).to.equal(false);
	});

	it("turns caller cancellation and timeout into incomplete results", async () => {
		const root = await createRawEntry();
		const descendant = await createRawEntry([root.entry]);
		const target = await rawCid(5);
		let alreadyAbortedResolverCalled = false;
		const alreadyAborted = await checkBoundedEntryV0CausalReachability({
			ancestorCid: target,
			descendant,
			limits,
			signal: AbortSignal.abort(),
			resolve: async () => {
				alreadyAbortedResolverCalled = true;
				return new Map();
			},
		});
		expect(alreadyAborted.status).to.equal("incomplete");
		expect(alreadyAborted.visited.bytes).to.equal(0);
		expect(alreadyAbortedResolverCalled).to.equal(false);

		const controller = new AbortController();
		let resolverSignal: AbortSignal | undefined;
		let resolverStarted!: () => void;
		const started = new Promise<void>((resolve) => {
			resolverStarted = resolve;
		});
		const pending = checkBoundedEntryV0CausalReachability({
			ancestorCid: target,
			descendant,
			limits,
			signal: controller.signal,
			resolve: async (_cids, options) => {
				resolverSignal = options.signal;
				resolverStarted();
				return new Promise<ReadonlyMap<string, Uint8Array>>(() => {});
			},
		});
		await started;
		controller.abort();
		const cancelled = await pending;
		expect(cancelled.status).to.equal("incomplete");
		expect(resolverSignal).to.equal(controller.signal);

		const timedOut = await checkBoundedEntryV0CausalReachability({
			ancestorCid: target,
			descendant,
			limits,
			signal: AbortSignal.timeout(5),
			resolve: async () =>
				new Promise<ReadonlyMap<string, Uint8Array>>(() => {}),
		});
		expect(timedOut.status).to.equal("incomplete");
	});

	it("captures hostile Uint8Arrays before hashing awaits", async () => {
		const ancestor = await createRawEntry();
		const descendant = await createRawEntry([ancestor.entry]);
		let iteratorCalls = 0;
		class HostileBytes extends Uint8Array {
			override [Symbol.iterator](): ArrayIterator<number> {
				iteratorCalls++;
				throw new Error("iterator must not run");
			}
		}
		const hostile = new HostileBytes(descendant.bytes);
		const pending = checkBoundedEntryV0CausalReachability({
			ancestorCid: ancestor.cid,
			descendant: { cid: descendant.cid, bytes: hostile },
			limits,
			resolve: async () => new Map(),
		});
		hostile.fill(0);
		const result = await pending;
		expect(result.status).to.equal("ancestor");
		expect(iteratorCalls).to.equal(0);

		const proxied = await checkBoundedEntryV0CausalReachability({
			ancestorCid: ancestor.cid,
			descendant: {
				cid: descendant.cid,
				bytes: new Proxy(descendant.bytes, {}),
			},
			limits,
			resolve: async () => new Map(),
		});
		expect(proxied.status).to.equal("incomplete");

		const detachedBytes = new Uint8Array(descendant.bytes);
		structuredClone(detachedBytes.buffer, { transfer: [detachedBytes.buffer] });
		const detached = await checkBoundedEntryV0CausalReachability({
			ancestorCid: ancestor.cid,
			descendant: { cid: descendant.cid, bytes: detachedBytes },
			limits,
			resolve: async () => new Map(),
		});
		expect(detached.status).to.equal("incomplete");
	});

	it("is strict (not self) and rejects invalid caller limits and CIDs", async () => {
		const entry = await createRawEntry();
		let resolverCalled = false;
		const self = await checkBoundedEntryV0CausalReachability({
			ancestorCid: entry.cid,
			descendant: entry,
			limits,
			resolve: async () => {
				resolverCalled = true;
				return new Map();
			},
		});
		expect(self.status).to.equal("not-ancestor");
		expect(resolverCalled).to.equal(false);

		await expect(
			checkBoundedEntryV0CausalReachability({
				ancestorCid: "not-a-cid",
				descendant: entry,
				limits,
				resolve: async () => new Map(),
			}),
		).to.be.rejectedWith(TypeError);
		await expect(
			checkBoundedEntryV0CausalReachability({
				ancestorCid: "z".repeat(1_000_001),
				descendant: entry,
				limits,
				resolve: async () => new Map(),
			}),
		).to.be.rejectedWith(TypeError);
		await expect(
			checkBoundedEntryV0CausalReachability({
				ancestorCid: entry.cid,
				descendant: entry,
				limits: { ...limits, maxParentLinks: 0 },
				resolve: async () => new Map(),
			}),
		).to.be.rejectedWith(RangeError);
	});
});
