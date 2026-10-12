import { deserialize, serialize } from "@dao-xyz/borsh";
import { DecryptedThing } from "@peerbit/crypto";
import {
	Entry,
	EntryType,
	EntryV0,
	LamportClock,
	Timestamp,
	calculateRawCidV1Batch,
	createEntry,
} from "@peerbit/log";
import { TestSession } from "@peerbit/test-utils";
import { expect } from "chai";
import {
	BORSH_ENCODING_OPERATION,
	DeleteOperation,
	Documents,
	type Operation,
	PutOperation,
	toId,
} from "../src/index.js";
import { Document, TestStore } from "./data.js";
import {
	MAX_DELETE_HISTORY_BLOCKS,
	MAX_DELETE_HISTORY_BYTES,
	MAX_DELETE_HISTORY_EDGES,
	authenticateCanonicalDeleteWitness,
	verifyDocumentDeleteHistory,
} from "./helpers/delete-history.js";

// Private authenticated-closure tests: no delete permission, store membership,
// production Documents retirement, physical pruning or crash durability claim.
describe("original-byte delete history verifier", () => {
	let session: TestSession;
	let store: TestStore;
	let clock = 100n;
	let a0: Entry<Operation>;
	let a1: Entry<Operation>;
	const key = () => toId("AQID");
	const resolveKey = (payload: Uint8Array) =>
		toId(deserialize(payload, Document).id);
	const make = async (
		name: string,
		next: Entry<Operation>[] = [],
		id = "AQID",
		data: Operation = new PutOperation({
			data: serialize(new Document({ id, name })),
		}),
	) =>
		createEntry<Operation>({
			store: store.docs.log.log.blocks,
			encoding: BORSH_ENCODING_OPERATION,
			identity: session.peers[0].identity,
			data,
			meta: {
				type:
					data instanceof DeleteOperation ? EntryType.CUT : EntryType.APPEND,
				next,
				clock: new LamportClock({
					id: session.peers[0].identity.publicKey.bytes,
					timestamp: new Timestamp({ wallTime: clock++ }),
				}),
			},
		});
	const bytesOf = async (entry: Entry<Operation>) => {
		const bytes = await store.docs.log.log.blocks.get(entry.hash, {
			remote: false,
		});
		if (!bytes) throw new Error("Missing fixture block");
		return new Uint8Array(bytes);
	};
	const verifyBlocks = async (
		target: string,
		blocks: Uint8Array[],
		declaredKey = key(),
	) => {
		const before = blocks.map((bytes) => new Uint8Array(bytes));
		try {
			return await verifyDocumentDeleteHistory({
				target,
				key: declaredKey,
				blocks,
				resolveKey,
			});
		} finally {
			expect(blocks).deep.equal(
				before,
				"verifier must not mutate original blocks",
			);
		}
	};
	const verify = async (
		target: Entry<Operation>,
		entries: Entry<Operation>[],
	) => verifyBlocks(target.hash, await Promise.all(entries.map(bytesOf)));

	before(async () => {
		session = await TestSession.disconnected(1);
		store = await session.peers[0].open(
			new TestStore({ docs: new Documents<Document>() }),
			{
				args: { mode: "compat", replicate: false, keep: "self" },
			},
		);
		a0 = await make("A0");
		a1 = await make("A1", [a0]);
	});
	after(async () => session?.stop());

	it("resolves an authenticated same-key chain root first", async () => {
		const result = await verify(a1, [a0, a1]);
		expect(result.targets.map((fact) => fact.hash)).deep.equal([
			a1.hash,
			a0.hash,
		]);
		expect(result.boundaries).deep.equal([]);
	});
	it("deduplicates a diamond independently of witness-array order", async () => {
		const right = await make("right", [a0]);
		const root = await make("root", [a1, right]);
		const entries = [root, right, a0, a1];
		const forward = await verify(root, entries);
		const reverse = await verify(root, [...entries].reverse());
		expect(forward.targets.map((fact) => fact.hash)).deep.equal(
			reverse.targets.map((fact) => fact.hash),
		);
		expect(forward.targets.map((fact) => fact.hash).sort()).deep.equal(
			entries.map((entry) => entry.hash).sort(),
		);
		expect(forward.boundaries).deep.equal([]);
	});
	it("stops at a different-key boundary even when its parent returns to the original key", async () => {
		const boundary = await make("boundary", [a0], "other-key");
		const root = await make("root", [boundary]);
		const result = await verify(root, [boundary, root]);
		expect(result.targets.map((fact) => fact.hash)).deep.equal([root.hash]);
		expect(result.boundaries.map((fact) => fact.hash)).deep.equal([
			boundary.hash,
		]);
		await expect(verify(root, [root, boundary, a0])).rejectedWith(
			"Unused history witness",
		);
	});
	it("rejects an omitted same-key ancestor instead of returning a partial closure", async () => {
		await expect(verify(a1, [a1])).rejectedWith("Missing ancestry witness");
	});
	it("requires authenticated bytes even to classify a cross-key boundary", async () => {
		const boundary = await make("boundary", [], "other-key");
		const root = await make("root", [boundary]);
		await expect(verify(root, [root])).rejectedWith("Missing ancestry witness");
	});
	it("does not substitute a legacy CUT for missing original ancestry", async () => {
		const oldCut = await make(
			"cut",
			[a0],
			"AQID",
			new DeleteOperation({ key: key() }),
		);
		await expect(verify(a1, [a1, oldCut])).rejectedWith("Expected put witness");
		const root = await make("after-cut", [oldCut]);
		await expect(verify(root, [root, oldCut])).rejectedWith(
			"Expected put witness",
		);
	});
	it("rejects duplicate and unrelated extra witnesses", async () => {
		await expect(verify(a1, [a1, a0, a0])).rejectedWith(
			"Duplicate history witness",
		);
		const unrelated = await make("unrelated");
		await expect(verify(a1, [a1, a0, unrelated])).rejectedWith(
			"Unused history witness",
		);
	});
	it("rejects a mismatched typed key even when primitives coincide", async () => {
		const wrongKey = toId(new Uint8Array([1, 2, 3]));
		expect(wrongKey.primitive).equal(key().primitive);
		await expect(
			verifyBlocks(a1.hash, await Promise.all([a1, a0].map(bytesOf)), wrongKey),
		).rejectedWith("Wrong target key");
	});
	it("rejects an invalid ancestor signature before trusting its metadata", async () => {
		const bytes = await bytesOf(a0);
		const decoded = deserialize(bytes, Entry) as Entry<Operation>;
		const signature = (await decoded.getSignatures())[0].signature;
		const offset = Buffer.from(bytes).indexOf(signature);
		expect(offset).greaterThan(-1);
		bytes[offset] ^= 1;
		await expect(
			verifyBlocks(a1.hash, [await bytesOf(a1), bytes]),
		).rejectedWith("Invalid signature");
	});
	it("rejects a forged cyclic edge instead of trusting relabeled signed bytes", async () => {
		// Authentic CID cycles require a hash fixed point. This is deliberately a
		// forged metadata cycle, not a mocked cryptographic/hash implementation.
		const entry = deserialize(await bytesOf(a0), Entry) as EntryV0<Operation>;
		entry.init({ encoding: BORSH_ENCODING_OPERATION });
		entry.meta.next = [a0.hash];
		// Replace the encoded metadata too; mutating only a decoded cache would
		// serialize the original valid entry and would not test a forgery.
		entry._meta = new DecryptedThing({ data: serialize(entry.meta) });
		const forged = serialize(entry);
		const [forgedHash] = await calculateRawCidV1Batch([forged]);
		await expect(verifyBlocks(forgedHash, [forged])).rejectedWith(
			"Invalid signature",
		);
	});
	it("rejects an encoded empty-string hash as noncanonical identity", async () => {
		const entry = deserialize(await bytesOf(a0), Entry) as Entry<Operation>;
		entry.hash = "";
		const bytes = serialize(entry);
		const [cid] = await calculateRawCidV1Batch([bytes]);
		await expect(verifyBlocks(cid, [bytes])).rejectedWith(
			"Embedded hash is not canonical stored evidence",
		);
	});
	it("authenticates a cross-key boundary's signature before using its key", async () => {
		const boundary = await make("boundary", [], "other-key");
		const root = await make("root", [boundary]);
		const bytes = await bytesOf(boundary);
		const decoded = deserialize(bytes, Entry) as Entry<Operation>;
		const signature = (await decoded.getSignatures())[0].signature;
		const offset = Buffer.from(bytes).indexOf(signature);
		expect(offset).greaterThan(-1);
		bytes[offset] ^= 1;
		await expect(
			verifyBlocks(root.hash, [await bytesOf(root), bytes]),
		).rejectedWith("Invalid signature");
	});
	it("snapshots the entire bundle and typed key before verification yields", async () => {
		const blocks = await Promise.all([a1, a0].map(bytesOf));
		const properties = { target: a1.hash, key: key(), blocks, resolveKey };
		const pending = verifyDocumentDeleteHistory(properties);
		properties.target = a0.hash;
		Object.assign(properties.key, { key: "changed" });
		for (const bytes of blocks) bytes.fill(0);
		blocks.reverse();
		const result = await pending;
		expect(result.targets.map((fact) => fact.hash)).deep.equal([
			a1.hash,
			a0.hash,
		]);
		expect(result.targets.map((fact) => fact.key.primitive)).deep.equal([
			"AQID",
			"AQID",
		]);
	});
	it("owns returned facts and detaches application decoder inputs", async () => {
		const bytes = await bytesOf(a0);
		const original = new Uint8Array(bytes);
		const returnedKeys: ReturnType<typeof toId>[] = [];
		const result = await verifyDocumentDeleteHistory({
			target: a0.hash,
			key: key(),
			blocks: [bytes],
			resolveKey: (payload) => {
				const resolved = resolveKey(payload);
				returnedKeys.push(resolved);
				payload.fill(0);
				return resolved;
			},
		});
		Object.assign(returnedKeys[0], { key: "changed" });
		expect(result.targets[0].key.primitive).equal("AQID");
		result.targets[0].bytes.fill(0);
		Object.assign(result.targets[0].key, { key: "caller-owned" });
		expect(bytes).deep.equal(original);
	});
	it("bounds aggregate original bytes before decoding", async () => {
		await expect(
			verifyBlocks(a0.hash, [new Uint8Array(MAX_DELETE_HISTORY_BYTES + 1)]),
		).rejectedWith("Delete history byte limit exceeded");
	});
	it("uses intrinsic byte length when the supplied view lies about its size", async () => {
		const bytes = new Uint8Array(MAX_DELETE_HISTORY_BYTES + 1);
		Object.defineProperty(bytes, "byteLength", { value: 0 });
		await expect(
			verifyDocumentDeleteHistory({
				target: a0.hash,
				key: key(),
				blocks: [bytes],
				resolveKey,
			}),
		).rejectedWith("Delete history byte limit exceeded");
	});
	it("ignores an overridden bundle iterator and uses its bounded indexed entries", async () => {
		const blocks = [await bytesOf(a0)];
		let calls = 0;
		Object.defineProperty(blocks, Symbol.iterator, {
			value: () => {
				calls++;
				throw new Error("Untrusted iterator must not run");
			},
		});
		const result = await verifyDocumentDeleteHistory({
			target: a0.hash,
			key: key(),
			blocks,
			resolveKey,
		});
		expect(calls).equal(0);
		expect(result.targets.map((fact) => fact.hash)).deep.equal([a0.hash]);
	});
	it("reads a caller's witness-bytes getter once before verification yields", async () => {
		const bytes = await bytesOf(a0);
		let reads = 0;
		const result = await authenticateCanonicalDeleteWitness({
			cid: a0.hash,
			get bytes() {
				reads++;
				if (reads > 1) throw new Error("Witness bytes read twice");
				return bytes;
			},
		});
		expect(reads).equal(1);
		expect(result.hash).equal(a0.hash);
	});
	it("bounds the number of unique valid witnesses", async () => {
		const entries: Entry<Operation>[] = [];
		for (let i = 0; i <= MAX_DELETE_HISTORY_BLOCKS; i++) {
			entries.push(
				await make(`chain-${i}`, entries.length ? [entries.at(-1)!] : []),
			);
		}
		await expect(verify(entries.at(-1)!, entries)).rejectedWith(
			"Delete history block limit exceeded",
		);
	});
	it("bounds authenticated edges separately from blocks and bytes", async () => {
		const entries: Entry<Operation>[] = [];
		let edges = 0;
		while (edges <= MAX_DELETE_HISTORY_EDGES) {
			edges += entries.length;
			entries.push(await make(`dense-${entries.length}`, [...entries]));
		}
		expect(entries.length).lessThan(MAX_DELETE_HISTORY_BLOCKS);
		const blocks = await Promise.all(entries.map(bytesOf));
		expect(blocks.reduce((sum, bytes) => sum + bytes.length, 0)).lessThan(
			MAX_DELETE_HISTORY_BYTES,
		);
		await expect(verifyBlocks(entries.at(-1)!.hash, blocks)).rejectedWith(
			"Delete history edge limit exceeded",
		);
	});
});
