import { deserialize, serialize } from "@dao-xyz/borsh";
import { type Identity, PreHash, Secp256k1Keypair } from "@peerbit/crypto";
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
import { equals } from "uint8arrays";
import {
	BORSH_ENCODING_OPERATION,
	DeleteOperation,
	Documents,
	type Operation,
	PutOperation,
	policy,
	toId,
} from "../src/index.js";
import { Document, TestStore } from "./data.js";
import {
	assertSupportedDeleteWitnessSignature,
	authenticateCanonicalDeleteWitness as authenticate,
	verifyDocumentDeleteHistory,
} from "./helpers/delete-history.js";

// Executable reference algebra, NOT a Documents implementation or wire proposal.
// Only one canonical plaintext Ed25519 signature with PreHash.NONE, signed by
// the captured fixed store owner, qualifies for ordinary puts AND witnesses.
// This is a supported reference profile, not generic signature/CID uniqueness.
// Unchanged put bytes prove content/authorship, NOT historical store admission.
// The delete alone carries a signed fixture store marker. An extra test-only
// veto can reject it; this is not an adapter for arbitrary production canPerform.
// Authenticated same-key ancestry is included; cross-key boundaries stop traversal.
// No arbitrary canPerform, historical admission/revocation, native implementation,
// production projection events, or crash-atomic persistence is modeled.
type Block = { cid: string; bytes: Uint8Array };
type WitnessedDelete = {
	deletion: Block;
	victim: Block;
	history?: readonly Uint8Array[];
};
const snapshotBlock = (block: Block): Block => ({
	cid: block.cid,
	bytes: new Uint8Array(block.bytes),
});

function requireFact(condition: unknown, message: string): asserts condition {
	if (!condition) throw new Error(message);
}

const authenticatePut = async (block: Block, owner: Uint8Array) => {
	const entry = await authenticate(block);
	const operation = await entry.getPayloadValue();
	requireFact(
		entry.meta.type === EntryType.APPEND && operation instanceof PutOperation,
		"Expected put witness",
	);
	const value = deserialize(operation.data, Document);
	const signers = await entry.getPublicKeys();
	requireFact(
		value.data &&
			equals(value.data, owner) &&
			signers.length === 1 &&
			equals(signers[0].bytes, owner),
		"Put must be signed by the fixed store owner",
	);
	assertSupportedDeleteWitnessSignature(entry, owner);
	return { entry, value };
};

const reference = (
	scope: Uint8Array,
	owner: Uint8Array,
	canDelete: (value: Readonly<Document>) => boolean = () => true,
) => {
	const storeScope = new Uint8Array(scope);
	const storeOwner = new Uint8Array(owner);
	const live = new Map<string, Document>();
	const retired = new Set<string>();
	return {
		live,
		retired,
		async put(block: Block) {
			const { entry, value } = await authenticatePut(block, storeOwner);
			if (!retired.has(entry.hash)) live.set(entry.hash, value);
		},
		async remove(evidence: WitnessedDelete) {
			// Own the complete witness pair before either verifier can yield.
			const deletion = snapshotBlock(evidence.deletion);
			const victim = snapshotBlock(evidence.victim);
			const history =
				evidence.history?.map((bytes) => new Uint8Array(bytes)) ?? [];
			const cut = await authenticate(deletion);
			requireFact(
				cut.meta.data && equals(cut.meta.data, storeScope),
				"Wrong store scope",
			);
			const operation = await cut.getPayloadValue();
			requireFact(
				cut.meta.type === EntryType.CUT && operation instanceof DeleteOperation,
				"Expected delete",
			);
			const { entry: target, value } = await authenticatePut(
				victim,
				storeOwner,
			);
			requireFact(
				cut.meta.next.length === 1 && cut.meta.next[0] === target.hash,
				"Wrong target",
			);
			// Compare serialized typed keys, not their potentially colliding primitives.
			requireFact(
				equals(serialize(operation.key), serialize(toId(value.id))),
				"Wrong typed key",
			);
			const signers = await cut.getPublicKeys();
			requireFact(
				signers.length === 1 && equals(signers[0].bytes, value.data!),
				"Delete must be signed by the target owner",
			);
			assertSupportedDeleteWitnessSignature(cut, storeOwner);
			const verified = await verifyDocumentDeleteHistory({
				target: target.hash,
				key: operation.key,
				blocks: [victim.bytes, ...history],
				resolveKey: (payload) => toId(deserialize(payload, Document).id),
			});
			for (const fact of verified.targets) {
				const ancestor =
					fact.hash === target.hash
						? { entry: target, value }
						: await authenticatePut(
								{ cid: fact.hash, bytes: fact.bytes },
								storeOwner,
							);
				requireFact(
					signers.length === 1 &&
						equals(signers[0].bytes, ancestor.value.data!),
					"Delete must be signed by every target owner",
				);
				requireFact(canDelete(ancestor.value), "Delete vetoed");
			}
			// All closure, per-target authorization and veto checks precede mutation.
			for (const fact of verified.targets) {
				retired.add(fact.hash);
				live.delete(fact.hash);
			}
		},
	};
};

describe("proof-backed delete reference (not production admission)", () => {
	let session: TestSession;
	let store: TestStore;
	let scope: Uint8Array;
	let owner: Uint8Array;
	let original: Entry<Operation>;
	let replacement: Entry<Operation>;
	let replacementAtCutTime: Entry<Operation>;
	let originalBlock: Block;
	let replacementBlock: Block;
	let cut: WitnessedDelete;

	const blockOf = async (
		entry: Entry<Operation>,
		source = store,
	): Promise<Block> => {
		const bytes = await source.docs.log.log.blocks.get(entry.hash, {
			remote: false,
		});
		requireFact(bytes, "Missing fixture block");
		return { cid: entry.hash, bytes: new Uint8Array(bytes) };
	};
	const makeEntry = (
		data: Operation,
		identity: Identity,
		time: bigint,
		next: Entry<Operation>[] = [],
		storeScope = scope,
	) =>
		createEntry<Operation>({
			store: store.docs.log.log.blocks,
			encoding: BORSH_ENCODING_OPERATION,
			identity,
			data,
			meta: {
				type:
					data instanceof DeleteOperation ? EntryType.CUT : EntryType.APPEND,
				next,
				clock: new LamportClock({
					id: identity.publicKey.bytes,
					timestamp: new Timestamp({ wallTime: time }),
				}),
				// Delete-only fixture marker; unchanged puts need no retrofit.
				data: data instanceof DeleteOperation ? storeScope : undefined,
			},
		});
	const makePut = (
		name: string,
		identity: Identity,
		time: bigint,
		next: Entry<Operation>[] = [],
		id = "AQID",
	) =>
		makeEntry(
			new PutOperation({
				data: serialize(
					new Document({ id, name, data: identity.publicKey.bytes }),
				),
			}),
			identity,
			time,
			next,
		);
	const makeDelete = async (
		target: Entry<Operation>,
		identity: Identity,
		key = toId("AQID"),
		storeScope = scope,
		victimStore = store,
	): Promise<WitnessedDelete> => ({
		deletion: await blockOf(
			await makeEntry(
				new DeleteOperation({ key }),
				identity,
				target.meta.clock.timestamp.wallTime + 9n,
				[target],
				storeScope,
			),
		),
		victim: await blockOf(target, victimStore),
	});

	before(async () => {
		session = await TestSession.disconnected(2);
		owner = new Uint8Array(session.peers[0].identity.publicKey.bytes);
		store = await session.peers[0].open(
			new TestStore({ docs: new Documents<Document>() }),
			{
				args: {
					mode: "compat",
					replicate: false,
					keep: "self",
					canPerform: policy.signedByPublicKey(
						session.peers[0].identity.publicKey,
					),
				},
			},
		);
		scope = new TextEncoder().encode(store.address);
		const put = async (name: string, wallTime: bigint) =>
			(
				await store.docs.put(new Document({ id: "AQID", name, data: owner }), {
					target: "none",
					unique: true,
					meta: { next: [], timestamp: new Timestamp({ wallTime }) },
				})
			).entry;
		original = await put("A", 1n);
		replacement = await put("B", 11n);
		replacementAtCutTime = await put("B", 10n);
		for (const entry of [original, replacement, replacementAtCutTime]) {
			expect(entry.meta.data && equals(entry.meta.data, scope)).not.equal(true);
			assertSupportedDeleteWitnessSignature(
				await authenticate(await blockOf(entry)),
				owner,
			);
		}
		originalBlock = await blockOf(original);
		replacementBlock = await blockOf(replacement);
		cut = await makeDelete(original, session.peers[0].identity);
	});
	after(async () => session?.stop());

	const permutations = ["ACB", "ABC", "CAB", "CBA", "BAC", "BCA"];
	for (const equalTime of [false, true]) {
		for (const order of permutations) {
			it(`retires only A for ${order}${equalTime ? " with equal B/C time" : ""}`, async () => {
				const model = reference(scope, owner);
				const b = equalTime
					? await blockOf(replacementAtCutTime)
					: replacementBlock;
				for (const operation of order) {
					if (operation === "C") await model.remove(cut);
					else await model.put(operation === "A" ? originalBlock : b);
				}
				await model.remove(cut);
				await model.put(originalBlock);
				expect([...model.retired]).deep.equal([originalBlock.cid]);
				expect([...model.live.keys()]).deep.equal([b.cid]);
			});
		}
	}

	it("snapshots put evidence, fixed authority and delete scope before verification yields", async () => {
		const suppliedScope = new Uint8Array(scope);
		const suppliedOwner = new Uint8Array(owner);
		const model = reference(suppliedScope, suppliedOwner);
		const supplied = structuredClone(originalBlock);
		const pending = model.put(supplied);
		// Each async verifier has started, but its first await cannot have resumed.
		suppliedScope.fill(0);
		suppliedOwner.fill(0);
		supplied.bytes.fill(0);
		supplied.cid = replacementBlock.cid;
		await pending;
		expect([...model.live.keys()]).deep.equal([originalBlock.cid]);
		expect(model.live.get(originalBlock.cid)?.name).equal("A");
		expect([...model.retired]).deep.equal([]);
		await model.remove(cut);
		expect([...model.retired]).deep.equal([originalBlock.cid]);
	});

	it("snapshots both delete blocks before a caller can relabel the victim", async () => {
		const model = reference(scope, owner);
		await model.put(originalBlock);
		await model.put(replacementBlock);
		const supplied = structuredClone(cut);
		const pending = model.remove(supplied);
		supplied.deletion.bytes.fill(0);
		supplied.deletion.cid = replacementBlock.cid;
		supplied.victim.bytes.fill(0);
		supplied.victim.cid = replacementBlock.cid;
		await pending;
		await model.put(originalBlock);
		expect([...model.retired]).deep.equal([originalBlock.cid]);
		expect([...model.live.keys()]).deep.equal([replacementBlock.cid]);
	});

	it("retains A's retirement after removing B and independently replaying only bytes", async () => {
		const deleteB = await makeDelete(
			replacementAtCutTime,
			session.peers[0].identity,
		);
		const b = await blockOf(replacementAtCutTime);
		const first = reference(scope, owner);
		await first.put(originalBlock);
		await first.remove(cut);
		await first.put(b);
		await first.remove(deleteB);
		await first.put(originalBlock);
		expect([...first.live.keys()]).deep.equal([]);
		// Reconstruction below uses only copied bytes; this is not crash recovery.
		const evidence = structuredClone({
			deletes: [cut, deleteB],
			puts: [originalBlock, b],
		});
		const model = reference(new Uint8Array(scope), owner);
		for (const item of evidence.deletes) await model.remove(item);
		for (const item of evidence.puts) await model.put(item);
		expect([...model.live.keys()]).deep.equal([]);
		expect([...model.retired].sort()).deep.equal(
			[original.hash, replacementAtCutTime.hash].sort(),
		);
	});

	it("does not recursively retire a cross-key causal parent or other conflict head", async () => {
		const parent = await makePut(
			"parent",
			session.peers[0].identity,
			1n,
			[],
			"other-key",
		);
		const child = await makePut("child", session.peers[0].identity, 2n, [
			parent,
		]);
		const conflict = await makePut("conflict", session.peers[0].identity, 3n);
		const model = reference(scope, owner);
		for (const entry of [parent, child, replacement, conflict])
			await model.put(await blockOf(entry));
		await model.remove({
			...(await makeDelete(child, session.peers[0].identity)),
			history: [(await blockOf(parent)).bytes],
		});
		await model.put(await blockOf(child));
		expect([...model.retired]).deep.equal([child.hash]);
		expect([...model.live.keys()].sort()).deep.equal(
			[parent.hash, replacement.hash, conflict.hash].sort(),
		);
	});
	it("retires the complete authorized same-key history before suppressing its replay", async () => {
		const child = await makePut("A1", session.peers[0].identity, 2n, [
			original,
		]);
		const childBlock = await blockOf(child);
		const model = reference(scope, owner);
		await model.put(originalBlock);
		await model.put(childBlock);
		await model.put(replacementBlock);
		await model.remove({
			...(await makeDelete(child, session.peers[0].identity)),
			history: [originalBlock.bytes],
		});
		await model.put(originalBlock);
		await model.put(childBlock);
		expect([...model.retired]).deep.equal([child.hash, original.hash]);
		expect([...model.live.keys()]).deep.equal([replacement.hash]);
	});
	it("does not partially retire the root when a required ancestor is missing", async () => {
		const child = await makePut("A1", session.peers[0].identity, 2n, [
			original,
		]);
		const model = reference(scope, owner);
		await model.put(originalBlock);
		await model.put(await blockOf(child));
		await expect(
			model.remove(await makeDelete(child, session.peers[0].identity)),
		).rejectedWith("Missing ancestry witness");
		expect([...model.retired]).deep.equal([]);
		expect([...model.live.keys()]).deep.equal([original.hash, child.hash]);
	});
	it("reuses prior proof bytes but never substitutes a retired CID for its missing closure", async () => {
		const middle = await makePut("A1", session.peers[0].identity, 2n, [
			original,
		]);
		const prior = {
			...(await makeDelete(middle, session.peers[0].identity)),
			history: [originalBlock.bytes],
		};
		const model = reference(scope, owner);
		await model.put(originalBlock);
		await model.put(await blockOf(middle));
		await model.remove(prior);
		const child = await makePut("A2", session.peers[0].identity, 3n, [middle]);
		await model.put(await blockOf(child));
		const deletion = await makeDelete(child, session.peers[0].identity);
		// Even though A1/A0 are already retired locally, A1 alone does not prove
		// the missing A0 boundary. Reused bytes receive normal fresh verification.
		await expect(
			model.remove({ ...deletion, history: [prior.victim.bytes] }),
		).rejectedWith("Missing ancestry witness");
		expect([...model.retired]).deep.equal([middle.hash, original.hash]);
		expect([...model.live.keys()]).deep.equal([child.hash]);
		await model.remove({
			...deletion,
			history: [prior.victim.bytes, ...prior.history],
		});
		expect([...model.retired].sort()).deep.equal(
			[original.hash, middle.hash, child.hash].sort(),
		);
		expect([...model.live.keys()]).deep.equal([]);
	});
	it("does not borrow the root owner's authority for a different same-key historical owner", async () => {
		const foreign = await makePut("old-owner", session.peers[1].identity, 1n);
		const child = await makePut("A1", session.peers[0].identity, 2n, [foreign]);
		const model = reference(scope, owner);
		await model.put(await blockOf(child));
		await model.put(replacementBlock);
		await expect(
			model.remove({
				...(await makeDelete(child, session.peers[0].identity)),
				history: [(await blockOf(foreign)).bytes],
			}),
		).rejectedWith("Put must be signed by the fixed store owner");
		expect([...model.retired]).deep.equal([]);
		expect([...model.live.keys()]).deep.equal([child.hash, replacement.hash]);
	});
	it("honors an ancestor's veto before changing any retirement or live fact", async () => {
		const child = await makePut("A1", session.peers[0].identity, 2n, [
			original,
		]);
		const checked: string[] = [];
		const model = reference(scope, owner, (value) => {
			checked.push(value.name!);
			return value.name !== "A";
		});
		await model.put(originalBlock);
		await model.put(await blockOf(child));
		await expect(
			model.remove({
				...(await makeDelete(child, session.peers[0].identity)),
				history: [originalBlock.bytes],
			}),
		).rejectedWith("Delete vetoed");
		expect(checked).deep.equal(["A1", "A"]);
		expect([...model.retired]).deep.equal([]);
		expect([...model.live.keys()]).deep.equal([original.hash, child.hash]);
	});

	const rejectsWithoutMutation = async (
		invalid: WitnessedDelete,
		reason: string,
	) => {
		const model = reference(scope, owner);
		await model.put(originalBlock);
		await model.put(replacementBlock);
		await expect(model.remove(invalid)).rejectedWith(reason);
		expect([...model.retired]).deep.equal([]);
		expect([...model.live.keys()]).deep.equal([
			original.hash,
			replacement.hash,
		]);
	};
	it("rejects altered victim bytes against the signed target CID", async () => {
		const invalid = structuredClone(cut);
		invalid.victim.bytes[20] ^= 1;
		await rejectsWithoutMutation(invalid, "CID mismatch");
	});
	it("rejects a signer-list CID alias both as an ordinary put and as a delete witness", async () => {
		const entry = deserialize(originalBlock.bytes, Entry) as EntryV0<Operation>;
		entry._signatures!.signatures.push(entry._signatures!.signatures[0]);
		const bytes = serialize(entry);
		const [cid] = await calculateRawCidV1Batch([bytes]);
		const alias = { cid, bytes };
		expect(cid).not.equal(originalBlock.cid);
		const authenticated = await authenticate(alias);
		expect(await authenticated.verifySignatures()).equal(true);
		const model = reference(scope, owner);
		await model.put(originalBlock);
		await model.remove(cut);
		await expect(model.put(alias)).rejectedWith(
			"Put must be signed by the fixed store owner",
		);
		expect([...model.live.keys()]).deep.equal([]);
		expect([...model.retired]).deep.equal([original.hash]);
		await rejectsWithoutMutation(
			{
				deletion: await blockOf(
					await makeEntry(
						new DeleteOperation({ key: toId("AQID") }),
						session.peers[0].identity,
						10n,
						[authenticated],
					),
				),
				victim: alias,
			},
			"Put must be signed by the fixed store owner",
		);
	});
	it("rejects a valid Secp256k1 owner profile on ordinary puts and witnesses", async () => {
		const signer = await Secp256k1Keypair.create();
		const victim = await makePut("secp", signer, 1n);
		const block = await blockOf(victim);
		expect(await (await authenticate(block)).verifySignatures()).equal(true);
		const model = reference(scope, signer.publicKey.bytes);
		await expect(model.put(block)).rejectedWith(
			"Expected canonical Ed25519 owner signature without prehash",
		);
		await expect(model.remove(await makeDelete(victim, signer))).rejectedWith(
			"Expected canonical Ed25519 owner signature without prehash",
		);
		expect([...model.live.keys()]).deep.equal([]);
		expect([...model.retired]).deep.equal([]);
	});
	it("rejects valid prehashed Ed25519 signatures on puts, witnesses and deletes", async () => {
		const signer: Identity = {
			publicKey: session.peers[0].identity.publicKey,
			sign: (bytes) => session.peers[0].identity.sign(bytes, PreHash.SHA_256),
		};
		const victim = await makePut("prehashed", signer, 1n);
		const block = await blockOf(victim);
		const authenticated = await authenticate(block);
		expect((await authenticated.getSignatures())[0].prehash).equal(
			PreHash.SHA_256,
		);
		const model = reference(scope, owner);
		await expect(model.put(block)).rejectedWith(
			"Expected canonical Ed25519 owner signature without prehash",
		);
		await expect(
			model.remove(await makeDelete(victim, session.peers[0].identity)),
		).rejectedWith(
			"Expected canonical Ed25519 owner signature without prehash",
		);
		await expect(model.remove(await makeDelete(original, signer))).rejectedWith(
			"Expected canonical Ed25519 owner signature without prehash",
		);
		expect([...model.live.keys()]).deep.equal([]);
		expect([...model.retired]).deep.equal([]);
	});
	it("rejects an invalid victim signature even when a valid delete binds its actual CID", async () => {
		const victim = structuredClone(originalBlock);
		const decoded = deserialize(victim.bytes, Entry) as Entry<Operation>;
		const signature = (await decoded.getSignatures())[0].signature;
		const offset = Buffer.from(victim.bytes).indexOf(signature);
		requireFact(offset >= 0, "Fixture signature bytes not found");
		victim.bytes[offset] ^= 1;
		[victim.cid] = await calculateRawCidV1Batch([victim.bytes]);
		decoded.hash = victim.cid;
		const deletion = await blockOf(
			await makeEntry(
				new DeleteOperation({ key: toId("AQID") }),
				session.peers[0].identity,
				10n,
				[decoded],
			),
		);
		await rejectsWithoutMutation({ deletion, victim }, "Invalid signature");
	});
	it("rejects a substituted valid victim", async () => {
		await rejectsWithoutMutation(
			{ ...cut, victim: replacementBlock },
			"Wrong target",
		);
	});
	it("rejects an authentic victim outside the fixed owner policy", async () => {
		const foreignOwner = await makePut(
			"unauthorized",
			session.peers[1].identity,
			1n,
		);
		await rejectsWithoutMutation(
			await makeDelete(foreignOwner, session.peers[0].identity),
			"Put must be signed by the fixed store owner",
		);
	});
	it("rejects a different typed key even when key primitives coincide", async () => {
		const wrongKey = toId(new Uint8Array([1, 2, 3]));
		expect(wrongKey.primitive).equal(toId("AQID").primitive);
		await rejectsWithoutMutation(
			await makeDelete(original, session.peers[0].identity, wrongKey),
			"Wrong typed key",
		);
	});
	it("rejects a delete signed by someone other than the fixed owner", async () => {
		await rejectsWithoutMutation(
			await makeDelete(original, session.peers[1].identity),
			"Delete must be signed by the target owner",
		);
	});
	it("rejects an authentic delete bound to another store", async () => {
		await rejectsWithoutMutation(
			await makeDelete(
				original,
				session.peers[0].identity,
				toId("AQID"),
				new TextEncoder().encode("other-store"),
			),
			"Wrong store scope",
		);
	});
	it("accepts an owner-signed legacy victim without asserting historical store admission", async () => {
		const foreignStore = await session.peers[0].open(
			new TestStore({ docs: new Documents<Document>() }),
			{
				args: {
					mode: "compat",
					replicate: false,
					keep: "self",
					canPerform: policy.signedByPublicKey(
						session.peers[0].identity.publicKey,
					),
				},
			},
		);
		const foreign = await foreignStore.docs.put(
			new Document({ id: "AQID", name: "foreign", data: owner }),
			{ target: "none" },
		);
		expect(foreignStore.address).not.equal(store.address);
		expect(await store.docs.log.log.has(foreign.entry.hash)).equal(false);
		const model = reference(scope, owner);
		await model.remove(
			await makeDelete(
				foreign.entry,
				session.peers[0].identity,
				toId("AQID"),
				scope,
				foreignStore,
			),
		);
		await model.put(await blockOf(foreign.entry, foreignStore));
		expect([...model.retired]).deep.equal([foreign.entry.hash]);
		expect([...model.live.keys()]).deep.equal([]);
	});
	it("honors an extra delete veto without mutation despite a valid owner proof", async () => {
		let calls = 0;
		const model = reference(scope, owner, (value) => {
			calls++;
			expect(value.name).equal("A");
			return false;
		});
		await model.put(originalBlock);
		await model.put(replacementBlock);
		await expect(model.remove(cut)).rejectedWith("Delete vetoed");
		expect(calls).equal(1);
		expect([...model.retired]).deep.equal([]);
		expect([...model.live.keys()]).deep.equal([
			original.hash,
			replacement.hash,
		]);
	});
	it("verifies a delete signature even with matching CID and createdLocally hints", async () => {
		const invalid = structuredClone(cut);
		const decoded = deserialize(
			invalid.deletion.bytes,
			Entry,
		) as Entry<Operation>;
		const signature = (await decoded.getSignatures())[0].signature;
		const offset = Buffer.from(invalid.deletion.bytes).indexOf(signature);
		requireFact(offset >= 0, "Fixture signature bytes not found");
		invalid.deletion.bytes[offset] ^= 1;
		[invalid.deletion.cid] = await calculateRawCidV1Batch([
			invalid.deletion.bytes,
		]);
		Object.assign(invalid.deletion, {
			createdLocally: true,
			hash: cut.deletion.cid,
		});
		await rejectsWithoutMutation(invalid, "Invalid signature");
	});
	it("rejects an embedded hash rather than trusting it as witness identity", async () => {
		const invalid = structuredClone(cut);
		const decoded = deserialize(
			invalid.victim.bytes,
			Entry,
		) as Entry<Operation>;
		decoded.hash = original.hash;
		invalid.victim.bytes = serialize(decoded);
		[invalid.victim.cid] = await calculateRawCidV1Batch([invalid.victim.bytes]);
		await rejectsWithoutMutation(
			invalid,
			"Embedded hash is not canonical stored evidence",
		);
	});
});
