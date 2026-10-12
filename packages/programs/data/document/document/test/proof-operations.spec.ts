import { deserialize, serialize } from "@dao-xyz/borsh";
import {
	DecryptedThing,
	Ed25519Keypair,
	PreHash,
	SignatureWithKey,
} from "@peerbit/crypto";
import {
	IntegerKey,
	StringKey,
	Uint8ArrayKey,
} from "@peerbit/indexer-interface";
import {
	Entry,
	EntryType,
	EntryV0,
	LamportClock,
	NO_ENCODING,
	Timestamp,
	calculateRawCidV1Batch,
	createEntry,
} from "@peerbit/log";
import { ExchangeHeadsMessage, SharedLog } from "@peerbit/shared-log";
import { TestSession } from "@peerbit/test-utils";
import { expect } from "chai";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { MAX_BATCH_SIZE } from "../src/constants.js";
import { Operation, PutOperation } from "../src/operation.js";
import {
	createProofDeleteLog,
	deriveProofDeleteScope,
	getProofDeleteContract,
} from "../src/proof-delete-manifest.js";
import {
	BORSH_ENCODING_PROOF_OPERATION,
	MAX_PROOF_HISTORY_BLOCKS,
	MAX_PROOF_HISTORY_EDGES,
	type OriginalProofEntry,
	ProofDeleteOperation,
	ProofOperation,
	ProofPutOperation,
	createProofOperationVerifier,
	deriveProofKeyGid,
} from "../src/proof-operations.js";

// Direct module contracts only. No production Documents activation, historical
// admission, physical deletion, checkpoint retention or receipt claim.
describe("proof operations authenticated owner history", () => {
	let session: TestSession;
	let owner: Ed25519Keypair;
	let other: Ed25519Keypair;
	let scope: Uint8Array;
	const nonce = new Uint8Array(32).fill(4);
	const key = () => new StringKey("AQID");
	const data = Uint8Array.of(1, 2, 3);
	const verifier = (kind: 0 | 1 = 0) =>
		createProofOperationVerifier({
			contract: {
				version: 1,
				nonce,
				owner: owner.publicKey.publicKey,
				keyKind: kind,
			},
		});
	let checked: ReturnType<typeof verifier>;
	const blockOf = async (
		entry: Entry<ProofOperation>,
	): Promise<OriginalProofEntry> => {
		const bytes = await session.peers[0].services.blocks.get(entry.hash, {
			remote: false,
		});
		if (!bytes) throw new Error("Missing original fixture bytes");
		return { cid: entry.hash, bytes: new Uint8Array(bytes) };
	};
	const make = async (
		operation: ProofPutOperation | ProofDeleteOperation,
		next: Entry<ProofOperation>[] = [],
		identity = owner,
		type = operation instanceof ProofDeleteOperation
			? EntryType.CUT
			: EntryType.APPEND,
		gid = deriveProofKeyGid(operation.scope, operation.key),
		clock?: LamportClock,
	) => {
		const entry = await (
			EntryV0 as unknown as {
				createWithExplicitGid<T>(
					properties: Parameters<typeof EntryV0.create<T>>[0],
					gid: string,
				): Promise<Entry<T>>;
			}
		).createWithExplicitGid(
			{
				store: session.peers[0].services.blocks,
				identity,
				encoding: BORSH_ENCODING_PROOF_OPERATION,
				data: operation,
				meta: { type, next, clock },
			},
			gid,
		);
		return { entry, block: await blockOf(entry) };
	};
	const put = (
		next: Entry<ProofOperation>[] = [],
		id = key(),
		identity = owner,
		storeScope = scope,
	) =>
		make(
			new ProofPutOperation({ scope: storeScope, key: id, data }),
			next,
			identity,
		);
	const history = async (
		target: Entry<ProofOperation>,
		entries: Entry<ProofOperation>[],
	) =>
		checked.history({
			target: target.hash,
			key: key(),
			witnesses: await Promise.all(
				entries.map(async (e) => (await blockOf(e)).bytes),
			),
		});
	const deletion = async (
		target: Entry<ProofOperation>,
		witnesses: Entry<ProofOperation>[],
		next: Entry<ProofOperation>[],
	) =>
		make(
			new ProofDeleteOperation({
				scope,
				key: key(),
				target: target.hash,
				witnesses: await Promise.all(
					witnesses.map(async (e) => (await blockOf(e)).bytes),
				),
			}),
			next,
		);
	const sorted = (entries: Entry<ProofOperation>[]) =>
		entries
			.slice()
			.sort((a, b) => (a.hash < b.hash ? -1 : a.hash > b.hash ? 1 : 0));
	const reencode = async (entry: Entry<ProofOperation>) => {
		const bytes = serialize(entry);
		const [cid] = await calculateRawCidV1Batch([bytes]);
		return { cid: cid!, bytes };
	};
	before(async () => {
		session = await TestSession.disconnected(1);
		owner = await Ed25519Keypair.create();
		other = await Ed25519Keypair.create();
		scope = deriveProofDeleteScope({
			version: 1,
			nonce,
			owner: owner.publicKey.publicKey,
			keyKind: 0,
		});
		checked = verifier();
	});
	after(async () => session?.stop());

	it("authenticates a scoped ordinary put and preserves original bytes", async () => {
		const { block } = await put();
		const fact = await checked.put(block);
		expect(fact.hash).equal(block.cid);
		expect(fact.bytes).deep.equal(block.bytes);
		expect(fact.bytes).not.equal(block.bytes);
		expect(fact.key).deep.equal(key());
		expect(fact.data).deep.equal(data);
	});

	describe("incoming original-byte capture", () => {
		const exchange = async (entry: Entry<ProofOperation>) => {
			// The real wire class is deliberately not a new document dependency API.
			const { EntryWithRefs } = await import(
				new URL(
					"./exchange-heads.js",
					import.meta.resolve("@peerbit/shared-log"),
				).href
			);
			const message = new ExchangeHeadsMessage({
				heads: [new EntryWithRefs({ entry, gidRefrences: [] })],
			});
			return deserialize(serialize(message), ExchangeHeadsMessage).heads[0]!
				.entry as Entry<ProofOperation>;
		};
		it("reconstructs exact canonical storage bytes from real ExchangeHeads PUT and CUT objects", async () => {
			const target = await put();
			const cut = await deletion(target.entry, [target.entry], [target.entry]);
			for (const { entry, block } of [target, cut]) {
				const transport = await exchange(entry);
				expect(transport.hash).equal(block.cid);
				expect(Entry.getPreparedStorageBytes(transport)).equal(undefined);
				const before = serialize(transport);
				const captured = await checked.incoming({ entry: transport });
				expect(captured).deep.equal({ ...block, source: "reconstructed" });
				expect(captured.bytes).not.equal(block.bytes);
				expect(serialize(transport)).deep.equal(before);
			}
		});

		it("prefers and snapshots prepared originals without consuming them or calling storage", async () => {
			const { block } = await put();
			const prepared = new Uint8Array(block.bytes);
			const entry = deserialize(prepared, Entry) as Entry<ProofOperation>;
			entry.hash = Entry.prepareMultihashBytes(entry, prepared, block.cid);
			const pending = checked.incoming({
				entry,
				resolveLocal: async () => {
					throw new Error("Prepared bytes must win");
				},
			});
			prepared.fill(0);
			entry.hash = "mutated";
			const captured = await pending;
			expect(captured).deep.equal({ ...block, source: "prepared" });
			expect(Entry.hasPreparedBlock(entry)).equal(true);
		});

		for (const found of [false, true]) {
			it(`owns transport fields and resolver across a held local lookup (${found ? "stored" : "reconstructed"})`, async () => {
				const { entry, block } = await put();
				const transport = await exchange(entry);
				let release!: () => void;
				const held = new Promise<void>((resolve) => (release = resolve));
				const input = {
					entry: transport,
					resolveLocal: async (hash: string) => {
						expect(hash).equal(block.cid);
						await held;
						return found ? block.bytes : undefined;
					},
				};
				const pending = checked.incoming(input);
				input.resolveLocal = async () => {
					throw new Error("Replaced resolver must not run");
				};
				transport.hash = "mutated";
				(transport as EntryV0<ProofOperation>)._meta.decrypted._data!.fill(0);
				release();
				const captured = await pending;
				expect(captured).deep.equal({
					...block,
					source: found ? "stored" : "reconstructed",
				});
				captured.bytes.fill(0);
				expect(block.bytes[0]).equal(0);
				expect(block.bytes.some((byte) => byte !== 0)).equal(true);
			});
		}

		it("ignores inconsistent decoded caches but never trusts validation hints or damaged raw fields", async () => {
			const { entry, block } = await put();
			const transport = await exchange(entry);
			transport.init({ encoding: BORSH_ENCODING_PROOF_OPERATION });
			transport.meta.gid = "unsigned-cache-change";
			((await transport.getPayloadValue()) as ProofPutOperation).key =
				new StringKey("other");
			const captured = await checked.incoming({ entry: transport });
			expect(captured.bytes).deep.equal(block.bytes);
			expect((await checked.put(captured)).key).deep.equal(key());
			const damaged = await exchange(entry);
			(damaged as EntryV0<ProofOperation>)._meta.decrypted._data![2] ^= 1;
			Object.assign(damaged, {
				__alreadyValidated: true,
				createdLocally: true,
			});
			await expect(checked.incoming({ entry: damaged })).rejectedWith(
				"CID mismatch",
			);
		});

		it("rejects incorrect CIDs, unsupported objects and inconsistent stored originals without fallback", async () => {
			const a = await put();
			const b = await put([], new StringKey("other"));
			const transport = await exchange(a.entry);
			transport.hash = b.block.cid;
			await expect(checked.incoming({ entry: transport })).rejectedWith(
				"CID mismatch",
			);
			await expect(
				checked.incoming({
					entry: await exchange(a.entry),
					resolveLocal: async () => b.block.bytes,
				}),
			).rejectedWith("CID mismatch");
			await expect(
				checked.incoming({
					entry: {
						hash: a.block.cid,
						getStorageBytes: () => a.block.bytes,
					} as Entry<ProofOperation>,
				}),
			).rejectedWith("Unsupported proof transport entry");
		});

		it("fully validates CUT witness closure before returning an incoming candidate", async () => {
			const parent = await put();
			const target = await put([parent.entry]);
			const incomplete = await deletion(
				target.entry,
				[target.entry],
				[target.entry],
			);
			await expect(
				checked.incoming({ entry: await exchange(incomplete.entry) }),
			).rejectedWith("Missing proof ancestry witness");
		});
	});

	for (const parentKey of ["AQID", "boundary"]) {
		it(`rejects a signed noncausal PUT edge to ${parentKey === "AQID" ? "same-key history" : "a cross-key boundary"}`, async () => {
			const parent = await put([], new StringKey(parentKey));
			const timestamp = parent.entry.meta.clock.timestamp;
			// A remote owner can sign a false causal clock; creation against a
			// substituted parent view must not make the real witnessed edge valid.
			const substituted = {
				hash: parent.entry.hash,
				meta: {
					clock: new LamportClock({
						id: owner.publicKey.bytes,
						timestamp: new Timestamp({ wallTime: 0n }),
					}),
				},
			} as Entry<ProofOperation>;
			const child = await make(
				new ProofPutOperation({ scope, key: key(), data }),
				[substituted],
				owner,
				EntryType.APPEND,
				deriveProofKeyGid(scope, key()),
				new LamportClock({
					id: owner.publicKey.bytes,
					timestamp: new Timestamp({
						wallTime: timestamp.wallTime,
						logical: timestamp.logical,
					}),
				}),
			);
			await expect(
				checked.history({
					target: child.entry.hash,
					key: key(),
					witnesses: [child.block.bytes, parent.block.bytes],
				}),
			).rejectedWith("Noncausal proof PUT edge");
		});
	}

	it("rejects a signed CUT clock that does not follow its authenticated targets", async () => {
		const target = await put();
		const substituted = {
			hash: target.entry.hash,
			meta: {
				clock: new LamportClock({
					id: owner.publicKey.bytes,
					timestamp: new Timestamp({ wallTime: 0n }),
				}),
			},
		} as Entry<ProofOperation>;
		const cut = await make(
			new ProofDeleteOperation({
				scope,
				key: key(),
				target: target.entry.hash,
				witnesses: [target.block.bytes],
			}),
			[substituted],
			owner,
			EntryType.CUT,
			deriveProofKeyGid(scope, key()),
			new LamportClock({
				id: owner.publicKey.bytes,
				timestamp: target.entry.meta.clock.timestamp.clone(),
			}),
		);
		await expect(checked.deletion(cut.block)).rejectedWith(
			"Noncausal proof CUT edge",
		);
	});

	it("groups independent same-key roots while separating typed keys and protocol scopes", async () => {
		const first = await put();
		const independent = await put();
		const otherKey = await put([], new StringKey("other"));
		const otherScope = await put([], key(), owner, new Uint8Array(32));
		expect(first.entry.hash).not.equal(independent.entry.hash);
		expect(first.entry.meta.gid).equal(independent.entry.meta.gid);
		expect(first.entry.meta.gid).equal(deriveProofKeyGid(scope, key()));
		expect(first.entry.meta.gid).match(/^[0-9a-f]{64}$/);
		expect(first.entry.meta.gid).not.equal(otherKey.entry.meta.gid);
		expect(first.entry.meta.gid).not.equal(otherScope.entry.meta.gid);
		const binary = new Uint8ArrayKey(Uint8Array.of(1, 2, 3));
		expect(binary.primitive).equal(key().primitive);
		expect(deriveProofKeyGid(scope, binary)).not.equal(first.entry.meta.gid);
		await checked.put(first.block);
		await checked.put(independent.block);
	});

	it("authenticates real cross-key parents without inheriting their placement group", async () => {
		const older = await put();
		const boundary = await put([older.entry], new StringKey("other"));
		const tip = await put([boundary.entry]);
		expect(tip.entry.meta.gid).equal(older.entry.meta.gid);
		expect(tip.entry.meta.gid).not.equal(boundary.entry.meta.gid);
		expect(tip.entry.meta.next).deep.equal([boundary.entry.hash]);
		expect(await tip.entry.verifySignatures()).equal(true);
		const result = await history(tip.entry, [tip.entry, boundary.entry]);
		expect(result.targets.map((fact) => fact.hash)).deep.equal([
			tip.entry.hash,
		]);
		expect(result.boundaries.map((fact) => fact.hash)).deep.equal([
			boundary.entry.hash,
		]);
	});

	it("rejects signed PUT and CUT entries with the wrong key-derived gid", async () => {
		const target = await put();
		const wrongPut = await make(
			new ProofPutOperation({ scope, key: key(), data }),
			[],
			owner,
			EntryType.APPEND,
			"wrong-group",
		);
		const wrongCut = await make(
			new ProofDeleteOperation({
				scope,
				key: key(),
				target: target.entry.hash,
				witnesses: [target.block.bytes],
			}),
			[target.entry],
			owner,
			EntryType.CUT,
			"wrong-group",
		);
		expect(await wrongPut.entry.verifySignatures()).equal(true);
		expect(await wrongCut.entry.verifySignatures()).equal(true);
		await expect(checked.put(wrongPut.block)).rejectedWith(
			"Wrong proof key gid",
		);
		await expect(checked.deletion(wrongCut.block)).rejectedWith(
			"Wrong proof key gid",
		);
		const child = await put([wrongPut.entry]);
		await expect(
			history(child.entry, [child.entry, wrongPut.entry]),
		).rejectedWith("Wrong proof key gid");
	});

	it("derives full diamond ancestry, stops at authenticated cross-key boundaries, and ignores input order", async () => {
		const older = await put();
		const boundary = await put([older.entry], new StringKey("other"));
		const root = await put([boundary.entry]);
		const left = await put([root.entry]);
		const right = await put([root.entry]);
		const tip = await put([left.entry, right.entry]);
		const entries = [
			tip.entry,
			left.entry,
			right.entry,
			root.entry,
			boundary.entry,
		];
		for (const evidence of [
			entries,
			entries.slice().reverse(),
			[root.entry, right.entry, boundary.entry, tip.entry, left.entry],
		]) {
			const result = await history(tip.entry, evidence);
			expect(result.targets.map((x) => x.hash).sort()).deep.equal(
				[tip.entry, left.entry, right.entry, root.entry]
					.map((x) => x.hash)
					.sort(),
			);
			expect(result.boundaries.map((x) => x.hash)).deep.equal([
				boundary.entry.hash,
			]);
			const cut = await deletion(
				tip.entry,
				evidence,
				sorted([tip.entry, left.entry, right.entry, root.entry]),
			);
			expect(
				(await checked.deletion(cut.block)).targets.map((x) => x.hash),
			).deep.equal(result.targets.map((x) => x.hash));
		}
	});

	it("rejects incomplete, duplicate, extra and wrongly rooted evidence", async () => {
		const a = await put();
		const b = await put([a.entry]);
		const unrelated = await put();
		await expect(history(b.entry, [b.entry])).rejectedWith(
			"Missing proof ancestry",
		);
		await expect(history(b.entry, [a.entry])).rejectedWith(
			"Missing proof target",
		);
		await expect(history(b.entry, [a.entry, b.entry, a.entry])).rejectedWith(
			"Duplicate proof witness",
		);
		await expect(
			history(b.entry, [a.entry, b.entry, unrelated.entry]),
		).rejectedWith("Unused proof witness");
		await expect(
			checked.history({
				target: a.entry.hash,
				key: new StringKey("wrong"),
				witnesses: [a.block.bytes],
			}),
		).rejectedWith("Wrong proof target key");
	});

	it("requires every exact target, canonical order, no duplicate links and no cross-key boundary in CUT.next", async () => {
		const boundary = await put([], new StringKey("boundary"));
		const a = await put([boundary.entry]);
		const b = await put([a.entry]);
		const targets = sorted([a.entry, b.entry]);
		for (const next of [
			[b.entry],
			[...targets, boundary.entry],
			targets.slice().reverse(),
			[...targets, targets[0]!],
		]) {
			const cut = await deletion(
				b.entry,
				[b.entry, a.entry, boundary.entry],
				next,
			);
			await expect(checked.deletion(cut.block)).rejectedWith(
				"all exact targets",
			);
		}
	});

	it("does not erase independent same-key replacements through extra witnesses or links", async () => {
		const a = await put();
		const independent = await put();
		const cut = await deletion(a.entry, [a.entry], [a.entry]);
		expect(
			(await checked.deletion(cut.block)).targets.map((x) => x.hash),
		).deep.equal([a.entry.hash]);
		await expect(
			checked.deletion(
				(
					await deletion(
						a.entry,
						[a.entry, independent.entry],
						sorted([a.entry, independent.entry]),
					)
				).block,
			),
		).rejectedWith("Unused proof witness");
	});

	it("rejects foreign store or owner at ordinary put, target and cross-key boundary", async () => {
		for (const foreign of [
			await put([], key(), other),
			await put([], key(), owner, new Uint8Array(32)),
		]) {
			await expect(checked.put(foreign.block)).rejected;
			await expect(
				checked.deletion(
					(await deletion(foreign.entry, [foreign.entry], [foreign.entry]))
						.block,
				),
			).rejected;
		}
		const boundary = await put([], new StringKey("boundary"), other);
		const a = await put([boundary.entry]);
		await expect(history(a.entry, [a.entry, boundary.entry])).rejectedWith(
			"canonical Ed25519 owner",
		);
	});

	it("rejects unauthorized deletes even when their target evidence is valid", async () => {
		const a = await put();
		const operation = new ProofDeleteOperation({
			scope,
			key: key(),
			target: a.entry.hash,
			witnesses: [a.block.bytes],
		});
		await expect(
			checked.deletion((await make(operation, [a.entry], other)).block),
		).rejectedWith("canonical Ed25519 owner");
		operation.scope = new Uint8Array(32);
		await expect(
			checked.deletion((await make(operation, [a.entry])).block),
		).rejectedWith("Wrong proof store scope");
	});

	it("allows binary profiles but rejects primitive aliases and unsupported numeric keys", async () => {
		const binaryKey = new Uint8ArrayKey(Uint8Array.of(1, 2, 3));
		expect(binaryKey.primitive).equal(key().primitive);
		const binaryScope = deriveProofDeleteScope({
			version: 1,
			nonce,
			owner: owner.publicKey.publicKey,
			keyKind: 1,
		});
		const binary = await put([], binaryKey as never, owner, binaryScope);
		await expect(checked.put(binary.block)).rejectedWith(
			"Wrong proof store scope",
		);
		expect(serialize((await verifier(1).put(binary.block)).key)).deep.equal(
			serialize(binaryKey),
		);
		await expect(verifier(1).put((await put()).block)).rejectedWith(
			"Wrong proof store scope",
		);
		await expect(
			checked.put((await put([], binaryKey as never)).block),
		).rejectedWith("Wrong proof key kind");
		await expect(
			checked.put((await put([], new IntegerKey(1) as never)).block),
		).rejectedWith("Wrong proof key kind");
	});

	it("rejects wrong operation types and CUT witnesses without silently truncating ancestry", async () => {
		const a = await put();
		const cut = await deletion(a.entry, [a.entry], [a.entry]);
		const afterCut = await put([cut.entry]);
		await expect(
			history(afterCut.entry, [afterCut.entry, cut.entry]),
		).rejectedWith("Expected proof APPEND/PUT");
		await expect(checked.put(cut.block)).rejectedWith(
			"Expected proof APPEND/PUT",
		);
		await expect(checked.deletion(a.block)).rejectedWith(
			"Expected proof CUT/DELETE",
		);
		const wrongType = await make(
			new ProofPutOperation({ scope, key: key(), data }),
			[],
			owner,
			EntryType.CUT,
		);
		await expect(checked.put(wrongType.block)).rejectedWith(
			"Expected proof APPEND/PUT",
		);
	});

	it("accepts separate recreate/delete history without prior CUT witnesses", async () => {
		const first = await put();
		await checked.deletion(
			(await deletion(first.entry, [first.entry], [first.entry])).block,
		);
		const recreated = await put();
		const result = await checked.deletion(
			(await deletion(recreated.entry, [recreated.entry], [recreated.entry]))
				.block,
		);
		expect(result.targets.map((x) => x.hash)).deep.equal([
			recreated.entry.hash,
		]);
	});

	it("captures manifest, original CID and bytes before yielding", async () => {
		const contract = {
			version: 1 as const,
			nonce: new Uint8Array(nonce),
			owner: new Uint8Array(owner.publicKey.publicKey),
			keyKind: 0 as const,
		};
		const own = createProofOperationVerifier({ contract });
		contract.owner.fill(0);
		contract.nonce.fill(0);
		const a = await put();
		const cut = await deletion(a.entry, [a.entry], [a.entry]);
		const input = {
			cid: cut.block.cid,
			bytes: new Uint8Array(cut.block.bytes),
		};
		const pending = own.deletion(input);
		input.cid = a.entry.hash;
		input.bytes.fill(0);
		const result = await pending;
		expect(result.hash).equal(cut.block.cid);
		expect(result.targets[0]!.hash).equal(a.entry.hash);
	});

	it("captures all preparation witnesses and key before yielding", async () => {
		const a = await put();
		const b = await put([a.entry]);
		const supplied = [
			new Uint8Array(a.block.bytes),
			new Uint8Array(b.block.bytes),
		];
		const declared = key();
		const pending = checked.history({
			target: b.entry.hash,
			key: declared,
			witnesses: supplied,
		});
		declared.key = "replacement";
		supplied[0]!.fill(0);
		supplied.reverse();
		expect((await pending).targets.map((x) => x.hash).sort()).deep.equal(
			[a.entry.hash, b.entry.hash].sort(),
		);
	});

	it("rejects CID mismatch, changed signature and duplicate-signature aliases", async () => {
		const a = await put();
		await expect(
			checked.put({ cid: "wrong", bytes: a.block.bytes }),
		).rejectedWith("Proof CID mismatch");
		const entry = deserialize(a.block.bytes, Entry) as EntryV0<ProofOperation>;
		entry._signatures!.signatures.push(entry._signatures!.signatures[0]!);
		await expect(checked.put(await reencode(entry))).rejectedWith(
			"one Ed25519 owner signature",
		);
		const invalid = deserialize(
			a.block.bytes,
			Entry,
		) as EntryV0<ProofOperation>;
		const signature =
			invalid._signatures!.signatures[0]!.decrypted.getValue(SignatureWithKey);
		signature.signature[0] ^= 1;
		invalid._signatures!.signatures = [
			new DecryptedThing({ data: serialize(signature) }),
		];
		await expect(checked.put(await reencode(invalid))).rejectedWith(
			"Invalid proof signature",
		);
	});

	it("rejects owner-produced prehashed signatures outside the immutable profile", async () => {
		const a = await put();
		const entry = deserialize(a.block.bytes, Entry) as EntryV0<ProofOperation>;
		entry.init({ encoding: BORSH_ENCODING_PROOF_OPERATION });
		entry._signatures!.signatures = [
			new DecryptedThing({
				data: serialize(
					await owner.sign(entry.getSignableBytes(), PreHash.SHA_256),
				),
			}),
		];
		await expect(checked.put(await reencode(entry))).rejectedWith(
			"without prehash",
		);
	});

	it("rejects an unsigned registered signature-subclass CID alias without polluting other codecs", async () => {
		const original = await put();
		const { stdout } = await promisify(execFile)(
			process.execPath,
			[
				"--input-type=module",
				"-e",
				`
import { deserialize, field, serialize, variant } from '@dao-xyz/borsh';
import { DecryptedThing, Ed25519Keypair, SignatureWithKey } from '@peerbit/crypto';
import { Entry, calculateRawCidV1Batch } from '@peerbit/log';
import { BORSH_ENCODING_PROOF_OPERATION, createProofOperationVerifier } from './dist/src/proof-operations.js';
const input = JSON.parse(process.argv[1]);
await Ed25519Keypair.create(); // Initialize the isolated process's crypto backend.
const entry = deserialize(new Uint8Array(input.bytes), Entry);
entry.init({ encoding: BORSH_ENCODING_PROOF_OPERATION });
const signature = entry._signatures.signatures[0].getValue(SignatureWithKey);
class AnnotatedSignature extends SignatureWithKey {}
variant(17)(AnnotatedSignature);
field({ type: 'u8' })(AnnotatedSignature.prototype, 'annotation');
Object.setPrototypeOf(signature, AnnotatedSignature.prototype);
signature.annotation = 1;
entry._signatures.signatures = [new DecryptedThing({ data: serialize(signature) })];
const bytes = serialize(entry);
const [cid] = await calculateRawCidV1Batch([bytes]);
if (cid === input.cid) throw Error('Alias CID did not change');
const decoded = deserialize(bytes, Entry).init({ encoding: BORSH_ENCODING_PROOF_OPERATION });
if (!await decoded.verifySignatures()) throw Error('Control signature did not verify');
const checked = createProofOperationVerifier({ contract: { version: 1, nonce: new Uint8Array(input.nonce), owner: new Uint8Array(input.owner), keyKind: 0 } });
let rejected = false;
try { await checked.put({ cid, bytes }); } catch (error) {
  if (!error.message.includes('canonical Ed25519 owner')) throw error;
  rejected = true;
}
if (!rejected) throw Error('Unsigned signature-subclass alias accepted');
console.log('verified alias rejected');
`,
				JSON.stringify({
					cid: original.block.cid,
					bytes: [...original.block.bytes],
					owner: [...owner.publicKey.publicKey],
					nonce: [...nonce],
				}),
			],
			{ cwd: new URL("../..", import.meta.url), timeout: 10_000 },
		);
		expect(stdout.trim()).equal("verified alias rejected");
	});

	it("rejects noncanonical signed operation bytes and legacy operation variants", async () => {
		const operation = new ProofPutOperation({ scope, key: key(), data });
		const bytes = serialize(operation);
		const entry = await createEntry({
			store: session.peers[0].services.blocks,
			identity: owner,
			encoding: NO_ENCODING,
			data: new Uint8Array([...bytes, 0]),
			meta: { next: [] },
		});
		await expect(checked.put(await blockOf(entry))).rejected;
		expect(() => deserialize(bytes, Operation)).to.throw();
		expect(() =>
			deserialize(serialize(new PutOperation({ data })), ProofOperation),
		).to.throw();
		const protectedLog = createProofDeleteLog({
			version: 1,
			nonce,
			owner: owner.publicKey.publicKey,
			keyKind: 0,
		});
		const stripped = deserialize(serialize(protectedLog), SharedLog);
		expect(getProofDeleteContract(stripped)).equal(undefined);
		expect(() =>
			createProofOperationVerifier({
				contract: getProofDeleteContract(stripped)!,
			}),
		).to.throw();
		const target = await put();
		expect(() =>
			deserialize(
				serialize(
					new ProofDeleteOperation({
						scope,
						key: key(),
						target: target.entry.hash,
						witnesses: [target.block.bytes],
					}),
				),
				Operation,
			),
		).to.throw();
	});

	it("enforces the full encoded entry cap before hashing or decoding, including spoofed lengths", async () => {
		const bytes = new Uint8Array(MAX_BATCH_SIZE + 1);
		Object.defineProperty(bytes, "byteLength", { value: 1 });
		await expect(checked.deletion({ cid: "irrelevant", bytes })).rejectedWith(
			"Proof byte limit",
		);
		await expect(checked.put({ cid: "irrelevant", bytes })).rejectedWith(
			"Proof byte limit",
		);
	});

	it("rejects a signed delete whose witness fits but whose full entry exceeds the cap", async () => {
		const small = await put();
		const large = await make(
			new ProofPutOperation({
				scope,
				key: key(),
				data: new Uint8Array(
					MAX_BATCH_SIZE - (small.block.bytes.length - data.length) - 16,
				),
			}),
		);
		expect(large.block.bytes.length).lessThan(MAX_BATCH_SIZE);
		const cut = await deletion(large.entry, [large.entry], [large.entry]);
		expect(cut.block.bytes.length).greaterThan(MAX_BATCH_SIZE);
		await expect(checked.deletion(cut.block)).rejectedWith("Proof byte limit");
	});

	it("rejects hostile wire witness counts and lengths before allocating their arrays", async () => {
		const a = await put();
		const encoded = serialize(
			new ProofDeleteOperation({
				scope,
				key: key(),
				target: a.entry.hash,
				witnesses: [a.block.bytes],
			}),
		);
		const offset = encoded.length - 4 - 4 - a.block.bytes.length;
		const count = new Uint8Array(encoded);
		new DataView(count.buffer).setUint32(offset, 0xffffffff, true);
		expect(() => deserialize(count, ProofOperation)).to.throw("block limit");
		const size = new Uint8Array(encoded);
		new DataView(size.buffer).setUint32(offset + 4, 0xffffffff, true);
		expect(() => deserialize(size, ProofOperation)).to.throw("byte limit");
		const truncated = new Uint8Array(encoded);
		new DataView(truncated.buffer).setUint32(
			offset + 4,
			a.block.bytes.length + 1,
			true,
		);
		expect(() => deserialize(truncated, ProofOperation)).to.throw(
			"Truncated proof bytes",
		);
	});

	it("rejects over-budget witness counts and aggregate bytes before authentication", async () => {
		const a = await put();
		await expect(
			checked.history({ target: a.entry.hash, key: key(), witnesses: [] }),
		).rejectedWith("block limit");
		await expect(
			checked.history({
				target: a.entry.hash,
				key: key(),
				witnesses: new Array(MAX_PROOF_HISTORY_BLOCKS + 1).fill(a.block.bytes),
			}),
		).rejectedWith("block limit");
		await expect(
			checked.history({
				target: a.entry.hash,
				key: key(),
				witnesses: [new Uint8Array(MAX_BATCH_SIZE), Uint8Array.of(1)],
			}),
		).rejectedWith("byte limit");
	});

	it("rejects over-budget causal edges rather than accepting a partial closure", async () => {
		const a = await put();
		const tooMany = await put(
			new Array(MAX_PROOF_HISTORY_EDGES + 1).fill(a.entry),
		);
		await expect(checked.put(tooMany.block)).rejectedWith("edge limit");
	});

	describe("bounded original-byte history collection", () => {
		it("deduplicates a diamond and never resolves beyond an authenticated cross-key boundary", async () => {
			const older = await put();
			const boundary = await put([older.entry], new StringKey("other"));
			const root = await put([boundary.entry]);
			const left = await put([root.entry]);
			const right = await put([root.entry]);
			const tip = await put([left.entry, right.entry]);
			const originals = [tip, left, right, root, boundary];
			const byHash = new Map(
				originals.map((x) => [x.block.cid, x.block.bytes]),
			);
			const calls: string[] = [];
			let active = 0;
			const result = await checked.collectHistory({
				target: tip.entry.hash,
				key: key(),
				resolve: async (hash) => {
					expect(active++).equal(0);
					calls.push(hash);
					await Promise.resolve();
					active--;
					return byHash.get(hash);
				},
			});
			expect(calls).to.have.length(5);
			expect(new Set(calls).size).equal(5);
			expect(calls).not.include(older.entry.hash);
			expect(result.targets.map((x) => x.hash).sort()).deep.equal(
				[tip, left, right, root].map((x) => x.entry.hash).sort(),
			);
			expect(result.boundaries.map((x) => x.hash)).deep.equal([
				boundary.entry.hash,
			]);
			const cut = await make(
				new ProofDeleteOperation({
					scope,
					key: key(),
					target: tip.entry.hash,
					witnesses: [...result.targets, ...result.boundaries].map(
						(fact) => fact.bytes,
					),
				}),
				sorted([tip.entry, left.entry, right.entry, root.entry]),
			);
			expect(
				(await checked.deletion(cut.block)).targets.map((x) => x.hash),
			).deep.equal(result.targets.map((x) => x.hash));
		});

		it("rejects a missing target or boundary instead of returning a partial proof", async () => {
			const boundary = await put([], new StringKey("other"));
			const tip = await put([boundary.entry]);
			for (const targetPresent of [false, true]) {
				const calls: string[] = [];
				await expect(
					checked.collectHistory({
						target: tip.entry.hash,
						key: key(),
						resolve: async (hash) => {
							calls.push(hash);
							return targetPresent && hash === tip.entry.hash
								? tip.block.bytes
								: undefined;
						},
					}),
				).rejectedWith("Missing proof ancestry witness");
				expect(calls).deep.equal(
					targetPresent
						? [tip.entry.hash, boundary.entry.hash]
						: [tip.entry.hash],
				);
			}
		});

		it("authenticates owner, scope and APPEND type before following resolved ancestry", async () => {
			const older = await put();
			const cut = await deletion(older.entry, [older.entry], [older.entry]);
			for (const invalid of [
				await put([older.entry], key(), other),
				await put([older.entry], key(), owner, new Uint8Array(32)),
				cut,
			]) {
				const tip = await put([invalid.entry]);
				const calls: string[] = [];
				await expect(
					checked.collectHistory({
						target: tip.entry.hash,
						key: key(),
						resolve: async (hash) => {
							calls.push(hash);
							return hash === tip.entry.hash
								? tip.block.bytes
								: invalid.block.bytes;
						},
					}),
				).rejected;
				expect(calls).deep.equal([tip.entry.hash, invalid.entry.hash]);
			}
		});

		it("cannot turn a substituted parent into a cyclic CID alias", async () => {
			const parent = await put();
			const tip = await put([parent.entry]);
			const calls: string[] = [];
			await expect(
				checked.collectHistory({
					target: tip.entry.hash,
					key: key(),
					resolve: async (hash) => {
						calls.push(hash);
						// Repeating the child's bytes cannot make them its own parent.
						return tip.block.bytes;
					},
				}),
			).rejectedWith("Proof CID mismatch");
			expect(calls).deep.equal([tip.entry.hash, parent.entry.hash]);
		});

		it("captures target, key, resolver and each resolved byte array before later callbacks", async () => {
			const parent = await put();
			const tip = await put([parent.entry]);
			const originalTip = new Uint8Array(tip.block.bytes);
			let release!: (bytes: Uint8Array) => void;
			const first = new Promise<Uint8Array>((resolve) => (release = resolve));
			const input = {
				target: tip.entry.hash,
				key: key(),
				resolve: async (hash: string) => {
					if (hash === tip.entry.hash) return first;
					expect(hash).equal(parent.entry.hash);
					tip.block.bytes.fill(0);
					return parent.block.bytes;
				},
			};
			const pending = checked.collectHistory(input);
			input.target = parent.entry.hash;
			input.key.key = "replacement";
			input.resolve = async () => {
				throw new Error("Mutated resolver used");
			};
			release(tip.block.bytes);
			const result = await pending;
			expect(result.targets.map((x) => x.hash)).deep.equal([
				tip.entry.hash,
				parent.entry.hash,
			]);
			expect(result.targets[0]!.bytes).deep.equal(originalTip);
			expect(result.targets[0]!.key).deep.equal(key());
		});

		it("rejects wrong root keys before fetching their ancestors", async () => {
			const parent = await put();
			const tip = await put([parent.entry]);
			let calls = 0;
			await expect(
				checked.collectHistory({
					target: tip.entry.hash,
					key: new StringKey("wrong"),
					resolve: async () => {
						calls++;
						return tip.block.bytes;
					},
				}),
			).rejectedWith("Wrong proof target key");
			expect(calls).equal(1);
		});

		it("bounds distinct scheduled hashes before invoking more resolvers", async () => {
			const parents: Entry<ProofOperation>[] = [];
			for (let i = 0; i < MAX_PROOF_HISTORY_BLOCKS; i++) {
				parents.push((await put()).entry);
			}
			const tip = await put(parents);
			let calls = 0;
			await expect(
				checked.collectHistory({
					target: tip.entry.hash,
					key: key(),
					resolve: async () => {
						calls++;
						return tip.block.bytes;
					},
				}),
			).rejectedWith("Proof history block limit");
			expect(calls).equal(1);
		});

		it("bounds aggregate bytes before decoding another resolved block", async () => {
			const parent = await put();
			const tip = await make(
				new ProofPutOperation({
					scope,
					key: key(),
					data: new Uint8Array(MAX_BATCH_SIZE / 2),
				}),
				[parent.entry],
			);
			let calls = 0;
			await expect(
				checked.collectHistory({
					target: tip.entry.hash,
					key: key(),
					resolve: async () =>
						++calls === 1
							? tip.block.bytes
							: new Uint8Array(MAX_BATCH_SIZE / 2),
				}),
			).rejectedWith("Proof byte limit");
			expect(calls).equal(2);
		});

		it("bounds aggregate edges before following another ancestor", async () => {
			const leaf = await put();
			const parent = await put(new Array(600).fill(leaf.entry));
			const tip = await put(new Array(600).fill(parent.entry));
			const calls: string[] = [];
			await expect(
				checked.collectHistory({
					target: tip.entry.hash,
					key: key(),
					resolve: async (hash) => {
						calls.push(hash);
						return hash === tip.entry.hash
							? tip.block.bytes
							: parent.block.bytes;
					},
				}),
			).rejectedWith("Proof history edge limit");
			expect(calls).deep.equal([tip.entry.hash, parent.entry.hash]);
		});

		it("forwards cancellation and stops before consuming an aborted resolution", async () => {
			const tip = await put();
			for (const alreadyAborted of [true, false]) {
				const abort = new AbortController();
				const reason = new Error("Collector cancelled");
				if (alreadyAborted) abort.abort(reason);
				let calls = 0;
				await expect(
					checked.collectHistory({
						target: tip.entry.hash,
						key: key(),
						signal: abort.signal,
						resolve: async (_, signal) => {
							calls++;
							expect(signal).equal(abort.signal);
							abort.abort(reason);
							return tip.block.bytes;
						},
					}),
				).rejectedWith("Collector cancelled");
				expect(calls).equal(alreadyAborted ? 0 : 1);
			}
		});
	});
});
