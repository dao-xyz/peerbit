import {
	deserialize,
	field,
	fixedArray,
	serialize,
	variant,
} from "@dao-xyz/borsh";
import {
	Ed25519Keypair,
	Ed25519PublicKey,
	PublicSignKey,
	sha256Sync,
} from "@peerbit/crypto";
import {
	IdKey,
	StringKey,
	Uint8ArrayKey,
	toId,
} from "@peerbit/indexer-interface";
import {
	BORSH_ENCODING,
	Entry,
	EntryType,
	NO_ENCODING,
	createEntry,
} from "@peerbit/log";
import { TestSession } from "@peerbit/test-utils";
import { expect } from "chai";
import { equals } from "uint8arrays";
import { Operation, PutOperation } from "../src/operation.js";
import {
	assertSupportedDeleteWitnessSignature,
	authenticateCanonicalDeleteWitness,
} from "./helpers/delete-history.js";

// Private codec contract, NOT production variants, manifest dispatch, projection,
// history retirement or durability. This tree never extends public Operation.
// The key is protocol identity; application decoding is only an admission check.
@variant("private-proof-scope-fixture")
class Scope {
	@field({ type: "u8" })
	version = 1;

	@field({ type: fixedArray("u8", 32) })
	nonce: Uint8Array;

	@field({ type: Uint8Array })
	owner: Uint8Array;

	// One kind avoids current backend primitive aliases; these are fixture tags.
	@field({ type: "u8" })
	keyKind: number;

	constructor(owner: Uint8Array, nonce = new Uint8Array(32), keyKind = 0) {
		this.owner = new Uint8Array(owner);
		this.nonce = new Uint8Array(nonce);
		this.keyKind = keyKind;
	}
}

@variant("private-proof-operation-fixture")
class ScopedOperation {}

@variant(0)
class ScopedPut extends ScopedOperation {
	@field({ type: fixedArray("u8", 32) })
	scope: Uint8Array;

	@field({ type: IdKey })
	key: IdKey;

	@field({ type: Uint8Array })
	data: Uint8Array;

	constructor(scope: Uint8Array, key: IdKey, data: Uint8Array) {
		super();
		this.scope = scope;
		this.key = key;
		this.data = data;
	}
}

@variant(1)
class ScopedDelete extends ScopedOperation {
	@field({ type: fixedArray("u8", 32) })
	scope: Uint8Array;

	@field({ type: IdKey })
	key: IdKey;

	@field({ type: "string" })
	target: string;

	@field({ type: Uint8Array })
	original: Uint8Array;

	constructor(scope: Uint8Array, key: IdKey, target: Block) {
		super();
		this.scope = scope;
		this.key = key;
		this.target = target.cid;
		this.original = target.bytes;
	}
}

type Block = { cid: string; bytes: Uint8Array };
const encoding = BORSH_ENCODING(ScopedOperation);
const sameKey = (a: IdKey, b: IdKey) => equals(serialize(a), serialize(b));
const requireFact = (fact: unknown, message: string): void => {
	if (!fact) throw new Error(message);
};

const verifier = (input: Scope) => {
	// Capture once: mutable caller objects cannot change a store's policy/scope.
	const manifest = deserialize(serialize(input), Scope);
	requireFact(manifest.version === 1, "Unsupported profile");
	const owner = deserialize(manifest.owner, PublicSignKey);
	requireFact(
		owner instanceof Ed25519PublicKey &&
			equals(serialize(owner), manifest.owner),
		"Unsupported owner encoding",
	);
	requireFact(
		manifest.keyKind === 0 || manifest.keyKind === 1,
		"Unsupported key kind",
	);
	const scope = sha256Sync(serialize(manifest));
	const checkKey = (key: IdKey) =>
		requireFact(
			manifest.keyKind === 0
				? key instanceof StringKey
				: key instanceof Uint8ArrayKey,
			"Wrong key kind",
		);
	const authenticate = async (block: Block) => {
		const entry = await authenticateCanonicalDeleteWitness(block);
		assertSupportedDeleteWitnessSignature(entry, manifest.owner);
		entry.init({ encoding: encoding as any });
		const operation = (await entry.getPayloadValue()) as unknown as
			| ScopedPut
			| ScopedDelete;
		requireFact(
			equals(serialize(operation), entry.payload.data),
			"Noncanonical operation",
		);
		requireFact(equals(operation.scope, scope), "Wrong store scope");
		checkKey(operation.key);
		return { entry, operation };
	};
	const witness = async (block: Block) => {
		const result = await authenticate(block);
		requireFact(
			result.entry.meta.type === EntryType.APPEND &&
				result.operation instanceof ScopedPut,
			"Expected scoped put",
		);
		return { entry: result.entry, operation: result.operation as ScopedPut };
	};
	return {
		scope: new Uint8Array(scope),
		witness,
		async admit(block: Block, decodeId: (bytes: Uint8Array) => IdKey) {
			const result = await witness(block);
			// Never let a local decoder relabel protocol identity.
			requireFact(
				sameKey(result.operation.key, decodeId(result.operation.data)),
				"Decoded key mismatch",
			);
			return result;
		},
		async deletion(block: Block) {
			const result = await authenticate(block);
			requireFact(
				result.entry.meta.type === EntryType.CUT &&
					result.operation instanceof ScopedDelete,
				"Expected scoped delete",
			);
			const operation = result.operation as ScopedDelete;
			const target = await witness({
				cid: operation.target,
				bytes: operation.original,
			});
			requireFact(
				result.entry.meta.next.length === 1 &&
					result.entry.meta.next[0] === target.entry.hash,
				"Wrong exact target",
			);
			requireFact(
				sameKey(operation.key, target.operation.key),
				"Wrong target key",
			);
			return target;
		},
	};
};

describe("proof-backed scoped operation codec (not production admission)", () => {
	let session: TestSession;
	let owner: Ed25519Keypair;
	let other: Ed25519Keypair;
	let manifest: Scope;
	let checked: ReturnType<typeof verifier>;
	let original: Block;
	const key = () => new StringKey("AQID");
	const data = new TextEncoder().encode("application payload");
	const encode = async (entry: Entry<any>): Promise<Block> => {
		const bytes = await session.peers[0].services.blocks.get(entry.hash, {
			remote: false,
		});
		if (!bytes) throw new Error("Missing original bytes");
		return { cid: entry.hash, bytes: new Uint8Array(bytes) };
	};
	const make = async (
		operation: ScopedPut | ScopedDelete,
		identity = owner,
	) => {
		const next =
			operation instanceof ScopedDelete
				? [
						await Entry.fromMultihash(
							session.peers[0].services.blocks,
							operation.target,
						),
					]
				: [];
		const entry = await createEntry({
			store: session.peers[0].services.blocks,
			identity,
			encoding,
			data: operation,
			meta: {
				type:
					operation instanceof ScopedDelete ? EntryType.CUT : EntryType.APPEND,
				next,
			},
		});
		return encode(entry);
	};
	before(async () => {
		session = await TestSession.disconnected(1);
		owner = await Ed25519Keypair.create();
		other = await Ed25519Keypair.create();
		manifest = new Scope(owner.publicKey.bytes);
		checked = verifier(manifest);
		original = await make(new ScopedPut(checked.scope, key(), data));
	});
	after(async () => session?.stop());

	it("binds owner, generation and one typed-key kind into immutable scope", () => {
		for (const changed of [
			new Scope(other.publicKey.bytes),
			new Scope(owner.publicKey.bytes, new Uint8Array(32).fill(1)),
			new Scope(owner.publicKey.bytes, new Uint8Array(32), 1),
		])
			expect(equals(verifier(changed).scope, checked.scope)).to.be.false;
		const unsupported = new Scope(owner.publicKey.bytes);
		unsupported.version = 2;
		expect(() => verifier(unsupported)).to.throw("Unsupported profile");
	});

	it("preserves original signed bytes and uses exact signed key for a delete", async () => {
		const admitted = await checked.admit(original, key);
		expect(admitted.entry.hash).equal(original.cid);
		const deletion = await make(
			new ScopedDelete(checked.scope, key(), original),
		);
		const target = await checked.deletion(deletion);
		expect(target.entry.hash).equal(original.cid);
		expect(sameKey(target.operation.key, key())).to.be.true;
		expect(equals((await checked.witness(original)).operation.data, data)).to.be
			.true;
	});

	it("denies a mismatched local decoder without changing witnessed identity", async () => {
		await expect(
			checked.admit(original, () => new StringKey("different")),
		).rejectedWith("Decoded key mismatch");
		const deletion = await make(
			new ScopedDelete(checked.scope, key(), original),
		);
		// Witness verification does not call any application decoder or claim this
		// receiver historically admitted the target under its chosen local type.
		expect((await checked.deletion(deletion)).entry.hash).equal(original.cid);
	});

	it("rejects primitive-key aliases instead of crossing document identities", async () => {
		const bytesKey = new Uint8ArrayKey(Uint8Array.of(1, 2, 3));
		expect(bytesKey.primitive).equal(key().primitive);
		expect(sameKey(bytesKey, key())).to.be.false;
		await expect(checked.admit(original, () => bytesKey)).rejectedWith(
			"Decoded key mismatch",
		);
		await expect(
			checked.witness(await make(new ScopedPut(checked.scope, bytesKey, data))),
		).rejectedWith("Wrong key kind");
		await expect(
			checked.deletion(
				await make(new ScopedDelete(checked.scope, bytesKey, original)),
			),
		).rejectedWith("Wrong key kind");
	});

	it("supports a binary-only store without coercing its keys to strings", async () => {
		const binary = verifier(
			new Scope(owner.publicKey.bytes, new Uint8Array(32), 1),
		);
		const id = () => new Uint8ArrayKey(Uint8Array.of(1, 2, 3));
		const target = await make(new ScopedPut(binary.scope, id(), data));
		expect((await binary.admit(target, id)).entry.hash).equal(target.cid);
		const deletion = await make(new ScopedDelete(binary.scope, id(), target));
		expect((await binary.deletion(deletion)).entry.hash).equal(target.cid);
		await expect(binary.admit(target, key)).rejectedWith(
			"Decoded key mismatch",
		);
	});

	it("rejects a same-owner foreign-store put as ordinary input and delete witness", async () => {
		const foreign = verifier(
			new Scope(owner.publicKey.bytes, new Uint8Array(32).fill(2)),
		);
		const target = await make(new ScopedPut(foreign.scope, key(), data));
		await expect(checked.admit(target, key)).rejectedWith("Wrong store scope");
		await expect(
			checked.deletion(
				await make(new ScopedDelete(checked.scope, key(), target)),
			),
		).rejectedWith("Wrong store scope");
		await expect(
			checked.deletion(
				await make(new ScopedDelete(foreign.scope, key(), original)),
			),
		).rejectedWith("Wrong store scope");
	});

	it("enforces the same immutable owner on ordinary puts, witnesses and deletes", async () => {
		const target = await make(new ScopedPut(checked.scope, key(), data), other);
		await expect(checked.admit(target, key)).rejectedWith(
			"Expected canonical Ed25519 owner signature without prehash",
		);
		await expect(
			checked.deletion(
				await make(new ScopedDelete(checked.scope, key(), target)),
			),
		).rejectedWith(
			"Expected canonical Ed25519 owner signature without prehash",
		);
		await expect(
			checked.deletion(
				await make(new ScopedDelete(checked.scope, key(), original), other),
			),
		).rejectedWith(
			"Expected canonical Ed25519 owner signature without prehash",
		);
	});

	it("rejects relabeled target keys and corrupted original witness bytes", async () => {
		await expect(
			checked.deletion(
				await make(
					new ScopedDelete(checked.scope, new StringKey("other"), original),
				),
			),
		).rejectedWith("Wrong target key");
		const corrupt = {
			cid: original.cid,
			bytes: new Uint8Array(original.bytes),
		};
		corrupt.bytes[corrupt.bytes.length - 1] ^= 1;
		await expect(
			checked.deletion(
				await make(new ScopedDelete(checked.scope, key(), corrupt)),
			),
		).rejectedWith("CID mismatch");
	});

	it("requires the delete's signed causal link to match its exact witnessed target", async () => {
		const entry = await createEntry({
			store: session.peers[0].services.blocks,
			identity: owner,
			encoding,
			data: new ScopedDelete(checked.scope, key(), original),
			meta: { type: EntryType.CUT, next: [] },
		});
		await expect(checked.deletion(await encode(entry))).rejectedWith(
			"Wrong exact target",
		);
	});

	it("rejects correctly signed operations with the wrong log entry type", async () => {
		for (const operation of [
			new ScopedPut(checked.scope, key(), data),
			new ScopedDelete(checked.scope, key(), original),
		]) {
			const put = operation instanceof ScopedPut;
			const entry = await createEntry({
				store: session.peers[0].services.blocks,
				identity: owner,
				encoding,
				data: operation,
				meta: { type: put ? EntryType.CUT : EntryType.APPEND, next: [] },
			});
			await expect(
				put
					? checked.witness(await encode(entry))
					: checked.deletion(await encode(entry)),
			).rejectedWith(put ? "Expected scoped put" : "Expected scoped delete");
		}
	});

	it("owns manifest and evidence bytes across asynchronous verification", async () => {
		const input = new Scope(owner.publicKey.bytes);
		const captured = verifier(input);
		input.owner.fill(0);
		input.nonce.fill(1);
		input.keyKind = 1;
		captured.scope.fill(1);
		const block = { cid: original.cid, bytes: new Uint8Array(original.bytes) };
		const pending = captured.admit(block, key);
		block.cid = "relabeled";
		block.bytes.fill(0);
		expect((await pending).entry.hash).equal(original.cid);
	});

	it("rejects noncanonical scoped payload bytes even when correctly signed", async () => {
		const bytes = serialize(new ScopedPut(checked.scope, key(), data));
		const trailing = new Uint8Array(bytes.length + 1);
		trailing.set(bytes);
		const entry = await createEntry({
			store: session.peers[0].services.blocks,
			identity: owner,
			encoding: NO_ENCODING,
			data: trailing,
			meta: { type: EntryType.APPEND, next: [] },
		});
		await expect(checked.witness(await encode(entry))).to.be.rejected;
	});

	it("does not register new operations in the legacy hierarchy or import unscoped puts", async () => {
		const legacy = new PutOperation({ data });
		expect(deserialize(serialize(legacy), Operation)).instanceOf(PutOperation);
		expect(() =>
			deserialize(
				serialize(new ScopedPut(checked.scope, key(), data)),
				Operation,
			),
		).to.throw();
		const entry = await createEntry({
			store: session.peers[0].services.blocks,
			identity: owner,
			encoding: BORSH_ENCODING(Operation),
			data: legacy,
			meta: { type: EntryType.APPEND, next: [] },
		});
		await expect(checked.witness(await encode(entry))).to.be.rejected;
		expect(sameKey(toId("AQID"), key())).to.be.true;
	});
});
