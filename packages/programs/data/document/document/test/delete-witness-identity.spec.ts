import { deserialize, serialize } from "@dao-xyz/borsh";
import {
	DecryptedThing,
	Ed25519Keypair,
	PreHash,
	type PublicSignKey,
	Secp256k1Keypair,
	SignatureWithKey,
} from "@peerbit/crypto";
import {
	Entry,
	EntryV0,
	Timestamp,
	calculateRawCidV1Batch,
	createEntry,
} from "@peerbit/log";
import { TestSession } from "@peerbit/test-utils";
import { expect } from "chai";
import { compare, equals } from "uint8arrays";
import {
	BORSH_ENCODING_OPERATION,
	Documents,
	type Operation,
	PutOperation,
	policy,
} from "../src/index.js";
import { Document, TestStore } from "./data.js";
import {
	assertSupportedDeleteWitnessSignature,
	authenticateCanonicalDeleteWitness as authenticate,
} from "./helpers/delete-history.js";

type Block = { cid: string; bytes: Uint8Array };

// Characterizes physical-CID aliases, not production retirement. The private
// profile must govern ordinary admission AND evidence; generic owner inclusion
// and canonical outer Borsh alone do not give a unique signed-entry identity.
describe("original-byte delete witness identity", () => {
	let session: TestSession;
	let store: TestStore;
	let original: Block;
	let attacker: Ed25519Keypair;

	const decode = (block: Block): EntryV0<Operation> => {
		const entry = deserialize(block.bytes, Entry) as EntryV0<Operation>;
		expect(entry).instanceOf(EntryV0);
		return entry.init({ encoding: BORSH_ENCODING_OPERATION });
	};
	const encode = async (entry: Entry<Operation>): Promise<Block> => {
		const bytes = serialize(entry);
		const [cid] = await calculateRawCidV1Batch([bytes]);
		return { cid, bytes };
	};
	const blockOf = async (entry: Entry<Operation>): Promise<Block> => {
		const bytes = await store.docs.log.log.blocks.get(entry.hash, {
			remote: false,
		});
		if (!bytes) throw new Error("Missing original fixture bytes");
		return { cid: entry.hash, bytes: new Uint8Array(bytes) };
	};
	const wrap = (signature: SignatureWithKey) =>
		new DecryptedThing<SignatureWithKey>({ data: serialize(signature) });
	const acceptsAlias = async (
		base: Block,
		alias: Block,
		owner: PublicSignKey = session.peers[0].identity.publicKey,
	) => {
		expect(alias.cid).not.equal(base.cid);
		expect(
			equals(serialize(deserialize(alias.bytes, Entry)), alias.bytes),
		).equal(true, "outer Borsh roundtrip is canonical");
		const entry = await authenticate(alias);
		expect(
			equals(entry.getSignableBytes(), decode(base).getSignableBytes()),
		).equal(true, "the owner's signed bytes did not change");
		expect(await entry.verifySignatures()).equal(true);
		const operation = await entry.getPayloadValue();
		expect(operation).instanceOf(PutOperation);
		expect(
			await policy.signedByPublicKey(owner)({
				type: "put",
				value: deserialize((operation as PutOperation).data, Document),
				operation: operation as PutOperation,
				entry: entry as unknown as Entry<PutOperation>,
			}),
		).equal(true, "general owner-inclusion policy accepts the alias");
		return entry;
	};

	before(async () => {
		session = await TestSession.disconnected(1);
		store = await session.peers[0].open(
			new TestStore({ docs: new Documents<Document>() }),
			{ args: { mode: "compat", replicate: false, keep: "self" } },
		);
		const { entry } = await store.docs.put(
			new Document({
				id: "victim",
				name: "original",
				data: session.peers[0].identity.publicKey.bytes,
			}),
			{
				target: "none",
				unique: true,
				meta: { next: [], timestamp: new Timestamp({ wallTime: 1n }) },
			},
		);
		original = await blockOf(entry);
		attacker = await Ed25519Keypair.create();
	});
	after(async () => session?.stop());

	it("accepts an unchanged ordinary Documents put under the private profile", async () => {
		const entry = await authenticate(original);
		expect(() =>
			assertSupportedDeleteWitnessSignature(
				entry,
				session.peers[0].identity.publicKey.bytes,
			),
		).not.throw();
	});

	it("rejects the otherwise valid duplicate-signature CID alias", async () => {
		const entry = decode(original);
		entry._signatures!.signatures.push(entry._signatures!.signatures[0]);
		const alias = await acceptsAlias(original, await encode(entry));
		expect(() =>
			assertSupportedDeleteWitnessSignature(
				alias,
				session.peers[0].identity.publicKey.bytes,
			),
		).throw("Expected one plaintext Ed25519 owner signature");
	});

	it("rejects a sorted countersignature alias without requiring the owner's key", async () => {
		const entry = decode(original);
		entry._signatures!.signatures.push(
			wrap(await attacker.sign(entry.getSignableBytes())),
		);
		entry._signatures!.signatures.sort((left, right) =>
			compare(
				left.decrypted.getValue(SignatureWithKey).signature,
				right.decrypted.getValue(SignatureWithKey).signature,
			),
		);
		const alias = await acceptsAlias(original, await encode(entry));
		expect(() =>
			assertSupportedDeleteWitnessSignature(
				alias,
				session.peers[0].identity.publicKey.bytes,
			),
		).throw("Expected one plaintext Ed25519 owner signature");
	});

	it("rejects both orders of an otherwise valid multisignature entry", async () => {
		const entry = decode(original);
		entry._signatures!.signatures.push(
			wrap(await attacker.sign(entry.getSignableBytes())),
		);
		const forward = await encode(entry);
		entry._signatures!.signatures.reverse();
		const reverse = await encode(entry);
		expect(forward.cid).not.equal(reverse.cid);
		for (const block of [forward, reverse]) {
			const alias = await acceptsAlias(original, block);
			expect(() =>
				assertSupportedDeleteWitnessSignature(
					alias,
					session.peers[0].identity.publicKey.bytes,
				),
			).throw("Expected one plaintext Ed25519 owner signature");
		}
	});

	it("rejects a one-signer secp256k1 alias made only by changing hex letter case", async () => {
		const owner = await Secp256k1Keypair.create();
		const entry = await createEntry({
			store: store.docs.log.log.blocks,
			identity: owner,
			encoding: BORSH_ENCODING_OPERATION,
			data: new PutOperation({
				data: serialize(
					new Document({
						id: "secp",
						name: "original",
						data: owner.publicKey.bytes,
					}),
				),
			}),
		});
		const base = await blockOf(entry);
		const copy = decode(base);
		const signature = (await copy.getSignatures())[0];
		const hex = new TextDecoder().decode(signature.signature);
		signature.signature = new TextEncoder().encode(
			`0x${hex.slice(2).toUpperCase()}`,
		);
		copy._signatures!.signatures = [wrap(signature)];
		const alias = await acceptsAlias(base, await encode(copy), owner.publicKey);
		expect((await alias.getSignatures()).length).equal(1);
		expect(() =>
			assertSupportedDeleteWitnessSignature(alias, owner.publicKey.bytes),
		).throw("Expected canonical Ed25519 owner signature without prehash");
	});

	it("rejects a valid owner-produced prehashed signature outside the profile", async () => {
		const entry = decode(original);
		entry._signatures!.signatures = [
			wrap(
				await session.peers[0].identity.sign(
					entry.getSignableBytes(),
					PreHash.SHA_256,
				),
			),
		];
		const alias = await acceptsAlias(original, await encode(entry));
		expect(() =>
			assertSupportedDeleteWitnessSignature(
				alias,
				session.peers[0].identity.publicKey.bytes,
			),
		).throw("Expected canonical Ed25519 owner signature without prehash");
	});

	it("rejects trailing bytes inside the unsigned nested signature envelope", async () => {
		const entry = decode(original);
		const bytes = entry._signatures!.signatures[0].decrypted._data!;
		const extended = new Uint8Array(bytes.length + 1);
		extended.set(bytes);
		entry._signatures!.signatures = [new DecryptedThing({ data: extended })];
		const alias = await encode(entry);
		expect(
			equals(entry.getSignableBytes(), decode(original).getSignableBytes()),
		).equal(true);
		await expect(authenticate(alias)).rejectedWith(
			"Unexpected 1 bytes after deserialized data",
		);
	});

	it("rejects noncanonical Ed25519 S plus the group order", async () => {
		const entry = decode(original);
		const signature = (await entry.getSignatures())[0];
		const bytes = new Uint8Array(signature.signature);
		const order = (1n << 252n) + 27742317777372353535851937790883648493n;
		let scalar = 0n;
		for (let i = 63; i >= 32; i--) scalar = (scalar << 8n) + BigInt(bytes[i]);
		scalar += order;
		for (let i = 32; i < 64; i++) {
			bytes[i] = Number(scalar & 255n);
			scalar >>= 8n;
		}
		signature.signature = bytes;
		entry._signatures!.signatures = [wrap(signature)];
		await expect(authenticate(await encode(entry))).rejectedWith(
			"Invalid signature",
		);
	});

	for (const field of ["_meta", "_payload"] as const) {
		it(`authenticates raw nested ${field} bytes, not just their outer wrapper`, async () => {
			const entry = decode(original);
			const data = entry[field].decrypted._data!;
			const extended = new Uint8Array(data.length + 1);
			extended.set(data);
			entry[field].decrypted._data = extended;
			expect(
				equals(entry.getSignableBytes(), decode(original).getSignableBytes()),
			).equal(false);
			await expect(authenticate(await encode(entry))).rejectedWith(
				"Invalid signature",
			);
		});
	}

	it("does not encode size, createdLocally, or decoded metadata caches into another CID", async () => {
		const entry = decode(original);
		entry.size = 123;
		entry.createdLocally = true;
		const originalGid = entry.meta.gid;
		entry.meta.gid = "unserialized local cache";
		const unchanged = await encode(entry);
		expect(unchanged.cid).equal(original.cid);
		const authenticated = await authenticate(unchanged);
		expect(authenticated.meta.gid).equal(originalGid);
		expect(authenticated.createdLocally).not.equal(true);
		expect(() => authenticated.size).throw("Size not set");
	});
});
