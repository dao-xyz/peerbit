import { deserialize, field, serialize, variant } from "@dao-xyz/borsh";
import { AnyBlockStore, type BlockStore } from "@peerbit/blocks";
import {
	Ed25519PublicKey,
	X25519Keypair,
	sha256Base64Sync,
} from "@peerbit/crypto";
import assert from "assert";
import { expect } from "chai";
import sodium from "libsodium-wrappers";
import { LamportClock, Timestamp } from "../src/clock.js";
import { createEntry } from "../src/entry-create.js";
import { Entry } from "../src/entry.js";
import { Payload } from "../src/payload.js";
import { signKey } from "./fixtures/privateKey.js";
import { JSON_ENCODING } from "./utils/encoding.js";

describe("entry", function () {
	let store: BlockStore;

	before(async () => {
		await sodium.ready;
		store = new AnyBlockStore();
		await store.start();
	});

	after(async () => {
		await store.stop();
	});
	describe("encoding", () => {
		@variant(0)
		class NestedEntry {
			@field({ type: Entry })
			entry: Entry<any>;

			constructor(entry: Entry<any>) {
				this.entry = entry;
			}
		}
		const create = () =>
			createEntry({
				store,
				identity: signKey,
				meta: {
					gidSeed: Buffer.from("a"),
				},
				data: new Uint8Array(10000),
			});
		it("root", async () => {
			const entry = await create();
			const bytes = serialize(entry);
			deserialize(bytes, Entry);
		});
		it("nested", async () => {
			const bytes = serialize(new NestedEntry(await create()));
			deserialize(bytes, NestedEntry);
		});
	});

	describe("create", () => {
		it("creates a an empty entry", async () => {
			const clock = new LamportClock({
				id: new Uint8Array([1, 2, 3]),
				timestamp: new Timestamp({ wallTime: 2n, logical: 3 }),
			});

			const entry = await createEntry({
				store,
				identity: signKey,

				data: new Uint8Array([1]),
				meta: {
					gidSeed: Buffer.from("a"),
					clock,
				},
			});
			expect(entry.hash).to.equal(
				"zb2rhkp7iF9qm87YVdLERWfiChsfs8FhnTEqUB426kQtq3zro",
			);
			expect(entry.meta.gid).equal(sha256Base64Sync(Buffer.from("a")));
			expect(entry.meta.clock.equals(clock)).to.be.true;
			expect(entry.payload.getValue()).to.deep.equal(new Uint8Array([1]));
			expect(entry.meta.next.length).equal(0);
			expect(entry.size).equal(215);
		});

		it("creates a entry with payload", async () => {
			const payload = new Uint8Array([1]);
			const clock = new LamportClock({
				id: new Uint8Array([1, 2, 3]),
				timestamp: new Timestamp({ wallTime: 2n, logical: 3 }),
			});
			const entry = await createEntry({
				store,
				identity: signKey,
				meta: {
					gidSeed: Buffer.from("a"),
					clock,
					next: [],
				},
				data: payload,
				encoding: JSON_ENCODING,
			});
			expect(entry.hash).to.equal(
				"zb2rhcD9YFepJetzdndygKFLjMorAQEEVremfwzxr2S8jkpwy",
			);
			expect(entry.payload.getValue()).to.deep.equal(payload);
			expect(entry.meta.gid).equal(sha256Base64Sync(Buffer.from("a")));
			expect(entry.meta.clock.equals(clock)).to.be.true;
			expect(entry.meta.next.length).equal(0);
		});

		it("creates a encrypted entry with payload", async () => {
			const payload = new Uint8Array([1]);
			const senderKey = await X25519Keypair.create();
			const receiverKey = await X25519Keypair.create();
			const entry = await createEntry({
				store,
				identity: signKey,
				meta: {
					gidSeed: Buffer.from("a"),
					next: [],
				},
				data: payload,
				encryption: {
					receiver: {
						meta: undefined,
						signatures: undefined,
						payload: receiverKey.publicKey,
					},
					keypair: senderKey,
				},
			});
			assert(entry.payload instanceof Payload);
			expect(entry.payload.getValue()).to.deep.equal(payload);

			// We can not have a hash check because nonce of encryption will always change
			expect(entry.meta.gid).equal(sha256Base64Sync(Buffer.from("a")));
			expect(entry.meta.clock.id).to.deep.equal(
				new Ed25519PublicKey({
					publicKey: signKey.publicKey.publicKey,
				}).bytes,
			);
			expect(entry.meta.clock.timestamp.logical).equal(0);
			expect(entry.meta.next.length).equal(0);
		});

		it("creates a entry with payload and next", async () => {
			const payload1 = new Uint8Array([1]);
			const payload2 = new Uint8Array([2]);
			const entry1 = await createEntry({
				store,
				identity: signKey,
				meta: {
					gidSeed: Buffer.from("a"),
					clock: new LamportClock({
						id: new Uint8Array([0]),
						timestamp: new Timestamp({ wallTime: 0n, logical: 0 }),
					}),
					next: [],
				},
				data: payload1,
			});
			const entry2 = await createEntry({
				store,
				identity: signKey,
				meta: {
					gidSeed: Buffer.from("a"),
					clock: new LamportClock({
						id: new Uint8Array([0]),
						timestamp: new Timestamp({ wallTime: 1n, logical: 0 }),
					}),
					next: [entry1],
				},
				data: payload2,
			});
			expect(entry2.payload.getValue()).to.deep.equal(payload2);
			expect(entry2.meta.next.length).equal(1);
			expect(entry2.hash).to.equal(
				"zb2rhi1Jy97WwuumkKVYWb8ZdpjLiZVzHxCivcYZcs5Qn2N4T",
			);
		});

		it("`next` parameter can be an array of strings", async () => {
			const entry1 = await createEntry({
				store,
				identity: signKey,
				meta: {
					gidSeed: Buffer.from("a"),
					next: [],
				},
				data: new Uint8Array([1]),
			});
			const entry2 = await createEntry({
				store,
				identity: signKey,
				meta: {
					gidSeed: Buffer.from("a"),
					next: [entry1],
				},
				data: new Uint8Array([2]),
			});
			assert.strictEqual(typeof entry2.meta.next[0] === "string", true);
		});

		it("`next` parameter can be an array of Entry instances", async () => {
			const entry1 = await createEntry({
				store,
				identity: signKey,
				meta: {
					gidSeed: Buffer.from("a"),
					next: [],
				},
				data: new Uint8Array([1]),
			});
			const entry2 = await createEntry({
				store,
				identity: signKey,
				meta: {
					gidSeed: Buffer.from("a"),
					next: [entry1],
				},
				data: new Uint8Array([2]),
			});
			assert.strictEqual(typeof entry2.meta.next[0] === "string", true);
		});

		it("can calculate join gid from `next` max chain length", async () => {
			const entry0A = await createEntry({
				store,
				identity: signKey,
				meta: {
					gidSeed: Buffer.from("a"),
					next: [],
				},
				data: new Uint8Array([1]),
			});

			const entry1A = await createEntry({
				store,
				identity: signKey,
				meta: {
					gidSeed: Buffer.from("a"),
					next: [entry0A],
				},
				data: new Uint8Array([1]),
			});

			const entry1B = await createEntry({
				store,
				identity: signKey,
				meta: {
					gidSeed: Buffer.from("b"),
					clock: entry1A.meta.clock,
					next: [],
				},

				data: new Uint8Array([1]),
			});

			expect(entry1A.meta.gid > entry1B.meta.gid); // so that gid is not choosen because A has smaller gid
			expect(entry1A.meta.clock.timestamp.logical).equal(
				entry1B.meta.clock.timestamp.logical,
			);

			const entry2 = await createEntry({
				store,
				identity: signKey,
				meta: {
					gidSeed: Buffer.from("Should not be used"),
					next: [entry1A, entry1B],
				},
				data: new Uint8Array([2]),
			});
			expect(entry2.meta.gid).equal(
				entry1A.meta.gid < entry1B.meta.gid
					? entry1A.meta.gid
					: entry1B.meta.gid,
			);
		});

		it("can calculate join gid from `next` max clock", async () => {
			const entry1A = await createEntry({
				store,
				identity: signKey,
				meta: {
					gidSeed: Buffer.from("b"),
					next: [],
				},
				data: new Uint8Array([1]),
			});

			const entry1B = await createEntry({
				store,
				identity: signKey,
				meta: {
					gidSeed: Buffer.from("a"),
					clock: entry1A.meta.clock.advance(),
					next: [],
				},

				data: new Uint8Array([1]),
			});

			expect(entry1B.meta.gid > entry1A.meta.gid); // so that gid is not choosen because B has smaller gid
			expect(
				entry1B.meta.clock.timestamp.compare(entry1A.meta.clock.timestamp),
			).greaterThan(0);

			const entry2 = await createEntry({
				store,
				identity: signKey,
				meta: {
					gidSeed: Buffer.from("Should not be used"),
					next: [entry1A, entry1B],
				},
				data: new Uint8Array([2]),
			});
			expect(entry2.meta.gid).equal(
				entry1A.meta.gid < entry1B.meta.gid
					? entry1A.meta.gid
					: entry1B.meta.gid,
			);
		});

		it("can calculate join gid from `next` gid comparison", async () => {
			const entry1A = await createEntry({
				store,
				identity: signKey,
				meta: {
					gidSeed: Buffer.from("a"),
					next: [],
				},
				data: new Uint8Array([1]),
			});

			const entry1B = await createEntry({
				store,
				identity: signKey,
				meta: {
					gidSeed: Buffer.from("b"),
					clock: entry1A.meta.clock,
					next: [],
				},

				data: new Uint8Array([1]),
			});

			expect(entry1B.meta.gid < entry1A.meta.gid).to.be.true; // so that B is choosen because of gid
			expect(entry1A.meta.clock.timestamp.logical).equal(
				entry1B.meta.clock.timestamp.logical,
			);

			const entry2 = await createEntry({
				store,
				identity: signKey,
				meta: {
					gidSeed: Buffer.from("Should not be used"),
					next: [entry1A, entry1B],
				},
				data: new Uint8Array([2]),
			});
			expect(entry2.meta.gid).equal(
				entry1A.meta.gid < entry1B.meta.gid
					? entry1A.meta.gid
					: entry1B.meta.gid,
			);
		});

		it("can calculate reuse gid from `next`", async () => {
			const entry1A = await createEntry({
				store,
				identity: signKey,
				meta: {
					gidSeed: Buffer.from("a"),
					next: [],
				},
				data: new Uint8Array([1]),
			});

			const entry1B = await createEntry({
				store,
				identity: signKey,
				meta: {
					gid: entry1A.meta.gid,
					next: [],
				},
				data: new Uint8Array([1]),
			});

			const entry2 = await createEntry({
				store,
				identity: signKey,
				meta: {
					gidSeed: Buffer.from("Should not be used"),
					next: [entry1A, entry1B],
				},
				data: new Uint8Array([2]),
			});
			expect(entry2.meta.gid).equal(entry1A.meta.gid);
			expect(entry1A.meta.gid).equal(entry1B.meta.gid);
		});

		it("will use next for gid instaed of gidSeed", async () => {
			const entry1 = await createEntry({
				store,
				identity: signKey,
				meta: {
					gidSeed: Buffer.from("a"),
					next: [],
				},
				data: new Uint8Array([1]),
			});

			const entry2 = await createEntry({
				store,
				identity: signKey,
				meta: {
					gidSeed: Buffer.from("Should not be used"),
					next: [entry1],
				},
				data: new Uint8Array([2]),
			});
			expect(entry2.meta.gid).equal(entry1.meta.gid);
		});

		it("throws an error if data is not defined", async () => {
			let err: any;
			try {
				await createEntry({
					store,
					identity: signKey,
					meta: {
						gidSeed: Buffer.from("a"),
						next: [],
					},
					data: null,
				});
			} catch (e: any) {
				err = e;
			}
			expect(err.message).equal("Entry requires data");
		});

		it("throws an error if next is not an array", async () => {
			let err: any;
			try {
				await createEntry({
					store,
					identity: signKey,
					meta: {
						gidSeed: Buffer.from("a"),
						next: {} as any,
					},
					data: new Uint8Array([1]),
				});
			} catch (e: any) {
				err = e;
			}
			expect(err.message).equal("'next' argument is not an array");
		});
	});

	describe("toMultihash", () => {
		it("returns an multihash", async () => {
			const entry = await createEntry({
				store,
				identity: signKey,
				meta: {
					gidSeed: Buffer.from("a"),
					clock: new LamportClock({
						id: new Uint8Array([1, 2, 3]),
						timestamp: new Timestamp({ wallTime: 2n, logical: 3 }),
					}),
					next: [],
				},
				data: new Uint8Array([1]),
			});
			const hash = entry.hash;
			entry.hash = undefined as any;
			const multihash = await Entry.toMultihash(store, entry);
			expect(multihash).equal(hash);
			expect(multihash).to.equal(
				"zb2rhkp7iF9qm87YVdLERWfiChsfs8FhnTEqUB426kQtq3zro",
			);
		});

		/*  TODO what is the point of this test?
    
	it('throws an error if the object being passed is invalid', async () => {
	  let err
	  try {
		const entry = await createEntry({ store, identity: signKey, gidSeed:   'A', data: 'hello', next: [] })
		delete ((entry.metadata as MetadataSecure)._metadata as DecryptedThing<Metadata>)
		await Entry.toMultihash(store, entry)
	  } catch (e: any) {
		err = e
	  }
	  expect(err.message).equal('Invalid object format, cannot generate entry hash')
	}) */
	});

	describe("fromMultihash", () => {
		it("creates a entry from hash", async () => {
			const payload1 = new Uint8Array([1]);
			const payload2 = new Uint8Array([2]);
			const entry1 = await createEntry({
				store,
				identity: signKey,
				meta: {
					gidSeed: Buffer.from("a"),
					clock: new LamportClock({
						id: new Uint8Array([1, 2, 3]),
						timestamp: new Timestamp({ wallTime: 2n, logical: 3 }),
					}),
					next: [],
				},
				data: payload1,
			});
			const entry2 = await createEntry({
				store,
				identity: signKey,
				meta: {
					gidSeed: Buffer.from("a"),
					clock: new LamportClock({
						id: new Uint8Array([1, 2, 3]),
						timestamp: new Timestamp({ wallTime: 3n, logical: 3 }),
					}),
					next: [entry1],
				},
				data: payload2,
			});
			const final = await Entry.fromMultihash<Uint8Array>(store, entry2.hash);
			final.init(entry2);
			assert(final.equals(entry2));
			expect(final.meta.gid).equal(sha256Base64Sync(Buffer.from("a")));
			expect(final.payload.getValue()).to.deep.equal(payload2);
			expect(final.meta.next.length).equal(1);
			expect(final.meta.next[0]).equal(entry1.hash);
			expect(final.hash).to.equal(
				"zb2rhcw32voNHstGRjjRE4X6Rb2oKS4tGtqzogERKvDKQf9iw",
			);
		});
	});

	describe("isParent", () => {
		it("returns true if entry has a child", async () => {
			const payload1 = new Uint8Array([1]);
			const payload2 = new Uint8Array([2]);
			const entry1 = await createEntry({
				store,
				identity: signKey,
				meta: {
					gidSeed: Buffer.from("a"),
					next: [],
				},
				data: payload1,
			});
			const entry2 = await createEntry({
				store,
				identity: signKey,
				meta: {
					gidSeed: Buffer.from("a"),
					next: [entry1],
				},
				data: payload2,
			});
			expect(Entry.isDirectParent(entry1, entry2)).equal(true);
		});

		it("returns false if entry does not have a child", async () => {
			const payload1 = new Uint8Array([1]);
			const payload2 = new Uint8Array([2]);
			const entry1 = await createEntry({
				store,
				identity: signKey,
				meta: {
					gidSeed: Buffer.from("a"),
					next: [],
				},
				data: payload1,
			});
			const entry2 = await createEntry({
				store,
				identity: signKey,
				meta: {
					gidSeed: Buffer.from("a"),
					next: [],
				},
				data: payload2,
			});
			const entry3 = await createEntry({
				store,
				identity: signKey,
				meta: {
					gidSeed: Buffer.from("a"),
					next: [entry2],
				},
				data: payload2,
			});
			expect(Entry.isDirectParent(entry1, entry2)).equal(false);
			expect(Entry.isDirectParent(entry1, entry1)).equal(false);
			expect(Entry.isDirectParent(entry2, entry3)).equal(true);
		});
	});

	describe("compare", () => {
		it("returns true if entries are the same", async () => {
			const payload1 = new Uint8Array([1]);
			const entry1 = await createEntry({
				store,
				identity: signKey,
				meta: {
					gidSeed: Buffer.from("a"),
					clock: new LamportClock({
						id: new Uint8Array([1]),
						timestamp: new Timestamp({ wallTime: 3n, logical: 2 }),
					}),
					next: [],
				},
				data: payload1,
			});
			const entry2 = await createEntry({
				store,
				identity: signKey,
				meta: {
					gidSeed: Buffer.from("a"),
					clock: new LamportClock({
						id: new Uint8Array([1]),
						timestamp: new Timestamp({ wallTime: 3n, logical: 2 }),
					}),
					next: [],
				},
				data: payload1,
			});
			expect(Entry.isEqual(entry1, entry2)).equal(true);
		});

		it("returns true if entries are not the same", async () => {
			const payload1 = new Uint8Array([0]);
			const payload2 = new Uint8Array([1]);
			const entry1 = await createEntry({
				store,
				identity: signKey,
				meta: {
					gidSeed: Buffer.from("a"),
					next: [],
				},
				data: payload1,
			});
			const entry2 = await createEntry({
				store,
				identity: signKey,
				meta: {
					gidSeed: Buffer.from("a"),
					next: [],
				},
				data: payload2,
			});
			expect(Entry.isEqual(entry1, entry2)).equal(false);
		});
	});

	describe("verifySignatures", () => {
		it("verifies", async () => {
			const entry1 = await createEntry({
				store,
				identity: signKey,
				data: new Uint8Array(0),
			});
			entry1.createdLocally = false; //

			expect(await entry1.verifySignatures()).to.be.true;
			entry1.signatures[0].signature = new Uint8Array(
				entry1.signatures[0].signature.length,
			);
			expect(await entry1.verifySignatures()).to.be.false;
		});
	});
});
