import { deserialize, field, serialize, variant } from "@dao-xyz/borsh";
import { AnyBlockStore, type BlockStore } from "@peerbit/blocks";
import {
	DecryptedThing,
	Ed25519PublicKey,
	X25519Keypair,
	sha256Base64Sync,
} from "@peerbit/crypto";
import assert from "assert";
import { expect } from "chai";
import sodium from "libsodium-wrappers";
import { LamportClock, Timestamp } from "../src/clock.js";
import { createEntry } from "../src/entry-create.js";
import {
	EntryV0,
	Signatures,
	verifyEntryV0Ed25519BatchFromEntries,
} from "../src/entry-v0.js";
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

		it("reuses canonical signable bytes", async () => {
			const entry = await create();
			entry.hash = undefined as any;
			expect(Array.from(entry.getSignableBytes())).to.deep.equal(
				Array.from(serialize(EntryV0.toSignable(entry as EntryV0<any>))),
			);
		});

		it("reuses canonical storage bytes", async () => {
			const entry = await create();
			const hash = entry.hash;
			entry.hash = undefined as any;
			expect(Array.from(entry.getStorageBytes())).to.deep.equal(
				Array.from(serialize(entry)),
			);
			entry.hash = hash;
		});
	});

	describe("EntryV0 reserved bytes", () => {
		const source = async (): Promise<EntryV0<any>> =>
			(await createEntry({
				store,
				identity: signKey,
				data: new Uint8Array([1]),
			})) as EntryV0<any>;

		const instantiate = (
			entry: EntryV0<any>,
			reserved?: unknown,
		): EntryV0<any> =>
			new EntryV0({
				meta: entry._meta,
				payload: entry._payload,
				reserved: reserved as Uint8Array | undefined,
			});

		it("defaults every entry to fresh zeroed reserved bytes", async () => {
			const entry = await source();
			const first = instantiate(entry);
			const second = instantiate(entry);

			expect(Array.from(first._reserved!)).to.deep.equal([0, 0, 0, 0]);
			expect(Array.from(second._reserved!)).to.deep.equal([0, 0, 0, 0]);
			expect(first._reserved).not.to.equal(second._reserved);
		});

		it("preserves nonzero bytes without retaining the caller's view", async () => {
			const entry = await source();
			const backing = new Uint8Array([9, 1, 2, 3, 4, 9]);
			const reserved = backing.subarray(1, 5);
			const withReserved = instantiate(entry, reserved);

			expect(Array.from(withReserved._reserved!)).to.deep.equal([1, 2, 3, 4]);
			expect(withReserved._reserved).not.to.equal(reserved);
			backing.fill(9);
			expect(Array.from(withReserved._reserved!)).to.deep.equal([1, 2, 3, 4]);
		});

		it("copies cross-realm and hostile Uint8Array subclasses intrinsically", async () => {
			const entry = await source();
			let byteLengthCalls = 0;
			let iteratorCalls = 0;
			let speciesCalls = 0;
			class HostileReserved extends Uint8Array {
				static get [Symbol.species]() {
					speciesCalls++;
					throw new Error("species must not run");
				}

				override [Symbol.iterator](): ArrayIterator<number> {
					iteratorCalls++;
					throw new Error("iterator must not run");
				}
			}
			const hostile = new HostileReserved([4, 3, 2, 1]);
			Object.defineProperty(hostile, "byteLength", {
				get: () => {
					byteLengthCalls++;
					return 1;
				},
			});
			const hostileCopy = instantiate(entry, hostile);
			expect(Array.from(hostileCopy._reserved!)).to.deep.equal([4, 3, 2, 1]);
			expect(byteLengthCalls).to.equal(0);
			expect(iteratorCalls).to.equal(0);
			expect(speciesCalls).to.equal(0);

			const { runInNewContext } = await import("node:vm");
			const crossRealm = runInNewContext(
				"new Uint8Array([5, 6, 7, 8])",
			) as Uint8Array;
			const crossRealmCopy = instantiate(entry, crossRealm);
			crossRealm.fill(0);
			expect(Array.from(crossRealmCopy._reserved!)).to.deep.equal([5, 6, 7, 8]);
		});

		it("rejects malformed, forged, proxied, and detached views", async () => {
			const entry = await source();
			for (const malformed of [
				new Uint8Array(0),
				new Uint8Array(3),
				new Uint8Array(5),
				new Uint16Array(2),
				new DataView(new ArrayBuffer(4)),
				{ byteLength: 4, 0: 0, 1: 0, 2: 0, 3: 0 },
				new Proxy(new Uint8Array(4), {}),
			]) {
				expect(() => instantiate(entry, malformed)).to.throw();
			}

			const shadowed = new Uint8Array(3);
			Object.defineProperty(shadowed, "byteLength", { value: 4 });
			expect(() => instantiate(entry, shadowed)).to.throw(
				RangeError,
				"exactly 4 bytes",
			);

			const detached = new Uint8Array(4);
			structuredClone(detached.buffer, { transfer: [detached.buffer] });
			expect(() => instantiate(entry, detached)).to.throw(
				TypeError,
				"must not be detached",
			);
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

		it("covers the reserved bytes", async () => {
			const entry = (await createEntry({
				store,
				identity: signKey,
				data: new Uint8Array([1]),
			})) as EntryV0<Uint8Array>;

			expect(await entry.verifySignatures()).to.be.true;
			entry._reserved![0] ^= 1;
			expect(await entry.verifySignatures()).to.be.false;
		});

		it("falls back when zero-only native fields cannot represent reserved bytes", async () => {
			const zero = (await createEntry({
				store,
				identity: signKey,
				data: new Uint8Array([1]),
				deferStore: true,
			})) as EntryV0<Uint8Array>;
			expect(Entry.takePreparedBlock(zero)).to.exist;
			expect(Entry.getPreparedStorageBytes(zero)).to.equal(undefined);
			expect(await verifyEntryV0Ed25519BatchFromEntries([zero])).to.deep.equal([
				true,
			]);

			zero._reserved![0] = 1;
			expect(await zero.verifySignatures()).to.equal(false);
			expect(await verifyEntryV0Ed25519BatchFromEntries([zero])).to.equal(
				undefined,
			);

			const reserved = new EntryV0<Uint8Array>({
				meta: zero._meta,
				payload: zero._payload,
				reserved: new Uint8Array([1, 2, 3, 4]),
			});
			const signature = await signKey.sign(reserved.getSignableBytes());
			reserved._signatures = new Signatures({
				signatures: [
					new DecryptedThing({
						data: serialize(signature),
						value: signature,
					}),
				],
			});
			expect(await reserved.verifySignatures()).to.equal(true);
			expect(Entry.getPreparedStorageBytes(reserved)).to.equal(undefined);
			expect(await verifyEntryV0Ed25519BatchFromEntries([reserved])).to.equal(
				undefined,
			);

			const detached = new Uint8Array(4);
			structuredClone(detached.buffer, { transfer: [detached.buffer] });
			for (const malformed of [
				undefined,
				new Uint8Array(3),
				new DataView(new ArrayBuffer(4)),
				new Proxy(new Uint8Array(4), {}),
				detached,
			]) {
				(zero as any)._reserved = malformed;
				expect(await verifyEntryV0Ed25519BatchFromEntries([zero])).to.equal(
					undefined,
				);
			}
		});
	});
});
