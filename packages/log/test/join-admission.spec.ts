import { deserialize } from "@dao-xyz/borsh";
import { AnyBlockStore } from "@peerbit/blocks";
import { Ed25519Keypair, X25519Keypair } from "@peerbit/crypto";
import { HashmapIndices } from "@peerbit/indexer-simple";
import { DefaultCryptoKeychain } from "@peerbit/keychain";
import { expect } from "chai";
import sinon from "sinon";
import { EntryType } from "../src/entry-type.js";
import { type EntryV0 } from "../src/entry-v0.js";
import { Entry } from "../src/entry.js";
import { Log, type LogOptions } from "../src/log.js";

describe("early join admission", () => {
	for (const nativeGraph of [false, true]) {
		describe(`nativeGraph=${nativeGraph}`, () => {
			let store: AnyBlockStore;
			let source: Log<Uint8Array>;
			let target: Log<Uint8Array>;
			let identity: Ed25519Keypair;

			beforeEach(async () => {
				store = new AnyBlockStore();
				await store.start();
				identity = await Ed25519Keypair.create();
				source = new Log<Uint8Array>();
				target = new Log<Uint8Array>();
				await source.open(store, identity, {
					indexer: new HashmapIndices(),
					nativeGraph,
				});
			});

			afterEach(async () => {
				sinon.restore();
				await target.close();
				await source.close();
				await store.stop();
			});

			const openTarget = async (options: LogOptions<Uint8Array> = {}) =>
				target.open(store, identity, {
					indexer: new HashmapIndices(),
					nativeGraph,
					...options,
				});

			const createChain = async () => {
				const { entry: ancestor } = await source.append(Uint8Array.of(1), {
					meta: { next: [] },
				});
				const { entry: parent } = await source.append(Uint8Array.of(2));
				const { entry: head } = await source.append(Uint8Array.of(3));
				return { ancestor, parent, head };
			};

			const observeReads = () => {
				const get = sinon.spy(store, "get");
				const getMany = sinon.spy(store, "getMany");
				return () => [
					...get.getCalls().map((call) => call.args[0]),
					...getMany.getCalls().flatMap((call) => call.args[0]),
				];
			};

			it("rejects a head without requesting its parents", async () => {
				const { head } = await createChain();
				const canJoin = sinon.spy(async () => false);
				const canAppend = sinon.spy(() => true);
				await openTarget({ canJoin, canAppend });
				const reads = observeReads();

				await target.join([head], { verifySignatures: true });

				expect(canJoin.callCount).to.equal(1);
				expect(canAppend.callCount).to.equal(0);
				expect(reads()).to.deep.equal([]);
				expect(target.length).to.equal(0);
			});

			it("rejects a resolved parent before requesting its ancestor", async () => {
				const { ancestor, parent, head } = await createChain();
				const considered: string[] = [];
				const admitted: string[] = [];
				await openTarget({
					canJoin: (entry) => {
						considered.push(entry.hash);
						return entry.hash !== parent.hash;
					},
					canAppend: (entry) => {
						admitted.push(entry.hash);
						return true;
					},
				});
				const reads = observeReads();

				await target.join([head]);

				expect(considered).to.deep.equal([head.hash, parent.hash]);
				expect(reads()).to.include(parent.hash);
				expect(reads()).not.to.include(ancestor.hash);
				expect(admitted).to.deep.equal([head.hash]);
				// Preserve ordinary Log semantics: a rejected parent is not automatic
				// child rejection. Applications enforce their causal rules in canAppend.
				expect(await target.has(head.hash)).to.equal(true);
				expect(await target.has(parent.hash)).to.equal(false);
			});

			it("keeps bottom-up canAppend admission after a successful preflight", async () => {
				const { ancestor, parent, head } = await createChain();
				const considered: string[] = [];
				const admitted: string[] = [];
				await openTarget({
					canJoin: (entry) => {
						considered.push(entry.hash);
						return true;
					},
					canAppend: (entry) => {
						admitted.push(entry.hash);
						return entry.hash !== head.hash;
					},
				});

				await target.join([head]);

				expect(considered).to.deep.equal([
					head.hash,
					parent.hash,
					ancestor.hash,
				]);
				expect(admitted).to.deep.equal([ancestor.hash, parent.hash, head.hash]);
				expect(await target.has(head.hash)).to.equal(false);
				expect(target.length).to.equal(2);
			});

			it("still verifies signatures when preflight allows a candidate", async () => {
				const { entry } = await source.append(Uint8Array.of(1));
				const canJoin = sinon.spy(() => true);
				await openTarget({ canJoin });
				sinon.stub(entry, "verifySignatures").returns(false);

				let error: unknown;
				try {
					await target.join([entry], { verifySignatures: true });
				} catch (caught) {
					error = caught;
				}
				expect(error).to.be.instanceOf(Error);
				expect((error as Error).message).to.include("Invalid signature");
				expect(target.length).to.equal(0);
			});

			it("isolates local flags and method overrides from later signature admission", async () => {
				const { entry } = await source.append(Uint8Array.of(1));
				const received = await Entry.fromMultihash<Uint8Array>(
					store,
					entry.hash,
				);
				received.init(source);
				received.signatures[0]!.signature.fill(0);
				expect(await received.verifySignatures()).to.equal(false);
				const originalVerifier = received.verifySignatures;
				let callbackEntry: Entry<Uint8Array> | undefined;
				let admissionCalls = 0;
				await openTarget({
					canJoin: (candidate) => {
						callbackEntry = candidate;
						expect(candidate.createdLocally).to.equal(undefined);
						candidate.createdLocally = true;
						candidate.verifySignatures = () => true;
						candidate.getStorageBytes = () => new Uint8Array();
						return true;
					},
					canAppend: async (candidate) => {
						admissionCalls++;
						return (
							candidate.createdLocally || (await candidate.verifySignatures())
						);
					},
				});

				await target.join([received]);

				expect(callbackEntry).not.to.equal(received);
				expect(admissionCalls).to.equal(1);
				expect(received.createdLocally).to.equal(undefined);
				expect(received.verifySignatures).to.equal(originalVerifier);
				expect(await received.verifySignatures()).to.equal(false);
				expect(target.length).to.equal(0);
			});

			it("isolates nested signed fields and lazy storage bytes from parent resolution", async () => {
				const { ancestor, parent, head } = await createChain();
				const { entry: unrelated } = await source.append(Uint8Array.of(4), {
					meta: { next: [] },
				});
				const rawBytes = new Uint8Array((await store.get(head.hash))!);
				const originalBytes = rawBytes.slice();
				const originalGid = head.meta.gid;
				// Prepared raw heads expose their storage view directly, without
				// materializing a second entry. Exercise that same reader contract.
				sinon.stub(head, "getStorageBytes").returns(rawBytes);
				const admitted: string[] = [];
				await openTarget({
					canJoin: (candidate) => {
						if (candidate.hash !== head.hash) return true;
						expect(candidate.size).to.equal(head.size);
						expect(candidate.createdLocally).to.equal(undefined);
						candidate.meta.next[0] = unrelated.hash;
						candidate.meta.type = EntryType.CUT;
						candidate.meta.gid = "changed";
						candidate.meta.clock.id.fill(0);
						candidate.payload.data.fill(0);
						candidate.signatures[0]!.signature.fill(0);
						(candidate as EntryV0<Uint8Array>).getMetaBytes()!.fill(0);
						candidate.getStorageBytes().fill(0);
						candidate.hash = unrelated.hash;
						return true;
					},
					canAppend: async (candidate) => {
						admitted.push(candidate.hash);
						return candidate.verifySignatures();
					},
				});
				const reads = observeReads();

				await target.join([head]);

				expect(rawBytes).to.deep.equal(originalBytes);
				expect(head.meta.next).to.deep.equal([parent.hash]);
				expect(head.meta.type).to.equal(EntryType.APPEND);
				expect(head.meta.gid).to.equal(originalGid);
				expect(await head.getPayloadValue()).to.deep.equal(Uint8Array.of(3));
				expect(await head.verifySignatures()).to.equal(true);
				expect(reads()).to.include(parent.hash);
				expect(reads()).to.include(ancestor.hash);
				expect(reads()).not.to.include(unrelated.hash);
				expect(admitted).to.deep.equal([ancestor.hash, parent.hash, head.hash]);
				expect(target.length).to.equal(3);
			});

			it("hydrates detached encrypted metadata and preserves entry size", async () => {
				const receiver = await X25519Keypair.create();
				const keychain = new DefaultCryptoKeychain();
				await keychain.import({ keypair: receiver });
				const { entry } = await source.append(Uint8Array.of(1), {
					encryption: {
						keypair: await X25519Keypair.create(),
						receiver: {
							meta: receiver.publicKey,
							signatures: receiver.publicKey,
							payload: receiver.publicKey,
						},
					},
				});
				let calls = 0;
				await openTarget({
					keychain,
					canJoin: async (candidate) => {
						calls++;
						expect(candidate.meta.next).to.deep.equal([]);
						expect(candidate.meta.gid).to.equal(entry.meta.gid);
						expect(candidate.size).to.equal(entry.size);
						expect(candidate.createdLocally).to.equal(undefined);
						expect(await candidate.getPayloadValue()).to.deep.equal(
							Uint8Array.of(1),
						);
						expect(await candidate.getSignatures()).to.have.length(1);
						return true;
					},
				});

				await target.join([entry], { verifySignatures: true });

				expect(calls).to.equal(1);
				expect(target.length).to.equal(1);
			});

			it("accepts a deserialized candidate without a runtime size cache", async () => {
				const { entry } = await source.append(Uint8Array.of(1));
				const rawBytes = (await store.get(entry.hash))!;
				const received = deserialize(rawBytes, Entry) as Entry<Uint8Array>;
				received.hash = entry.hash;
				expect(() => received.size).to.throw();
				let observedSize: number | undefined;
				await openTarget({
					canJoin: (candidate) => {
						observedSize = candidate.size;
						return true;
					},
				});

				await target.join([received], { verifySignatures: true });

				expect(observedSize).to.equal(rawBytes.byteLength);
				expect(await target.has(entry.hash)).to.equal(true);
			});

			it("does not let trusted independent joins bypass preflight or canAppend", async () => {
				const entries: Entry<Uint8Array>[] = [];
				for (const value of [1, 2]) {
					entries.push(
						(await source.append(Uint8Array.of(value), { meta: { next: [] } }))
							.entry,
					);
				}
				const canJoin = sinon.spy(
					(entry: Entry<Uint8Array>) => entry.hash !== entries[0]!.hash,
				);
				const canAppend = sinon.spy(() => false);
				await openTarget({ canJoin, canAppend });
				const trustedOptions = {
					verifySignatures: false,
					__peerbitBatchIndependent: true,
					__peerbitCanAppendAlreadyValidated: true,
				};

				await target.join(entries, trustedOptions);

				expect(canJoin.callCount).to.equal(2);
				expect(canAppend.callCount).to.equal(1);
				expect(target.length).to.equal(0);
			});

			it("declines prepared-facts joins so callers fall back to preflight", async () => {
				const { entry } = await source.append(Uint8Array.of(1));
				const canJoin = sinon.spy(() => false);
				await openTarget({ canJoin });
				const bytes = entry.getStorageBytes();
				const nativeCommit = sinon.spy(async () => true);
				const joined = await target["joinPreparedAppendFactsBatch"](
					[
						{
							hash: entry.hash,
							meta: entry.meta,
							size: entry.size,
							bytes,
							byteLength: bytes.byteLength,
							materializeEntry: () => entry,
						},
					],
					{
						__peerbitCanAppendAlreadyValidated: true,
						__peerbitNativePreparedJoinCommit: nativeCommit,
					},
				);

				expect(joined).to.equal(false);
				expect(nativeCommit.callCount).to.equal(0);
				expect(target.length).to.equal(0);
				await target.join([entry]);
				expect(canJoin.callCount).to.equal(1);
				expect(target.length).to.equal(0);
			});

			it("retains ordinary recursive joins when no preflight is configured", async () => {
				const { ancestor, parent, head } = await createChain();
				const admitted: string[] = [];
				await openTarget({
					canAppend: (entry) => {
						admitted.push(entry.hash);
						return entry.hash !== parent.hash;
					},
				});

				await target.join([head]);

				expect(admitted).to.deep.equal([ancestor.hash, parent.hash, head.hash]);
				expect(await target.has(ancestor.hash)).to.equal(true);
				expect(await target.has(parent.hash)).to.equal(false);
				expect(await target.has(head.hash)).to.equal(true);
			});
		});
	}
});
