// Intentionally failing legacy-format characterization, not a default spec.
// Baseline: 8 passing / 7 failing at 2375d4b723f1bfc17601120bc996bfe6ae840c5f.
// Run the built dist/test/cut-admission-order.repro.js explicitly with mocha.
// New-format protections are asserted by proof-documents-order.spec.ts.
import { Entry, EntryType, Timestamp, createEntry } from "@peerbit/log";
import { TestSession } from "@peerbit/test-utils";
import { expect } from "chai";
import {
	DeleteOperation,
	Documents,
	type Operation,
	policy,
	toId,
} from "../src/index.js";
import { Document, TestStore } from "./data.js";

describe("CUT document admission order", () => {
	let session: TestSession;
	let source: TestStore;
	let target: TestStore;

	beforeEach(async () => {
		session = await TestSession.disconnected(2);
		source = new TestStore({ docs: new Documents<Document>() });
		target = source.clone();
		for (const [i, store] of [source, target].entries()) {
			await session.peers[i].open(store, {
				args: {
					mode: "compat",
					replicate: false,
					keep: "self",
					nativeGraph: false,
					nativeBackbone: false,
					nativeRangePlanner: false,
				},
			});
		}
	});

	afterEach(async () => {
		await session?.stop();
	});

	const join = async (entry: Entry<Operation>) => {
		await target.docs.log.join([entry], { verifySignatures: true });
	};

	const cases = [
		{ order: ["cut", "replacement"], equalTime: false },
		{ order: ["replacement", "cut"], equalTime: false },
		{ order: ["original", "cut", "replacement"], equalTime: false },
		{ order: ["original", "replacement", "cut"], equalTime: false },
		{ order: ["cut", "original", "replacement"], equalTime: false },
		{ order: ["cut", "replacement", "original"], equalTime: false },
		{ order: ["replacement", "original", "cut"], equalTime: false },
		{ order: ["replacement", "cut", "original"], equalTime: false },
		{ order: ["cut", "replacement"], equalTime: true },
		{ order: ["replacement", "cut"], equalTime: true },
	] as const;
	for (const { order, equalTime } of cases) {
		it(`retains CUT and independent replacement after ${order.join(" → ")}${equalTime ? " at equal CUT/replacement wall time" : ""}`, async () => {
			const id = "recreated-document";
			const original = await source.docs.put(
				new Document({ id, name: "original" }),
				{ target: "none" },
			);
			const cut = await source.docs.del(id, { target: "none" });
			const replacement = await source.docs.put(
				new Document({ id, name: "replacement" }),
				{
					target: "none",
					unique: true,
					meta: {
						next: [],
						...(equalTime
							? {
									timestamp: new Timestamp({
										wallTime: cut.entry.meta.clock.timestamp.wallTime,
									}),
								}
							: {}),
					},
				},
			);
			expect(cut.entry.meta.type).equal(EntryType.CUT);
			expect(cut.entry.meta.next).deep.equal([original.entry.hash]);
			expect(replacement.entry.meta.next).deep.equal([]);
			expect(await source.docs.log.log.has(original.entry.hash)).equal(false);
			if (equalTime) {
				expect(replacement.entry.meta.clock.timestamp.wallTime).equal(
					cut.entry.meta.clock.timestamp.wallTime,
				);
			}
			const entries = { original, cut, replacement };
			for (const name of order) await join(entries[name].entry);

			expect(
				await target.docs.log.log.has(cut.entry.hash),
				"CUT retained",
			).equal(true);
			expect(await target.docs.log.log.has(replacement.entry.hash)).equal(true);
			expect(await target.docs.log.log.has(original.entry.hash)).equal(false);
			expect(
				(await target.docs.index.get(id, { local: true, remote: false }))?.name,
			).equal("replacement");
		});
	}

	it("does not let the owner of one key retire an absent victim from another key", async () => {
		const protectedStore = await session.peers[1].open(
			new TestStore({ docs: new Documents<Document>() }),
			{
				args: {
					mode: "compat",
					replicate: false,
					keep: "self",
					nativeGraph: false,
					nativeBackbone: false,
					canPerform: policy.or(
						policy.put(policy.signedByField<Document>("data")),
						policy.deleteSignedByExistingField<Document>("data"),
					),
				},
			},
		);
		const victim = await source.docs.put(
			new Document({
				id: "victim-key",
				data: source.node.identity.publicKey.bytes,
			}),
			{ target: "none" },
		);
		const owned = await protectedStore.docs.put(
			new Document({
				id: "attacker-key",
				data: protectedStore.node.identity.publicKey.bytes,
			}),
			{ target: "none" },
		);
		const forged = await createEntry<Operation>({
			store: protectedStore.docs.log.log.blocks,
			encoding: protectedStore.docs.log.log.encoding,
			identity: protectedStore.node.identity,
			data: new DeleteOperation({ key: toId("attacker-key") }),
			meta: {
				type: EntryType.CUT,
				next: [victim.entry],
				data: victim.entry.meta.data,
			},
		});
		expect(await forged.verifySignatures()).equal(true);
		expect(forged.meta.gid).equal(victim.entry.meta.gid);
		expect(await protectedStore.docs.log.log.has(victim.entry.hash)).equal(
			false,
		);
		await protectedStore.docs.log.join([forged], { verifySignatures: true });
		expect(await protectedStore.docs.log.log.has(forged.hash)).equal(false);
		await protectedStore.docs.log.join([victim.entry], {
			verifySignatures: true,
		});
		expect(await protectedStore.docs.log.log.has(victim.entry.hash)).equal(
			true,
		);
		expect(await protectedStore.docs.log.log.has(owned.entry.hash)).equal(true);
		expect(await protectedStore.docs.index.getSize()).equal(2);
	});

	for (const crossKeyParent of [false, true]) {
		it(`preserves another owner's document when deleting an owned ${crossKeyParent ? "causal child" : "independent entry"}`, async () => {
			const ownedStore = await session.peers[1].open(
				new TestStore({ docs: new Documents<Document>() }),
				{
					args: {
						mode: "compat",
						replicate: false,
						keep: "self",
						nativeGraph: false,
						nativeBackbone: false,
						nativeRangePlanner: false,
						canPerform: policy.or(
							policy.put(policy.signedByField<Document>("data")),
							policy.deleteSignedByExistingField<Document>("data"),
						),
					},
				},
			);
			const victim = await source.docs.put(
				new Document({
					id: "other-owner",
					data: source.node.identity.publicKey.bytes,
				}),
				{ target: "none" },
			);
			await ownedStore.docs.log.join([victim.entry], {
				verifySignatures: true,
			});
			const child = await ownedStore.docs.put(
				new Document({
					id: "my-document",
					data: ownedStore.node.identity.publicKey.bytes,
				}),
				{
					target: "none",
					meta: { next: crossKeyParent ? [victim.entry] : [] },
				},
			);
			expect(child.entry.meta.next).deep.equal(
				crossKeyParent ? [victim.entry.hash] : [],
			);
			expect(await ownedStore.docs.index.getSize()).equal(2);
			// An ordinary, authorized delete of our own key must not acquire authority
			// over another owner's key through the put's causal dependency.
			const removed = await ownedStore.docs.del("my-document", {
				target: "none",
			});
			expect(removed.entry.meta.next).deep.equal([child.entry.hash]);
			// Replay B's signed operations on the victim owner's separately opened
			// replica, not only on the deleter's own projection.
			const receiver = await session.peers[0].open(ownedStore.clone(), {
				args: {
					mode: "compat",
					replicate: false,
					keep: "self",
					nativeGraph: false,
					nativeBackbone: false,
					nativeRangePlanner: false,
					canPerform: policy.or(
						policy.put(policy.signedByField<Document>("data")),
						policy.deleteSignedByExistingField<Document>("data"),
					),
				},
			});
			for (const entry of [victim.entry, child.entry]) {
				await receiver.docs.log.join([entry], { verifySignatures: true });
			}
			expect([
				await receiver.docs.log.log.has(victim.entry.hash),
				await receiver.docs.log.log.has(child.entry.hash),
				(
					await receiver.docs.index.get("other-owner", {
						local: true,
						remote: false,
					})
				)?.id,
			]).deep.equal([true, true, "other-owner"]);
			await receiver.docs.log.join([removed.entry], {
				verifySignatures: true,
			});
			const state = async (store: TestStore) => ({
				victimRetained: await store.docs.log.log.has(victim.entry.hash),
				victimVisible: (
					await store.docs.index.get("other-owner", {
						local: true,
						remote: false,
					})
				)?.id,
				childRetained: await store.docs.log.log.has(child.entry.hash),
			});
			const expected = {
				victimRetained: true,
				victimVisible: "other-owner",
				childRetained: false,
			};
			expect({
				local: await state(ownedStore),
				received: await state(receiver),
			}).deep.equal({
				local: expected,
				received: expected,
			});
		});
	}

	for (const order of [
		["cut", "replacement"],
		["replacement", "cut"],
	] as const) {
		it(`does not resurrect the original after ${order.join(" → ")} → replacement delete → original replay`, async () => {
			const ownedSource = new TestStore({ docs: new Documents<Document>() });
			const ownedTarget = ownedSource.clone();
			// Fixed authority deliberately does not depend on the indexed replacement.
			const owner = session.peers[0].identity.publicKey;
			for (const [i, store] of [ownedSource, ownedTarget].entries()) {
				await session.peers[i].open(store, {
					args: {
						mode: "compat",
						replicate: false,
						keep: "self",
						nativeGraph: false,
						nativeBackbone: false,
						nativeRangePlanner: false,
						canPerform: policy.signedByPublicKey<Document>(owner),
					},
				});
			}
			const id = "replayed-original";
			const original = await ownedSource.docs.put(
				new Document({ id, name: "original", data: owner.bytes }),
				{ target: "none" },
			);
			const cut = await ownedSource.docs.del(id, { target: "none" });
			const replacement = await ownedSource.docs.put(
				new Document({ id, name: "replacement", data: owner.bytes }),
				{ target: "none", unique: true, meta: { next: [] } },
			);
			const replacementCut = await ownedSource.docs.del(id, { target: "none" });
			expect(cut.entry.meta.next).deep.equal([original.entry.hash]);
			expect(replacement.entry.meta.next).deep.equal([]);
			expect(replacementCut.entry.meta.next).deep.equal([
				replacement.entry.hash,
			]);
			expect(await ownedSource.docs.log.log.has(original.entry.hash)).equal(
				false,
			);
			expect(await ownedSource.docs.log.log.has(replacement.entry.hash)).equal(
				false,
			);
			const entries = { cut, replacement };
			for (const name of order) {
				await ownedTarget.docs.log.join([entries[name].entry], {
					verifySignatures: true,
				});
			}
			expect(
				(await ownedTarget.docs.index.get(id, { local: true, remote: false }))
					?.name,
			).equal("replacement");
			await ownedTarget.docs.log.join([replacementCut.entry], {
				verifySignatures: true,
			});
			expect(await ownedTarget.docs.index.getSize()).equal(0);
			await ownedTarget.docs.log.join([original.entry], {
				verifySignatures: true,
			});
			// Inspect both the actual log and public projection after the complete replay.
			expect({
				originalDeleteRetained: await ownedTarget.docs.log.log.has(
					cut.entry.hash,
				),
				replacementDeleteRetained: await ownedTarget.docs.log.log.has(
					replacementCut.entry.hash,
				),
				originalRetained: await ownedTarget.docs.log.log.has(
					original.entry.hash,
				),
				replacementRetained: await ownedTarget.docs.log.log.has(
					replacement.entry.hash,
				),
				visible: (
					await ownedTarget.docs.index.get(id, { local: true, remote: false })
				)?.name,
			}).deep.equal({
				originalDeleteRetained: true,
				replacementDeleteRetained: true,
				originalRetained: false,
				replacementRetained: false,
				visible: undefined,
			});
		});
	}
});
