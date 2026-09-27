import { deserialize, field, serialize, variant, vec } from "@dao-xyz/borsh";
import {
	type PublicSignKey,
	type SignatureWithKey,
	verify,
} from "@peerbit/crypto";
import { Context } from "@peerbit/document-interface";
import { toId } from "@peerbit/indexer-interface";
import { Entry, EntryV0, LamportClock, Timestamp } from "@peerbit/log";
import { expect } from "chai";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Peerbit } from "peerbit";
import sinon from "sinon";
import { Documents, type Operation, PutOperation } from "../src/index.js";
import { Document, TestStore } from "./data.js";

// Test-only owner-certified state transfer. This is not a production checkpoint
// API: there is no concurrent import, publication protocol, epoch admission or
// history deletion. In particular, exporting every frontier head does not bound
// the frontier when many historical deletes have accumulated.
@variant("checkpoint-bootstrap-test-row")
class SnapshotRow {
	@field({ type: Document })
	value: Document;

	@field({ type: Context })
	context: Context;

	constructor(value: Document, context: Context) {
		this.value = value;
		this.context = context;
	}
}

@variant("checkpoint-bootstrap-test-snapshot")
class Snapshot {
	@field({ type: Uint8Array })
	logId: Uint8Array;

	@field({ type: vec(SnapshotRow) })
	rows: SnapshotRow[];

	@field({ type: vec(Uint8Array) })
	heads: Uint8Array[];

	constructor(logId: Uint8Array, rows: SnapshotRow[], heads: Uint8Array[]) {
		this.logId = logId;
		this.rows = rows;
		this.heads = heads;
	}
}

const openArgs = () => ({
	mode: "compat" as const,
	replicate: false as const,
	keep: "self" as const,
	nativeGraph: false as const,
	nativeBackbone: false as const,
	nativeRangePlanner: false as const,
	index: { cache: { resolver: 0 } },
});

const visibleRows = async (
	docs: Documents<Document>,
): Promise<SnapshotRow[]> => {
	const values = await docs.index.search({}, { local: true, remote: false });
	return Promise.all(
		values
			.sort((a, b) => a.id.localeCompare(b.id))
			.map(async (value) => {
				const indexed = await docs.index.index.get(toId(value.id));
				if (!indexed)
					throw new Error("Visible document has no indexed context");
				return new SnapshotRow(
					deserialize(serialize(value), Document),
					deserialize(serialize(indexed.value.__context), Context),
				);
			}),
	);
};

const exportSnapshot = async (store: TestStore) => {
	const heads = await store.docs.log.log.getHeads(true).all();
	heads.sort((a, b) => a.hash.localeCompare(b.hash));
	const bytes = serialize(
		new Snapshot(
			store.docs.log.log.id,
			await visibleRows(store.docs),
			await Promise.all(
				heads.map(async (entry) => {
					const bytes = await store.docs.log.log.blocks.get(entry.hash, {
						remote: false,
					});
					if (!bytes) throw new Error("Missing local snapshot frontier block");
					return new Uint8Array(bytes);
				}),
			),
		),
	);
	return { bytes, signature: await store.node.identity.sign(bytes) };
};

const importSnapshot = async (
	docs: Documents<Document>,
	input: { bytes: Uint8Array; signature: SignatureWithKey },
	owner: PublicSignKey,
) => {
	// The configured owner explicitly certifies the complete projection in this
	// fixture. Entry signatures alone do not prove the projection is complete.
	if (
		!input.signature.publicKey.equals(owner) ||
		!(await verify(input.signature, input.bytes))
	) {
		throw new Error("Invalid snapshot owner signature");
	}
	const snapshot = deserialize(input.bytes, Snapshot);
	expect(serialize(snapshot)).deep.equal(input.bytes);
	expect(snapshot.logId).deep.equal(docs.log.log.id);
	expect(await docs.index.getSize()).equal(0);
	expect(docs.log.log.length).equal(0);
	const heads = new Map<string, Entry<Operation>>();
	for (const bytes of snapshot.heads) {
		const entry = deserialize(bytes, Entry) as Entry<Operation>;
		expect(entry.getStorageBytes()).deep.equal(bytes);
		entry.hash = await Entry.prepareMultihash(entry);
		entry.init(docs.log.log);
		expect(await entry.verifySignatures()).equal(true);
		const signers = await entry.getPublicKeys();
		expect(signers).to.have.length(1);
		expect(signers[0].equals(owner)).equal(true);
		expect(heads.has(entry.hash)).equal(false);
		heads.set(entry.hash, entry);
	}
	const ids = new Set<string>();
	for (const row of snapshot.rows) {
		expect(ids.has(row.value.id)).equal(false);
		ids.add(row.value.id);
		const entry = heads.get(row.context.head);
		if (!entry) throw new Error("Snapshot document has no signed head");
		const operation = await entry.getPayloadValue();
		expect(operation).instanceOf(PutOperation);
		expect((operation as PutOperation).data).deep.equal(serialize(row.value));
		expect(row.context.gid).equal(entry.meta.gid);
		expect(row.context.modified).equal(entry.meta.clock.timestamp.wallTime);
		expect(row.context.size).equal(entry.payload.byteLength);
		expect(row.context.created <= row.context.modified).equal(true);
	}

	// Seed only after complete fixture validation. These existing low-level
	// methods deliberately bypass ordinary causal joining and admission; the
	// production adapter must provide its own durable publication/read gate.
	for (const entry of heads.values()) {
		await docs.log.log.entryIndex.put(entry, {
			unique: true,
			isHead: true,
			toMultiHash: true,
		});
	}
	for (const row of snapshot.rows) {
		await docs.index.putWithContext(
			row.value,
			toId(row.value.id),
			row.context,
			{
				transformFacts: { entryPublicKeys: [owner] },
			},
		);
	}
	return snapshot;
};

describe("Documents checkpoint boundary fixture", function () {
	this.timeout(60_000);

	it("replays a complete delete suffix but diagnoses unresolved concurrent-branch restoration", async () => {
		const source = await Peerbit.create();
		const recipients: Peerbit[] = [];
		const sandbox = sinon.createSandbox();
		try {
			const owner = await source.open(
				new TestStore({ docs: new Documents<Document>() }),
				{ args: openArgs() },
			);
			const localPut = (wallTime: bigint) => ({
				target: "none" as const,
				meta: { timestamp: new Timestamp({ wallTime }) },
			});
			const p = await owner.docs.put(
				new Document({ id: "k", name: "P" }),
				localPut(1_000n),
			);
			const a = await owner.docs.put(
				new Document({ id: "k", name: "A" }),
				localPut(2_000n),
			);
			const snapshot = await exportSnapshot(owner);
			const aBytes = new Uint8Array(
				(await owner.docs.log.log.blocks.get(a.entry.hash))!,
			);
			const b = await owner.docs.put(
				new Document({ id: "k", name: "B" }),
				localPut(3_000n),
			);
			const bBytes = new Uint8Array(
				(await owner.docs.log.log.blocks.get(b.entry.hash))!,
			);
			const c = await EntryV0.create({
				store: owner.docs.log.log.blocks,
				identity: source.identity,
				encoding: owner.docs.log.log.encoding,
				data: new PutOperation({
					data: serialize(new Document({ id: "k", name: "C" })),
				}),
				meta: {
					next: [a.entry],
					data: b.entry.meta.data,
					clock: new LamportClock({
						id: source.identity.publicKey.bytes,
						timestamp: new Timestamp({ wallTime: 5_000n }),
					}),
				},
			});
			const cBytes = new Uint8Array(
				(await owner.docs.log.log.blocks.get(c.hash))!,
			);
			const cut = await owner.docs.del("k", localPut(4_000n));
			const cutBytes = new Uint8Array(
				(await owner.docs.log.log.blocks.get(cut.entry.hash))!,
			);
			expect(await visibleRows(owner.docs)).deep.equal([]);
			// CUT(B) does not reject a replay of the older A. With full ancestry
			// available, ordinary joining makes A visible again before C arrives.
			await owner.docs.log.log.join([
				{ entry: a.entry, references: [p.entry] },
			]);
			expect(
				(await visibleRows(owner.docs)).map((row) => [
					row.value.name,
					row.context.created,
				]),
			).deep.equal([["A", 1_000n]]);
			// The full-history oracle has the source prefix available even after
			// CUT pruning; explicitly supply those signed entries as references.
			await owner.docs.log.log.join([
				{ entry: c, references: [a.entry, p.entry] },
			]);
			const expected = await visibleRows(owner.docs);
			expect(
				expected.map((row) => [row.value.name, row.context.created]),
			).deep.equal([["C", 1_000n]]);
			const raw = { A: aBytes, B: bBytes, D: cutBytes, C: cBytes };
			type Label = keyof typeof raw;
			const expectedRows = new Map<string, Uint8Array>();
			for (const [name, entry] of [
				["A", a.entry],
				["B", b.entry],
				["C", c],
			] as const) {
				expectedRows.set(
					name,
					serialize(
						new SnapshotRow(
							new Document({ id: "k", name }),
							new Context({
								created: 1_000n,
								modified: entry.meta.clock.timestamp.wallTime,
								head: entry.hash,
								gid: entry.meta.gid,
								size: entry.payload.byteLength,
							}),
						),
					),
				);
			}
			expect(expected.map((row) => serialize(row))).deep.equal([
				expectedRows.get("C"),
			]);
			const labelByHash = new Map([
				[p.entry.hash, "P"],
				[a.entry.hash, "A"],
				[b.entry.hash, "B"],
				[cut.entry.hash, "D"],
				[c.hash, "C"],
			]);
			const graph = async (docs: Documents<Document>) => ({
				entries: (await docs.log.log.toArray())
					.map((entry) => labelByHash.get(entry.hash))
					.sort(),
				heads: (await docs.log.log.getHeads().all())
					.map((entry) => labelByHash.get(entry.hash))
					.sort(),
			});
			expect(await graph(owner.docs)).deep.equal({
				entries: ["A", "C", "D", "P"],
				heads: ["C", "D"],
			});
			const decode = async (label: Label, docs: Documents<Document>) => {
				const entry = deserialize(
					new Uint8Array(raw[label]),
					Entry,
				) as Entry<Operation>;
				entry.hash = await Entry.prepareMultihash(entry);
				entry.init(docs.log.log);
				return entry;
			};
			// Each order is the entry order within one join(batch), not separate
			// network arrivals or independent join calls.
			const run = async (order: Label[], topological: boolean) => {
				const recipient = await Peerbit.create();
				recipients.push(recipient);
				const target = await recipient.open(owner.clone(), {
					args: openArgs(),
				});
				await importSnapshot(target.docs, snapshot, source.identity.publicKey);
				const forbidden: string[] = [];
				const guarded = new Set<object>();
				for (const blocks of [
					recipient.services.blocks,
					target.docs.log.log.blocks,
				]) {
					if (guarded.has(blocks)) continue;
					guarded.add(blocks);
					const check = (hash: string) => {
						if (hash !== p.entry.hash) return;
						forbidden.push(hash);
						throw new Error("Read omitted checkpoint prefix P");
					};
					const get = blocks.get.bind(blocks);
					sandbox.stub(blocks, "get").callsFake(async (hash, options) => {
						check(hash);
						return get(hash, options);
					});
					if (blocks.getMany) {
						const getMany = blocks.getMany.bind(blocks);
						sandbox
							.stub(blocks, "getMany")
							.callsFake(async (hashes, options) => {
								for (const hash of hashes) check(hash);
								return getMany(hashes, options);
							});
					}
				}
				const entries = await Promise.all(
					order.map((label) => decode(label, target.docs)),
				);
				if (topological)
					entries.sort((x, y) =>
						Number(
							x.meta.clock.timestamp.wallTime - y.meta.clock.timestamp.wallTime,
						),
					);
				let error: Error | undefined;
				try {
					// A's bytes are already part of the signed checkpoint. Keep them as
					// normal references even if CUT removes the installed A, so restoring
					// C probes A's missing prefix without a missing-A network timeout.
					const boundary = await decode("A", target.docs);
					await target.docs.log.log.join(
						entries.map((entry) => ({ entry, references: [boundary] })),
					);
				} catch (caught) {
					if (!(caught instanceof Error)) throw caught;
					error = caught;
				}
				const rows = await visibleRows(target.docs);
				expect(rows.length).at.most(1);
				if (rows.length > 0)
					expect(serialize(rows[0])).deep.equal(
						expectedRows.get(rows[0].value.name!),
					);
				const result = {
					order: order.join(""),
					topological,
					value: rows[0]?.value.name,
					created: rows[0]?.context.created?.toString(),
					forbidden: forbidden.length,
					error: error?.message,
					graph: await graph(target.docs),
				};
				await recipient.stop();
				return result;
			};
			const failure = {
				created: "1000",
				forbidden: 1,
				error: "Read omitted checkpoint prefix P",
			};
			// Complete B→CUT(B) can now delete the certified A using only admitted
			// graph rows. This is a successful ordinary delete suffix, not a general
			// checkpoint replay protocol or concurrent-branch restoration guarantee.
			expect(await run(["B", "D"], false)).deep.equal({
				order: "BD",
				topological: false,
				value: undefined,
				created: undefined,
				forbidden: 0,
				error: undefined,
				graph: { entries: ["D"], heads: ["D"] },
			});
			expect(await run(["D"], false)).deep.equal({
				...failure,
				order: "D",
				topological: false,
				value: "A",
				graph: { entries: ["A"], heads: ["A"] },
			});
			expect(await run(["B", "D", "A"], false)).deep.equal({
				...failure,
				order: "BDA",
				topological: false,
				value: undefined,
				created: undefined,
				graph: { entries: ["D"], heads: ["D"] },
			});
			const partial = {
				BDC: {
					value: undefined,
					created: undefined,
					graph: { entries: ["D"], heads: ["D"] },
				},
				BCD: { value: "C", graph: { entries: ["A", "B", "C"], heads: ["C"] } },
				CDB: { value: "C", graph: { entries: ["A", "C"], heads: ["C"] } },
			};
			// These representative batch orders leave three distinct partial graphs.
			// Equal visible C rows in BCD and CDB do not prove equivalent replay.
			for (const order of [
				["B", "D", "C"],
				["B", "C", "D"],
				["C", "D", "B"],
			] as Label[][]) {
				const name = order.join("") as keyof typeof partial;
				expect(await run(order, false)).deep.equal({
					...failure,
					...partial[name],
					order: name,
					topological: false,
				});
			}
			// Complete input and a deterministic topological batch order still cannot
			// restore C: it rejoins the removed A, then crosses A's boundary to P.
			for (const order of [
				["B", "C", "D"],
				["D", "C", "B"],
			] as Label[][]) {
				expect(await run(order, true)).deep.equal({
					...failure,
					...partial.BDC,
					order: order.join(""),
					topological: true,
				});
			}
		} finally {
			for (const recipient of recipients) await recipient.stop();
			await source.stop();
			sandbox.restore();
		}
	});

	it("restores live state and ordinary writes without reading omitted ancestry, including disk reopen", async () => {
		const directory = await mkdtemp(
			join(tmpdir(), "peerbit-document-checkpoint-"),
		);
		const sandbox = sinon.createSandbox();
		let source: Peerbit | undefined;
		let recipient: Peerbit | undefined;
		const forbiddenReads: string[] = [];
		const omitted = new Set<string>();
		const guarded = new Set<object>();
		const guard = (blocks: Peerbit["services"]["blocks"]) => {
			if (guarded.has(blocks)) return;
			guarded.add(blocks);
			const assertAllowed = (hash: string) => {
				if (omitted.has(hash)) {
					forbiddenReads.push(hash);
					throw new Error(`Read omitted checkpoint ancestor: ${hash}`);
				}
			};
			const get = blocks.get.bind(blocks);
			sandbox.stub(blocks, "get").callsFake(async (hash, options) => {
				assertAllowed(hash);
				return get(hash, options);
			});
			if (blocks.getMany) {
				const getMany = blocks.getMany.bind(blocks);
				sandbox.stub(blocks, "getMany").callsFake(async (hashes, options) => {
					for (const hash of hashes) assertAllowed(hash);
					return getMany(hashes, options);
				});
			}
		};
		try {
			source = await Peerbit.create();
			const owner = await source.open(
				new TestStore({
					docs: new Documents<Document>({ id: new Uint8Array(32).fill(43) }),
				}),
				{ args: openArgs() },
			);
			const localPut = (wallTime: bigint) => ({
				target: "none" as const,
				meta: { timestamp: new Timestamp({ wallTime }) },
			});
			const a = await owner.docs.put(
				new Document({ id: "survivor", name: "A" }),
				localPut(1_000n),
			);
			const b = await owner.docs.put(
				new Document({ id: "survivor", name: "B" }),
				localPut(2_000n),
			);
			const c = await owner.docs.log.append(
				new PutOperation({
					data: serialize(new Document({ id: "survivor", name: "C" })),
				}),
				{
					...localPut(3_000n),
					meta: {
						timestamp: new Timestamp({ wallTime: 3_000n }),
						next: [a.entry],
					},
				},
			);
			expect(b.entry.meta.next).deep.equal([a.entry.hash]);
			expect(c.entry.meta.next).deep.equal([a.entry.hash]);
			expect(await owner.docs.log.log.getShallow(a.entry.hash)).to.exist;
			const deleted = await owner.docs.put(
				new Document({ id: "deleted", name: "gone" }),
				localPut(4_000n),
			);
			await owner.docs.del("deleted", localPut(5_000n));
			omitted.add(a.entry.hash);
			omitted.add(deleted.entry.hash);
			const expected = await visibleRows(owner.docs);
			expect(expected.map((row) => [row.value.id, row.value.name])).deep.equal([
				["survivor", "C"],
			]);
			const snapshot = await exportSnapshot(owner);
			const clone = owner.clone();
			const authority = source.identity.publicKey;

			recipient = await Peerbit.create({ directory });
			guard(recipient.services.blocks);
			let imported = await recipient.open(clone, { args: openArgs() });
			guard(imported.docs.log.log.blocks as Peerbit["services"]["blocks"]);
			expect(recipient.libp2p.getConnections()).to.have.length(0);
			const invalid = {
				bytes: new Uint8Array(snapshot.bytes),
				signature: snapshot.signature,
			};
			invalid.bytes[invalid.bytes.length - 1] ^= 1;
			let rejected: unknown;
			try {
				await importSnapshot(imported.docs, invalid, authority);
			} catch (error) {
				rejected = error;
			}
			expect(rejected).instanceOf(Error);
			expect(imported.docs.log.log.length).equal(0);
			expect(await imported.docs.index.getSize()).equal(0);
			const captured = await importSnapshot(imported.docs, snapshot, authority);
			expect(captured.heads).to.have.length(3); // B, C, and the independent CUT.
			expect(
				(await visibleRows(imported.docs)).map((row) => serialize(row)),
			).deep.equal(expected.map((row) => serialize(row)));
			expect(
				(await imported.docs.get("survivor", { local: true, remote: false }))
					?.name,
			).equal("C");
			expect(
				await imported.docs.get("deleted", { local: true, remote: false }),
			).equal(undefined);
			for (const hash of omitted)
				expect(await recipient.services.blocks.has(hash)).equal(false);

			const reopen = imported.clone();
			await recipient.stop();
			recipient = await Peerbit.create({ directory });
			guard(recipient.services.blocks);
			imported = await recipient.open(reopen, { args: openArgs() });
			guard(imported.docs.log.log.blocks as Peerbit["services"]["blocks"]);
			expect(
				(await visibleRows(imported.docs)).map((row) => serialize(row)),
			).deep.equal(expected.map((row) => serialize(row)));
			expect(
				await imported.docs.get("deleted", { local: true, remote: false }),
			).equal(undefined);
			const updated = await imported.docs.put(
				new Document({ id: "survivor", name: "recent" }),
				localPut(6_000n),
			);
			expect(updated.entry.meta.next).deep.equal([c.entry.hash]);
			expect(
				(await imported.docs.get("survivor", { local: true, remote: false }))
					?.name,
			).equal("recent");
			await imported.docs.del("survivor", localPut(7_000n));
			expect(
				await imported.docs.index.search({}, { local: true, remote: false }),
			).deep.equal([]);
			expect(forbiddenReads).deep.equal([]);
		} finally {
			try {
				await recipient?.stop();
				await source?.stop();
			} finally {
				sandbox.restore();
				await rm(directory, { recursive: true, force: true });
			}
		}
	});
});
