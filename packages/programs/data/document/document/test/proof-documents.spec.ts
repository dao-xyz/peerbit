import { deserialize, serialize } from "@dao-xyz/borsh";
import { toId } from "@peerbit/indexer-interface";
import { EntryType } from "@peerbit/log";
import { expect } from "chai";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Peerbit } from "peerbit";
import sinon from "sinon";
import { Documents } from "../src/program.js";
import {
	deriveProofDeleteScope,
	getProofDeleteContract,
} from "../src/proof-delete-manifest.js";
import { ProofPutOperation } from "../src/proof-operations.js";
import { Document } from "./data.js";

describe("owner-proofs Documents public lifecycle", () => {
	for (const nativeGraph of [false, true]) {
		describe(nativeGraph ? "native graph" : "JS graph", () => {
			let directory: string;
			let peer: Peerbit;
			let docs: Documents<Document>;
			const args = { type: Document, nativeGraph, timeUntilRoleMaturity: 0 };
			beforeEach(async () => {
				directory = await mkdtemp(join(tmpdir(), "peerbit-proof-documents-"));
				peer = await Peerbit.create({ directory });
				docs = await peer.open(
					new Documents<Document>({
						format: {
							version: "owner-proofs-v1",
							owner: peer.identity.publicKey,
							keyType: "string",
						},
					}),
					{ args },
				);
				expect(Boolean(docs.log.log.entryIndex.properties.nativeGraph)).equal(
					nativeGraph,
				);
			});
			afterEach(async () => {
				sinon.restore();
				await peer?.stop();
				await rm(directory, { recursive: true, force: true });
			});

			it("deletes same-key history, preserves another key and reopens offline", async () => {
				const manifest = serialize(docs);
				const events: { added: string[]; removed: string[] }[] = [];
				docs.events.addEventListener("change", (event) => {
					events.push({
						added: event.detail.added.map((value) => value.id),
						removed: event.detail.removed.map((value) => value.id),
					});
				});
				const first = await docs.put(new Document({ id: "a", name: "first" }));
				const second = await docs.put(
					new Document({ id: "a", name: "second" }),
				);
				const survivor = await docs.put(
					new Document({ id: "b", name: "keep" }),
				);
				expect(
					(await docs.get("a", { local: true, remote: false }))?.name,
				).equal("second");
				const removed = await docs.del("a");
				expect(removed.entry.meta.type).equal(EntryType.CUT);
				expect(removed.entry.meta.next).to.have.members([
					first.entry.hash,
					second.entry.hash,
				]);
				for (const hash of [first.entry.hash, second.entry.hash]) {
					expect(await docs.log.log.entryIndex.properties.index.get(toId(hash)))
						.undefined;
					expect(await docs.log.entryCoordinatesIndex.get(toId(hash)))
						.undefined;
				}
				expect(await docs.get("a", { local: true, remote: false })).undefined;
				expect(
					(await docs.get("b", { local: true, remote: false }))?.name,
				).equal("keep");
				expect(await docs.count()).equal(1);
				expect(events).deep.equal([
					{ added: ["a"], removed: [] },
					{ added: ["a"], removed: [] },
					{ added: ["b"], removed: [] },
					{ added: [], removed: ["a"] },
				]);
				await peer.stop();
				peer = await Peerbit.create({ directory });
				docs = await peer.open(
					deserialize(manifest, Documents) as unknown as Documents<Document>,
					{ args },
				);
				expect(peer.libp2p.getConnections()).to.have.length(0);
				expect(await docs.get("a", { local: true, remote: false })).undefined;
				expect(
					(await docs.get("b", { local: true, remote: false }))?.name,
				).equal("keep");
				expect(await docs.count()).equal(1);
				expect(await docs.log.log.has(removed.entry.hash)).true;
				expect(await docs.log.log.has(survivor.entry.hash)).true;
				await docs.put(new Document({ id: "a", name: "new after delete" }));
				expect(
					(await docs.get("a", { local: true, remote: false }))?.name,
				).equal("new after delete");
			});

			it("rejects unsupported batch and mutation options before committing", async () => {
				await expect(
					docs.putMany([new Document({ id: "a" }), new Document({ id: "b" })]),
				).rejectedWith("Owner-proofs");
				await expect(
					docs.put(new Document({ id: "a" }), { replicate: true }),
				).rejectedWith("do not support");
				expect(docs.log.log.length).equal(0);
				expect(await docs.count()).equal(0);
			});

			it("derives query context from the winner consistently across reopen", async () => {
				const manifest = serialize(docs);
				await docs.put(new Document({ id: "a", name: "old" }));
				const latest = await docs.put(
					new Document({ id: "a", name: "latest" }),
				);
				const before = (await docs.index.index.get(toId("a")))!.value.__context;
				expect(before.created).equal(
					latest.entry.meta.clock.timestamp.wallTime,
				);
				expect(before.modified).equal(before.created);
				await docs.close();
				docs = await peer.open(
					deserialize(manifest, Documents) as unknown as Documents<Document>,
					{ args },
				);
				const after = (await docs.index.index.get(toId("a")))!.value.__context;
				expect(serialize(after)).deep.equal(serialize(before));
			});

			it("rejects a signed key/value mismatch before mutation without poisoning the store", async () => {
				const operation = new ProofPutOperation({
					scope: deriveProofDeleteScope(getProofDeleteContract(docs.log)!),
					key: toId("signed-key"),
					data: serialize(new Document({ id: "different-key" })),
				});
				await expect(
					docs.log.append(operation, { meta: { next: [] } }),
				).rejectedWith("ID disagrees");
				expect(docs.log.log.length).equal(0);
				expect(await docs.count()).equal(0);
				await docs.put(new Document({ id: "healthy" }));
				expect(await docs.count()).equal(1);
			});

			it("reports committed deletion hashes on projection failure and repairs before offline reads", async () => {
				const manifest = serialize(docs);
				const victim = await docs.put(new Document({ id: "deleted" }));
				await docs.put(new Document({ id: "survivor" }));
				const failure = new Error("injected projection failure");
				const remove = sinon.stub(docs.index, "del").rejects(failure);
				let rejected: any;
				try {
					await docs.del("deleted", {
						delivery: { reliability: "persisted", minAcks: 1, timeout: 5_000 },
					});
				} catch (error) {
					rejected = error;
				}
				expect(rejected?.name).equal("PersistedDeliveryError");
				expect(rejected.committedHashes).to.have.length(1);
				const marker = rejected.committedHashes[0];
				expect(await docs.log.log.entryIndex.properties.index.get(toId(marker)))
					.to.exist;
				expect(
					await docs.log.log.entryIndex.properties.index.get(
						toId(victim.entry.hash),
					),
				).undefined;
				await expect(docs.get("deleted", { local: true, remote: false }))
					.rejected;
				await expect(docs.put(new Document({ id: "blocked" }))).rejected;
				remove.restore();
				await peer.stop();
				peer = await Peerbit.create({ directory });
				docs = await peer.open(
					deserialize(manifest, Documents) as unknown as Documents<Document>,
					{ args },
				);
				expect(await docs.get("deleted", { local: true, remote: false }))
					.undefined;
				expect(
					(await docs.get("survivor", { local: true, remote: false }))?.id,
				).equal("survivor");
				expect(await docs.count()).equal(1);
				await docs.put(new Document({ id: "healthy after recovery" }));
				expect(await docs.count()).equal(2);
			});

			it("rejects an oversized full-history deletion before retiring any entry", async () => {
				const first = await docs.put(
					new Document({ id: "large", data: new Uint8Array(2_500_000) }),
				);
				const second = await docs.put(
					new Document({ id: "large", data: new Uint8Array(2_500_000) }),
				);
				await expect(docs.del("large")).rejectedWith(
					"Proof byte limit exceeded",
				);
				expect(docs.log.log.length).equal(2);
				expect(await docs.log.log.has(first.entry.hash)).true;
				expect(await docs.log.log.has(second.entry.hash)).true;
				expect(
					(await docs.get("large", { local: true, remote: false }))?.data,
				).to.have.length(2_500_000);
			});
		});
	}
});
