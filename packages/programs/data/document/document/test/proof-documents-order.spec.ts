import { deserialize, serialize } from "@dao-xyz/borsh";
import { toId } from "@peerbit/indexer-interface";
import { expect } from "chai";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Peerbit } from "peerbit";
import { Documents } from "../src/program.js";
import {
	deriveProofDeleteScope,
	getProofDeleteContract,
} from "../src/proof-delete-manifest.js";
import { ProofPutOperation } from "../src/proof-operations.js";
import { Document } from "./data.js";

describe("owner-proofs Documents delivery order", () => {
	for (const nativeGraph of [false, true]) {
		describe(nativeGraph ? "native graph" : "JS graph", () => {
			let directory: string;
			let peers: Peerbit[] = [];
			let source: Documents<Document>;
			let target: Documents<Document>;
			beforeEach(async () => {
				directory = await mkdtemp(join(tmpdir(), "peerbit-proof-order-"));
				for (const name of ["source", "target"])
					peers.push(
						await Peerbit.create({ directory: join(directory, name) }),
					);
				const args = { type: Document, nativeGraph, timeUntilRoleMaturity: 0 };
				source = await peers[0]!.open(
					new Documents<Document>({
						format: {
							version: "owner-proofs-v1",
							owner: peers[0]!.identity.publicKey,
							keyType: "string",
						},
					}),
					{ args },
				);
				target = await peers[1]!.open(
					deserialize(
						serialize(source),
						Documents,
					) as unknown as Documents<Document>,
					{ args },
				);
				for (const docs of [source, target])
					expect(Boolean(docs.log.log.entryIndex.properties.nativeGraph)).equal(
						nativeGraph,
					);
			});
			afterEach(async () => {
				await Promise.all(peers.map((peer) => peer.stop()));
				peers = [];
				await rm(directory, { recursive: true, force: true });
			});
			for (const order of [
				["cut", "replacement"],
				["replacement", "cut"],
				["original", "cut", "replacement"],
				["original", "replacement", "cut"],
				["cut", "original", "replacement"],
				["cut", "replacement", "original"],
				["replacement", "original", "cut"],
				["replacement", "cut", "original"],
			] as const) {
				it(`retains exact retirement after ${order.join(" -> ")} and subsequent replacement deletion`, async () => {
					const original = await source.put(
						new Document({ id: "a", name: "original" }),
					);
					const cut = await source.del("a");
					const replacement = await source.put(
						new Document({ id: "a", name: "replacement" }),
						{ unique: true },
					);
					const entries = { original, cut, replacement };
					for (const name of order)
						await target.log.join([entries[name].entry], {
							verifySignatures: true,
						});
					expect(
						(await target.get("a", { local: true, remote: false }))?.name,
					).equal("replacement");
					expect(await target.log.log.has(original.entry.hash)).false;
					expect(await target.log.log.has(cut.entry.hash)).true;
					await target.log.join([original.entry, cut.entry], {
						verifySignatures: true,
					});
					expect(
						(await target.get("a", { local: true, remote: false }))?.name,
					).equal("replacement");
					const removedReplacement = await source.del("a");
					await target.log.join([removedReplacement.entry], {
						verifySignatures: true,
					});
					await target.log.join([original.entry, replacement.entry], {
						verifySignatures: true,
					});
					expect(await target.get("a", { local: true, remote: false }))
						.undefined;
					expect(target.log.log.length).equal(2);
				});
			}

			it("never retires or follows a cross-document boundary", async () => {
				const a = await source.put(new Document({ id: "a", name: "keep" }));
				const b = await source.log.append(
					new ProofPutOperation({
						scope: deriveProofDeleteScope(getProofDeleteContract(source.log)!),
						key: toId("b"),
						data: serialize(new Document({ id: "b", name: "child of a" })),
					}),
					{ meta: { next: [a.entry] } },
				);
				const b2 = await source.put(new Document({ id: "b", name: "updated" }));
				const cut = await source.del("b");
				expect(cut.entry.meta.next).to.have.members([
					b.entry.hash,
					b2.entry.hash,
				]);
				expect(
					(await source.get("a", { local: true, remote: false }))?.name,
				).equal("keep");
				await target.log.join([cut.entry], { verifySignatures: true });
				// A is authenticated boundary evidence, not an implicitly admitted value.
				expect(await target.get("a", { local: true, remote: false })).undefined;
				await target.log.join([a.entry, b.entry, b2.entry], {
					verifySignatures: true,
				});
				expect(
					(await target.get("a", { local: true, remote: false }))?.name,
				).equal("keep");
				expect(await target.get("b", { local: true, remote: false })).undefined;
				expect(target.log.log.length).equal(2);
			});
		});
	}
});
