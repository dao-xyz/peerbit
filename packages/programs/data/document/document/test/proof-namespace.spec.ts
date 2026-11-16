import { deserialize, serialize } from "@dao-xyz/borsh";
import { SharedLog } from "@peerbit/shared-log";
import { expect } from "chai";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Peerbit } from "peerbit";
import sinon from "sinon";
import { Documents } from "../src/program.js";
import { getProofDeleteContract } from "../src/proof-delete-manifest.js";
import { Document } from "./data.js";

const local = { local: true, remote: false } as const;
const strip = (proof: Documents<Document>) => {
	const legacy = new Documents<Document>();
	legacy.log = deserialize(
		serialize(proof.log),
		SharedLog,
	) as typeof legacy.log;
	expect(getProofDeleteContract(legacy.log)).undefined;
	expect(legacy.log.log.id).deep.equal(proof.log.log.id);
	return legacy;
};

// Same-process lifecycle ownership only: this is not an access-control boundary
// against an application that directly modifies its own offline database files.
describe("owner-proofs namespace ownership", () => {
	for (const nativeGraph of [false, true]) {
		describe(nativeGraph ? "native graph" : "JS graph", () => {
			let directory: string;
			let peer: Peerbit;
			const args = { type: Document, nativeGraph, timeUntilRoleMaturity: 0 };
			const createProof = () =>
				new Documents<Document>({
					format: {
						version: "owner-proofs-v1",
						owner: peer.identity.publicKey,
						keyType: "string",
					},
				});
			beforeEach(async () => {
				directory = await mkdtemp(join(tmpdir(), "peerbit-proof-scope-"));
				peer = await Peerbit.create({ directory });
			});
			afterEach(async () => {
				sinon.restore();
				await peer.stop();
				await rm(directory, { recursive: true, force: true });
			});

			const assertHealthy = async (docs: Documents<Document>) => {
				const committed = await docs.put(new Document({ id: "healthy" }));
				// Ordinary native stores may retain admitted rows in pending native
				// facts; use their authoritative read, not the raw SQLite projection.
				expect(await docs.log.log.getShallow(committed.entry.hash)).to.exist;
				expect((await docs.get("healthy", local))?.id).equal("healthy");
			};
			const reopen = async (docs: Documents<Document>) => {
				const manifest = serialize(docs);
				await docs.close();
				return peer.open(
					deserialize(manifest, Documents) as unknown as Documents<Document>,
					{ args },
				);
			};

			it("rejects a stripped legacy view before index opening and preserves the proof owner", async () => {
				let proof = await peer.open(createProof(), { args });
				const legacy = strip(proof);
				const prepare = sinon.spy(legacy.index, "open");
				await expect(peer.open(legacy, { args })).rejectedWith(
					/namespace|projection/i,
				);
				expect(prepare.called).equal(false);
				await assertHealthy(proof);
				proof = await reopen(proof);
				expect((await proof.get("healthy", local))?.id).equal("healthy");
				await assertHealthy(proof);
			});

			it("rejects a proof view while a legacy namespace is live and preserves that legacy owner", async () => {
				const proof = createProof();
				let legacy = await peer.open(strip(proof), { args });
				const prepare = sinon.spy(proof.index as any, "openWithDeferredQuery");
				await expect(peer.open(proof, { args })).rejectedWith(
					/namespace|projection/i,
				);
				expect(prepare.called).equal(false);
				await assertHealthy(legacy);
				legacy = await reopen(legacy);
				expect((await legacy.get("healthy", local))?.id).equal("healthy");
				await assertHealthy(legacy);
			});

			it("releases an empty legacy reservation after complete close so the proof owner can open", async () => {
				const proof = createProof();
				const legacy = await peer.open(strip(proof), { args });
				await expect(peer.open(proof, { args })).rejectedWith(
					/namespace|projection/i,
				);
				await legacy.close();
				const opened = await peer.open(proof, { args });
				await assertHealthy(opened);
			});

			it("keeps a failed opening's original claim across an ID-change retry until cleanup", async () => {
				const proof = createProof();
				const legacy = strip(proof);
				await legacy.beforeOpen(peer);
				const failure = new Error("injected index opening failure");
				const prepare = sinon.stub(legacy.index, "open").rejects(failure);
				try {
					await expect(legacy.open(args)).rejectedWith(failure);
					expect(prepare.callCount).equal(1);
					legacy.log.log.id = new Uint8Array(32).fill(200);
					await expect(legacy.open(args)).rejectedWith("terminal cleanup");
					expect(prepare.callCount).equal(1);
					await expect(peer.open(proof, { args })).rejectedWith(
						/namespace|projection/i,
					);
				} finally {
					prepare.restore();
					await legacy.close();
				}
				await assertHealthy(await peer.open(proof, { args }));
			});

			it("rejects a standalone stripped SharedLog before lower opening", async () => {
				const proof = await peer.open(createProof(), { args });
				const alias = strip(proof).log;
				// Direct child lifecycle avoids Handler's separate root-address check.
				await alias.beforeOpen(peer);
				const lowerOpen = sinon.spy(alias.log, "open");
				try {
					await expect(
						alias.open({ replicate: 1, nativeGraph, nativeBackbone: false }),
					).rejectedWith(/namespace|projection/i);
					expect(lowerOpen.called).equal(false);
					await assertHealthy(proof);
				} finally {
					await alias.close();
				}
				const reopened = await reopen(proof);
				expect((await reopened.get("healthy", local))?.id).equal("healthy");
			});

			it("rejects a second protected projection even when both views share one SharedLog object", async () => {
				const proof = await peer.open(createProof(), { args });
				const other = new Documents<Document>();
				other.log = proof.log;
				// Exercise the child lifecycle directly, without registering a test
				// subclass that would change the global Documents decoder dispatch.
				await other.beforeOpen(peer);
				const prepare = sinon.spy(other.index as any, "openWithDeferredQuery");
				try {
					await expect(other.open(args)).rejectedWith(/namespace|projection/i);
					expect(prepare.called).equal(false);
					await assertHealthy(proof);
				} finally {
					await other.close();
				}
				const reopened = await reopen(proof);
				expect((await reopened.get("healthy", local))?.id).equal("healthy");
			});
		});
	}
});
