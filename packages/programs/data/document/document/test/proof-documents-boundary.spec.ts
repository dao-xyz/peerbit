import { deserialize, serialize } from "@dao-xyz/borsh";
import { toId } from "@peerbit/indexer-interface";
import { expect } from "chai";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import pDefer from "p-defer";
import { Peerbit } from "peerbit";
import sinon from "sinon";
import { Documents } from "../src/program.js";
import {
	deriveProofDeleteScope,
	getProofDeleteContract,
} from "../src/proof-delete-manifest.js";
import { ProofPutOperation } from "../src/proof-operations.js";
import { Document } from "./data.js";

// These direct opening negatives never reach Program.beforeOpen. Do not
// register a Borsh subclass globally: that changes the base-class decoder.
class CustomCanAppend extends Documents<Document> {
	protected override async canAppend() {
		return true;
	}
}

class CustomInternalCanAppend extends Documents<Document> {
	protected override async _canAppend() {
		return false as const;
	}
}

describe("owner-proofs Documents admission boundaries", () => {
	for (const nativeGraph of [false, true]) {
		describe(nativeGraph ? "native graph" : "JS graph", () => {
			let directory: string;
			let peer: Peerbit;
			let docs: Documents<Document>;
			const args = { type: Document, nativeGraph, timeUntilRoleMaturity: 0 };
			const format = () => ({
				version: "owner-proofs-v1" as const,
				owner: peer.identity.publicKey,
				keyType: "string" as const,
			});
			beforeEach(async () => {
				directory = await mkdtemp(join(tmpdir(), "peerbit-proof-boundary-"));
				peer = await Peerbit.create({ directory });
				docs = new Documents<Document>({ format: format() });
			});
			afterEach(async () => {
				sinon.restore();
				await docs?.close();
				await peer?.stop();
				await rm(directory, { recursive: true, force: true });
			});

			it("blocks public and lower writes after late opening failure until cleanup and reopen", async () => {
				await docs.beforeOpen(peer);
				const failure = new Error(
					"query activation failed after owner recovery",
				);
				const activation = sinon
					.stub(docs.index as any, "finishDeferredQueryOpen")
					.rejects(failure);
				await expect(docs.open(args)).rejectedWith(failure);
				const operation = new ProofPutOperation({
					scope: deriveProofDeleteScope(getProofDeleteContract(docs.log)!),
					key: toId("blocked"),
					data: serialize(new Document({ id: "blocked" })),
				});
				await expect(docs.put(new Document({ id: "blocked" }))).rejected;
				await expect(docs.log.append(operation, { meta: { next: [] } }))
					.rejected;
				await expect(docs.log.log.append(operation, { meta: { next: [] } }))
					.rejected;
				await expect(docs.count()).rejected;
				await expect(docs.open(args)).rejectedWith("terminal cleanup");
				expect(docs.log.log.length).equal(0);
				activation.restore();
				await docs.close();
				docs = await peer.open(docs, { args });
				await docs.put(new Document({ id: "healthy" }));
				expect(await docs.count()).equal(1);
			});

			it("rejects both application admission overrides before opening the lower log", async () => {
				for (const Type of [CustomCanAppend, CustomInternalCanAppend]) {
					const custom = new Type({ format: format() });
					const lowerOpen = sinon.spy(custom.log as any, "openWithRecovery");
					await expect(custom.open(args)).rejectedWith(
						"fixed owner authorization",
					);
					expect(lowerOpen.called).equal(false);
					expect(custom.closed).equal(true);
				}
			});

			it("rejects terminal reentrancy from durable document change notification", async () => {
				docs = await peer.open(docs, { args });
				const terminal: Promise<{ value?: boolean; error?: unknown }>[] = [];
				docs.events.addEventListener("change", () => {
					for (const operation of [docs.close(), docs.drop()])
						terminal.push(
							operation.then(
								(value) => ({ value }),
								(error) => ({ error }),
							),
						);
				});
				const writing = await Promise.allSettled([
					docs.put(new Document({ id: "a" })),
				]);
				const outcomes = await Promise.all(terminal);
				expect(terminal).to.have.length(2);
				for (const outcome of outcomes) {
					expect(outcome.value).equal(undefined);
					expect(String(outcome.error)).contains("callback");
				}
				expect(writing[0]!.status).equal("fulfilled");
				expect(docs.closed).equal(false);
				expect(await docs.count()).equal(1);
			});

			it("rejects external terminal cleanup while a projection callback is pending and permits a safe retry", async () => {
				docs = await peer.open(docs, { args });
				const entered = pDefer();
				const release = pDefer();
				const original = docs.index.putWithContext.bind(docs.index);
				sinon
					.stub(docs.index, "putWithContext")
					.callsFake(async (...values) => {
						entered.resolve();
						await release.promise;
						return original(...values);
					});
				const writing = docs.put(new Document({ id: "a" }));
				await entered.promise;
				try {
					await expect(docs.close()).rejectedWith("callback");
					expect(docs.closed).equal(false);
					release.resolve();
					await writing;
					await docs.close();
					expect(docs.closed).equal(true);
				} finally {
					release.resolve();
					await writing.catch(() => undefined);
				}
			});

			it("drains a physical block write before external terminal cleanup", async () => {
				docs = await peer.open(docs, { args });
				const manifest = serialize(docs);
				const entered = pDefer();
				const release = pDefer();
				const store = docs.log.log.entryIndex.properties.store;
				const original = store.put.bind(store);
				const pause = sinon.stub(store, "put").callsFake(async (...values) => {
					entered.resolve();
					await release.promise;
					return original(...values);
				});
				const writing = docs
					.put(new Document({ id: "a" }), {
						delivery: { reliability: "persisted", minAcks: 1, timeout: 5_000 },
					})
					.then(
						(value) => ({ value, error: undefined }),
						(error) => ({ value: undefined, error }),
					);
				await entered.promise;
				let settled = false;
				const closing = docs.close().then(() => {
					settled = true;
				});
				try {
					await Promise.resolve();
					expect(settled).equal(false);
					release.resolve();
					const outcome = await writing;
					await closing;
					expect(outcome.value).equal(undefined);
					expect(outcome.error?.name).equal("PersistedDeliveryError");
					expect(outcome.error.committedHashes).to.have.length(1);
					expect(settled).equal(true);
					expect(docs.closed).equal(true);
					pause.restore();
					await peer.stop();
					peer = await Peerbit.create({ directory });
					docs = await peer.open(
						deserialize(manifest, Documents) as unknown as Documents<Document>,
						{ args },
					);
					expect(peer.libp2p.getConnections()).to.have.length(0);
					expect(
						(await docs.get("a", { local: true, remote: false }))?.id,
					).equal("a");
					expect(
						await docs.log.log.has(outcome.error.committedHashes[0]),
					).equal(true);
				} finally {
					release.resolve();
					await writing.catch(() => undefined);
					await closing;
				}
			});
		});
	}
});
