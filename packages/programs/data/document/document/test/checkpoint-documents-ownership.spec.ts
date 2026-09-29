import { Ed25519PublicKey, type Identity } from "@peerbit/crypto";
import type { SharedLog } from "@peerbit/shared-log";
import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Peerbit } from "peerbit";
import { CheckpointDocuments } from "../src/checkpoint-documents.js";
import type { Operation } from "../src/operation.js";
import { DocumentIndex } from "../src/search.js";

const local = { local: true, remote: false } as const;
const shared = (docs: CheckpointDocuments) =>
	(docs as unknown as { shared: SharedLog<Operation, any, any> }).shared;

describe("checkpoint Documents ownership", function () {
	this.timeout(60_000);
	let peer: Peerbit;
	let directory: string;
	beforeEach(async () => {
		directory = await mkdtemp(join(tmpdir(), "peerbit-checkpoint-ownership-"));
		peer = await Peerbit.create({ directory });
	});
	afterEach(async () => {
		await peer.stop();
		await rm(directory, { recursive: true, force: true });
	});
	const resource = async () => {
		assert(peer.identity.publicKey instanceof Ed25519PublicKey);
		const identity = peer.identity as Identity<Ed25519PublicKey>;
		const docs = new CheckpointDocuments({ owner: identity.publicKey });
		const checkpoint = await docs.createGenesis(peer.services.blocks, identity);
		return peer.open(docs, { args: { checkpoint } });
	};
	const attached = async () => {
		const docs = await resource();
		const parent = await resource();
		assert.equal(await peer.open(docs, { parent, existing: "reuse" }), docs);
		assert.deepEqual(docs.parents, [undefined, parent]);
		await docs.put({ id: "retained", name: "before" });
		return { docs, parent };
	};

	for (const release of ["parent", "root"] as const) {
		it(`keeps the remaining owner ready after a nonterminal ${release} release`, async () => {
			const { docs, parent } = await attached();
			assert.equal(
				await docs.close(release === "parent" ? parent : undefined),
				false,
			);
			assert.equal(docs.closed, false);
			assert.equal(docs.status, "ready");
			assert.equal(shared(docs).closed, false);
			assert.deepEqual(docs.parents, [
				release === "parent" ? undefined : parent,
			]);
			assert.equal(
				(await docs.index.get("retained", local)).decode().name,
				"before",
			);
			await docs.put({ id: "retained", name: "after" });
			assert.equal(
				(await docs.index.get("retained", local)).decode().name,
				"after",
			);
		});
	}

	it("rejects a seal with multiple owners before fencing or releasing any owner", async () => {
		const { docs, parent } = await attached();
		const checkpoint = docs.currentCheckpoint;
		await assert.rejects(docs.prepareCheckpoint(), /owner|exclusive|parent/i);
		assert.deepEqual(docs.parents, [undefined, parent]);
		assert.equal(docs.closed, false);
		assert.equal(docs.status, "ready");
		assert.equal(shared(docs).closed, false);
		assert.equal(docs.currentCheckpoint, checkpoint);
		await docs.put({ id: "retained", name: "not frozen" });
		assert.equal(
			(await docs.index.get("retained", local)).decode().name,
			"not frozen",
		);
	});

	it("rejects an unrelated owner without poisoning the active resource", async () => {
		const docs = await resource();
		const unrelated = await resource();
		await assert.rejects(docs.close(unrelated), /parents/);
		assert.equal(docs.status, "ready");
		assert.equal(docs.closed, false);
		await docs.put({ id: "valid", name: "still ready" });
		assert.equal(
			(await docs.index.get("valid", local)).decode().name,
			"still ready",
		);
	});

	it("rejects unsupported drop without deleting accepted epoch evidence", async () => {
		const docs = await resource();
		const head = await docs.put({ id: "durable", name: "keep" });
		const bytes = await shared(docs).log.blocks.get(head, { remote: false });
		assert(bytes);
		await assert.rejects(docs.drop(), /unsupported|not supported/i);
		assert.equal(docs.closed, false);
		assert.equal(docs.status, "ready");
		assert.deepEqual(
			await shared(docs).log.blocks.get(head, { remote: false }),
			bytes,
		);
		assert.equal((await docs.index.get("durable", local)).__context.head, head);
	});

	it("rejects extra delivery options before committing or poisoning writes", async () => {
		const docs = await resource();
		const head = await docs.put({ id: "durable", name: "original" });
		const legacy = docs as unknown as {
			put(
				value: { id: string; name: string },
				options: unknown,
			): Promise<string>;
			del(key: string, options: unknown): Promise<string>;
		};
		for (const options of [{ delivery: { minReplicas: 2 } }, {}, undefined]) {
			assert.throws(
				() => legacy.put({ id: "durable", name: "must not commit" }, options),
				/does not accept delivery options/,
			);
			assert.throws(
				() => legacy.del("durable", options),
				/does not accept delivery options/,
			);
		}
		assert.equal(docs.status, "ready");
		assert.equal(shared(docs).log.length, 1);
		assert.equal((await docs.index.get("durable", local)).__context.head, head);
		await docs.put({ id: "durable", name: "supported" });
		assert.equal(
			(await docs.index.get("durable", local)).decode().name,
			"supported",
		);
	});

	it("serves local queries without opening a query RPC on open or reopen", async () => {
		type LocalIndex = {
			_query: { open(...args: unknown[]): Promise<void> };
			openForDocuments(...args: unknown[]): Promise<void>;
		};
		const prototype = DocumentIndex.prototype as unknown as LocalIndex;
		const original = prototype.openForDocuments;
		const restoreQueries: Array<() => void> = [];
		let queryOpens = 0;
		prototype.openForDocuments = async function (...args) {
			const query = this._query;
			const open = query.open;
			query.open = async () => {
				queryOpens++;
				throw new Error("Checkpoint local index must not start a query RPC");
			};
			restoreQueries.push(() => {
				query.open = open;
			});
			await original.apply(this, args);
		};
		const assertLocalQueries = async (docs: CheckpointDocuments) => {
			assert.equal(docs.status, "ready");
			assert.equal((await docs.index.get("local", local)).decode().name, "row");
			assert.equal(await docs.index.getSize(), 1);
			const searched = await docs.index.search({ query: [] }, local);
			assert.deepEqual(
				searched.map((row) => row.decode()),
				[{ id: "local", name: "row" }],
			);
			const iterator = docs.index.iterate({ query: [] }, local);
			try {
				assert.deepEqual(
					(await iterator.all()).map((row) => row.decode()),
					[{ id: "local", name: "row" }],
				);
			} finally {
				await iterator.close();
			}
			assert.deepEqual(docs.getTopics(), shared(docs).rpc.getTopics());
		};
		try {
			const docs = await resource();
			await docs.put({ id: "local", name: "row" });
			await assertLocalQueries(docs);
			await docs.close();
			const reopened = await peer.open<CheckpointDocuments>(docs.address);
			await assertLocalQueries(reopened);
			assert.equal(restoreQueries.length, 2);
			assert.equal(queryOpens, 0);
		} finally {
			prototype.openForDocuments = original;
			for (const restore of restoreQueries) restore();
		}
	});
});
