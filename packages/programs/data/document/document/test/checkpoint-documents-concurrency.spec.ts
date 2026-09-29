import { Ed25519PublicKey, type Identity } from "@peerbit/crypto";
import type { SharedLog } from "@peerbit/shared-log";
import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import pDefer from "p-defer";
import { Peerbit } from "peerbit";
import { CheckpointDocuments } from "../src/checkpoint-documents.js";
import type { Operation } from "../src/operation.js";

const local = { local: true, remote: false } as const;
const identity = (peer: Peerbit): Identity<Ed25519PublicKey> => {
	assert(peer.identity.publicKey instanceof Ed25519PublicKey);
	return peer.identity as Identity<Ed25519PublicKey>;
};
const settle = <T>(promise: Promise<T>) =>
	promise.then(
		(value) => ({ value, error: undefined }),
		(error: unknown) => ({ value: undefined, error }),
	);

describe("checkpoint Documents transition concurrency", function () {
	this.timeout(60_000);
	let peer: Peerbit | undefined;
	let directory: string | undefined;

	const open = async () => {
		directory = await mkdtemp(
			join(tmpdir(), "peerbit-checkpoint-concurrency-"),
		);
		peer = await Peerbit.create({ directory });
		const resource = new CheckpointDocuments({
			owner: identity(peer).publicKey,
		});
		const genesis = await resource.createGenesis(
			peer.services.blocks,
			identity(peer),
		);
		const docs = await peer.open(resource, { args: { checkpoint: genesis } });
		return { peer, docs };
	};

	afterEach(async () => {
		await peer?.stop();
		peer = undefined;
		if (directory) await rm(directory, { recursive: true, force: true });
		directory = undefined;
	});

	it("rejects invalid requests without faulting or reopening the active resource", async () => {
		const { docs } = await open();
		assert.throws(() => docs.del(""), /key/i);
		assert.equal(docs.status, "ready");
		await assert.rejects(
			docs.open({ nativeGraph: true } as unknown as Parameters<
				typeof docs.open
			>[0]),
			/Unsupported Checkpoint Documents open option/,
		);
		assert.equal(docs.status, "ready");
		await assert.rejects(docs.open(), /already active/);
		await docs.put({ id: "still-ready", name: "not faulted" });
		assert.deepEqual((await docs.index.get("still-ready", local)).decode(), {
			id: "still-ready",
			name: "not faulted",
		});
	});

	it("drains already admitted queued puts, rejects later writes and overlapping seals, and pins one proposal", async () => {
		const { peer, docs } = await open();
		const shared = (
			docs as unknown as { shared: SharedLog<Operation, any, any> }
		).shared;
		const append = shared.append;
		const entered = pDefer<void>();
		const release = pDefer<void>();
		let appendCalls = 0;
		shared.append = async function (...args) {
			if (++appendCalls === 1) {
				entered.resolve();
				await release.promise;
			}
			return append.apply(this, args);
		};
		const writes = settle(
			Promise.all([
				docs.put({ id: "first", name: "admitted first" }),
				docs.put({ id: "queued", name: "admitted while queued" }),
			]),
		);
		let preparation: ReturnType<typeof settle<string>> | undefined;
		let proposal: string | undefined;
		try {
			await entered.promise;
			assert.equal(appendCalls, 1);
			preparation = settle(docs.prepareCheckpoint());
			assert.throws(
				() => docs.put({ id: "late", name: "must not commit" }),
				/not ready/,
			);
			await assert.rejects(
				docs.prepareCheckpoint(),
				/transition is already in progress/,
			);
			assert.equal(appendCalls, 1);
			release.resolve();
			const committed = await writes;
			if (committed.error) throw committed.error;
			assert.equal(committed.value?.length, 2);
			const prepared = await preparation;
			if (prepared.error) throw prepared.error;
			proposal = prepared.value;
			assert.equal(appendCalls, 2);
		} finally {
			release.resolve();
			await writes;
			await preparation;
			shared.append = append;
		}
		assert(proposal);
		assert.equal(docs.status, "frozen");
		assert.equal(await docs.prepareCheckpoint(), proposal);
		const approval = await docs.approveCheckpoint(proposal);
		const checkpoint = await docs.publishCheckpoint(proposal, [approval]);
		const reopened = await peer.open(docs, { args: { checkpoint } });
		assert.equal(reopened, docs);
		assert.deepEqual((await reopened.index.get("first", local)).decode(), {
			id: "first",
			name: "admitted first",
		});
		assert.deepEqual((await reopened.index.get("queued", local)).decode(), {
			id: "queued",
			name: "admitted while queued",
		});
		assert.equal(await reopened.index.get("late", local), undefined);
	});

	it("invalidates captured iterators across freeze and same-instance successor reopen", async () => {
		const { peer, docs } = await open();
		for (let index = 0; index < 3; index++) {
			await docs.put({ id: `row-${index}`, name: `value-${index}` });
		}
		const iterator = docs.index.iterate({}, local);
		try {
			assert.equal((await iterator.next(1)).length, 1);
			const proposal = await docs.prepareCheckpoint();
			await assert.rejects(async () => iterator.next(1), /not ready/);
			await assert.rejects(async () => iterator.all(), /not ready/);
			const checkpoint = await docs.publishCheckpoint(proposal, [
				await docs.approveCheckpoint(proposal),
			]);
			assert.equal(await peer.open(docs, { args: { checkpoint } }), docs);
			assert.equal(docs.status, "ready");
			await assert.rejects(
				async () => iterator.next(1),
				/closed checkpoint session/,
			);
			await assert.rejects(
				async () => iterator.all(),
				/closed checkpoint session/,
			);
			const fresh = docs.index.iterate({}, local);
			try {
				assert.equal((await fresh.all()).length, 3);
			} finally {
				await fresh.close();
			}
		} finally {
			await iterator.close();
		}
	});
});
