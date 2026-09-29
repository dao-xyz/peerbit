import { deserialize } from "@dao-xyz/borsh";
import { Ed25519PublicKey, type Identity, sha256Sync } from "@peerbit/crypto";
import {
	Entry,
	EntryType,
	LamportClock,
	Timestamp,
	createEntry,
} from "@peerbit/log";
import {
	AbsoluteReplicas,
	type SharedLog,
	encodeReplicas,
} from "@peerbit/shared-log";
import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Peerbit } from "peerbit";
import {
	createCheckpointCertificate,
	signCheckpointApproval,
} from "../src/checkpoint-certificate.js";
import { CheckpointDocuments } from "../src/checkpoint-documents.js";
import {
	CheckpointDocument,
	CheckpointOperation,
} from "../src/checkpoint-operation.js";
import {
	type CheckpointSnapshot,
	createCheckpointSnapshot,
	readCheckpointSnapshot,
} from "../src/checkpoint-snapshot.js";
import { BORSH_ENCODING_OPERATION, type Operation } from "../src/operation.js";

const local = { local: true, remote: false } as const;
const encoder = new TextEncoder();
const identity = (peer: Peerbit): Identity<Ed25519PublicKey> => {
	assert(peer.identity.publicKey instanceof Ed25519PublicKey);
	return peer.identity as Identity<Ed25519PublicKey>;
};
type Fact = {
	cid: string;
	bytes: Uint8Array;
	entry: Entry<Operation>;
	operation: CheckpointOperation;
	created: bigint;
};
// Observe exact admitted state and inject signed remote entries; these handles
// are deliberately not part of the public checkpoint resource API.
const state = (docs: CheckpointDocuments) =>
	docs as unknown as {
		resource: Uint8Array;
		checkpoint: CheckpointSnapshot;
		checkpointDigest: Uint8Array;
		shared: SharedLog<Operation, any, any>;
		frontier: Map<string, Map<string, Fact>>;
	};

describe("checkpoint Documents tombstone compaction", function () {
	this.timeout(60_000);
	const peers: Peerbit[] = [];
	const directories: string[] = [];
	const createPeer = async () => {
		const directory = await mkdtemp(
			join(tmpdir(), "peerbit-checkpoint-compaction-"),
		);
		directories.push(directory);
		const peer = await Peerbit.create({ directory });
		peers.push(peer);
		return peer;
	};
	afterEach(async () => {
		for (const peer of peers.splice(0).reverse()) await peer.stop();
		for (const directory of directories.splice(0))
			await rm(directory, { recursive: true, force: true });
	});
	const openOwner = async () => {
		const peer = await createPeer();
		const resource = new CheckpointDocuments({
			owner: identity(peer).publicKey,
		});
		const genesis = await resource.createGenesis(
			peer.services.blocks,
			identity(peer),
		);
		const docs = await peer.open(resource, { args: { checkpoint: genesis } });
		return { peer, resource, genesis, docs };
	};
	const copyBlocks = async (source: Peerbit, target: Peerbit) => {
		for await (const [, bytes] of source.services.blocks.iterator())
			await target.services.blocks.put(bytes);
	};
	const seal = async (peer: Peerbit, docs: CheckpointDocuments) => {
		const address = docs.address;
		const proposal = await docs.prepareCheckpoint();
		const approval = await docs.approveCheckpoint(proposal);
		const checkpoint = await docs.publishCheckpoint(proposal, [approval]);
		return peer.open<CheckpointDocuments>(address, { args: { checkpoint } });
	};
	const signed = async (
		peer: Peerbit,
		docs: CheckpointDocuments,
		options: {
			key: string;
			kind?: 0 | 1;
			name?: string;
			time: bigint;
			parents?: Entry<Operation>[];
		},
	) => {
		const internal = state(docs);
		const entry = await createEntry<Operation>({
			store: peer.services.blocks,
			identity: identity(peer),
			encoding: BORSH_ENCODING_OPERATION,
			data: new CheckpointOperation({
				resource: internal.resource,
				epoch: internal.checkpoint.epoch,
				checkpoint: internal.checkpointDigest,
				kind: options.kind ?? 0,
				key: options.key,
				data:
					options.kind === 1
						? new Uint8Array()
						: CheckpointDocument.from({
								id: options.key,
								name: options.name ?? "value",
							}).value,
			}),
			meta: {
				type: EntryType.APPEND,
				data: encodeReplicas(new AbsoluteReplicas(1)),
				gidSeed: sha256Sync(encoder.encode(options.key)),
				next: options.parents ?? [],
				clock: new LamportClock({
					id: identity(peer).publicKey.bytes,
					timestamp: new Timestamp({ wallTime: options.time }),
				}),
			},
		});
		const remote = deserialize(
			new Uint8Array(entry.getStorageBytes()),
			Entry,
		) as Entry<Operation>;
		remote.hash = entry.hash;
		remote.init(internal.shared.log);
		remote.createdLocally = false;
		return remote;
	};

	it("keeps repeated empty churn checkpoints empty instead of accumulating deleted keys", async () => {
		const opened = await openOwner();
		let docs = opened.docs;
		for (let round = 0; round < 3; round++) {
			for (const id of ["reused", `round-${round}`]) {
				await docs.put({ id, name: `written-${round}` });
				await docs.del(id);
			}
			assert.equal(await docs.index.getSize(), 0);
			assert(state(docs).frontier.size > 0);
			docs = await seal(opened.peer, docs);
			assert.equal(state(docs).checkpoint.count, 0);
			assert.equal(state(docs).frontier.size, 0);
			assert.equal(await docs.index.getSize(), 0);
		}
		const recipient = await createPeer();
		await copyBlocks(opened.peer, recipient);
		const imported = await recipient.open(opened.resource.clone(), {
			args: { checkpoint: docs.currentCheckpoint },
		});
		assert.equal(state(imported).checkpoint.count, 0);
		assert.equal(await imported.index.getSize(), 0);
	});

	it("assigns a new creation timestamp when a key is recreated after only DELETE parents", async () => {
		const { docs } = await openOwner();
		await docs.put({ id: "key", name: "original" });
		const original = await docs.index.get("key", local);
		await docs.del("key");
		await docs.put({ id: "key", name: "recreated" });
		const recreated = await docs.index.get("key", local);
		assert.equal(recreated.__context.created, recreated.__context.modified);
		assert(recreated.__context.created > original.__context.created);
	});

	it("gives equivalent recreation graphs the same creation context before and after a seal", async () => {
		const { peer: owner, docs: after, resource, genesis } = await openOwner();
		const recipient = await createPeer();
		await copyBlocks(owner, recipient);
		const before = await recipient.open(resource.clone(), {
			args: { checkpoint: genesis },
		});
		const a = await signed(owner, after, { key: "key", time: 1000n });
		const d = await signed(owner, after, {
			key: "key",
			kind: 1,
			time: 2000n,
			parents: [a],
		});
		for (const docs of [before, after]) {
			for (const entry of [a, d]) await state(docs).shared.join([entry]);
			assert.equal(await docs.index.get("key", local), undefined);
		}
		const oldEpoch = await signed(owner, before, {
			key: "key",
			name: "recreated",
			time: 3000n,
			parents: [d],
		});
		await state(before).shared.join([oldEpoch]);
		const reopened = await seal(owner, after);
		assert.equal(state(reopened).checkpoint.count, 0);
		const nextEpoch = await signed(owner, reopened, {
			key: "key",
			name: "recreated",
			time: 3000n,
		});
		await state(reopened).shared.join([nextEpoch]);
		const left = await before.index.get("key", local);
		const right = await reopened.index.get("key", local);
		assert.deepEqual(left.decode(), right.decode());
		for (const context of [left.__context, right.__context]) {
			assert.equal(context.created, 3000n);
			assert.equal(context.modified, 3000n);
		}
		assert.equal(left.__context.gid, right.__context.gid);
		assert.equal(left.__context.size, right.__context.size);
		assert.notEqual(left.__context.head, right.__context.head);
	});

	it("retains every mixed PUT/DELETE frontier tip even when the DELETE wins projection", async () => {
		const { peer, docs } = await openOwner();
		const a = await signed(peer, docs, {
			key: "mixed",
			time: 1000n,
			name: "A",
		});
		const b = await signed(peer, docs, {
			key: "mixed",
			time: 2000n,
			name: "B",
			parents: [a],
		});
		const c = await signed(peer, docs, {
			key: "mixed",
			time: 2500n,
			name: "C",
			parents: [a],
		});
		const d = await signed(peer, docs, {
			key: "mixed",
			kind: 1,
			time: 3000n,
			parents: [b],
		});
		for (const entry of [a, b, c, d]) await state(docs).shared.join([entry]);
		assert.equal(await docs.index.get("mixed", local), undefined);
		const reopened = await seal(peer, docs);
		assert.equal(state(reopened).checkpoint.count, 2);
		assert.deepEqual(
			[...state(reopened).frontier.get("mixed")!.keys()].sort(),
			[c.hash, d.hash].sort(),
		);
		assert.equal(await reopened.index.get("mixed", local), undefined);
		await reopened.put({ id: "mixed", name: "merge" });
		assert.equal(
			(await reopened.index.get("mixed", local)).__context.created,
			1000n,
		);
	});

	it("keeps the live creation anchor when a bounded local append cannot name all tips", async () => {
		const { peer, docs } = await openOwner();
		// Select signed roots by their actual CIDs, rather than assuming a hash
		// order. Only the selected PUT and 33 preceding DELETEs are admitted.
		const puts: Entry<Operation>[] = [];
		for (let i = 0; i < 64; i++)
			puts.push(
				await signed(peer, docs, {
					key: "many",
					name: `candidate-${i}`,
					time: 1000n,
				}),
			);
		puts.sort((a, b) => (a.hash < b.hash ? -1 : a.hash === b.hash ? 0 : 1));
		const live = puts.at(-1)!;
		const deletes: Entry<Operation>[] = [];
		for (let i = 0; i < 128; i++) {
			const entry = await signed(peer, docs, {
				key: "many",
				kind: 1,
				time: BigInt(2000 + i),
			});
			if (entry.hash < live.hash) deletes.push(entry);
		}
		deletes.sort((a, b) => (a.hash < b.hash ? -1 : a.hash === b.hash ? 0 : 1));
		assert(
			deletes.length >= 33,
			"Generated roots must exercise the live anchor beyond the first 32 CIDs",
		);
		for (const entry of [...deletes.slice(0, 33), live])
			await state(docs).shared.join([entry]);
		assert.equal(state(docs).frontier.get("many")!.size, 34);
		const ordered = [...state(docs).frontier.get("many")!.keys()].sort();
		assert.equal(ordered.indexOf(live.hash), 33);
		const head = await docs.put({ id: "many", name: "before-seal" });
		const admitted = await state(docs).shared.log.get(head);
		assert(admitted);
		assert.equal(admitted.meta.next.length, 32);
		assert(admitted.meta.next.includes(live.hash));
		assert.deepEqual(
			admitted.meta.next,
			[...deletes.slice(0, 31).map((entry) => entry.hash), live.hash].sort(),
		);
		assert.equal(
			(await docs.index.get("many", local)).__context.created,
			1000n,
		);
		const reopened = await seal(peer, docs);
		await reopened.put({ id: "many", name: "after-seal" });
		assert.equal(
			(await reopened.index.get("many", local)).__context.created,
			1000n,
		);
	});

	it("rejects a fully signed snapshot that contains an all-DELETE key group", async () => {
		const { peer: owner, docs, resource, genesis } = await openOwner();
		const deleted = await docs.del("only-deleted");
		const fact = state(docs).frontier.get("only-deleted")!.get(deleted)!;
		await owner.services.blocks.put(fact.bytes);
		const proposal = await createCheckpointSnapshot({
			blocks: owner.services.blocks,
			resource: state(docs).resource,
			owner: identity(owner),
			epoch: 1n,
			previous: genesis,
			frontier: [{ cid: deleted, created: fact.created }],
		});
		assert.equal(
			(
				await readCheckpointSnapshot({
					blocks: owner.services.blocks,
					cid: proposal.cid,
					resource: state(docs).resource,
					owner: identity(owner).publicKey,
				})
			).count,
			1,
		);
		const approval = await signCheckpointApproval({
			resource: state(docs).resource,
			proposal: proposal.cid,
			identity: identity(owner),
		});
		const certificate = await createCheckpointCertificate({
			blocks: owner.services.blocks,
			resource: state(docs).resource,
			proposal: proposal.cid,
			writers: [identity(owner).publicKey.publicKey],
			approvals: [approval],
		});
		const recipient = await createPeer();
		await copyBlocks(owner, recipient);
		const candidate = resource.clone();
		await assert.rejects(
			recipient.open(candidate, { args: { checkpoint: certificate.cid } }),
			/tombstone|delete/i,
		);
		assert.throws(() => candidate.index, /not ready/);
	});
});
