import { deserialize, serialize } from "@dao-xyz/borsh";
import {
	Ed25519Keypair,
	Ed25519PublicKey,
	type Identity,
	sha256Sync,
} from "@peerbit/crypto";
import {
	Entry,
	EntryType,
	LamportClock,
	Log,
	Timestamp,
	createEntry,
} from "@peerbit/log";
import { RPC } from "@peerbit/rpc";
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
import { CheckpointDocuments } from "../src/checkpoint-documents.js";
import {
	CheckpointDocument,
	CheckpointOperation,
} from "../src/checkpoint-operation.js";
import {
	type CheckpointSnapshot,
	createCheckpointFreeze,
} from "../src/checkpoint-snapshot.js";
import { BORSH_ENCODING_OPERATION, type Operation } from "../src/operation.js";

const local = { local: true, remote: false } as const;
const encoder = new TextEncoder();
const identity = (peer: Peerbit): Identity<Ed25519PublicKey> => {
	assert(peer.identity.publicKey instanceof Ed25519PublicKey);
	return peer.identity as Identity<Ed25519PublicKey>;
};
const state = (docs: CheckpointDocuments) =>
	docs as unknown as {
		resource: Uint8Array;
		checkpoint: CheckpointSnapshot;
		checkpointDigest: Uint8Array;
		shared: SharedLog<Operation, any, any>;
		frontier: Map<string, Map<string, unknown>>;
		readFloor(): { proposal?: string; collection?: string[] };
	};

describe("checkpoint Documents freeze-first sealing", function () {
	this.timeout(60_000);
	const peers: Peerbit[] = [];
	const directories: string[] = [];
	const createPeer = async (directory?: string) => {
		if (!directory) {
			directory = await mkdtemp(join(tmpdir(), "peerbit-checkpoint-sealing-"));
			directories.push(directory);
		}
		const peer = await Peerbit.create({ directory });
		peers.push(peer);
		return { peer, directory };
	};
	afterEach(async () => {
		for (const peer of peers.splice(0).reverse()) await peer.stop();
		for (const directory of directories.splice(0))
			await rm(directory, { recursive: true, force: true });
	});
	const copyBlocks = async (source: Peerbit, target: Peerbit) => {
		for await (const [, bytes] of source.services.blocks.iterator())
			await target.services.blocks.put(bytes);
	};
	const setup = async () => {
		const { peer: owner, directory: ownerDirectory } = await createPeer();
		const { peer: writer, directory: writerDirectory } = await createPeer();
		const resource = new CheckpointDocuments({
			owner: identity(owner).publicKey,
			writers: [identity(owner).publicKey, identity(writer).publicKey],
		});
		const genesis = await resource.createGenesis(
			owner.services.blocks,
			identity(owner),
		);
		await copyBlocks(owner, writer);
		const left = await owner.open(resource, { args: { checkpoint: genesis } });
		const right = await writer.open(resource.clone(), {
			args: { checkpoint: genesis },
		});
		return {
			owner,
			ownerDirectory,
			writer,
			writerDirectory,
			resource,
			genesis,
			left,
			right,
		};
	};
	const signed = async (
		peer: Peerbit,
		docs: CheckpointDocuments,
		options: {
			name?: string;
			time: bigint;
			kind?: 0 | 1;
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
				key: "mixed",
				data:
					options.kind === 1
						? new Uint8Array()
						: CheckpointDocument.from({
								id: "mixed",
								name: options.name ?? "value",
							}).value,
			}),
			meta: {
				type: EntryType.APPEND,
				data: encodeReplicas(new AbsoluteReplicas(1)),
				gidSeed: sha256Sync(encoder.encode("mixed")),
				next: options.parents ?? [],
				clock: new LamportClock({
					id: identity(peer).publicKey.bytes,
					timestamp: new Timestamp({ wallTime: options.time }),
				}),
			},
		});
		// Exercise the receive path, without local-signature/admission trust flags.
		const remote = deserialize(
			new Uint8Array(entry.getStorageBytes()),
			Entry,
		) as Entry<Operation>;
		remote.hash = entry.hash;
		remote.init(internal.shared.log);
		remote.createdLocally = false;
		return remote;
	};

	it("reconciles disconnected accepted histories before publishing a unanimous successor", async () => {
		const { owner, writer, resource, left, right } = await setup();
		assert.equal(owner.libp2p.getConnections().length, 0);
		assert.equal(writer.libp2p.getConnections().length, 0);
		await left.put({ id: "owner-only", name: "owner" });
		await right.put({ id: "writer-only", name: "writer" });
		const ownerRow = await left.index.get("owner-only", local);
		const writerRow = await right.index.get("writer-only", local);
		for (const [docs, key] of [
			[left, "expired-owner"],
			[right, "expired-writer"],
		] as const) {
			await docs.put({ id: key, name: "deleted" });
			await docs.del(key);
		}
		const a = await signed(owner, left, { name: "A", time: 1000n });
		const b = await signed(owner, left, {
			name: "B",
			time: 2000n,
			parents: [a],
		});
		const c = await signed(writer, right, {
			name: "C",
			time: 2500n,
			parents: [a],
		});
		const d = await signed(owner, left, { kind: 1, time: 3000n, parents: [b] });
		for (const entry of [a, b, d]) await state(left).shared.join([entry]);
		for (const entry of [a, c]) await state(right).shared.join([entry]);
		assert.equal(await left.index.get("mixed", local), undefined);
		assert.equal((await right.index.get("mixed", local)).decode().name, "C");

		const ownerFreeze = await left.freezeCheckpoint();
		const writerFreeze = await right.freezeCheckpoint();
		for (const docs of [left, right]) {
			assert.equal(docs.status, "frozen");
			assert.throws(
				() => docs.put({ id: "unreported-later", name: "forbidden" }),
				/not ready/,
			);
			assert.throws(() => docs.del("owner-only"), /not ready/);
		}
		// Only published immutable blocks are transferred. No private log/index
		// state or mutable runtime maps are copied between the disconnected peers.
		await copyBlocks(owner, writer);
		await copyBlocks(writer, owner);
		const originalRpcOpen = RPC.prototype.open;
		let rpcOpens = 0;
		RPC.prototype.open = async function (...args) {
			rpcOpens++;
			return originalRpcOpen.apply(this, args);
		};
		let checkpoint: string;
		try {
			const proposal = await left.prepareCheckpoint([
				writerFreeze,
				ownerFreeze,
			]);
			await copyBlocks(owner, writer);
			const approvals = [
				await left.approveCheckpoint(proposal),
				await right.approveCheckpoint(proposal),
			];
			checkpoint = await left.publishCheckpoint(proposal, approvals);
		} finally {
			RPC.prototype.open = originalRpcOpen;
		}
		assert.equal(rpcOpens, 0, "Frozen reconciliation must not start any RPC");
		await copyBlocks(owner, writer);
		const leftNext = await owner.open<CheckpointDocuments>(left.address, {
			args: { checkpoint },
		});
		const rightNext = await writer.open<CheckpointDocuments>(right.address, {
			args: { checkpoint },
		});
		const { peer: fresh } = await createPeer();
		await copyBlocks(owner, fresh);
		const freshNext = await fresh.open(resource.clone(), {
			args: { checkpoint },
		});
		for (const docs of [leftNext, rightNext, freshNext]) {
			assert.equal(state(docs).checkpoint.count, 4);
			assert.deepEqual(
				[...state(docs).checkpoint.freezes].sort(),
				[ownerFreeze, writerFreeze].sort(),
			);
			assert.equal(await docs.index.getSize(), 2);
			for (const expected of [ownerRow, writerRow]) {
				const actual = await docs.index.get(expected.id, local);
				assert.deepEqual(actual.value, expected.value);
				assert.deepEqual(
					serialize(actual.__context),
					serialize(expected.__context),
				);
			}
			assert.equal(await docs.index.get("mixed", local), undefined);
			assert.equal(await docs.index.get("expired-owner", local), undefined);
			assert.equal(await docs.index.get("expired-writer", local), undefined);
			assert.deepEqual(
				[...state(docs).frontier.get("mixed")!.keys()].sort(),
				[c.hash, d.hash].sort(),
			);
		}
		await leftNext.put({ id: "mixed", name: "merged" });
		assert.equal(
			(await leftNext.index.get("mixed", local)).__context.created,
			1000n,
		);
	});

	it("rejects missing and wrong writer manifests without silently freezing an unprepared owner", async () => {
		const { owner, writer, left, right, genesis } = await setup();
		await assert.rejects(left.prepareCheckpoint(), /freeze|writer|manifest/i);
		assert.equal(left.status, "ready");
		await assert.rejects(left.prepareCheckpoint([]), /freeze|writer|manifest/i);
		assert.equal(left.status, "ready");
		await left.put({ id: "still-writable", name: "after-rejected-plan" });
		const ownerFreeze = await left.freezeCheckpoint();
		const writerFreeze = await right.freezeCheckpoint();
		await copyBlocks(writer, owner);
		await assert.rejects(
			left.prepareCheckpoint([ownerFreeze]),
			/freeze|writer|manifest/i,
		);
		await assert.rejects(
			left.prepareCheckpoint([ownerFreeze, ownerFreeze]),
			/duplicate|writer|manifest/i,
		);
		const outsider = await Ed25519Keypair.create();
		const wrong = await createCheckpointFreeze({
			blocks: owner.services.blocks,
			resource: state(left).resource,
			owner: outsider,
			epoch: 1n,
			previous: genesis,
			frontier: [],
		});
		await assert.rejects(
			left.prepareCheckpoint([ownerFreeze, wrong.cid]),
			/Invalid checkpoint root/,
		);
		assert.equal(state(left).readFloor().collection, undefined);
		const proposal = await left.prepareCheckpoint([ownerFreeze, writerFreeze]);
		await copyBlocks(owner, writer);
		const checkpoint = await left.publishCheckpoint(proposal, [
			await left.approveCheckpoint(proposal),
			await right.approveCheckpoint(proposal),
		]);
		const next = await owner.open<CheckpointDocuments>(left.address, {
			args: { checkpoint },
		});
		assert.equal(
			(await next.index.get("still-writable", local)).decode().name,
			"after-rejected-plan",
		);
	});

	it("resumes a partially committed frozen merge after an offline owner restart", async () => {
		const { owner, ownerDirectory, writer, left, right } = await setup();
		const expected = [];
		for (let i = 0; i < 4; i++) {
			const id = `offline-${i}`;
			await right.put({ id, name: `committed-${i}` });
			expected.push(await right.index.get(id, local));
		}
		const ownerFreeze = await left.freezeCheckpoint();
		const writerFreeze = await right.freezeCheckpoint();
		await copyBlocks(writer, owner);
		const logId = state(left).shared.log.idString;
		const originalJoin = Log.prototype.join;
		let attempts = 0;
		const committedSizes: number[] = [];
		Log.prototype.join = async function (...args) {
			if (this.idString !== logId) return originalJoin.apply(this, args);
			if (++attempts === 3)
				throw new Error("Interrupted frozen reconciliation");
			const result = await originalJoin.apply(this, args);
			committedSizes.push(this.length);
			return result;
		};
		try {
			await assert.rejects(
				left.prepareCheckpoint([ownerFreeze, writerFreeze]),
				/Interrupted frozen reconciliation/,
			);
		} finally {
			Log.prototype.join = originalJoin;
		}
		assert.deepEqual(committedSizes, [1, 2]);
		assert.equal(left.status, "frozen");
		assert.throws(() => left.index, /not ready/);
		const floor = state(left).readFloor();
		assert.equal(floor.proposal, undefined);
		assert.deepEqual(floor.collection, [ownerFreeze, writerFreeze].sort());
		const address = left.address;
		await owner.stop();
		const { peer: restoredOwner } = await createPeer(ownerDirectory);
		const restored = await restoredOwner.open<CheckpointDocuments>(address);
		assert.equal(restoredOwner.libp2p.getConnections().length, 0);
		assert.equal(restored.status, "frozen");
		assert.equal(state(restored).frontier.size, 2);
		assert.equal(await restored.freezeCheckpoint(), ownerFreeze);
		const proposal = await restored.prepareCheckpoint([
			writerFreeze,
			ownerFreeze,
		]);
		assert.equal(
			await restored.prepareCheckpoint([ownerFreeze, writerFreeze]),
			proposal,
		);
		await copyBlocks(restoredOwner, writer);
		const checkpoint = await restored.publishCheckpoint(proposal, [
			await restored.approveCheckpoint(proposal),
			await right.approveCheckpoint(proposal),
		]);
		const next = await restoredOwner.open<CheckpointDocuments>(address, {
			args: { checkpoint },
		});
		assert.equal(await next.index.getSize(), 4);
		assert.deepEqual(
			[...state(next).checkpoint.freezes].sort(),
			[ownerFreeze, writerFreeze].sort(),
		);
		for (const row of expected) {
			const actual = await next.index.get(row.id, local);
			assert.deepEqual(actual.value, row.value);
			assert.deepEqual(serialize(actual.__context), serialize(row.__context));
		}
	});

	it("reuses a durable freeze manifest after an offline writer restart", async () => {
		const { owner, writer, writerDirectory, left, right } = await setup();
		const address = right.address;
		await right.put({ id: "before-restart", name: "durable" });
		const expected = await right.index.get("before-restart", local);
		const originalFreeze = await right.freezeCheckpoint();
		assert.equal(await right.freezeCheckpoint(), originalFreeze);
		await writer.stop();
		const { peer: restoredWriter } = await createPeer(writerDirectory);
		const restored = await restoredWriter.open<CheckpointDocuments>(address);
		assert.equal(restoredWriter.libp2p.getConnections().length, 0);
		assert.equal(restored.status, "frozen");
		assert.throws(() => restored.index, /not ready/);
		assert.throws(
			() => restored.put({ id: "late", name: "forbidden" }),
			/not ready/,
		);
		assert.equal(await restored.freezeCheckpoint(), originalFreeze);
		const ownerFreeze = await left.freezeCheckpoint();
		await copyBlocks(restoredWriter, owner);
		await copyBlocks(owner, restoredWriter);
		const proposal = await left.prepareCheckpoint([
			ownerFreeze,
			originalFreeze,
		]);
		await copyBlocks(owner, restoredWriter);
		const checkpoint = await left.publishCheckpoint(proposal, [
			await left.approveCheckpoint(proposal),
			await restored.approveCheckpoint(proposal),
		]);
		await copyBlocks(owner, restoredWriter);
		const next = await restoredWriter.open<CheckpointDocuments>(address, {
			args: { checkpoint },
		});
		const actual = await next.index.get("before-restart", local);
		assert.deepEqual(actual.value, expected.value);
		assert.deepEqual(
			serialize(actual.__context),
			serialize(expected.__context),
		);
		await next.put({ id: "after-restart", name: "ready" });
		assert.equal(
			(await next.index.get("after-restart", local)).decode().name,
			"ready",
		);
	});
});
