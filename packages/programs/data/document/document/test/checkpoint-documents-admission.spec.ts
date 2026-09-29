import { deserialize, serialize } from "@dao-xyz/borsh";
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
import { CheckpointDocuments } from "../src/checkpoint-documents.js";
import {
	CheckpointDocument,
	CheckpointOperation,
} from "../src/checkpoint-operation.js";
import { BORSH_ENCODING_OPERATION, type Operation } from "../src/operation.js";

const local = { local: true, remote: false } as const;
const encoder = new TextEncoder();
const identity = (peer: Peerbit): Identity<Ed25519PublicKey> => {
	assert(peer.identity.publicKey instanceof Ed25519PublicKey);
	return peer.identity as Identity<Ed25519PublicKey>;
};

// Deliberate white-box injection into the genuine receive/admission path. The
// public resource does not expose mutable log handles or arbitrary signing.
const internals = (docs: CheckpointDocuments) =>
	docs as unknown as {
		resource: Uint8Array;
		checkpoint: { epoch: bigint };
		checkpointDigest: Uint8Array;
		shared: SharedLog<Operation, any, any>;
		frontier: Map<string, Map<string, unknown>>;
	};

describe("checkpoint Documents signed admission", function () {
	this.timeout(60_000);
	const directories: string[] = [];
	const peers: Peerbit[] = [];
	const createPeer = async () => {
		const directory = await mkdtemp(
			join(tmpdir(), "peerbit-checkpoint-admission-"),
		);
		directories.push(directory);
		const peer = await Peerbit.create({ directory });
		peers.push(peer);
		return peer;
	};
	afterEach(async () => {
		for (const peer of peers.splice(0).reverse()) await peer.stop();
		for (const directory of directories.splice(0)) {
			await rm(directory, { recursive: true, force: true });
		}
	});
	const copyBlocks = async (source: Peerbit, target: Peerbit) => {
		for await (const [, bytes] of source.services.blocks.iterator()) {
			await target.services.blocks.put(bytes);
		}
	};
	const signed = async (
		peer: Peerbit,
		docs: CheckpointDocuments,
		options: {
			key: string;
			resource?: Uint8Array;
			checkpoint?: Uint8Array;
			epoch?: bigint;
			type?: EntryType;
			timestamp?: Timestamp;
			parents?: Entry<Operation>[];
			kind?: 0 | 1;
			name?: string;
		},
	) => {
		const state = internals(docs);
		const document = CheckpointDocument.from({
			id: options.key,
			name: options.name ?? "signed",
		});
		const entry = await createEntry<Operation>({
			store: peer.services.blocks,
			identity: identity(peer),
			encoding: BORSH_ENCODING_OPERATION,
			data: new CheckpointOperation({
				resource: options.resource ?? state.resource,
				epoch: options.epoch ?? state.checkpoint.epoch,
				checkpoint: options.checkpoint ?? state.checkpointDigest,
				kind: options.kind ?? 0,
				key: options.key,
				data: options.kind === 1 ? new Uint8Array() : document.value,
			}),
			meta: {
				type: options.type ?? EntryType.APPEND,
				data: encodeReplicas(new AbsoluteReplicas(1)),
				gidSeed: sha256Sync(encoder.encode(options.key)),
				next: options.parents ?? [],
				clock: options.timestamp
					? new LamportClock({
							id: identity(peer).publicKey.bytes,
							timestamp: options.timestamp,
						})
					: undefined,
			},
		});
		// Cross the same serialization boundary as a remote peer and discard all
		// createdLocally/verifier caches from the test's signing process.
		const remote = deserialize(
			new Uint8Array(entry.getStorageBytes()),
			Entry,
		) as Entry<Operation>;
		remote.hash = entry.hash;
		remote.init(state.shared.log);
		remote.createdLocally = false;
		return remote;
	};

	it("admits exact signed bytes from either fixed writer", async () => {
		const owner = await createPeer();
		const writer = await createPeer();
		const resource = new CheckpointDocuments({
			owner: identity(owner).publicKey,
			writers: [identity(owner).publicKey, identity(writer).publicKey],
		});
		const genesis = await resource.createGenesis(
			owner.services.blocks,
			identity(owner),
		);
		const docs = await owner.open(resource, { args: { checkpoint: genesis } });
		for (const [index, signer] of [owner, writer].entries()) {
			const key = `writer-${index}`;
			const entry = await signed(signer, docs, { key });
			const canonical = await signer.services.blocks.get(entry.hash, {
				remote: false,
			});
			assert(canonical);
			await internals(docs).shared.join([entry]);
			assert.equal(
				(await docs.index.get(key, local)).__context.head,
				entry.hash,
			);
			const stored = await internals(docs).shared.log.blocks.get(entry.hash, {
				remote: false,
			});
			assert.deepEqual(stored, canonical);
		}
	});

	it("preserves equal-clock branches and logical deletion across reversed deliveries", async () => {
		const owner = await createPeer();
		const writer = await createPeer();
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
		const key = "concurrent";
		const at = (wallTime: bigint) => new Timestamp({ wallTime });
		const a = await signed(owner, left, {
			key,
			name: "A",
			timestamp: at(1000n),
		});
		const b = await signed(owner, left, {
			key,
			name: "B",
			timestamp: at(2000n),
			parents: [a],
		});
		const c = await signed(writer, right, {
			key,
			name: "C",
			timestamp: at(2000n),
			parents: [a],
		});
		for (const entry of [a, b, c]) await internals(left).shared.join([entry]);
		for (const entry of [a, c, b]) await internals(right).shared.join([entry]);
		const firstLeft = await left.index.get(key, local);
		const firstRight = await right.index.get(key, local);
		assert.equal(firstLeft.__context.head, [b.hash, c.hash].sort().at(-1));
		assert.deepEqual(
			serialize(firstLeft.__context),
			serialize(firstRight.__context),
		);
		assert.equal(firstLeft.__context.created, 1000n);
		assert.deepEqual(firstLeft.decode(), firstRight.decode());

		// A second key also permutes the delete relative to the concurrent branch.
		const otherKey = "delete-race";
		const a2 = await signed(owner, left, {
			key: otherKey,
			name: "A",
			timestamp: at(1000n),
		});
		const b2 = await signed(owner, left, {
			key: otherKey,
			name: "B",
			timestamp: at(2000n),
			parents: [a2],
		});
		const c2 = await signed(writer, right, {
			key: otherKey,
			name: "C",
			timestamp: at(2000n),
			parents: [a2],
		});
		const d2 = await signed(owner, left, {
			key: otherKey,
			kind: 1,
			timestamp: at(3000n),
			parents: [b2],
		});
		for (const entry of [a2, b2, c2, d2])
			await internals(left).shared.join([entry]);
		for (const entry of [a2, b2, d2, c2])
			await internals(right).shared.join([entry]);
		for (const docs of [left, right]) {
			assert.deepEqual(
				[...internals(docs).frontier.get(otherKey)!.keys()].sort(),
				[c2.hash, d2.hash].sort(),
			);
			assert.equal(await docs.index.get(otherKey, local), undefined);
			assert.equal(
				await internals(docs).shared.log.has(b2.hash),
				true,
				"logical deletes retain signed causal evidence",
			);
		}
		const e = await signed(owner, left, {
			key: otherKey,
			name: "E",
			timestamp: at(4000n),
			parents: [c2, d2],
		});
		for (const docs of [left, right]) await internals(docs).shared.join([e]);
		const afterLeft = await left.index.get(otherKey, local);
		const afterRight = await right.index.get(otherKey, local);
		assert.equal(afterLeft.__context.created, 1000n);
		assert.deepEqual(
			serialize(afterLeft.__context),
			serialize(afterRight.__context),
		);
		assert.deepEqual(afterLeft.decode(), afterRight.decode());

		// Both fixed writers certify the same frontier only after each has
		// drained and durably frozen its own independent epoch store.
		const address = left.address;
		const expectedRows = [firstLeft, afterLeft];
		const expectedFrontiers = [key, otherKey].map((id) =>
			[...internals(left).frontier.get(id)!.keys()].sort(),
		);
		const freezes = [
			await left.freezeCheckpoint(),
			await right.freezeCheckpoint(),
		];
		await copyBlocks(writer, owner);
		const proposal = await left.prepareCheckpoint(freezes);
		await copyBlocks(owner, writer);
		const approvals = [
			await left.approveCheckpoint(proposal),
			await right.approveCheckpoint(proposal),
		];
		const checkpoint = await left.publishCheckpoint(proposal, approvals);
		await copyBlocks(owner, writer);
		for (const peer of [owner, writer]) {
			const imported = await peer.open<CheckpointDocuments>(address, {
				args: { checkpoint },
			});
			assert.equal(imported.currentCheckpoint, checkpoint);
			for (const [index, id] of [key, otherKey].entries()) {
				const row = await imported.index.get(id, local);
				assert.deepEqual(row.decode(), expectedRows[index]!.decode());
				assert.deepEqual(
					serialize(row.__context),
					serialize(expectedRows[index]!.__context),
				);
				assert.deepEqual(
					[...internals(imported).frontier.get(id)!.keys()].sort(),
					expectedFrontiers[index],
				);
			}
		}
	});

	it("incrementally merges large concurrent frontiers and reopens every certified tip", async () => {
		const owner = await createPeer();
		const resource = new CheckpointDocuments({
			owner: identity(owner).publicKey,
		});
		const genesis = await resource.createGenesis(
			owner.services.blocks,
			identity(owner),
		);
		let docs = await owner.open(resource, { args: { checkpoint: genesis } });
		const address = docs.address;
		const key = "wide-frontier";
		const roots: Entry<Operation>[] = [];
		for (let index = 0; index < 40; index++) {
			const entry = await signed(owner, docs, {
				key,
				name: `branch-${index}`,
				timestamp: new Timestamp({ wallTime: 1000n }),
			});
			roots.push(entry);
			await internals(docs).shared.join([entry]);
		}
		const ordered = roots.map((entry) => entry.hash).sort();
		assert.deepEqual(
			[...internals(docs).frontier.get(key)!.keys()].sort(),
			ordered,
		);
		const merged = await docs.put({ id: key, name: "merged" });
		const mergeEntry = await internals(docs).shared.log.get(merged);
		assert(mergeEntry);
		assert.deepEqual(mergeEntry.meta.next, ordered.slice(0, 32));
		assert.deepEqual(
			[...internals(docs).frontier.get(key)!.keys()].sort(),
			[...ordered.slice(32), merged].sort(),
		);
		assert.equal(docs.status, "ready");

		// Keep more than 32 tips at the seal itself, proving that the operation's
		// direct-parent bound is not incorrectly applied to certified frontiers.
		for (let index = 0; index < 32; index++) {
			const entry = await signed(owner, docs, {
				key,
				name: `late-branch-${index}`,
				timestamp: new Timestamp({ wallTime: 1000n }),
			});
			await internals(docs).shared.join([entry]);
		}
		const expectedTips = [...internals(docs).frontier.get(key)!.keys()].sort();
		assert.equal(expectedTips.length, 41);
		const expected = await docs.index.get(key, local);
		assert.equal(expected.__context.head, merged);
		assert.equal(expected.__context.created, 1000n);
		const proposal = await docs.prepareCheckpoint();
		const checkpoint = await docs.publishCheckpoint(proposal, [
			await docs.approveCheckpoint(proposal),
		]);
		docs = await owner.open<CheckpointDocuments>(address, {
			args: { checkpoint },
		});
		assert.deepEqual(
			[...internals(docs).frontier.get(key)!.keys()].sort(),
			expectedTips,
		);
		const imported = await docs.index.get(key, local);
		assert.deepEqual(imported.decode(), expected.decode());
		assert.deepEqual(
			serialize(imported.__context),
			serialize(expected.__context),
		);
		const next = await docs.put({ id: key, name: "after checkpoint" });
		assert.deepEqual([...internals(docs).frontier.get(key)!.keys()], [next]);
		assert.equal((await docs.index.get(key, local)).__context.created, 1000n);
		assert.equal(docs.status, "ready");
	});

	it("rejects signed foreign scope, stale certificate and CUT before omitted-parent resolution", async () => {
		const owner = await createPeer();
		const writer = await createPeer();
		const resource = new CheckpointDocuments({
			owner: identity(owner).publicKey,
			writers: [identity(owner).publicKey, identity(writer).publicKey],
		});
		const genesis = await resource.createGenesis(
			owner.services.blocks,
			identity(owner),
		);
		const docs = await owner.open(resource, { args: { checkpoint: genesis } });
		const cases = [
			{ resource: new Uint8Array(32).fill(3) },
			{ checkpoint: new Uint8Array(32).fill(4) },
			{ epoch: 1n },
			{ type: EntryType.CUT },
		];
		for (const [index, invalid] of cases.entries()) {
			const key = `reject-${index}`;
			const parent = await signed(writer, docs, { key });
			const child = await signed(writer, docs, {
				key,
				parents: [parent],
				...invalid,
			});
			const blocks = internals(docs).shared.log.blocks;
			const get = blocks.get;
			const requested: string[] = [];
			blocks.get = async function (cid, options) {
				requested.push(cid);
				if (cid === parent.hash)
					throw new Error("Unexpected omitted-parent read");
				return get.call(this, cid, options);
			};
			try {
				await internals(docs).shared.join([child]);
			} finally {
				blocks.get = get;
			}
			assert.equal(requested.includes(parent.hash), false);
			assert.equal(await internals(docs).shared.log.has(child.hash), false);
			assert.equal(await docs.index.get(key, local), undefined);
		}
	});

	it("rejects an implicit checkpoint successor with a non-causal signed clock", async () => {
		const owner = await createPeer();
		const resource = new CheckpointDocuments({
			owner: identity(owner).publicKey,
		});
		const genesis = await resource.createGenesis(
			owner.services.blocks,
			identity(owner),
		);
		let docs = await owner.open(resource, { args: { checkpoint: genesis } });
		const address = docs.address;
		const original = await docs.put({ id: "key", name: "certified" });
		const originalEntry = await internals(docs).shared.log.get(original);
		assert(originalEntry);
		const proposal = await docs.prepareCheckpoint();
		const checkpoint = await docs.publishCheckpoint(proposal, [
			await docs.approveCheckpoint(proposal),
		]);
		docs = await owner.open<CheckpointDocuments>(address, {
			args: { checkpoint },
		});
		for (const wallTime of [
			originalEntry.meta.clock.timestamp.wallTime - 1n,
			originalEntry.meta.clock.timestamp.wallTime,
		]) {
			const entry = await signed(owner, docs, {
				key: "key",
				timestamp: new Timestamp({ wallTime, logical: 0 }),
			});
			assert.equal(entry.meta.next.length, 0);
			await internals(docs).shared.join([entry]);
			assert.equal(await internals(docs).shared.log.has(entry.hash), false);
			const visible = await docs.index.get("key", local);
			assert.equal(visible.__context.head, original);
			assert.equal(visible.decode().name, "certified");
		}
	});

	it("keeps the resource unreadable when the authenticated checkpoint lacks a boundary block", async () => {
		const owner = await createPeer();
		const recipient = await createPeer();
		const resource = new CheckpointDocuments({
			owner: identity(owner).publicKey,
		});
		const genesis = await resource.createGenesis(
			owner.services.blocks,
			identity(owner),
		);
		const docs = await owner.open(resource, { args: { checkpoint: genesis } });
		const head = await docs.put({ id: "required", name: "present" });
		const proposal = await docs.prepareCheckpoint();
		const checkpoint = await docs.publishCheckpoint(proposal, [
			await docs.approveCheckpoint(proposal),
		]);
		await copyBlocks(owner, recipient);
		await recipient.services.blocks.rm(head);
		const clone = resource.clone();
		// Resolve the deliberately missing block locally: this test covers the
		// publication barrier, not the transport's separate 30-second fetch policy.
		const blocks = recipient.services.blocks;
		const get = blocks.get;
		blocks.get = function (cid, options) {
			return get.call(this, cid, cid === head ? { remote: false } : options);
		};
		try {
			await assert.rejects(
				recipient.open(clone, { args: { checkpoint } }),
				/Missing checkpoint boundary entry/,
			);
		} finally {
			blocks.get = get;
		}
		assert.throws(() => clone.index, /not ready/);
		assert.equal(
			internals(clone).shared,
			undefined,
			"no SharedLog is constructed before complete checkpoint verification",
		);
	});
});
