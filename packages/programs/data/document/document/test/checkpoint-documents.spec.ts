import { serialize } from "@dao-xyz/borsh";
import { Ed25519PublicKey, type Identity } from "@peerbit/crypto";
import { EntryType, scanCanonicalPublicEntryV0 } from "@peerbit/log";
import { waitForResolved } from "@peerbit/time";
import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Peerbit } from "peerbit";
import { CheckpointDocuments } from "../src/checkpoint-documents.js";
import { decodeCheckpointOperation } from "../src/checkpoint-operation.js";
import { createCheckpointSnapshot } from "../src/checkpoint-snapshot.js";

const local = { local: true, remote: false } as const;
const ownerIdentity = (peer: Peerbit): Identity<Ed25519PublicKey> => {
	assert(peer.identity.publicKey instanceof Ed25519PublicKey);
	return peer.identity as Identity<Ed25519PublicKey>;
};

describe("checkpoint Documents runtime", function () {
	this.timeout(60_000);
	const directories: string[] = [];
	const peers: Peerbit[] = [];
	const createPeer = async (directory?: string) => {
		if (!directory) {
			directory = await mkdtemp(join(tmpdir(), "peerbit-checkpoint-runtime-"));
			directories.push(directory);
		}
		const peer = await Peerbit.create({ directory });
		peers.push(peer);
		return { peer, directory };
	};
	afterEach(async () => {
		for (const peer of peers.splice(0).reverse()) await peer.stop();
		for (const directory of directories.splice(0)) {
			await rm(directory, { recursive: true, force: true });
		}
	});

	it("supports durable CRUD, logical tombstones, seal, post-import writes and offline reopen", async () => {
		const { peer, directory } = await createPeer();
		const resource = new CheckpointDocuments({
			owner: ownerIdentity(peer).publicKey,
		});
		assert.throws(() => resource.index, /not ready/);
		const genesis = await resource.createGenesis(
			peer.services.blocks,
			ownerIdentity(peer),
		);
		let docs = await peer.open(resource, { args: { checkpoint: genesis } });
		const address = docs.address;
		const descriptor = serialize(docs);
		const first = await docs.put({ id: "survivor", name: "before" });
		await docs.put({ id: "deleted", name: "remove" });
		const deleted = await docs.del("deleted");
		assert.equal(await docs.index.get("deleted", local), undefined);
		const before = await docs.index.get("survivor", local);
		assert.deepEqual(before.decode(), { id: "survivor", name: "before" });
		assert.equal(before.__context.head, first);
		const proposal = await docs.prepareCheckpoint();
		assert.throws(() => docs.index, /not ready/);
		// Sealing retains each authenticated frontier block outside the epoch log.
		const tombstone = await peer.services.blocks.get(deleted, {
			remote: false,
		});
		assert(tombstone);
		const scanned = scanCanonicalPublicEntryV0(tombstone, {
			label: "Runtime tombstone",
			minimumSignatures: 1,
			maximumSignatures: 1,
		});
		assert.equal(scanned.meta.type, EntryType.APPEND);
		assert.equal(decodeCheckpointOperation(scanned.payloadBytes).kind, 1);

		const approval = await docs.approveCheckpoint(proposal);
		const checkpoint = await docs.publishCheckpoint(proposal, [approval]);
		docs = await peer.open<CheckpointDocuments>(address, {
			args: { checkpoint },
		});
		assert.deepEqual(serialize(docs), descriptor);
		const imported = await docs.index.get("survivor", local);
		assert.deepEqual(imported.decode(), before.decode());
		assert.deepEqual(
			serialize(imported.__context),
			serialize(before.__context),
		);
		assert.equal(await docs.index.get("deleted", local), undefined);
		const next = await docs.put({ id: "survivor", name: "after" });
		await docs.put({ id: "new", name: "post-checkpoint" });
		const after = await docs.index.get("survivor", local);
		assert.deepEqual(after.decode(), { id: "survivor", name: "after" });
		assert.equal(after.__context.head, next);
		assert.equal(after.__context.created, before.__context.created);
		assert(after.__context.modified >= before.__context.modified);
		await peer.stop();

		const reopened = (await createPeer(directory)).peer;
		assert.equal(reopened.libp2p.getConnections().length, 0);
		docs = await reopened.open<CheckpointDocuments>(address);
		assert.equal(docs.currentCheckpoint, checkpoint);
		const restored = await docs.index.get("survivor", local);
		assert.deepEqual(restored.decode(), after.decode());
		assert.deepEqual(serialize(restored.__context), serialize(after.__context));
		assert.deepEqual((await docs.index.get("new", local)).decode(), {
			id: "new",
			name: "post-checkpoint",
		});
		assert.equal(await docs.index.get("deleted", local), undefined);
		await docs.del("survivor");
		assert.equal(await docs.index.get("survivor", local), undefined);
	});

	it("imports a sealed checkpoint on a fresh disk peer and replicates the live successor epoch", async () => {
		const { peer: owner } = await createPeer();
		const { peer: recipient, directory } = await createPeer();
		const resource = new CheckpointDocuments({
			owner: ownerIdentity(owner).publicKey,
		});
		const genesis = await resource.createGenesis(
			owner.services.blocks,
			ownerIdentity(owner),
		);
		let source = await owner.open(resource, { args: { checkpoint: genesis } });
		const address = source.address;
		await source.put({ id: "history", name: "old" });
		await source.put({ id: "history", name: "checkpoint" });
		const baseline = await source.index.get("history", local);
		const proposal = await source.prepareCheckpoint();
		const approval = await source.approveCheckpoint(proposal);
		const checkpoint = await source.publishCheckpoint(proposal, [approval]);
		await recipient.dial(owner);
		let imported = await recipient.open<CheckpointDocuments>(address, {
			args: { checkpoint },
		});
		const row = await imported.index.get("history", local);
		assert.deepEqual(row.decode(), baseline.decode());
		assert.deepEqual(serialize(row.__context), serialize(baseline.__context));
		source = await owner.open<CheckpointDocuments>(address, {
			args: { checkpoint },
		});
		const head = await source.put({ id: "history", name: "live" });
		await source.put({ id: "live-new", name: "replicated" });
		await waitForResolved(
			async () => {
				const result = await imported.index.get("history", local);
				assert.equal(result?.__context.head, head);
				assert.equal(
					(await imported.index.get("live-new", local))?.decode().name,
					"replicated",
				);
			},
			{ timeout: 15_000 },
		);
		const beforeSecondSeal = await imported.index.get("history", local);
		const secondProposal = await source.prepareCheckpoint();
		const secondApproval = await source.approveCheckpoint(secondProposal);
		const secondCheckpoint = await source.publishCheckpoint(secondProposal, [
			secondApproval,
		]);
		await imported.close();
		imported = await recipient.open<CheckpointDocuments>(address, {
			args: { checkpoint: secondCheckpoint },
		});
		const afterSecondSeal = await imported.index.get("history", local);
		assert.deepEqual(
			serialize(afterSecondSeal.__context),
			serialize(beforeSecondSeal.__context),
		);
		source = await owner.open<CheckpointDocuments>(address, {
			args: { checkpoint: secondCheckpoint },
		});
		const secondHead = await source.put({
			id: "history",
			name: "second-epoch",
		});
		await source.del("live-new");
		await waitForResolved(
			async () => {
				assert.equal(
					(await imported.index.get("history", local))?.__context.head,
					secondHead,
				);
				assert.equal(await imported.index.get("live-new", local), undefined);
			},
			{ timeout: 15_000 },
		);
		const expected = await imported.index.get("history", local);
		await owner.stop();
		await recipient.stop();
		const offline = (await createPeer(directory)).peer;
		assert.equal(offline.libp2p.getConnections().length, 0);
		imported = await offline.open<CheckpointDocuments>(address);
		assert.equal(imported.currentCheckpoint, secondCheckpoint);
		const restored = await imported.index.get("history", local);
		assert.deepEqual(restored.decode(), expected.decode());
		assert.deepEqual(
			serialize(restored.__context),
			serialize(expected.__context),
		);
		assert.equal(await imported.index.get("live-new", local), undefined);
	});

	it("refuses a seal that omits an offline fixed writer's accepted operation", async () => {
		const { peer: owner } = await createPeer();
		const { peer: offline } = await createPeer();
		const resource = new CheckpointDocuments({
			owner: ownerIdentity(owner).publicKey,
			writers: [
				ownerIdentity(owner).publicKey,
				ownerIdentity(offline).publicKey,
			],
		});
		const genesis = await resource.createGenesis(
			owner.services.blocks,
			ownerIdentity(owner),
		);
		const copyOwnerBlocks = async () => {
			for await (const [, bytes] of owner.services.blocks.iterator()) {
				await offline.services.blocks.put(bytes);
			}
		};
		await copyOwnerBlocks();
		const source = await owner.open(resource, {
			args: { checkpoint: genesis },
		});
		const other = await offline.open(resource.clone(), {
			args: { checkpoint: genesis },
		});
		assert.equal(offline.libp2p.getConnections().length, 0);
		await other.put({ id: "unreported", name: "accepted offline" });
		const freezes = [
			await source.freezeCheckpoint(),
			await other.freezeCheckpoint(),
		];
		for await (const [, bytes] of offline.services.blocks.iterator())
			await owner.services.blocks.put(bytes);
		// Even an owner-signed proposal carrying the correct frozen writer set
		// cannot omit that set's previously unreported live operation.
		const omitted = await createCheckpointSnapshot({
			blocks: owner.services.blocks,
			resource: (source as unknown as { resource: Uint8Array }).resource,
			owner: ownerIdentity(owner),
			epoch: 1n,
			previous: genesis,
			frontier: [],
			freezes,
		});
		await copyOwnerBlocks();
		await assert.rejects(
			other.approveCheckpoint(omitted.cid),
			/omits accepted operations/,
		);
		assert.throws(() => other.index, /not ready/);
		const proposal = await source.prepareCheckpoint(freezes);
		const approval = await source.approveCheckpoint(proposal);
		await assert.rejects(
			source.publishCheckpoint(proposal, [approval]),
			/every|writer|approval/i,
		);
	});

	it("recovers a frozen owner after restart without serving reads or accepting writes", async () => {
		const { peer, directory } = await createPeer();
		const resource = new CheckpointDocuments({
			owner: ownerIdentity(peer).publicKey,
		});
		const genesis = await resource.createGenesis(
			peer.services.blocks,
			ownerIdentity(peer),
		);
		const source = await peer.open(resource, { args: { checkpoint: genesis } });
		const address = source.address;
		await source.put({ id: "retained", name: "before-freeze" });
		const before = await source.index.get("retained", local);
		const proposal = await source.prepareCheckpoint();
		await peer.stop();

		const reopened = (await createPeer(directory)).peer;
		const frozen = await reopened.open<CheckpointDocuments>(address);
		assert.equal(frozen.status, "frozen");
		assert.throws(() => frozen.index, /not ready/);
		assert.throws(
			() => frozen.put({ id: "forbidden", name: "while-frozen" }),
			/not ready/,
		);
		assert.equal(reopened.libp2p.getConnections().length, 0);
		const approval = await frozen.approveCheckpoint(proposal);
		const checkpoint = await frozen.publishCheckpoint(proposal, [approval]);
		const active = await reopened.open<CheckpointDocuments>(address, {
			args: { checkpoint },
		});
		assert.equal(active.status, "ready");
		const after = await active.index.get("retained", local);
		assert.deepEqual(after.decode(), before.decode());
		assert.deepEqual(serialize(after.__context), serialize(before.__context));
		await active.put({ id: "accepted", name: "after-resume" });
		assert.equal(
			(await active.index.get("accepted", local)).decode().name,
			"after-resume",
		);
	});
});
