// Durable-native restart gate for a NON-REPLICATING native document store.
//
// This is the case the replicating gate in shared-log's
// durable-native-restart.spec.ts cannot reach: a non-replicating native node
// (`replicate: false`) whose default document put options are
// `{ replicate: false, target: "none" }` (NATIVE_LOCAL_PUT_OPTIONS). Those
// options make `shouldDeferHeadCoordinatePersistence` true, which routes the
// append through the native commit-only fast path
// (`appendLocallyPreparedPayloadNativeBackboneCommitOnly` ->
// `prepareEntryV0PlainEntryCommit`).
//
// That fast path commits the entry block into the in-wasm block map ONLY. When
// the durable write-through wrapper is active (native backbone + on-disk
// directory) the block must instead be written through to the durable `blocks`
// sublevel, or it is lost on restart and the reopened program fails to load its
// heads / documents.
//
// Regression note: before the FIX-1 guard, this path took the wasm-commit-only
// route even with the wrapper active, so the blocks never reached durable. On
// reopen with no peers the documents were gone (index size 0 / undefined docs)
// and resolving heads walked an empty block store. This test pins the fix:
// a non-replicating native node reopens from disk with ALL documents intact.
import { NativeBackboneNodeCoordinatePersistence } from "@peerbit/native-backbone";
import { expect } from "chai";
import fs from "fs/promises";
import os from "os";
import path from "path";
import { Peerbit } from "peerbit";
import { createRustPeerbitOptions } from "peerbit/rust";
import sinon from "sinon";
import { policy, transform } from "../src/index.js";
import { Documents } from "../src/program.js";
import { Document, TestStore } from "./data.js";

describe("durable native block store restart (non-replicating)", function () {
	this.timeout(120_000);

	let client: Peerbit | undefined;
	let directory: string | undefined;

	afterEach(async () => {
		await client?.stop();
		client = undefined;
		if (directory) {
			await fs.rm(directory, { recursive: true, force: true });
			directory = undefined;
		}
	});

	it("reopens a non-replicating native document store from disk with all docs restored", async () => {
		directory = await fs.mkdtemp(
			path.join(os.tmpdir(), "peerbit-durable-native-nonrepl-"),
		);

		// Deterministic program id keeps the on-disk layout predictable across the
		// two sessions (TestStore randomizes its id by default).
		const storeId = new Uint8Array(32);
		for (let index = 0; index < storeId.length; index++) {
			storeId[index] = (index * 7 + 3) & 0xff;
		}

		const entryCount = 8;
		const docId = (index: number) => `nonrepl-doc-${index}`;

		// Native mode + non-replicating: default document puts use
		// NATIVE_LOCAL_PUT_OPTIONS { replicate:false, target:"none" }, which makes
		// shouldDeferHeadCoordinatePersistence true and routes the append through
		// appendLocallyPreparedPayloadNativeBackboneCommitOnly (the native
		// commit-only fast path that contains the wasm-commit-only
		// prepareEntryV0PlainEntryCommit branch FIX-1 guards). The coordinate
		// persistence is a durable on-disk store rooted under the same test
		// directory, so both sessions read/write the same coordinate journal
		// (mirrors the durable block wrapper, which is auto-derived from the node
		// directory).
		const coordinateDir = path.join(directory, "coordinate-wal");
		const nativeOpenArgs = () => ({
			mode: "native" as const,
			replicate: false as const,
			nativeGraph: true,
			nativeBackbone: {
				optional: false,
				documentIndex: true,
				coordinatePersistence: new NativeBackboneNodeCoordinatePersistence(
					coordinateDir,
					{ flushOnAppend: true },
				),
			},
			canPerform: policy.allowAll<Document>(),
			index: {
				type: Document,
				transform: transform.identity<Document>(),
			},
		});

		// --- Session 1: on-disk native client, NON-REPLICATING put N docs -------
		// No peer is ever dialed, so disk is the only block source on reopen.
		client = await Peerbit.create({
			directory,
			...createRustPeerbitOptions(),
		});

		// Deterministic log id (passed through Documents -> SharedLog) so the
		// per-program durable `blocks` sublevel resolves to the SAME on-disk
		// location on reopen.
		const store1 = new TestStore<Document>({
			docs: new Documents<Document>({ id: storeId }),
		});
		store1.id = storeId;
		await client.open(store1, { args: nativeOpenArgs() });

		// Clone the (deserializable) program tree now, while it is open, so the
		// reopened session opens an identical program (same ids/addresses).
		const clone = store1.clone();

		const log1 = store1.docs.log as any;
		expect(log1._nativeBackbone, "session 1 native backbone").to.exist;
		// The write-through wrapper is active: the log's block store is NOT the raw
		// native wasm block map. This is precisely the condition the FIX-1 guard
		// keys on.
		expect(
			log1.remoteBlocks?.localStore,
			"session 1 write-through wrapper active (localStore !== backbone.blocks)",
		).to.not.equal(log1._nativeBackbone.blocks);

		// Force the append through the pure wasm-commit-only fast path
		// (prepareEntryV0PlainEntryCommit at index.ts:~6656). A healthy native node
		// with resident coordinate state normally diverts document-index puts to the
		// storage-transaction path (which already writes blocks through the wrapper);
		// the wasm-commit-only branch is the fallback taken when resident coordinate
		// state is unavailable. Stubbing canUseNativeBackboneResidentCoordinateState
		// to false makes every put take that exact branch, so this test exercises the
		// FIX-1 code path. It is the branch that, before FIX-1, committed the block to
		// the wasm map only and never wrote it through to durable.
		const residentStub = sinon
			.stub(log1, "canUseNativeBackboneResidentCoordinateState")
			.returns(false);

		const entryHashes: string[] = [];
		try {
			for (let index = 0; index < entryCount; index++) {
				// Default options -> NATIVE_LOCAL_PUT_OPTIONS { replicate:false, target:"none" }.
				const put = await store1.docs.put(
					new Document({ id: docId(index), name: `v-${index}` }),
				);
				entryHashes.push(put.entry.hash);
			}
		} finally {
			residentStub.restore();
		}

		expect(
			await store1.docs.index.getSize(),
			"session 1 document count",
		).to.equal(entryCount);

		const identity = client.identity;

		// Clean stop flushes the durable block sublevel to disk.
		await client.stop();
		client = undefined;

		// --- Session 2: FRESH client on the SAME directory, NO peers ------------
		client = await Peerbit.create({
			directory,
			...createRustPeerbitOptions(),
		});
		expect(client.identity.publicKey.equals(identity.publicKey)).to.equal(
			true,
			"same identity restored from disk",
		);

		// Reopen the identical program clone. Must NOT throw
		// "Failed to load entry from head".
		const store2 = await client.open(clone, { args: nativeOpenArgs() });
		const log2 = store2.docs.log as any;
		expect(log2._nativeBackbone, "session 2 native backbone").to.exist;

		// The core durable guarantee of FIX-1: every entry block a non-replicating
		// native node committed is present in the reopened block store (read through
		// from durable storage and repopulated into the native wasm map lazily).
		// Before FIX-1, the native commit-only fast path committed the block into the
		// wasm map ONLY and bypassed the write-through wrapper, so after a restart
		// these blocks were gone and
		// remoteBlocks.get returned undefined (falling through to a remote read
		// that never resolves on a peerless node).
		for (const hash of entryHashes) {
			const block = await log2.remoteBlocks.get(hash, { remote: false });
			expect(block, `restored block ${hash}`).to.exist;
		}

		// And the entries themselves materialize from those blocks.
		for (const hash of entryHashes) {
			const entry = await store2.docs.log.log.get(hash);
			expect(entry, `restored entry ${hash}`).to.exist;
		}
	});
});
