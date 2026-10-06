import { deserialize, serialize } from "@dao-xyz/borsh";
import { toId } from "@peerbit/indexer-interface";
import { ShallowEntry } from "@peerbit/log";
import { expect } from "chai";
import fs from "fs/promises";
import os from "os";
import path from "path";
import { Peerbit } from "peerbit";
import { createRustPeerbitOptions } from "peerbit/rust";
import { EventStore } from "./utils/stores/event-store.js";

describe("native recovery before-image provenance", function () {
	this.timeout(120_000);
	let client: Peerbit | undefined;
	let directory: string | undefined;

	afterEach(async () => {
		await client?.stop();
		client = undefined;
		if (directory) await fs.rm(directory, { recursive: true, force: true });
		directory = undefined;
	});

	const prepare = async (
		beforePendingOnly: true | undefined,
		conflictingCurrent = false,
	) => {
		directory = await fs.mkdtemp(
			path.join(os.tmpdir(), "peerbit-native-recovery-provenance-"),
		);
		client = await Peerbit.create({ directory, ...createRustPeerbitOptions() });
		const store = await client.open(new EventStore<string, any>(), {
			args: { replicate: { factor: 1 }, timeUntilRoleMaturity: 0 },
		});
		const shared = store.log as any;
		expect(shared._nativeBackbone, "real native backbone").to.exist;
		expect(shared._nativeBackboneCoordinatePersistenceStore).to.exist;
		const { entry } = await store.add("before-image", { meta: { next: [] } });
		const entryIndex = store.log.log.entryIndex;
		await entryIndex.flushPendingWrites();
		const index = entryIndex.properties.index;
		const original = (await index.get(toId(entry.hash)))!.value;
		const before = deserialize(serialize(original), ShallowEntry);
		before.head = true;
		const after = deserialize(serialize(before), ShallowEntry);
		after.head = false;
		const conflicting = deserialize(serialize(after), ShallowEntry);
		// A present row that matches neither image must never be overwritten,
		// even when the old intent carries pending-only before-image evidence.
		conflicting.payloadSize++;
		expect(serialize(before)).not.to.deep.equal(serialize(after));
		expect(serialize(conflicting)).not.to.deep.equal(serialize(before));
		expect(serialize(conflicting)).not.to.deep.equal(serialize(after));

		await entryIndex.withExclusiveMutationRecovery(async () => {
			if (conflictingCurrent) {
				await index.put(conflicting);
			} else {
				await index.del({ query: { hash: entry.hash } });
			}
			await entryIndex.init();
			await shared.writeNativeStrictDurableTransactionIntent({
				version: 1,
				lowerMarkerCommitted: false,
				appendHashes: [entry.hash],
				trimHashes: [],
				coordinateDeleteHashes: [],
				lowerIndexRows: [
					{
						hash: entry.hash,
						before: [...serialize(before)],
						...(beforePendingOnly ? { beforePendingOnly } : {}),
						after: [...serialize(after)],
					},
				],
				coordinates: [],
				documents: [],
			});
		});
		shared.poisonNativeStrictDurableTransaction(
			new Error("injected retained recovery intent"),
		);
		// Force recovery to read the checksummed record, not its in-memory cache.
		shared._nativeStrictDurableTransactionJournalState = undefined;
		const persisted =
			await shared.loadNativeStrictDurableTransactionJournalState();
		expect(persisted.intent.lowerIndexRows[0].beforePendingOnly).to.equal(
			beforePendingOnly,
		);
		expect(persisted.intent.lowerMarkerCommitted).to.equal(false);
		const read = async () => (await index.get(toId(entry.hash)))?.value;
		if (conflictingCurrent) {
			expect(serialize((await read())!)).to.deep.equal(serialize(conflicting));
		} else {
			expect(await read(), "durable row is absent before replay").to.equal(
				undefined,
			);
		}
		return { shared, entryIndex, before, conflicting, read };
	};

	const recover = async (shared: any) => {
		expect(() => shared.throwIfNativeDurableCommitFailed()).to.throw();
		expect(
			await shared.recoverNativeStrictDurableTransactionIntent(true),
		).to.equal(true);
		expect(() => shared.throwIfNativeDurableCommitFailed()).not.to.throw();
		shared._nativeStrictDurableTransactionJournalState = undefined;
		expect(
			(await shared.loadNativeStrictDurableTransactionJournalState()).intent,
			"successful replay retires the durable intent",
		).to.equal(undefined);
	};

	it("restores an absent before-image only with captured pending-only provenance", async () => {
		const { shared, entryIndex, before, read } = await prepare(true);
		await recover(shared);
		expect(serialize((await read())!)).to.deep.equal(serialize(before));
		expect(entryIndex.length).to.equal(1);
	});

	it("does not resurrect an absent row from an older intent without provenance", async () => {
		const { shared, entryIndex, read } = await prepare(undefined);
		await recover(shared);
		expect(await read()).to.equal(undefined);
		expect(entryIndex.length).to.equal(0);
	});

	it("does not overwrite a present conflicting row with a pending-only before-image", async () => {
		const { shared, entryIndex, conflicting, read } = await prepare(true, true);
		await recover(shared);
		expect(serialize((await read())!)).to.deep.equal(serialize(conflicting));
		expect(entryIndex.length).to.equal(1);
	});
});
