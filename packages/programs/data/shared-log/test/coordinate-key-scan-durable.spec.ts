import { serialize } from "@dao-xyz/borsh";
import { keys } from "@libp2p/crypto";
import type { IndexKeyScan } from "@peerbit/indexer-interface";
import { NativeBackboneNodeCoordinatePersistence } from "@peerbit/native-backbone";
import { expect } from "chai";
import fs from "fs/promises";
import os from "os";
import pDefer from "p-defer";
import path from "path";
import { Peerbit } from "peerbit";
import { createRustPeerbitOptions } from "peerbit/rust";
import sinon from "sinon";
import { EventStore } from "./utils/stores/event-store.js";

describe("entry inventory durable native finalization", function () {
	this.timeout(60_000);

	it("invalidates through the real journal retirement barrier and reopens exact native inventory offline", async () => {
		const directory = await fs.mkdtemp(
			path.join(os.tmpdir(), "entry-inventory-finalization-"),
		);
		const diskOptions = {
			...createRustPeerbitOptions(),
			directory,
			libp2p: { privateKey: await keys.generateKeyPair("Ed25519") },
		};
		const openArgs = () => ({
			replicate: { factor: 1 },
			timeUntilRoleMaturity: 0,
			nativeGraph: true,
			nativeBackbone: {
				optional: false,
				coordinatePersistence: new NativeBackboneNodeCoordinatePersistence(
					path.join(directory, "coordinate-wal"),
					{ flushOnAppend: true },
				),
			},
		});
		let client: Peerbit | undefined;
		const sandbox = sinon.createSandbox();
		const entered = pDefer<void>();
		const release = pDefer<void>();
		const pending: Promise<unknown>[] = [];
		const scans: IndexKeyScan[] = [];
		const scan = (shared: any, pageSize = 1): IndexKeyScan => {
			const result = shared._coordinates.scanAuthoritativeCoordinateKeys({
				pageSize,
			});
			expect(result, "native raw inventory capability").to.exist;
			scans.push(result);
			return result;
		};
		const assertNativeAuthority = (shared: any) => {
			expect(shared._nativeBackbone, "required real native backbone").to.exist;
			expect(shared.log.entryIndex.properties.nativeGraph).to.exist;
			expect(shared._nativeBackboneCoordinatePersistence).to.exist;
			expect(
				shared._nativeBackboneCoordinatePersistenceStore.durableBarrier,
			).to.be.a("function");
			expect(
				shared._coordinates.canUseNativeBackboneResidentCoordinateState(),
			).to.equal(true);
		};
		const assertInventory = async (shared: any, expected: string[]) => {
			assertNativeAuthority(shared);
			const inventory = scan(shared, 2);
			const observed: unknown[] = [];
			let complete = false;
			try {
				for (let page = 0; page <= expected.length; page++) {
					const result = await inventory.next();
					expect(result.keys.length).to.be.at.most(2);
					observed.push(...result.keys);
					if (result.status === "complete") {
						complete = true;
						break;
					}
					expect(result.status).to.equal("more");
				}
				expect(complete).to.equal(true);
				expect(observed.sort()).to.deep.equal([...expected].sort());
				expect(
					[...shared._nativeBackbone.getEntryCoordinateHashes()].sort(),
				).to.deep.equal([...expected].sort());
			} finally {
				await inventory.close();
			}
		};
		try {
			client = await Peerbit.create(diskOptions);
			const store = await client.open(new EventStore<string, any>(), {
				args: openArgs(),
			});
			const template = store.clone();
			const identity = client.identity.publicKey;
			const shared = store.log as any;
			assertNativeAuthority(shared);
			const lower = store.log.log.entryIndex;
			// Exercise the ordinary trusted native payload path, including actual
			// durable intent/marker/retirement work; never manufacture a lower owner.
			const append = (value: string) =>
				Promise.resolve().then(() =>
					shared.appendLocallyPreparedPayloadCommitOnly(
						new TextEncoder().encode(JSON.stringify({ op: "ADD", value })),
						{ target: "none", replicate: false, meta: { next: [] } },
						{ resolveTrimmedEntries: false, skipMissingNextJoin: true },
					),
				);
			const seeds = [await append("first"), await append("second")];
			for (const seed of seeds)
				expect(seed?.appendCommit.hash).to.be.a("string");
			const oldGeneration = lower.captureMutationGeneration();
			expect(oldGeneration).to.not.equal(undefined);
			const oldScan = scan(shared);
			expect((await oldScan.next()).status).to.equal("more");

			const acknowledged = sandbox.spy(
				lower as any,
				"acknowledgeNativeCommittedAppendFacts",
			);
			const prepare = sandbox.spy(
				shared._nativeBackbone,
				"preparePlainCommittedNoNextStorageAppendTransaction",
			);
			let clearingIntent = false;
			let heldBarriers = 0;
			const write =
				shared.writeNativeStrictDurableTransactionIntent.bind(shared);
			sandbox
				.stub(shared, "writeNativeStrictDurableTransactionIntent")
				.callsFake(async (intent) => {
					if (intent !== undefined) return write(intent);
					clearingIntent = true;
					try {
						return await write(intent);
					} finally {
						clearingIntent = false;
					}
				});
			const journalStore = shared._nativeBackboneCoordinatePersistenceStore;
			const barrier = journalStore.durableBarrier.bind(journalStore);
			sandbox
				.stub(journalStore, "durableBarrier")
				.callsFake(async (...args) => {
					if (clearingIntent) {
						heldBarriers++;
						entered.resolve();
						await release.promise;
					}
					return barrier(...args);
				});
			let settled = false;
			const appending = append("during-finalization").then((result) => {
				settled = true;
				return result;
			});
			void appending.catch(() => {});
			pending.push(appending);
			let deadline: ReturnType<typeof setTimeout> | undefined;
			try {
				await Promise.race([
					entered.promise,
					appending.then(() => {
						throw new Error(
							"Native append skipped its durable retirement barrier",
						);
					}),
					new Promise<never>((_, reject) => {
						deadline = setTimeout(
							() =>
								reject(new Error("Durable retirement barrier was not reached")),
							10_000,
						);
					}),
				]);
			} finally {
				clearTimeout(deadline);
			}
			expect(prepare.calledOnce, "actual fused native append").to.equal(true);
			expect(
				acknowledged.calledOnce,
				"inner lower transaction acknowledged",
			).to.equal(true);
			expect(acknowledged.firstCall.args[0].state).to.equal("acknowledged");
			expect(heldBarriers).to.equal(1);
			expect(settled).to.equal(false);
			const journal =
				await shared.loadNativeStrictDurableTransactionJournalState();
			expect(journal.intent.lowerMarkerCommitted).to.equal(true);
			expect(journal.intent.appendHashes).to.have.length(1);
			const appendedHash = journal.intent.appendHashes[0] as string;
			expect(shared._nativeStrictDurableTransactions.size).to.equal(1);
			expect(await store.log.log.blocks.has(appendedHash)).to.equal(true);
			expect(
				(
					await shared._coordinates.getAuthoritativeCoordinateEntryForInventory(
						appendedHash,
					)
				)?.hash,
			).to.equal(appendedHash);
			expect(
				lower.captureMutationGeneration(),
				"outer owner remains busy",
			).to.equal(undefined);
			expect(lower.isMutationGenerationCurrent(oldGeneration!)).to.equal(false);
			expect(await oldScan.next()).to.deep.equal({
				status: "invalidated",
				keys: [],
			});
			const during = scan(shared);
			expect(await during.next()).to.deep.equal({
				status: "invalidated",
				keys: [],
			});

			release.resolve();
			const appended = await appending;
			expect(appended.appendCommit.hash).to.equal(appendedHash);
			expect(shared._nativeStrictDurableTransactions.size).to.equal(0);
			expect(
				(await shared.loadNativeStrictDurableTransactionJournalState()).intent,
			).to.equal(undefined);
			expect(lower.captureMutationGeneration()).to.not.equal(undefined);
			expect(await during.next()).to.deep.equal({
				status: "invalidated",
				keys: [],
			});
			const expected = [
				...seeds.map((seed) => seed.appendCommit.hash),
				appendedHash,
			];
			await assertInventory(shared, expected);
			const bytes = serialize(appended.entry);
			sandbox.restore();
			await client.stop();
			client = undefined;

			client = await Peerbit.create(diskOptions);
			expect(client.identity.publicKey.equals(identity)).to.equal(true);
			expect(client.libp2p.getConnections()).to.have.length(0);
			const restored = await client.open(template, { args: openArgs() });
			await assertInventory(restored.log as any, expected);
			const entry = await restored.log.log.get(appendedHash, { remote: false });
			expect(entry).to.exist;
			expect(serialize(entry!)).to.deep.equal(bytes);
			expect(await entry!.verifySignatures()).to.equal(true);
			expect((await entry!.getPayloadValue()).value).to.equal(
				"during-finalization",
			);
			expect(client.libp2p.getConnections()).to.have.length(0);
		} finally {
			release.resolve();
			await Promise.allSettled(pending);
			await Promise.allSettled(scans.map((inventory) => inventory.close()));
			sandbox.restore();
			try {
				await client?.stop();
			} finally {
				await fs.rm(directory, { recursive: true, force: true });
			}
		}
	});
});
