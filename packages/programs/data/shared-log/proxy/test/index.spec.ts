import { serialize } from "@dao-xyz/borsh";
import { AnyBlockStore } from "@peerbit/blocks";
import { Ed25519Keypair } from "@peerbit/crypto";
import { toId } from "@peerbit/indexer-interface";
import { createEntry } from "@peerbit/log";
import { expect } from "chai";
import { createSharedLogProxyFromService } from "../src/client.js";
import {
	SharedLogBytes,
	SharedLogEntriesBatch,
	SharedLogEntriesIteratorService,
	SharedLogEvent,
	SharedLogReplicateFixed,
	SharedLogReplicateRequest,
	SharedLogReplicationBatch,
	SharedLogReplicationIndexResult,
	SharedLogReplicationIteratorService,
	SharedLogService,
	SharedLogUnreplicateRequest,
} from "../src/protocol.js";

describe("@peerbit/shared-log-proxy", () => {
	let store: AnyBlockStore;

	const importSharedLog = async (): Promise<any> => {
		const candidates = [
			new URL("../../src/index.js", import.meta.url).href,
			new URL("../../dist/src/index.js", import.meta.url).href,
			new URL("../../../dist/src/index.js", import.meta.url).href,
		];
		let lastError: any;
		for (const href of candidates) {
			try {
				const mod = await import(href);
				if (mod?.ReplicationRangeIndexableU32) return mod;
				lastError = new Error(
					`Missing ReplicationRangeIndexableU32 export in ${href}`,
				);
			} catch (error) {
				lastError = error;
			}
		}
		throw lastError ?? new Error("Failed to import @peerbit/shared-log");
	};

	before(async () => {
		store = new AnyBlockStore();
		await store.start();
	});

	after(async () => {
		await store.stop();
	});

	it("wraps SharedLogService with log and replication helpers", async () => {
		const { ReplicationRangeIndexableU32 } = await importSharedLog();
		const identity = await Ed25519Keypair.create();
		const entry = await createEntry({
			store,
			identity,
			meta: {
				gidSeed: new Uint8Array([1, 2, 3]),
			},
			data: new Uint8Array([1]),
		});
		const entryBytes = serialize(entry);

		const range = new ReplicationRangeIndexableU32({
			publicKey: identity.publicKey,
			offset: 0,
			width: 10,
		});
		const rangeBytes = serialize(range);

		let headsDone = false;
		const headsService = new SharedLogEntriesIteratorService({
			next: async (_amount: number) => {
				if (headsDone) {
					return new SharedLogEntriesBatch({ entries: [], done: true });
				}
				headsDone = true;
				return new SharedLogEntriesBatch({
					entries: [new SharedLogBytes({ value: entryBytes })],
					done: true,
				});
			},
			pending: async () => undefined,
			done: async () => headsDone,
			close: async () => {
				headsDone = true;
			},
		});

		let replicationDone = false;
		const replicationService = new SharedLogReplicationIteratorService({
			next: async (_amount: number) => {
				if (replicationDone) {
					return new SharedLogReplicationBatch({ results: [], done: true });
				}
				replicationDone = true;
				return new SharedLogReplicationBatch({
					results: [
						new SharedLogReplicationIndexResult({
							id: toId("replicator"),
							value: new SharedLogBytes({ value: rangeBytes }),
						}),
					],
					done: true,
				});
			},
			pending: async () => undefined,
			done: async () => replicationDone,
			close: async () => {
				replicationDone = true;
			},
		});

		let lastReplicate: SharedLogReplicateRequest | undefined;
		let lastUnreplicate: SharedLogUnreplicateRequest | undefined;
		const service = new SharedLogService({
			logGet: async (hash) => {
				return hash === entry.hash
					? new SharedLogBytes({ value: entryBytes })
					: undefined;
			},
			logHas: async (hash) => hash === entry.hash,
			logToArray: async () => [new SharedLogBytes({ value: entryBytes })],
			logGetHeads: async () => headsService,
			logLength: async () => BigInt(1),
			logBlockHas: async () => false,
			replicationIterate: async (_request) => replicationService,
			replicationCount: async (_request) => BigInt(1),
			getReplicators: async () => [identity.publicKey.hashcode()],
			waitForReplicator: async (_request) => {},
			waitForReplicators: async (_request) => {},
			replicate: async (request) => {
				lastReplicate = request;
			},
			unreplicate: async (request) => {
				lastUnreplicate = request;
			},
			calculateCoverage: async (_request) => 1,
			getMyReplicationSegments: async () => [
				new SharedLogBytes({ value: rangeBytes }),
			],
			getAllReplicationSegments: async () => [
				new SharedLogBytes({ value: rangeBytes }),
			],
			resolution: async () => "u32",
			publicKey: async () => identity.publicKey,
			close: async () => {},
		});

		const proxy = await createSharedLogProxyFromService(service);

		const got = await proxy.log.get(entry.hash);
		expect(got?.hash).to.equal(entry.hash);

		const heads = await proxy.log.getHeads().all();
		expect(heads).to.have.length(1);
		expect(heads[0]?.hash).to.equal(entry.hash);

		const ranges = await proxy.replicationIndex.iterate().all();
		expect(ranges).to.have.length(1);
		expect(ranges[0]?.value.hash).to.equal(range.hash);

		const replicators = await proxy.getReplicators();
		expect([...replicators]).to.include(identity.publicKey.hashcode());

		const joinEvents: string[] = [];
		proxy.events.addEventListener("replicator:join", (event: any) => {
			joinEvents.push(event.detail.publicKey.hashcode());
		});

		service.events.dispatchEvent(
			new CustomEvent("replicator:join", {
				detail: new SharedLogEvent({ publicKey: identity.publicKey }),
			}),
		);
		expect(joinEvents).to.deep.equal([identity.publicKey.hashcode()]);

		await proxy.replicate({ factor: 1 });
		expect(lastReplicate).to.be.instanceOf(SharedLogReplicateRequest);
		expect(lastReplicate?.value).to.be.instanceOf(SharedLogReplicateFixed);
		expect(
			(lastReplicate?.value as SharedLogReplicateFixed).range.factor,
		).to.equal(1);

		const unreplicateId = new Uint8Array([9, 9, 9]);
		await proxy.unreplicate([{ id: unreplicateId }]);
		expect(lastUnreplicate?.ids[0]).to.deep.equal(unreplicateId);

		await proxy.close();
	});
});
