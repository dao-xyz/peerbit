import { create as createSQLiteIndices } from "@peerbit/indexer-sqlite3";
import { TestSession } from "@peerbit/test-utils";
import { expect } from "chai";
import sinon from "sinon";
import {
	RawEntryWithRefs,
	RawExchangeHeadsMessage,
} from "../src/exchange-heads.js";
import { createReplicationDomainHash } from "../src/replication-domain-hash.js";
import { SimpleSyncronizer } from "../src/sync/simple.js";
import { EventStore } from "./utils/stores/event-store.js";

describe("receive coordinate completion notifications", () => {
	for (const observeChanges of [false, true]) {
		it(`preserves ${observeChanges ? "program change events" : "hash-only notifications"} for independent fallback admission`, async () => {
			const session = await TestSession.disconnected(2, {
				indexer: createSQLiteIndices,
			});
			const sandbox = sinon.createSandbox();
			try {
				const args = {
					replicate: false as const,
					keep: () => true,
					timeUntilRoleMaturity: 0,
					nativeGraph: true,
					setup: {
						domain: createReplicationDomainHash("u32"),
						type: "u32" as const,
						syncronizer: SimpleSyncronizer,
						name: "receive-coordinate-notifications",
					},
				};
				const source = await session.peers[0].open(
					new EventStore<string, any>(),
					{ args },
				);
				const authorized: string[] = [];
				const observed: string[] = [];
				const target = await session.peers[1].open(source.clone(), {
					args: {
						...args,
						// Force the independent lower batch, not prepared-facts admission.
						canAppend: (entry) => {
							authorized.push(entry.hash);
							return true;
						},
						...(observeChanges
							? {
									onChange: (change: {
										added: { entry: { hash: string } }[];
									}) => {
										observed.push(
											...change.added.map(({ entry }) => entry.hash),
										);
									},
								}
							: {}),
					},
				});
				const entries = [];
				for (const value of ["first", "second"]) {
					entries.push((await source.add(value, { meta: { next: [] } })).entry);
				}
				const hashes = entries.map((entry) => entry.hash);
				const message = new RawExchangeHeadsMessage({
					heads: await Promise.all(
						entries.map(async (entry) => {
							const bytes = await source.log.log.blocks.get(entry.hash);
							expect(bytes).to.be.instanceOf(Uint8Array);
							return new RawEntryWithRefs({
								hash: entry.hash,
								bytes: bytes!,
								gidRefrences: [],
							});
						}),
					),
				});
				const lower = target.log.log.entryIndex;
				expect(lower.properties.nativeGraph).to.exist;
				const lowerBatch = sandbox.spy(lower, "putAppendBatch");
				const preparedBatch = sandbox.spy(lower, "putAppendFactsBatch");
				const coordinates = target.log.entryCoordinatesIndex;
				const coordinateBatch = sandbox.spy(coordinates, "putBatch");
				const coordinateSingle = sandbox.spy(coordinates, "put");
				const hashOnly = sandbox.spy(
					target.log.syncronizer as any,
					"onEntryAddedHashes",
				);
				const fullChange = sandbox.spy(target.log, "onChange");

				await target.log.onMessage(message, {
					from: source.node.identity.publicKey,
				} as any);

				expect(authorized).to.have.members(hashes);
				expect(lowerBatch.callCount).to.equal(1);
				expect(lowerBatch.firstCall.args[0]).to.have.length(2);
				expect(preparedBatch.callCount).to.equal(0);
				expect(coordinateBatch.callCount).to.equal(1);
				expect(
					coordinateBatch.firstCall.args[0].map(
						(row: { hash: string }) => row.hash,
					),
				).to.have.members(hashes);
				expect(coordinateSingle.callCount).to.equal(0);
				expect(
					(await coordinates.iterate({}).all()).map(({ value }) => value.hash),
				).to.have.members(hashes);
				expect(target.log.log.length).to.equal(2);
				expect(hashOnly.callCount).to.equal(observeChanges ? 0 : 1);
				expect(fullChange.callCount).to.equal(observeChanges ? 1 : 0);
				if (observeChanges) {
					expect(observed).to.have.members(hashes);
					expect(coordinateBatch.calledBefore(fullChange)).to.equal(true);
				} else {
					expect(hashOnly.firstCall.args[0]).to.have.members(hashes);
					expect(observed).to.have.length(0);
					expect(coordinateBatch.calledBefore(hashOnly)).to.equal(true);
				}
			} finally {
				sandbox.restore();
				await session.stop();
			}
		});
	}
});
