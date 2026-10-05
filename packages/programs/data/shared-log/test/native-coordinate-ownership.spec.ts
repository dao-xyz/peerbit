import { create as createRustIndexer } from "@peerbit/indexer-rust";
import { TestSession } from "@peerbit/test-utils";
import { expect } from "chai";
import pDefer from "p-defer";
import sinon from "sinon";
import { EventStore } from "./utils/stores/event-store.js";

const payload = (value: string) =>
	new TextEncoder().encode(JSON.stringify({ op: "ADD", value }));

describe("native local coordinate mutation ownership", () => {
	let session: TestSession | undefined;

	afterEach(async () => {
		sinon.restore();
		await session?.stop();
		session = undefined;
	});

	for (const kind of ["parent", "storage trim", "commit-only trim"] as const) {
		it(`does not mutate a leased ${kind} before owning its lower hash`, async () => {
			const trims = kind !== "parent";
			const commitOnly = kind === "commit-only trim";
			session = await TestSession.disconnected(1, {
				indexer: (directory) => createRustIndexer(directory),
			});
			const store = await session.peers[0].open(new EventStore<string, any>(), {
				args: {
					replicate: commitOnly ? false : { factor: 1 },
					timeUntilRoleMaturity: 0,
					nativeGraph: true,
					nativeBackbone: { optional: false },
					...(trims ? { trim: { type: "length" as const, to: 1 } } : {}),
				},
			});
			const shared = store.log as any;
			const backbone = shared._nativeBackbone;
			expect(backbone, "required real native backbone").to.exist;
			const index = store.log.log.entryIndex;
			const append = (value: string, next: unknown[] = []) =>
				Promise.resolve().then(() =>
					shared.appendLocallyPreparedPayloadCommitOnly(
						payload(value),
						{ target: "none", replicate: false, meta: { next } },
						{ resolveTrimmedEntries: false, skipMissingNextJoin: true },
					),
				);
			const first = await append("first");
			expect(first, "prepared native seed append").to.exist;
			const victim = first.appendCommit.hash as string;
			expect(backbone.graph.has(victim), "seed graph entry").to.equal(true);
			expect(backbone.blocks.get(victim), "seed native block").to.exist;
			const snapshot = () => ({
				victimPresent: backbone.graph.has(victim),
				heads: [...backbone.graph.heads()].sort(),
				coordinates: [...backbone.getEntryCoordinateHashes()].sort(),
				victimBlock: backbone.blocks.get(victim)?.slice(),
			});
			const before = snapshot();
			const nativePrepare = commitOnly
				? sinon.spy(backbone.graph, "prepareEntryV0PlainEntryCommit")
				: sinon.spy(
						backbone,
						trims
							? "preparePlainCommittedNoNextStorageAppendTransaction"
							: "preparePlainCommittedStorageAppendTransaction",
					);
			const owner = await index.acquireHashMutationLocks([victim]);
			const acquiring = pDefer<void>();
			const acquire = index.acquireExclusiveMutationLock.bind(index);
			sinon.stub(index, "acquireExclusiveMutationLock").callsFake(() => {
				const pending = acquire();
				acquiring.resolve();
				return pending;
			});
			const appending = append("second", trims ? [] : [first.entry]);
			// Attach rejection ownership immediately, including on an assertion failure.
			void appending.catch(() => {});
			let boundaryTimeout: ReturnType<typeof setTimeout> | undefined;
			const boundaryDeadline = new Promise<never>((_, reject) => {
				boundaryTimeout = setTimeout(
					() => reject(new Error("Append did not reach hash ownership")),
					10_000,
				);
			});
			try {
				// Observe the actual contested acquisition, not a timer or a fake native
				// delay. Before the fix, the fused call already changed this state.
				await Promise.race([
					acquiring.promise,
					boundaryDeadline,
					appending.then(() => {
						throw new Error(
							"Append completed without acquiring its victim hash",
						);
					}),
				]);
				expect(
					snapshot(),
					"native state while another mutation owns the hash",
				).to.deep.equal(before);
				expect(
					nativePrepare.callCount,
					"native mutation before ownership",
				).to.equal(0);
			} finally {
				clearTimeout(boundaryTimeout);
				index.releaseHashMutationLocks(owner);
				await Promise.allSettled([appending]);
			}
			const second = await appending;
			expect(second, "prepared native append after release").to.exist;
			expect(
				nativePrepare.callCount,
				"the intended fused native path",
			).to.equal(1);
			expect(await store.log.log.has(second.appendCommit.hash)).to.equal(true);
			expect(backbone.graph.heads()).to.include(second.appendCommit.hash);
			if (trims) {
				expect(second.removedHashes).to.deep.equal([victim]);
				expect(backbone.graph.has(victim)).to.equal(false);
			} else {
				expect(backbone.graph.has(victim)).to.equal(true);
				expect(backbone.graph.heads()).not.to.include(victim);
			}
		});
	}

	for (const batch of [false, true]) {
		it(`does not resurrect a ${batch ? "batch" : "single"} head superseded by its change callback`, async () => {
			session = await TestSession.disconnected(1, {
				indexer: (directory) => createRustIndexer(directory),
			});
			const store = await session.peers[0].open(new EventStore<string, any>(), {
				args: {
					replicate: { factor: 1 },
					timeUntilRoleMaturity: 0,
					nativeGraph: true,
					nativeBackbone: { optional: false },
				},
			});
			const shared = store.log as any;
			let childHash: string | undefined;
			const onChange = async (change: any) => {
				const parent = change.added.at(-1).entry;
				childHash = (
					await store.add("nested", {
						target: "none",
						meta: { next: [parent] },
					})
				).entry.hash;
			};
			const options = { target: "none" as const, onChange };
			const parents = batch
				? (await store.addMany(["first", "second"], options)).entries
				: [(await store.add("first", options)).entry];
			expect(childHash).to.be.a("string");
			expect(shared._nativeBackbone.graph.heads()).to.deep.equal([childHash]);
			expect(shared._nativeBackbone.getEntryCoordinateHashes()).to.deep.equal([
				childHash,
			]);
			expect(
				await shared._coordinates.getAuthoritativeCoordinateEntryForReceipt(
					childHash,
				),
			).to.have.property("hash", childHash);
			for (const parent of parents) {
				expect(
					await shared._coordinates.getAuthoritativeCoordinateEntryForReceipt(
						parent.hash,
					),
				).to.equal(undefined);
			}
		});
	}
});
