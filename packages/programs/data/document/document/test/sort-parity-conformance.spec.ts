// Cross-backend SORT PARITY: given the SAME entries, a pure-native (rust) index
// and an all-default (JS) index must return query results in the IDENTICAL
// order — including how they break TIES between equal sort keys.
//
// Sort order (and especially tie-breaking on equal keys) is comparator-driven,
// which is exactly where a native comparator can silently diverge from JS. The
// existing sort tests are 100% default-only, so a native index that ordered
// ties differently (e.g. by hash vs by clock, or a different byte comparison)
// would pass every test while returning a different order in a real hybrid
// deployment.
//
// This authors half the documents on each backend and syncs, so both peers hold
// the identical set of content-addressed entries (identical clocks/heads =
// identical tie-break inputs), then runs the same sorted query on both and
// asserts the full result ORDER matches.
import { policy, transform } from "../src/index.js";
import { SearchRequest } from "@peerbit/document-interface";
import { Sort, SortDirection } from "@peerbit/indexer-interface";
import {
	NativeBackboneCoordinatePersistence,
	NativeBackboneMemoryCoordinatePersistenceStore,
} from "@peerbit/native-backbone";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import { Peerbit } from "peerbit";
import { createRustPeerbitOptions } from "peerbit/rust";
import { Documents } from "../src/program.js";
import { Document, TestStore } from "./data.js";

const nativeBackboneDocumentIndexOptions = () => ({
	optional: false,
	documentIndex: true,
	coordinatePersistence: new NativeBackboneCoordinatePersistence(
		new NativeBackboneMemoryCoordinatePersistenceStore(),
		{ flushOnAppend: false },
	),
});

const nativeOpenArgs = () => ({
	mode: "native" as const,
	replicate: { factor: 1 },
	nativeGraph: true,
	nativeBackbone: nativeBackboneDocumentIndexOptions(),
	canPerform: policy.allowAll<Document>(),
	index: { type: Document, transform: transform.identity<Document>() },
});

const defaultOpenArgs = () => ({
	replicate: { factor: 1 },
	canPerform: policy.allowAll<Document>(),
	index: { type: Document },
});

describe("sort parity (native vs default)", function () {
	this.timeout(120_000);

	let nativePeer: Peerbit;
	let defaultPeer: Peerbit;

	beforeEach(async () => {
		nativePeer = await Peerbit.create({ ...createRustPeerbitOptions() });
		defaultPeer = await Peerbit.create();
	});

	afterEach(async () => {
		await nativePeer?.stop();
		await defaultPeer?.stop();
	});

	it("orders sorted queries identically, including tie-breaks", async () => {
		const store = new TestStore({ docs: new Documents<Document>() as any });
		const nativeStore = await nativePeer.open(store.clone(), {
			args: nativeOpenArgs() as any,
		});
		const defaultStore = await defaultPeer.open(store.clone(), {
			args: defaultOpenArgs() as any,
		});
		await defaultPeer.dial(nativePeer.getMultiaddrs());

		// Numbers with MANY duplicates force tie-breaking; interleaving
		// authorship across backends makes the ties span both origins.
		const nativeNumbers = [3n, 1n, 3n, 2n, 1n, 2n];
		const defaultNumbers = [2n, 3n, 1n, 3n, 2n, 1n];
		for (let i = 0; i < nativeNumbers.length; i++) {
			await nativeStore.docs.put(
				new Document({ id: `n-${i}`, name: `n-${i}`, number: nativeNumbers[i] }),
			);
		}
		for (let i = 0; i < defaultNumbers.length; i++) {
			await defaultStore.docs.put(
				new Document({
					id: `d-${i}`,
					name: `d-${i}`,
					number: defaultNumbers[i],
				}),
			);
		}
		const total = nativeNumbers.length + defaultNumbers.length;

		await waitForResolved(
			async () => {
				expect(await nativeStore.docs.index.getSize()).to.equal(total);
				expect(await defaultStore.docs.index.getSize()).to.equal(total);
			},
			{ timeout: 60_000, timeoutMessage: "sort-parity convergence" },
		);

		const collectOrder = async (store: TestStore, direction: SortDirection) => {
			const iterator = store.docs.index.iterate(
				new SearchRequest({
					query: [],
					sort: [new Sort({ direction, key: "number" })],
				}),
			);
			const results = await iterator.next(total);
			await iterator.close();
			return results.map((doc: any) => ({ id: doc.id, number: doc.number }));
		};

		for (const direction of [SortDirection.ASC, SortDirection.DESC]) {
			const nativeOrder = await collectOrder(nativeStore, direction);
			const defaultOrder = await collectOrder(defaultStore, direction);

			expect(nativeOrder.length, `${direction} count`).to.equal(total);
			// The `number` sequences must be monotonic in the requested direction.
			const nums = nativeOrder.map((r) => r.number as bigint);
			for (let i = 1; i < nums.length; i++) {
				if (direction === SortDirection.ASC) {
					expect(nums[i] >= nums[i - 1], `${direction} monotonic`).to.equal(true);
				} else {
					expect(nums[i] <= nums[i - 1], `${direction} monotonic`).to.equal(true);
				}
			}
			// THE parity assertion: identical full order, ties and all.
			expect(
				nativeOrder,
				`native and default order identically (${direction}, incl. tie-breaks)`,
			).to.deep.equal(defaultOrder);
		}
	});

	// String ids only exercise the UTF-8/BINARY tie-break. The numeric (bigint)
	// and raw-byte (Uint8Array) id tie-breaks — the two id kinds that diverge
	// between the native and default comparators — are covered end-to-end at the
	// indexer level by the shared @peerbit/indexer-tests `tieParityTests` suite,
	// which asserts @peerbit/indexer-simple == @peerbit/indexer-sqlite3 for real
	// bigint and Uint8Array primary-key ids in both directions. The document layer
	// itself only supports string/base64 primary keys through its put path, so the
	// bigint/Uint8Array parity is asserted where those ids are natively indexable.
});
