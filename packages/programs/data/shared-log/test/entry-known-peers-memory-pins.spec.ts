// `_entryKnownPeerObservedAt` is a 30-second recency cache that used to be a
// permanent, unbounded map: one row per (entry hash, peer) mark, created for
// every gossiped head hash — including hashes this node never holds — with no
// entry-dimension removal path anywhere. Its only reader is
// isEntryRecentlyKnownByPeer, which treats an over-age row and an absent row
// identically, so dropping over-age rows is behaviour-identical rather than
// merely safe. These pins hold that line: raw map contents and raw reader
// results, no identity predicates, plus a boundary leg so the retention window
// cannot silently widen.
import { TestSession } from "@peerbit/test-utils";
import { expect } from "chai";
import sinon from "sinon";
import { EventStore } from "./utils/stores/index.js";

const RETENTION_MS = 30_000;

describe("entry known-peer recency memory", () => {
	let session: TestSession;
	let store: EventStore<string, any>;
	let clock: sinon.SinonFakeTimers;

	const internals = () => store.log as any;
	const mark = (hashes: string[], peer: string) =>
		internals().markEntriesKnownByPeer(hashes, peer);
	const observedRows = (): Map<string, Map<string, number>> =>
		internals()._entryKnownPeerObservedAt;
	const isRecent = (hash: string, peer: string, maxAge = RETENTION_MS) =>
		internals().isEntryRecentlyKnownByPeer(hash, peer, maxAge) as boolean;

	before(async () => {
		session = await TestSession.disconnected(1);
	});

	after(async () => {
		await session.stop();
	});

	beforeEach(async () => {
		store = new EventStore<string, any>();
		await session.peers[0].open(store, { args: { replicate: false } });
		// Fake timers only AFTER open: libp2p/session startup needs real ones.
		// Deterministic clock keeps the boundary leg from drifting on slow CI.
		clock = sinon.useFakeTimers({
			now: Date.now(),
			shouldAdvanceTime: false,
			toFake: ["Date"],
		});
	});

	afterEach(async () => {
		clock?.restore();
		await store?.close();
		sinon.restore();
	});

	it("drops recency rows once no reader can act on them", async () => {
		mark(["a", "b", "c"], "peer-1");
		// Mechanism live before claiming any bound: a sweep that wiped
		// everything unconditionally would also pass the assertions below.
		expect(observedRows().size).to.equal(3);
		expect(isRecent("a", "peer-1")).to.be.true;

		clock.tick(RETENTION_MS + 1);
		mark(["fresh"], "peer-2");

		expect(observedRows().size).to.equal(1);
		expect(observedRows().get("fresh")?.has("peer-2")).to.be.true;
		expect(observedRows().has("a")).to.be.false;
	});

	it("keeps rows that are still inside the window", async () => {
		mark(["kept"], "peer-1");
		// Exactly at the boundary. The reader uses `<= maxAgeMs`, so this row is
		// still actionable and must survive — pins the sweep's comparison as
		// strictly-greater, not greater-or-equal.
		clock.tick(RETENTION_MS);
		mark(["driver"], "peer-2");

		expect(observedRows().has("kept")).to.be.true;
		expect(isRecent("kept", "peer-1")).to.be.true;
	});

	it("leaves the membership map and its reader untouched", async () => {
		mark(["a"], "peer-1");
		clock.tick(RETENTION_MS + 1);
		mark(["b"], "peer-2");

		// Recency for "a" is gone, but membership is a separate dimension with
		// its own peer-side lifetime and must not be swept with it.
		expect(observedRows().has("a")).to.be.false;
		const membership = internals()._entryKnownPeers as Map<string, Set<string>>;
		expect(membership.get("a")?.has("peer-1")).to.be.true;
		expect(internals().isEntryKnownByPeer("a", "peer-1")).to.be.true;
		expect(isRecent("a", "peer-1")).to.be.false;
	});

	it("bounds the map to one window under sustained marking", async () => {
		// The leak shape: many distinct hashes over a long run. Unswept this
		// reaches 200 rows and keeps climbing; swept, only the current window
		// survives.
		for (let i = 0; i < 100; i++) {
			mark([`old-${i}`], `peer-${i % 5}`);
			clock.tick(1_000);
		}
		expect(observedRows().size).to.be.greaterThan(0);

		clock.tick(RETENTION_MS + 1);
		for (let i = 0; i < 100; i++) {
			mark([`new-${i}`], `peer-${i % 5}`);
		}

		const rows = observedRows();
		expect(rows.size).to.equal(100);
		for (let i = 0; i < 100; i++) {
			expect(rows.has(`old-${i}`), `old-${i} must be swept`).to.be.false;
		}
	});
});
