// Cross-backend WIRE CONFORMANCE: a pure-native (rust) peer and an all-default
// (JS) peer that sync over the frozen /peerbit/* wire must converge to
// BYTE-IDENTICAL log state, not merely the same entry count.
//
// The existing "keeps a mixed pure-native and all-default pair in sync" test
// asserts convergence by log.length only. That would pass even if the native
// and JS encoders produced subtly different (still-valid-looking) entries: each
// peer would still end up with count = N+M. Entries are content-addressed by
// hash, so this test asserts the full SET of entry hashes is identical on both
// peers after a bidirectional sync — an identical hash set proves the native
// and JS wire/encoding produce byte-identical entries. A native encoder that
// emitted a different byte layout would hash differently on the receiver and
// the two hash sets would diverge here.
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import { Peerbit } from "peerbit";
import { createRustPeerbitOptions } from "peerbit/rust";
import { EventStore } from "./utils/stores/event-store.js";

describe("network e2e mixed-backend conformance", function () {
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

	it("native and default peers converge to byte-identical log entries", async () => {
		// The default peer must be the unmodified JS wire path.
		expect(defaultPeer.nativeNetwork).to.equal(undefined);
		expect((defaultPeer.services.pubsub as any).rustCore).to.equal(undefined);

		const store = new EventStore<string, any>();
		const nativeDb = await nativePeer.open(store.clone(), {
			args: { replicate: { factor: 1 } },
		});
		const defaultDb = await defaultPeer.open(store.clone(), {
			args: { replicate: { factor: 1 } },
		});
		await defaultPeer.dial(nativePeer.getMultiaddrs());

		const half = 20;
		const nativeValues = Array.from({ length: half }, (_, i) => `native-${i}`);
		const defaultValues = Array.from({ length: half }, (_, i) => `default-${i}`);

		// Each backend authors half the entries, so both wire directions
		// (native->default and default->native) are exercised.
		await nativeDb.addMany(nativeValues);
		await defaultDb.addMany(defaultValues);

		await waitForResolved(
			() => {
				expect(nativeDb.log.log.length, "native reached full set").to.equal(
					half * 2,
				);
				expect(defaultDb.log.log.length, "default reached full set").to.equal(
					half * 2,
				);
			},
			{ timeout: 60_000, timeoutMessage: "mixed bidirectional sync" },
		);

		const hashesOf = async (db: typeof nativeDb) =>
			(await db.log.log.toArray()).map((entry: any) => entry.hash).sort();
		const gidsOf = async (db: typeof nativeDb) =>
			(await db.log.log.toArray()).map((entry: any) => entry.meta.gid).sort();
		const valuesOf = async (db: typeof nativeDb) =>
			(await db.log.log.toArray())
				.map((entry: any) => entry.payload.getValue().value)
				.sort();

		// THE conformance assertion: identical content-addressed hash SET on
		// both backends => byte-identical entries across the frozen wire.
		const nativeHashes = await hashesOf(nativeDb);
		const defaultHashes = await hashesOf(defaultDb);
		expect(nativeHashes.length).to.equal(half * 2);
		expect(
			nativeHashes,
			"native and default converge to identical entry hashes (byte-identical entries)",
		).to.deep.equal(defaultHashes);

		// gids and values must agree too.
		expect(await gidsOf(nativeDb)).to.deep.equal(await gidsOf(defaultDb));
		const expectedValues = [...nativeValues, ...defaultValues].sort();
		expect(await valuesOf(nativeDb)).to.deep.equal(expectedValues);
		expect(await valuesOf(defaultDb)).to.deep.equal(expectedValues);

		// Heads converge identically too (same tips => same DAG frontier).
		const headsOf = async (db: typeof nativeDb) =>
			(await db.log.log.getHeads(true).all())
				.map((entry: any) => entry.hash)
				.sort();
		expect(await headsOf(nativeDb)).to.deep.equal(await headsOf(defaultDb));
	});
});
