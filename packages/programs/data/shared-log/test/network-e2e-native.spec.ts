// End-to-end proof of the pure native network hot path through the real
// peerbit client preset (`peerbit/rust`): inbound frames are decoded and
// signature-verified in wasm, shared-log raw exchange-head payloads are
// stashed by the wire-sync session and committed by the native backbone
// without any per-entry JS decode/verify/copy. The counters below assert the
// absence of JS-side per-message work mechanically.
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import { Peerbit } from "peerbit";
import { createRustPeerbitOptions } from "peerbit/rust";
import type { SyncProfileEvent } from "../src/sync/index.js";
import { EventStore } from "./utils/stores/event-store.js";

type WireCounters = {
	nativeFrames: number;
	nativeFallbackFrames: number;
	tsFrames: number;
	tsSignatureVerifies: number;
	tsEnvelopeDecodes: number;
};

const wireCountersOf = (client: Peerbit): WireCounters =>
	(client.services.pubsub as unknown as { wireCounters: WireCounters })
		.wireCounters;

const sumEvents = (
	events: SyncProfileEvent[],
	name: string,
	pick: (event: SyncProfileEvent) => number,
) =>
	events
		.filter((event) => event.name === name)
		.reduce((sum, event) => sum + pick(event), 0);

describe("network e2e native preset", function () {
	this.timeout(120_000);

	let peer1: Peerbit;
	let peer2: Peerbit;

	beforeEach(async () => {
		peer1 = await Peerbit.create({ ...createRustPeerbitOptions() });
		peer2 = await Peerbit.create({ ...createRustPeerbitOptions() });
	});

	afterEach(async () => {
		await peer1?.stop();
		await peer2?.stop();
	});

	it("syncs the bulk data plane with zero per-entry JS decode/verify/copy", async () => {
		const entryCount = 200;
		const profileEvents1: SyncProfileEvent[] = [];
		const profileEvents2: SyncProfileEvent[] = [];

		const store = new EventStore<string, any>();
		// no onChange consumer: the client preset defaults apply the raw
		// exchange-heads sync and the wire-sync receive fusion
		const db1 = await peer1.open(store.clone(), {
			args: {
				replicate: { factor: 1 },
				sync: {
					profile: (event: SyncProfileEvent) => profileEvents1.push(event),
				},
			},
		});
		const db2 = await peer2.open(store.clone(), {
			args: {
				replicate: { factor: 1 },
				sync: {
					profile: (event: SyncProfileEvent) => profileEvents2.push(event),
				},
			},
		});

		// the preset injected the native backbone data plane and registered
		// the program topic on the wire-sync session
		expect((db1.log as any)._nativeBackbone, "peer1 native backbone").to
			.exist;
		expect((db2.log as any)._nativeBackbone, "peer2 native backbone").to
			.exist;
		expect(
			(peer2.nativeNetwork!.wireSync! as unknown as { topicCount: number })
				.topicCount,
		).to.be.greaterThan(0);

		// independent heads: the bulk data plane ships them in raw
		// exchange-head batches instead of resolving a next-chain
		for (let index = 0; index < entryCount; index++) {
			await db1.add(`entry-${index}`, { meta: { next: [] } });
		}

		await peer2.dial(peer1.getMultiaddrs());
		await waitForResolved(
			() => {
				expect(db2.log.log.length).to.equal(entryCount);
			},
			{ timeout: 60_000, timeoutMessage: "native preset cold sync" },
		);

		for (const [label, events] of [
			["peer1", profileEvents1],
			["peer2", profileEvents2],
		] as const) {
			// no sync message had its entries borsh-decoded in JS
			expect(
				sumEvents(
					events,
					"sharedLog.rawReceive.jsEntryDecode",
					(event) => event.entries ?? 0,
				),
				label + " jsEntryDecode entries",
			).to.equal(0);
			// no raw message fell back to the TS borsh entry decode
			expect(
				sumEvents(
					events,
					"sharedLog.rawReceive.deserializeFallback",
					() => 1,
				),
				label + " deserialize fallbacks",
			).to.equal(0);
			// no stashed head bytes were copied back out to JS
			expect(
				sumEvents(
					events,
					"sharedLog.rawReceive.wireStashRelease",
					(event) => (event.details?.bytesMaterialized as number) ?? 0,
				),
				label + " bytes materialized",
			).to.equal(0);
		}

		// every synced entry was resolved from the wire stash (native memory)
		expect(
			sumEvents(
				profileEvents2,
				"sharedLog.rawReceive.wireStashResolve",
				(event) => event.entries ?? 0,
			),
		).to.be.greaterThanOrEqual(entryCount);

		// wire-sync session counters: frames were stashed in wasm, consumed
		// there, released, and no block bytes ever crossed back into JS
		const counters2 = peer2.nativeNetwork!.wireSync!.counters!();
		expect(counters2.stashed).to.be.greaterThan(0);
		expect(counters2.blockCopyOuts).to.equal(0);
		expect(counters2.evicted).to.equal(0);
		expect(peer1.nativeNetwork!.wireSync!.counters!().blockCopyOuts).to.equal(
			0,
		);

		// DirectStream wire counters: every inbound frame was decoded and
		// signature-verified natively; no frame fell back to the TS path and
		// no TS-side signature verification ran. (The TS envelope object per
		// frame is the deliberate exception: it feeds the routing state
		// machine and app-facing events and holds the payload as a zero-copy
		// view.)
		for (const [label, client] of [
			["peer1", peer1],
			["peer2", peer2],
		] as const) {
			const counters = wireCountersOf(client);
			expect(counters.tsFrames, label + " tsFrames").to.equal(0);
			expect(
				counters.nativeFallbackFrames,
				label + " nativeFallbackFrames",
			).to.equal(0);
			expect(
				counters.tsSignatureVerifies,
				label + " tsSignatureVerifies",
			).to.equal(0);
			expect(
				counters.nativeFrames,
				label + " nativeFrames",
			).to.be.greaterThan(0);
		}
	});

	it("keeps a mixed pure-native and all-default pair in sync", async () => {
		const defaultPeer = await Peerbit.create();
		try {
			expect(defaultPeer.nativeNetwork).to.equal(undefined);
			expect((defaultPeer.services.pubsub as any).rustCore).to.equal(
				undefined,
			);
			expect((defaultPeer.services.pubsub as any).nativeWire).to.equal(
				undefined,
			);

			const store = new EventStore<string, any>();
			const nativeDb = await peer1.open(store.clone(), {
				args: { replicate: { factor: 1 } },
			});
			const defaultDb = await defaultPeer.open(store.clone(), {
				args: { replicate: { factor: 1 } },
			});

			await defaultPeer.dial(peer1.getMultiaddrs());

			await nativeDb.addMany(
				Array.from({ length: 32 }, (_, index) => `native-${index}`),
			);
			await waitForResolved(
				() => {
					expect(defaultDb.log.log.length).to.equal(32);
				},
				{ timeout: 60_000, timeoutMessage: "mixed native->default sync" },
			);

			await defaultDb.addMany(
				Array.from({ length: 32 }, (_, index) => `default-${index}`),
			);
			await waitForResolved(
				() => {
					expect(nativeDb.log.log.length).to.equal(64);
				},
				{ timeout: 60_000, timeoutMessage: "mixed default->native sync" },
			);

			// the native peer stayed native against a JS-only sender
			// (byte-identical wire): nothing fell back and nothing was
			// verified in JS
			const nativeCounters = wireCountersOf(peer1);
			expect(nativeCounters.tsFrames).to.equal(0);
			expect(nativeCounters.nativeFallbackFrames).to.equal(0);
			expect(nativeCounters.tsSignatureVerifies).to.equal(0);
			// the default peer processed everything on the unchanged TS path
			expect(wireCountersOf(defaultPeer).nativeFrames).to.equal(0);
			expect(wireCountersOf(defaultPeer).tsFrames).to.be.greaterThan(0);
		} finally {
			await defaultPeer.stop();
		}
	});
});
