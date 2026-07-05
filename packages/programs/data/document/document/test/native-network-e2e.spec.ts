// End-to-end coverage for the pure native network path: `peerbit/rust`'s
// preset must turn on the whole chain (native wire decode+verify, rust-core
// DirectStream + protocol ports, wire-sync receive fusion and the native
// backbone data plane) so that on the message hot path bytes flow
// socket -> native engine -> native raw receive -> native index commit with
// no per-entry JS decode/verify/copy, while a mixed network with an
// all-default peer keeps working.
import {
	NativeBackboneCoordinatePersistence,
	NativeBackboneMemoryCoordinatePersistenceStore,
} from "@peerbit/native-backbone";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import { Peerbit } from "peerbit";
import { createRustPeerbitOptions } from "peerbit/rust";
import sinon from "sinon";
import { policy, transform } from "../src/index.js";
import { Documents } from "../src/program.js";
import { Document, TestStore } from "./data.js";

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

const nativeBackboneDocumentIndexOptions = () => ({
	optional: false,
	documentIndex: true,
	coordinatePersistence: new NativeBackboneCoordinatePersistence(
		new NativeBackboneMemoryCoordinatePersistenceStore(),
		{ flushOnAppend: false },
	),
});

// sync.rawExchangeHeads and sync.nativeWireSync are intentionally NOT set
// in either args variant: the client's native preset must supply them.
const nativeOpenArgs = () => ({
	mode: "native" as const,
	replicate: { factor: 1 },
	nativeGraph: true,
	nativeBackbone: nativeBackboneDocumentIndexOptions(),
	canPerform: policy.allowAll<Document>(),
	index: {
		type: Document,
		transform: transform.identity<Document>(),
	},
});

const documentsOf = (store: TestStore) => store.docs;

describe("native network e2e", function () {
	this.timeout(120_000);

	describe("pure native pair", () => {
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

		it("enables the whole native chain from the preset", () => {
			for (const client of [peer1, peer2]) {
				const runtime = client.nativeNetwork;
				expect(runtime?.rustCore, "rust core").to.exist;
				expect(runtime?.wireSync, "wire-sync session").to.exist;
				const pubsub = client.services.pubsub as any;
				const blocks = client.services.blocks as any;
				const fanout = (client.services as any).fanout;
				expect(pubsub.rustCore).to.equal(runtime!.rustCore);
				expect(blocks.rustCore).to.equal(runtime!.rustCore);
				expect(fanout.rustCore).to.equal(runtime!.rustCore);
				// the pubsub inbound decoder is the wire-sync session so
				// shared-log raw exchange-head payloads get stashed in wasm
				expect(pubsub.nativeWire).to.equal(runtime!.wireSync);
				expect(client.sharedLogNativeDefaults?.sync?.nativeWireSync).to.equal(
					runtime!.wireSync,
				);
				expect(
					client.sharedLogNativeDefaults?.sync?.rawExchangeHeads,
				).to.equal(true);
				expect(client.sharedLogNativeDefaults?.nativeBackbone).to.exist;
			}
		});

		it("syncs document puts through the strict native chain", async () => {
			const entryCount = 96;
			const source = new TestStore({ docs: new Documents<Document>() });
			const target = source.clone();

			await peer1.open(source, { args: nativeOpenArgs() });
			const documents = Array.from(
				{ length: entryCount },
				(_, index) =>
					new Document({
						id: `native-e2e-${index}`,
						name: `native-e2e-name-${index}`,
						tags: [`batch-${Math.floor(index / 16)}`],
					}),
			);
			await documentsOf(source).putMany(documents, { unique: true });

			await peer2.open(target, { args: nativeOpenArgs() });

			// the client preset raw-sync defaults apply to document stores:
			// the program topic is registered on the wire-sync session and
			// raw exchange-head payloads get stashed in wasm
			const wireSync2 = peer2.nativeNetwork!.wireSync! as unknown as {
				topicCount: number;
			};
			expect(wireSync2.topicCount).to.equal(1);

			const documentPutSpy = sinon.spy(documentsOf(target).index, "put");
			const decoderSpy = sinon.spy(
				documentsOf(target).index.valueEncoding,
				"decoder",
			);

			await peer2.dial(peer1.getMultiaddrs());
			await waitForResolved(
				async () =>
					expect(await documentsOf(target).index.getSize()).equal(entryCount),
				{ timeout: 60_000, timeoutMessage: "native e2e cold sync" },
			);

			// live puts keep flowing in both directions after the cold sync
			await documentsOf(source).put(
				new Document({ id: "native-e2e-live-1to2", name: "live-1to2" }),
			);
			await documentsOf(target).put(
				new Document({ id: "native-e2e-live-2to1", name: "live-2to1" }),
			);
			await waitForResolved(
				async () => {
					expect(
						(
							await documentsOf(target).get("native-e2e-live-1to2", {
								local: true,
								remote: false,
							})
						)?.name,
					).to.equal("live-1to2");
					expect(
						(
							await documentsOf(source).get("native-e2e-live-2to1", {
								local: true,
								remote: false,
							})
						)?.name,
					).to.equal("live-2to1");
				},
				{ timeout: 30_000, timeoutMessage: "native e2e live puts" },
			);

			// wire-sync session counters: document syncs ride the raw path
			// (payloads stashed in wasm), and the per-entry change consumer
			// (the document store's handleChanges) is what pulls entry bytes
			// back into JS — counted as blockCopyOuts. The zero-copy invariant
			// for programs without a per-entry consumer is covered by the
			// shared-log network e2e.
			const targetCounters = peer2.nativeNetwork!.wireSync!.counters!();
			expect(targetCounters.stashed, "peer2 stashed").to.be.greaterThan(0);
			expect(
				targetCounters.blockCopyOuts,
				"peer2 blockCopyOuts",
			).to.be.greaterThan(0);

			// DirectStream wire counters: every inbound frame was decoded and
			// signature-verified natively; no frame fell back to the TS path
			// and no TS-side signature verification ran. (The TS envelope
			// object per frame is the deliberate exception: it feeds the
			// routing state machine and app-facing events and holds the
			// payload as a zero-copy view.)
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

			// the document index commit ran natively: no JS document decode,
			// no generic JS index put
			expect(decoderSpy.callCount).to.equal(0);
			expect(documentPutSpy.callCount).to.equal(0);
			const targetBackbone = (documentsOf(target).log as any)._nativeBackbone;
			expect(targetBackbone).to.exist;
			// 96 cold-synced + one live put from each peer
			expect(targetBackbone.documentValueLength).to.equal(entryCount + 2);
		});

	});

	describe("mixed pair", () => {
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

		it("syncs both directions between a pure-native and an all-default peer", async () => {
			// the default peer runs the unmodified JS wire path
			expect(defaultPeer.nativeNetwork).to.equal(undefined);
			expect((defaultPeer.services.pubsub as any).rustCore).to.equal(
				undefined,
			);
			expect((defaultPeer.services.pubsub as any).nativeWire).to.equal(
				undefined,
			);
			expect(defaultPeer.sharedLogNativeDefaults).to.equal(undefined);

			const source = new TestStore({ docs: new Documents<Document>() });
			const target = source.clone();

			await nativePeer.open(source, { args: nativeOpenArgs() });
			await defaultPeer.open(target, {
				args: { replicate: { factor: 1 } },
			});

			await defaultPeer.dial(nativePeer.getMultiaddrs());

			const nativeToDefault = Array.from(
				{ length: 24 },
				(_, index) =>
					new Document({
						id: `mixed-n2d-${index}`,
						name: `mixed-n2d-name-${index}`,
					}),
			);
			await documentsOf(source).putMany(nativeToDefault, { unique: true });
			await waitForResolved(
				async () =>
					expect(await documentsOf(target).index.getSize()).equal(24),
				{ timeout: 60_000, timeoutMessage: "mixed native->default sync" },
			);
			expect(
				(
					await documentsOf(target).get("mixed-n2d-7", {
						local: true,
						remote: false,
					})
				)?.name,
			).to.equal("mixed-n2d-name-7");

			await documentsOf(target).put(
				new Document({ id: "mixed-d2n", name: "mixed-d2n-name" }),
			);
			await waitForResolved(
				async () =>
					expect(
						(
							await documentsOf(source).get("mixed-d2n", {
								local: true,
								remote: false,
							})
						)?.name,
					).to.equal("mixed-d2n-name"),
				{ timeout: 60_000, timeoutMessage: "mixed default->native sync" },
			);

			// the native peer never verified or fell back in JS even against
			// a JS-only sender (byte-identical wire)
			const counters = wireCountersOf(nativePeer);
			expect(counters.tsFrames).to.equal(0);
			expect(counters.nativeFallbackFrames).to.equal(0);
			expect(counters.tsSignatureVerifies).to.equal(0);
			// the default peer processed everything on the TS path
			expect(wireCountersOf(defaultPeer).nativeFrames).to.equal(0);
			expect(wireCountersOf(defaultPeer).tsFrames).to.be.greaterThan(0);
		});
	});
});
