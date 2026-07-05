// Mixed-topology interop for the fanout-tree port: rust-core and default
// (pure TS) FanoutTree peers must form one tree and exchange data through
// each other. Byte identity of the /peerbit/fanout-tree/0.5.0 frames is the
// named interop risk of this stage, so the join handshake (including the
// capacity-reject + redirect path), downstream data forwarding and the
// upstream publish-proxy path are asserted in both directions: rust-core
// publisher with default subscribers, and the reverse.
import { TestSession } from "@peerbit/libp2p-test-utils";
import { FanoutTree } from "@peerbit/pubsub";
import type { RustCoreStream } from "@peerbit/stream";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import { createRustCoreStream } from "../src/index.js";

type Session = TestSession<{ fanout: FanoutTree }>;

describe("fanout rust-core mixed topology", () => {
	let core: RustCoreStream;
	let session: Session;

	before(async () => {
		core = await createRustCoreStream();
	});

	afterEach(async () => {
		await session?.stop();
	});

	// line topology root - relay - leaf; `rustPeers[i]` picks the engine.
	// `rustCore: false` keeps the default peers pure TS even under the
	// PEERBIT_STREAM_RUST_CORE-injected suite rerun, so the topology stays
	// mixed in both test modes.
	const createMixedSession = async (rustPeers: [boolean, boolean, boolean]) => {
		session = await TestSession.disconnected(
			3,
			rustPeers.map((rust) => ({
				services: {
					fanout: (components: any) =>
						new FanoutTree(components, {
							connectionManager: false,
							rustCore: rust ? core : false,
						}),
				},
			})),
		);
		await session.connect([
			[session.peers[0], session.peers[1]],
			[session.peers[1], session.peers[2]],
		]);
		return {
			root: session.peers[0].services.fanout,
			relay: session.peers[1].services.fanout,
			leaf: session.peers[2].services.fanout,
		};
	};

	const formTreeAndExchange = async (rustPeers: [boolean, boolean, boolean]) => {
		const { root, relay, leaf } = await createMixedSession(rustPeers);
		const topic = "mixed-concert";
		const rootId = root.publicKeyHash;

		// maxChildren 1 forces the leaf through the reject/redirect path and
		// a relayed attachment, so mixed peers exercise JOIN_REQ/ACCEPT/
		// REJECT, IHAVE and forwarded DATA rather than a single direct edge.
		root.openChannel(topic, rootId, {
			role: "root",
			msgRate: 10,
			msgSize: 32,
			uploadLimitBps: 1_000_000,
			maxChildren: 1,
			repair: true,
		});

		await relay.joinChannel(
			topic,
			rootId,
			{
				msgRate: 10,
				msgSize: 32,
				uploadLimitBps: 1_000_000,
				maxChildren: 1,
				repair: true,
			},
			{ timeoutMs: 10_000 },
		);

		await leaf.joinChannel(
			topic,
			rootId,
			{
				msgRate: 10,
				msgSize: 32,
				uploadLimitBps: 0,
				maxChildren: 0,
				repair: true,
			},
			{ timeoutMs: 10_000 },
		);

		const receivedByLeaf = new Map<number, Uint8Array>();
		leaf.addEventListener("fanout:data", (ev: any) => {
			if (ev.detail.topic !== topic || ev.detail.root !== rootId) return;
			receivedByLeaf.set(ev.detail.seq, ev.detail.payload);
		});
		const receivedByRoot = new Map<number, Uint8Array>();
		root.addEventListener("fanout:data", (ev: any) => {
			if (ev.detail.topic !== topic || ev.detail.root !== rootId) return;
			receivedByRoot.set(ev.detail.seq, ev.detail.payload);
		});

		// Downstream: root publish reaches the leaf through the relay.
		const downstream = new Uint8Array([1, 2, 3, 4]);
		await root.publishData(topic, rootId, downstream);
		await waitForResolved(() =>
			expect(receivedByLeaf.get(0)).to.exist,
		);
		expect([...receivedByLeaf.get(0)!]).to.deep.equal([...downstream]);

		// Upstream: a leaf publish-proxy climbs to the root, which assigns
		// the next seq and broadcasts it back down.
		const upstream = new Uint8Array([9, 8, 7]);
		await leaf.publishToChannel(topic, rootId, upstream);
		await waitForResolved(() => expect(receivedByRoot.get(1)).to.exist);
		expect([...receivedByRoot.get(1)!]).to.deep.equal([...upstream]);
		await waitForResolved(() => expect(receivedByLeaf.get(1)).to.exist);
		expect([...receivedByLeaf.get(1)!]).to.deep.equal([...upstream]);
	};

	it("rust-core publisher, default subscribers", async () => {
		await formTreeAndExchange([true, false, false]);
	});

	it("default publisher, rust-core subscribers", async () => {
		await formTreeAndExchange([false, true, true]);
	});
});
