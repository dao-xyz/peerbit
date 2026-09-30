import { TestSession } from "@peerbit/libp2p-test-utils";
import { waitForNeighbour } from "@peerbit/stream";
import { expect } from "chai";
import { parentUpgradeProbeReqSent } from "../benchmark/fanout-tree-parent-upgrade-preset.js";
import { FanoutTree } from "../src/index.js";

describe("fanout parent probe metrics", () => {
	it("allows health-only traffic in a zero-upgrade probe budget", () => {
		expect(
			parentUpgradeProbeReqSent({
				parentProbeReqSentTotal: 2,
				parentHealthProbeReqSentTotal: 2,
			}),
		).to.equal(0);
	});

	it("does not hide an upgrade probe behind health traffic", () => {
		expect(
			parentUpgradeProbeReqSent({
				parentProbeReqSentTotal: 3,
				parentHealthProbeReqSentTotal: 2,
			}),
		).to.be.greaterThan(0);
	});

	it("fails closed on missing, fractional, overflowing, or inconsistent counters", () => {
		for (const [total, health] of [
			[undefined, 0],
			[2, undefined],
			[NaN, 0],
			[2, NaN],
			[Infinity, 0],
			[2, Infinity],
			[-1, 0],
			[2, -1],
			[0.5, 0],
			[2, 0.5],
			[Number.MAX_SAFE_INTEGER + 1, 0],
			[2, 3],
		]) {
			expect(() =>
				parentUpgradeProbeReqSent({
					parentProbeReqSentTotal: total!,
					parentHealthProbeReqSentTotal: health!,
				}),
			).to.throw("Invalid parent probe counters");
		}
	});

	it("counts actual health sends separately from default upgrade sends", async () => {
		const session = await TestSession.disconnected<{ fanout: FanoutTree }>(2, {
			services: {
				fanout: (components) =>
					new FanoutTree(components, { connectionManager: false }),
			},
		});
		try {
			await session.connect([session.peers]);
			const [root, leaf] = session.peers.map((peer) => peer.services.fanout);
			await waitForNeighbour(root, leaf);
			const topic = "parent-probe-metric-purpose";
			const rootId = root.publicKeyHash;
			const options = {
				msgRate: 1,
				msgSize: 8,
				uploadLimitBps: 1_000_000,
				maxChildren: 1,
				repair: false,
			};
			root.openChannel(topic, rootId, { ...options, role: "root" });
			const id = leaf.openChannel(topic, rootId, { ...options, role: "node" });
			const internals = leaf as any;
			const channel = internals.channelsBySuffixKey.get(id.suffixKey);
			const controller = new AbortController();
			const probe = (purpose?: "health") =>
				internals.probeParentCandidate(
					channel,
					rootId,
					1_000,
					controller.signal,
					0,
					false,
					purpose,
				);
			const metrics = leaf.getChannelMetrics(topic, rootId);
			expect((await probe("health"))?.hash).to.equal(rootId);
			expect((await probe("health"))?.hash).to.equal(rootId);
			expect(metrics.parentProbeReqSent).to.equal(2);
			expect(metrics.parentHealthProbeReqSent).to.equal(2);
			// Identical reservation flags must not classify the default upgrade
			// request as health work; only the explicit private purpose does that.
			expect((await probe())?.hash).to.equal(rootId);
			expect(metrics.parentProbeReqSent).to.equal(3);
			expect(metrics.parentHealthProbeReqSent).to.equal(2);
			expect(metrics.parentProbeReplyReceived).to.equal(3);
			expect(metrics.controlSends).to.equal(3);
			expect(metrics.controlBytesSent).to.be.greaterThan(0);
			expect(root.getChannelMetrics(topic, rootId).parentProbeReqReceived).to.equal(3);
			controller.abort(new Error("already closed"));
			await probe("health").catch((): undefined => undefined);
			expect(metrics.parentProbeReqSent).to.equal(3);
			expect(metrics.parentHealthProbeReqSent).to.equal(2);
		} finally {
			await session.stop();
		}
	});
});
