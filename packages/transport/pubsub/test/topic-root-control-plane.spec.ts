import { expect } from "chai";
import { TestSession } from "@peerbit/libp2p-test-utils";
import { TopicControlPlane, TopicRootControlPlane } from "../src/index.js";

describe("topic-root-control-plane", () => {
	it("handles explicit roots", async () => {
		const controlPlane = new TopicRootControlPlane();
		controlPlane.setTopicRoot("orders", "peer-x");

		expect(controlPlane.getTopicRoot("orders")).to.equal("peer-x");
		expect(await controlPlane.resolveTopicRoot("orders")).to.equal("peer-x");

		controlPlane.clearTopicRoot("orders");
		expect(controlPlane.getTopicRoot("orders")).to.equal(undefined);
	});

	it("uses deterministic candidate hashing by default", async () => {
		const controlPlane = new TopicRootControlPlane({
			defaultCandidates: ["peer-c", "peer-a", "peer-b", "peer-a"],
		});

		const first = await controlPlane.resolveTopicRoot("topic-1");
		const second = await controlPlane.resolveTopicRoot("topic-1");

		expect(first).to.equal(second);
		expect(controlPlane.getTopicRootCandidates()).to.deep.equal([
			"peer-a",
			"peer-b",
			"peer-c",
		]);
	});

	it("uses resolver before candidate hashing", async () => {
		const controlPlane = new TopicRootControlPlane({
			defaultCandidates: ["peer-a", "peer-b"],
			resolver: (topic) => (topic === "rpc" ? "peer-z" : undefined),
		});

		expect(await controlPlane.resolveTopicRoot("rpc")).to.equal("peer-z");
		expect(await controlPlane.resolveTopicRoot("other")).to.not.equal(undefined);
	});

	it("uses trackers before deterministic candidates", async () => {
		const controlPlane = new TopicRootControlPlane({
			defaultCandidates: ["peer-a"],
			trackers: [
				{
					resolveRoot: (topic) => (topic === "rpc" ? "peer-tracker" : undefined),
				},
			],
		});

		expect(await controlPlane.resolveTopicRoot("rpc")).to.equal("peer-tracker");
	});

	it("ignores failing trackers and falls back", async () => {
		const controlPlane = new TopicRootControlPlane({
			defaultCandidates: ["peer-a"],
			trackers: [
				{
					resolveRoot: () => {
						throw new Error("tracker down");
					},
				},
			],
		});

		expect(await controlPlane.resolveTopicRoot("rpc")).to.equal("peer-a");
	});

	it("can be injected into TopicControlPlane", async () => {
		const topicRootControlPlane = new TopicRootControlPlane();
		const session = await TestSession.connected<{ pubsub: TopicControlPlane }>(1, {
			services: {
				pubsub: (c) =>
					new TopicControlPlane(c, {
						topicRootControlPlane,
					}),
			},
		});

		const pubsub = session.peers[0].services.pubsub;
		expect(pubsub.topicRootControlPlane).to.equal(topicRootControlPlane);

		await session.stop();
	});
});
