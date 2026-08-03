import { expect } from "chai";
import { TestSession } from "@peerbit/libp2p-test-utils";
import { delay } from "@peerbit/time";
import { FanoutTree, TopicControlPlane, TopicRootControlPlane } from "../src/index.js";

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
		expect(await controlPlane.resolveTrackedTopicRoot("rpc")).to.equal(
			"peer-tracker",
		);
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
		expect(await controlPlane.resolveTrackedTopicRoot("rpc")).to.equal(undefined);
		expect(await controlPlane.resolveCanonicalTopicRoot("rpc")).to.equal("peer-a");
	});

	it("aborts a hanging resolver even when the callback ignores its signal", async () => {
		let markEntered!: (signal?: AbortSignal) => void;
		const entered = new Promise<AbortSignal | undefined>((resolve) => {
			markEntered = resolve;
		});
		const controlPlane = new TopicRootControlPlane({
			defaultCandidates: ["peer-a"],
			resolver: (_topic, options) => {
				markEntered(options?.signal);
				return new Promise<string | undefined>(() => {});
			},
		});
		const abortController = new AbortController();
		const reason = new Error("caller cancelled root resolution");
		const resolving = controlPlane.resolveTopicRoot("rpc", {
			signal: abortController.signal,
		});

		expect(await entered).to.equal(abortController.signal);
		abortController.abort(reason);
		await expect(resolving).to.be.rejectedWith(reason);
	});

	it("handles a synchronous resolver abort without an unhandled rejection", async () => {
		const abortController = new AbortController();
		const reason = new Error("resolver cancelled its own lookup");
		const unhandled: unknown[] = [];
		const onUnhandled = (error: unknown) => unhandled.push(error);
		const controlPlane = new TopicRootControlPlane({
			resolver: () => {
				abortController.abort(reason);
				return Promise.reject(new Error("late resolver rejection"));
			},
		});
		process.on("unhandledRejection", onUnhandled);

		try {
			await expect(
				controlPlane.resolveTopicRoot("rpc", {
					signal: abortController.signal,
				}),
			).to.be.rejectedWith(reason);
			await delay(0);
			expect(unhandled).to.deep.equal([]);
		} finally {
			process.removeListener("unhandledRejection", onUnhandled);
		}
	});

	it("aborts a hanging tracker instead of falling back to candidates", async () => {
		let markEntered!: (signal?: AbortSignal) => void;
		const entered = new Promise<AbortSignal | undefined>((resolve) => {
			markEntered = resolve;
		});
		const controlPlane = new TopicRootControlPlane({
			defaultCandidates: ["peer-a"],
			trackers: [
				{
					resolveRoot: (_topic, options) => {
						markEntered(options?.signal);
						return new Promise<string | undefined>(() => {});
					},
				},
			],
		});
		const abortController = new AbortController();
		const reason = new Error("caller cancelled tracker resolution");
		const resolving = controlPlane.resolveTopicRoot("rpc", {
			signal: abortController.signal,
		});

		expect(await entered).to.equal(abortController.signal);
		abortController.abort(reason);
		await expect(resolving).to.be.rejectedWith(reason);
	});

	it("can be injected into TopicControlPlane", async () => {
		const topicRootControlPlane = new TopicRootControlPlane();
		let fanoutInstance: FanoutTree | undefined;
		const getOrCreateFanout = (c: any) => {
			if (!fanoutInstance) {
				fanoutInstance = new FanoutTree(c, {
					connectionManager: false,
					topicRootControlPlane,
				});
			}
			return fanoutInstance;
		};
		const session = await TestSession.connected<{
			pubsub: TopicControlPlane;
			fanout: FanoutTree;
		}>(1, {
			services: {
				pubsub: (c: any) =>
					new TopicControlPlane(c, {
						topicRootControlPlane,
						fanout: getOrCreateFanout(c),
					}),
				fanout: (c: any) => getOrCreateFanout(c),
			},
		});

		const pubsub = session.peers[0].services.pubsub;
		expect(pubsub.topicRootControlPlane).to.equal(topicRootControlPlane);

		await session.stop();
	});
});
