import { TestSession } from "@peerbit/libp2p-test-utils";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import { FanoutTree } from "../src/fanout-tree.js";

type FanoutServices = { fanout: FanoutTree };

const createSession = () =>
	TestSession.disconnected<FanoutServices>(1, {
		services: {
			fanout: (components) =>
				new FanoutTree(components, { connectionManager: false }),
		},
	});

const expectToSettlePromptly = async (
	promises: Promise<unknown>[],
	label: string,
) => {
	let timeout: ReturnType<typeof setTimeout> | undefined;
	try {
		await Promise.race([
			Promise.all(promises),
			new Promise<never>((_resolve, reject) => {
				timeout = setTimeout(
					() => reject(new Error(`${label} did not settle after close`)),
					500,
				);
			}),
		]);
	} finally {
		if (timeout) clearTimeout(timeout);
	}
};

const longRunningChannelOptions = {
	msgRate: 1,
	msgSize: 8,
	uploadLimitBps: 1_000_000,
	maxChildren: 1,
	repair: true,
	repairIntervalMs: 2_000,
	neighborRepair: true,
	neighborMeshPeers: 1,
	neighborAnnounceIntervalMs: 2_000,
	neighborMeshRefreshIntervalMs: 2_000,
} as const;

describe("fanout-tree background loop lifecycle", () => {
	it("cancels a provider announce sleep when its handle closes", async () => {
		const session = await createSession();
		try {
			const fanout = session.peers[0]!.services.fanout;
			const handle = fanout.provide("provider-close", {
				announceIntervalMs: 2_000,
			});
			const state = [
				...(fanout as any).providerAnnounceBySuffixKey.values(),
			][0];
			expect(state?.loop).to.be.instanceOf(Promise);

			handle.close();

			await expectToSettlePromptly([state.loop], "provider announce loop");
		} finally {
			await session.stop();
		}
	});

	it("cancels a provider watch sleep when its handle closes", async () => {
		const session = await createSession();
		try {
			const fanout = session.peers[0]!.services.fanout;
			const handle = fanout.watchProviders("provider-watch-close", {
				renewIntervalMs: 2_000,
				onProviders: () => {},
			});
			const state = [...(fanout as any).providerWatchesBySuffixKey.values()][0]
				.values()
				.next().value;
			expect(state?.loop).to.be.instanceOf(Promise);

			handle.close();

			await expectToSettlePromptly([state.loop], "provider watch loop");
		} finally {
			await session.stop();
		}
	});

	it("cancels every channel loop when the channel closes", async () => {
		const session = await createSession();
		try {
			const fanout = session.peers[0]!.services.fanout;
			const topic = "channel-close";
			const root = "unavailable-root";
			const joining = fanout
				.joinChannel(topic, root, longRunningChannelOptions, {
					retryMs: 2_000,
					timeoutMs: 10_000,
					announceIntervalMs: 2_000,
				})
				.catch(() => {});
			const id = fanout.getChannelId(topic, root);
			let state: any;
			await waitForResolved(
				() => {
					state = (fanout as any).channelsBySuffixKey.get(id.suffixKey);
					expect(state?.announceLoop).to.be.instanceOf(Promise);
					expect(state?.repairLoop).to.be.instanceOf(Promise);
					expect(state?.meshLoop).to.be.instanceOf(Promise);
					expect(state?.joinLoop).to.be.instanceOf(Promise);
				},
				{ timeout: 1_000, delayInterval: 10 },
			);
			const loops = [
				state.announceLoop,
				state.repairLoop,
				state.meshLoop,
				state.joinLoop,
			];

			await fanout.closeChannel(topic, root);

			await expectToSettlePromptly(loops, "channel loops");
			await joining;
		} finally {
			await session.stop();
		}
	});

	it("cancels existing and concurrently-created loops when the service stops", async () => {
		const session = await createSession();
		try {
			const fanout = session.peers[0]!.services.fanout;
			fanout.provide("service-stop", { announceIntervalMs: 2_000 });
			fanout.watchProviders("service-stop", {
				renewIntervalMs: 2_000,
				onProviders: () => {},
			});
			const providerState = [
				...(fanout as any).providerAnnounceBySuffixKey.values(),
			][0];
			const watchState = [
				...(fanout as any).providerWatchesBySuffixKey.values(),
			][0]
				.values()
				.next().value;
			const id = fanout.openChannel("service-stop", "unavailable-root", {
				...longRunningChannelOptions,
				role: "node",
			});
			const channelState = (fanout as any).channelsBySuffixKey.get(
				id.suffixKey,
			);
			const loops = [
				providerState.loop,
				watchState.loop,
				channelState.announceLoop,
				channelState.repairLoop,
				channelState.meshLoop,
			];
			for (const loop of loops) {
				expect(loop).to.be.instanceOf(Promise);
			}

			const stopping = fanout.stop();
			fanout.provide("late-service-stop", { announceIntervalMs: 2_000 });
			fanout.watchProviders("late-service-stop", {
				renewIntervalMs: 2_000,
				onProviders: () => {},
			});
			const lateProviderState = [
				...(fanout as any).providerAnnounceBySuffixKey.values(),
			].at(-1);
			const lateWatchState = [
				...(fanout as any).providerWatchesBySuffixKey.values(),
			]
				.at(-1)
				.values()
				.next().value;
			const lateId = fanout.openChannel(
				"late-service-stop",
				"unavailable-root",
				{
					...longRunningChannelOptions,
					role: "node",
				},
			);
			const lateChannelState = (fanout as any).channelsBySuffixKey.get(
				lateId.suffixKey,
			);
			loops.push(
				lateProviderState.loop,
				lateWatchState.loop,
				lateChannelState.announceLoop,
				lateChannelState.repairLoop,
				lateChannelState.meshLoop,
			);
			for (const loop of loops) {
				expect(loop).to.be.instanceOf(Promise);
			}

			await stopping;

			await expectToSettlePromptly(loops, "service loops");
		} finally {
			await session.stop();
		}
	});
});
