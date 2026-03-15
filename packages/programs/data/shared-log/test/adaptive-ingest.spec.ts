import { TestSession } from "@peerbit/test-utils";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import sinon from "sinon";
import { EventStore } from "./utils/stores/index.js";

describe("adaptive ingest burst control", () => {
	let session: TestSession | undefined;

	const openAdaptiveStore = async () => {
		session = await TestSession.disconnected(1);

		const store = await session.peers[0].open(new EventStore<string, any>(), {
			args: {
				replicate: {
					limits: {
						interval: 20,
					},
				},
			},
		});

		(store.log as any).adaptiveRebalanceIdleMs = 120;

		sinon.stub(store.log, "findLeaders").callsFake(async (_coords, _entry, opts) => {
			opts?.onLeader?.("foreign-leader");
			return new Map([["foreign-leader", { intersecting: true }]]) as any;
		});

		return store;
	};

	afterEach(async () => {
		sinon.restore();
		if (session) {
			await session.stop();
			session = undefined;
		}
	});

	it("tracks recent local append activity for adaptive appends", async () => {
		const store = await openAdaptiveStore();

		await store.add("a", { target: "none" });

		expect((store.log as any)._lastLocalAppendAt).to.be.greaterThan(0);
		expect((store.log as any).shouldDelayAdaptiveRebalance()).to.equal(true);
	});

	it("skips immediate prune and append-triggered rebalance while the writer is still hot", async () => {
		const store = await openAdaptiveStore();
		const pruneAdd = sinon.spy(store.log.pruneDebouncedFn, "add");
		const rebalanceCall = sinon.spy(
			(store.log as any).rebalanceParticipationDebounced,
			"call",
		);

		await store.add("a", { target: "none" });

		expect(pruneAdd.callCount).to.equal(0);
		expect(rebalanceCall.callCount).to.equal(0);
	});

	it("requeues adaptive rebalance until the ingest window goes idle", async () => {
		const store = await openAdaptiveStore();
		const rebalanceCall = sinon.spy(
			(store.log as any).rebalanceParticipationDebounced,
			"call",
		);

		await store.add("a", { target: "none" });

		const rebalanced = await store.log.rebalanceParticipation();

		expect(rebalanced).to.equal(false);
		expect(rebalanceCall.callCount).to.equal(1);

		await waitForResolved(() => {
			expect((store.log as any).shouldDelayAdaptiveRebalance()).to.equal(false);
		}, {
			timeout: 2_000,
			delayInterval: 20,
		});
	});
});
