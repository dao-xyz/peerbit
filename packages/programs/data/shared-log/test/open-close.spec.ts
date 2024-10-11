// Include test utilities
import { TestSession } from "@peerbit/test-utils";
import { delay, waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import {
	ExchangeHeadsMessage,
	RequestMaybeSync,
} from "../src/exchange-heads.js";
import { slowDownSend } from "./utils.js";
import { EventStore } from "./utils/stores/index.js";

describe("replicators", () => {
	let session: TestSession;

	afterEach(async () => {
		await session.stop();
	});

	it("uses existing subsription", async () => {
		session = await TestSession.connected(2);

		const store = new EventStore();
		const db1 = await session.peers[0].open(store);
		await session.peers[1].services.pubsub.requestSubscribers(db1.log.topic);
		await waitForResolved(async () =>
			expect(
				(await session.peers[1].services.pubsub.getSubscribers(
					db1.log.topic,
				))!.find((x) => x.equals(session.peers[0].identity.publicKey)),
			),
		);

		// Adding a delay is necessary so that old subscription messages are not flowing around
		// so that we are sure the we are "really" using existing subscriptions on start to build replicator set
		await delay(1000);

		const db2 = await session.peers[1].open(store.clone());
		await waitForResolved(async () =>
			expect([...(await db1.log.getReplicators())]).to.have.members(
				session.peers.map((x) => x.identity.publicKey.hashcode()),
			),
		);
		await waitForResolved(async () =>
			expect([...(await db2.log.getReplicators())]).to.have.members(
				session.peers.map((x) => x.identity.publicKey.hashcode()),
			),
		);
	});

	it("clears in flight info when leaving", async () => {
		const store = new EventStore<string>();

		session = await TestSession.connected(3);

		const db1 = await session.peers[0].open(store.clone(), {
			args: {
				replicate: {
					factor: 1,
				},
				replicas: {
					min: 3,
				},
			},
		});
		const db2 = await session.peers[1].open(store.clone(), {
			args: {
				replicate: {
					factor: 1,
				},
				replicas: {
					min: 3,
				},
			},
		});

		const abortController = new AbortController();
		const { entry } = await db1.add("hello!");
		await waitForResolved(() => expect(db2.log.log.length).equal(1));

		slowDownSend(db1.log, ExchangeHeadsMessage, 1e4, abortController.signal);
		slowDownSend(db2.log, ExchangeHeadsMessage, 1e4, abortController.signal);
		slowDownSend(db2.log, RequestMaybeSync, 2e3, abortController.signal); // make db2 a bit slower so the assertions below become deterministic (easily)

		const db3 = await session.peers[2].open(store, {
			args: {
				replicate: {
					factor: 1,
				},
				replicas: {
					min: 3,
				},
			},
		});

		await waitForResolved(async () => {
			expect((await db3.log.getReplicators()).size).equal(3);
		});

		await waitForResolved(
			() =>
				expect(
					db3.log["syncInFlight"].has(db1.node.identity.publicKey.hashcode()),
				).to.be.true,
		);
		await waitForResolved(
			() =>
				expect(
					!!db3.log["syncInFlightQueue"]
						.get(entry.hash)
						?.find((x) => x.equals(db2.node.identity.publicKey)),
				).to.be.true,
		);
		await waitForResolved(
			() =>
				expect(
					db3.log["syncInFlightQueueInverted"].has(
						db2.node.identity.publicKey.hashcode(),
					),
				).to.be.true,
		); // because db2 is slower
		await waitForResolved(
			() =>
				expect(
					db3.log["syncInFlightQueueInverted"].has(
						db1.node.identity.publicKey.hashcode(),
					),
				).to.be.false,
		);

		await db1.close();
		await db2.close();

		await waitForResolved(
			() =>
				expect(
					db3.log["syncInFlight"].has(db1.node.identity.publicKey.hashcode()),
				).to.be.false,
		);
		await waitForResolved(
			() => expect(db3.log["syncInFlightQueue"].has(entry.hash)).to.be.false,
		);
		await waitForResolved(
			() =>
				expect(
					db3.log["syncInFlightQueueInverted"].has(
						db2.node.identity.publicKey.hashcode(),
					),
				).to.be.false,
		);

		abortController.abort("Done");
	});
});
