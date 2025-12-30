import { ACK } from "@peerbit/stream-interface";
import { TestSession } from "@peerbit/test-utils";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import pDefer from "p-defer";
import { NoPeersError } from "../src/index.js";
import { EventStore } from "./utils/stores/index.js";

describe("append delivery options", () => {
	let session: TestSession;

	afterEach(async () => {
		await session?.stop();
	});

	it("awaits transport acks when delivery is set", async () => {
		session = await TestSession.connected(2);

		const db1 = await session.peers[0].open(new EventStore<string, any>());
		await EventStore.open<EventStore<string, any>>(
			db1.address!,
			session.peers[1],
		);

		const remoteHash = session.peers[1].identity.publicKey.hashcode();
		await waitForResolved(async () => {
			const subscribers = await session.peers[0].services.pubsub.getSubscribers(
				db1.log.rpc.topic,
			);
			expect((subscribers || []).map((x) => x.hashcode())).to.include(
				remoteHash,
			);
		});

		const gate = pDefer<void>();
		const ackAttempted = pDefer<void>();

		const remotePubsub: any = session.peers[1].services.pubsub;
		const originalPublishMessage =
			remotePubsub.publishMessage.bind(remotePubsub);
		remotePubsub.publishMessage = async (...args: any[]) => {
			const message = args[1];
			if (message instanceof ACK) {
				ackAttempted.resolve();
				await gate.promise;
			}
			return originalPublishMessage(...args);
		};

		let resolved = false;
		const promise = db1
			.add("hello", {
				target: "all",
				delivery: true,
			} as any)
			.then((result) => {
				resolved = true;
				return result;
			});

		await ackAttempted.promise;
		expect(resolved).to.equal(false);

		gate.resolve();
		await promise;
		expect(resolved).to.equal(true);
	});

	it("throws when requireRecipients is true and there are no remotes", async () => {
		session = await TestSession.disconnected(1);

		const db1 = await session.peers[0].open(new EventStore<string, any>());

		await expect(
			db1.add("hello", {
				target: "all",
				delivery: { requireRecipients: true },
			} as any),
		).to.be.rejectedWith(NoPeersError);
	});
});
