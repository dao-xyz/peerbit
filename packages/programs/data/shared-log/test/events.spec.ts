import { TestSession } from "@peerbit/test-utils";
import { delay, waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import { EventStore } from "./utils/stores/index.js";

describe("events", () => {
	let session: TestSession;

	afterEach(async () => {
		await session.stop();
	});

	it("replicator:(join|leave)", async () => {
		session = await TestSession.connected(2);

		let db1JoinEvents: string[] = [];
		let db1LeaveEvents: string[] = [];

		const db1a = await session.peers[0].open(new EventStore(), {
			args: { replicate: 1 },
		});
		db1a.log.events.addEventListener("replicator:join", (event) => {
			db1JoinEvents.push(event.detail.publicKey.hashcode());
		});

		db1a.log.events.addEventListener("replicator:leave", (event) => {
			db1LeaveEvents.push(event.detail.publicKey.hashcode());
		});

		const db1b = await session.peers[0].open(new EventStore(), {
			args: { replicate: 1 },
		});

		const db2a = await session.peers[1].open(db1a.clone(), {
			args: { replicate: 0.6 },
		});

		const db2b = await session.peers[1].open(db1b.clone(), {
			args: { replicate: 0.4 },
		});
		await delay(2e3); // some time for all join events to emit
		expect(db1JoinEvents).to.have.members([
			session.peers[1].identity.publicKey.hashcode(),
		]);

		await db2a.close();
		await db2b.close();

		// try open another db and make sure it does not trigger join event to db1
		await delay(2e3); // some time for all leave events to emit
		expect(db1LeaveEvents).to.have.members([
			session.peers[1].identity.publicKey.hashcode(),
		]);
		expect(db1JoinEvents).to.have.length(1); // no new join event
	});

	it("replicate:join not emitted on update", async () => {
		session = await TestSession.connected(2);

		const store = new EventStore();
		let db1JoinEvents: string[] = [];
		const store1 = await session.peers[0].open(store, {
			args: {
				replicate: { factor: 1 },
			},
		});
		store1.log.events.addEventListener("replicator:join", (event) => {
			db1JoinEvents.push(event.detail.publicKey.hashcode());
		});

		const store2 = await session.peers[1].open(store.clone(), {
			args: {
				replicate: { factor: 1 },
			},
		});
		await waitForResolved(() =>
			expect(db1JoinEvents).to.have.members([
				session.peers[1].identity.publicKey.hashcode(),
			]),
		);

		await store2.log.replicate({ factor: 0.5 }, { reset: true });

		await waitForResolved(async () => {
			const store2Role = await store1.log.replicationIndex
				.iterate({ query: { hash: store2.node.identity.publicKey.hashcode() } })
				.all();
			expect(store2Role).to.have.length(1);
			expect(store2Role[0].value.widthNormalized).to.be.closeTo(0.5, 0.01);
		});

		expect(db1JoinEvents).to.have.members([
			session.peers[1].identity.publicKey.hashcode(),
		]); // no new join events
	});
});
