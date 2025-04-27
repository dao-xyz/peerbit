import { delay, waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import { type ProgramClient } from "../src/index.js";
import { TestProgramWithTopics } from "./samples.js";
import { createLibp2pPeer } from "./utils.js";

describe("events", () => {
	let client1: ProgramClient, client2: ProgramClient;

	beforeEach(async () => {
		client1 = await createLibp2pPeer();
		client2 = await createLibp2pPeer();
	});

	afterEach(async () => {
		await client1.stop();
		await client2?.stop();
	});

	it("only emits join/leave events once per user", async () => {
		const db1 = await client1.open(new TestProgramWithTopics());
		expect(await client1.open(db1)).equal(db1);

		let joinEvents: string[] = [];
		db1.events.addEventListener("join", (event) => {
			joinEvents.push(event.detail.hashcode());
		});

		let leaveEvents: string[] = [];
		db1.events.addEventListener("leave", (event) => {
			leaveEvents.push(event.detail.hashcode());
		});

		await client2.open(db1.clone());

		await client1.dial(client2.getMultiaddrs());

		await waitForResolved(() =>
			expect(joinEvents).to.deep.equal([client2.identity.publicKey.hashcode()]),
		);
		await waitForResolved(() => expect(leaveEvents).to.have.length(0));

		await delay(1000); // allow extra events for additional events to be processed

		// but no change should be observerd
		expect(joinEvents).to.have.length(1);
		expect(leaveEvents).to.have.length(0);

		await client2.stop();

		await waitForResolved(() =>
			expect(joinEvents).to.deep.equal([client2.identity.publicKey.hashcode()]),
		);
		await waitForResolved(() =>
			expect(leaveEvents).to.deep.equal([
				client2.identity.publicKey.hashcode(),
			]),
		);

		await delay(1000); // allow extra events for additional events to be processed
		// but no change should be observerd
		expect(joinEvents).to.have.length(1);
		expect(leaveEvents).to.have.length(1);
	});
});
