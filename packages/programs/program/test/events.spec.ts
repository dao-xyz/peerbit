import { deserialize, serialize } from "@dao-xyz/borsh";
import { delay, waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import { ClosedError, type ProgramClient } from "../src/index.js";
import {
	P3,
	ProgramWithoutTopics,
	TestProgram,
	TestProgramWithTopics,
} from "./samples.js";
import { creatMockPeer, createLibp2pPeer } from "./utils.js";

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

	it("only emits join/leave events on join", async () => {
		const db1 = await client1.open(new TestProgramWithTopics(0));
		expect(await client1.open(db1)).equal(db1);

		let joinEvents: string[] = [];
		db1.events.addEventListener("join", (event) => {
			joinEvents.push(event.detail.hashcode());
		});

		let leaveEvents: string[] = [];
		db1.events.addEventListener("leave", (event) => {
			leaveEvents.push(event.detail.hashcode());
		});

		const db2 = await client2.open(new TestProgramWithTopics(1)); // another program
		expect(db2.address).not.equal(db1.address);

		await client1.dial(client2.getMultiaddrs());
		await delay(3e3);
		expect(joinEvents).to.have.length(0);
	});

	it("only emits join/leave events if actually subscribing to something", async () => {
		// in this test we are testing that the join/leave events are only emitted if the program is actually subscribing to something
		// this is important because otherwise we would have a lot of events being emitted for no reason
		// we also open another program that has topics, and we check that that does not interfere with the join/leave events
		const db1 = await client1.open(new ProgramWithoutTopics()); // test program does not have any addresses
		const db1b = await client1.open(new TestProgramWithTopics(1));

		expect(db1.programs.map((x) => x.getTopics?.()).flat()).to.have.length(0);
		expect(
			db1b.programs.map((x) => x.getTopics?.()).flat().length,
		).to.be.greaterThan(0);

		expect(await client1.open(db1)).equal(db1);

		let joinEvents: string[] = [];
		db1.events.addEventListener("join", (event) => {
			joinEvents.push(event.detail.hashcode());
		});

		let leaveEvents: string[] = [];
		db1.events.addEventListener("leave", (event) => {
			leaveEvents.push(event.detail.hashcode());
		});

		const db2 = await client2.open(db1.clone()); // another program
		const db2b = await client2.open(db1b.clone()); // another program

		expect(db2.address).to.equal(db1.address);
		expect(db2.programs.map((x) => x.getTopics?.()).flat()).to.have.length(0);
		expect(
			db2b.programs.map((x) => x.getTopics?.()).flat().length,
		).to.be.greaterThan(0);
		expect(db2b.address).to.equal(db1b.address);

		await client1.dial(client2.getMultiaddrs());
		await delay(3e3);
		expect(joinEvents).to.have.length(0);
	});

	it("join/leave  multiple times", async () => {
		// TODO this test is not clean
		const eventHandlers = new Map();
		const subscriptions = new Map();
		const peers = new Map();
		const peer = await creatMockPeer({
			subsribers: subscriptions,
			pubsubEventHandlers: eventHandlers,
			peers,
		});
		const peer2 = await creatMockPeer({
			subsribers: subscriptions,
			pubsubEventHandlers: eventHandlers,
			peers,
		});

		const p = new P3();
		const joinEvents: string[] = [];
		const leaveEvents: string[] = [];

		p.events.addEventListener("join", (e) => {
			joinEvents.push(e.detail.hashcode());
		});
		p.events.addEventListener("leave", (e) => {
			leaveEvents.push(e.detail.hashcode());
		});

		await peer.open(p);

		const joinEvents2: string[] = [];
		const leaveEvents2: string[] = [];

		const p2 = deserialize(serialize(p), P3);
		p2.events.addEventListener("join", (e) => {
			joinEvents2.push(e.detail.hashcode());
		});

		p2.events.addEventListener("leave", (e) => {
			leaveEvents2.push(e.detail.hashcode());
		});

		await peer2.open(p2);

		expect(joinEvents).to.deep.equal([peer2.identity.publicKey.hashcode()]);
		expect(joinEvents2).to.be.empty;

		await peer2.services.pubsub.requestSubscribers(p.getTopics()[0]);

		expect(joinEvents2).to.deep.equal([peer.identity.publicKey.hashcode()]);

		expect(leaveEvents).to.be.empty;
		expect(leaveEvents2).to.be.empty;

		await p2.close();

		expect(leaveEvents).to.deep.equal([peer2.identity.publicKey.hashcode()]);
		expect(leaveEvents2).to.be.empty;
	});

	it("will not ask for topics on closed subprogram", async () => {
		const test = new TestProgram();
		const peer = await creatMockPeer();
		await peer.open(test, { args: { dontOpenNested: true } });
		expect(() => test.nested.getTopics()).to.throw(ClosedError);
		await test.getReady();
	});

	describe("getReady", () => {
		let peer: ProgramClient, peer2: ProgramClient;
		afterEach(async () => {
			await peer?.stop();
			await peer2?.stop();
		});

		it("throws on no topics", async () => {
			const test = new ProgramWithoutTopics();
			expect(test.getReady()).rejectedWith(
				"Program has no topics, cannot get ready",
			);
		});

		it("returns ready when another peer join", async () => {
			const test = new TestProgramWithTopics();
			peer = await createLibp2pPeer();
			await peer.open(test);

			peer2 = await createLibp2pPeer();
			await peer2.open(test.clone());
			await peer.dial(peer2.getMultiaddrs());
			await waitForResolved(async () => {
				expect(
					(await test.getReady())
						.get(peer2.identity.publicKey.hashcode())
						?.equals(peer2.identity.publicKey),
				).to.be.true;
			});
		});
		// TODO test if for example 50 % of programs are open, what is the expected join/leave events???
	});
	// TODO test if for example 50 % of programs are open, what is the expected join/leave events???
});
