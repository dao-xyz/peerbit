import { deserialize, serialize } from "@dao-xyz/borsh";
import { AbortError, delay, waitForResolved } from "@peerbit/time";
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

		const db2 = await client2.open(db1.clone());

		await client1.dial(client2.getMultiaddrs());

		await waitForResolved(() =>
			expect(joinEvents).to.deep.equal([client2.identity.publicKey.hashcode()]),
		);
		await waitForResolved(() => expect(leaveEvents).to.have.length(0));

		await delay(1000); // allow extra events for additional events to be processed

		// but no change should be observerd
		expect(joinEvents).to.have.length(1);
		expect(leaveEvents).to.have.length(0);

		await db2.close();

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

	it("emit leaves event when one of the subprograms is left", async () => {
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

		const db2 = await client2.open(db1.clone());

		await client1.dial(client2.getMultiaddrs());

		await waitForResolved(() =>
			expect(joinEvents).to.deep.equal([client2.identity.publicKey.hashcode()]),
		);
		await waitForResolved(() => expect(leaveEvents).to.have.length(0));

		await delay(1000); // allow extra events for additional events to be processed

		// but no change should be observerd
		expect(joinEvents).to.have.length(1);
		expect(leaveEvents).to.have.length(0);

		await db2.subprogram.close();

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

	it("does not emit join/leave events on different program join", async () => {
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

	it("does not emits join/leave events on partial join", async () => {
		const db1 = await client1.open(new TestProgramWithTopics(0, 1));
		expect(await client1.open(db1)).equal(db1);

		let joinEvents: string[] = [];
		db1.events.addEventListener("join", (event) => {
			joinEvents.push(event.detail.hashcode());
		});

		let leaveEvents: string[] = [];
		db1.events.addEventListener("leave", (event) => {
			leaveEvents.push(event.detail.hashcode());
		});

		const db2 = await client2.open(new TestProgramWithTopics(0, 2)); // another program
		expect(db2.address).not.equal(db1.address);

		await client1.dial(client2.getMultiaddrs());
		await delay(3e3);
		expect(joinEvents).to.have.length(0);

		await client2.stop();
		await delay(2e3);
		expect(leaveEvents).to.have.length(0);
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
		await delay(2e3);
		expect(leaveEvents).to.have.length(0);
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

		await waitForResolved(() =>
			expect(joinEvents).to.deep.equal([peer2.identity.publicKey.hashcode()]),
		);
		expect(joinEvents2).to.be.empty;

		await peer2.services.pubsub.requestSubscribers(p.getTopics()[0]);

		expect(joinEvents2).to.deep.equal([peer.identity.publicKey.hashcode()]);

		expect(leaveEvents).to.be.empty;
		expect(leaveEvents2).to.be.empty;

		await p2.close();

		await waitForResolved(() =>
			expect(leaveEvents).to.deep.equal([peer2.identity.publicKey.hashcode()]),
		);
		expect(leaveEvents2).to.be.empty;
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

		it("will not ask for topics on closed subprogram", async () => {
			const test = new TestProgram();
			const peer = await creatMockPeer();
			await peer.open(test, { args: { dontOpenNested: true } });
			expect(() => test.nested.getTopics()).to.throw(ClosedError);
			expect(test.getReady()).rejectedWith(
				"Program has no topics, cannot get ready", // will throw this error because now no topics will exist
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

	describe("waitFor", () => {
		let peer: ProgramClient;
		let state: any;
		const createState = () => ({
			pubsubEventHandlers: new Map(),
			subsribers: new Map(),
			peers: new Map(),
		});

		beforeEach(async () => {
			state = createState();
			peer = await creatMockPeer(state);
		});
		afterEach(async () => {
			await peer?.stop();
		});
		it("self", async () => {
			const p = await peer.open(new TestProgramWithTopics());
			await p.waitFor(p.node.identity.publicKey);
		});

		it("rejects immediately when signal already aborted", async () => {
			const p = await peer.open(new TestProgramWithTopics());
			(peer.services.pubsub as any).waitFor = async () => ["other"];

			const baseSubscribeListeners =
				(state.pubsubEventHandlers.get("subscribe")?.length as number) ?? 0;

			const controller = new AbortController();
			controller.abort(new AbortError("aborted"));

			await expect(
				p.waitFor(p.node.identity.publicKey, {
					signal: controller.signal,
					timeout: 50,
				}),
			).rejectedWith(AbortError);

			expect(state.pubsubEventHandlers.get("subscribe")?.length).to.equal(
				baseSubscribeListeners,
			);
		});

		it("cleans up listeners when getReady throws", async () => {
			const p = await peer.open(new ProgramWithoutTopics());
			(peer.services.pubsub as any).waitFor = async () => ["other"];

			const baseSubscribeListeners =
				(state.pubsubEventHandlers.get("subscribe")?.length as number) ?? 0;

			await expect(
				p.waitFor(p.node.identity.publicKey, { timeout: 50 }),
			).rejectedWith("Program has no topics, cannot get ready");

			expect(state.pubsubEventHandlers.get("subscribe")?.length).to.equal(
				baseSubscribeListeners,
			);
		});

		it("rejects with AbortError when aborted during wait", async () => {
			const p = await peer.open(new TestProgramWithTopics());
			(peer.services.pubsub as any).waitFor = async () => ["other"];

			const baseSubscribeListeners =
				(state.pubsubEventHandlers.get("subscribe")?.length as number) ?? 0;

			const controller = new AbortController();
			const promise = p.waitFor(p.node.identity.publicKey, {
				signal: controller.signal,
				timeout: 1000,
			});

			// Ensure we’re past `await pubsub.waitFor` and have registered listeners.
			await Promise.resolve();
			controller.abort(new AbortError("aborted"));

			await expect(promise).rejectedWith(AbortError);
			expect(state.pubsubEventHandlers.get("subscribe")?.length).to.equal(
				baseSubscribeListeners,
			);
		});
	});

	// TODO test if for example 50 % of programs are open, what is the expected join/leave events???
});
