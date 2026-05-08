import { TestSession } from "@peerbit/test-utils";
import { expect } from "chai";
import sinon from "sinon";
import { EventStore } from "./utils/stores/index.js";

describe("append", () => {
	let session: TestSession;

	before(async () => {});

	afterEach(async () => {
		await session.stop();
	});

	it("canAppend checked once", async () => {
		session = await TestSession.disconnected(1);

		const store = await session.peers[0].open(new EventStore<string, any>());
		const canAppend = sinon.spy(store.log.canAppend);
		store.log.canAppend = canAppend;
		await store.add("a");

		expect(canAppend.callCount).to.be.eq(1);
	});

	it("override option canAppend checked once", async () => {
		session = await TestSession.disconnected(1);

		const store = await session.peers[0].open(new EventStore<string, any>());
		const canAppend = sinon.spy(store.log.canAppend);
		store.log.canAppend = canAppend;

		let canAppendOverride = false;
		await store.add("a", {
			canAppend: () => {
				canAppendOverride = true;
				return true;
			},
		});
		expect(canAppend.callCount).to.be.eq(1);
		expect(canAppendOverride).to.be.true;
	});

	it("appendMany appends a local chain with one shared-log change", async () => {
		session = await TestSession.disconnected(1);
		const changes: any[] = [];
		const store = await session.peers[0].open(new EventStore<string, any>(), {
			args: {
				onChange: (change) => {
					changes.push(change);
				},
				replicate: false,
			},
		});

		const result = await store.addMany(["a", "b", "c"], {
			replicate: false,
			target: "none",
		});

		expect(result.entries).to.have.length(3);
		expect(result.entries[1].meta.next).to.deep.equal([result.entries[0].hash]);
		expect(result.entries[2].meta.next).to.deep.equal([result.entries[1].hash]);
		expect((await store.log.log.getHeads().all()).map((head) => head.hash)).to.deep.equal([
			result.entries[2].hash,
		]);
		expect(changes).to.have.length(1);
		expect(changes[0].added.map((added: any) => added.entry.hash)).to.deep.equal(
			result.entries.map((entry) => entry.hash),
		);
		expect(changes[0].added.map((added: any) => added.head)).to.deep.equal([
			false,
			false,
			true,
		]);
	});
});
