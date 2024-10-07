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

		const store = await session.peers[0].open(new EventStore<string>());
		const canAppend = sinon.spy(store.log.canAppend);
		store.log.canAppend = canAppend;
		await store.add("a");

		expect(canAppend.callCount).to.be.eq(1);
	});

	it("override option canAppend checked once", async () => {
		session = await TestSession.disconnected(1);

		const store = await session.peers[0].open(new EventStore<string>());
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
});
