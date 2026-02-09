import { delay } from "@peerbit/time";
import { expect } from "chai";
import { Routes } from "../src/routes.js";

const me = "me";
const a = "a";
const b = "b";
const c = "c";
const d = "d";

describe("routes", () => {
	describe("add", () => {
		it("one", async () => {
			const routes = new Routes(me);
			routes.add(me, a, b, 0, +new Date(), +new Date());
			expect(routes.getFanout(me, [b], 1)?.get(a)?.get(b)?.to).equal(b);
		});

		it("add new will expire old", async () => {
			const now = +new Date();
			let routeMaxRetentionPeriod = 1000;
			const routes = new Routes(me, { routeMaxRetentionPeriod });
			routes.add(me, a, b, 0, now, now);
			expect(
				routes.routes
					.get(me)!
					.get(b)!
					.list.map((x) => x.hash),
			).to.deep.equal([a]);
			routes.add(me, c, b, 0, now + 1, now + 1);
			expect(
				routes.routes
					.get(me)!
					.get(b)!
					.list.map((x) => x.hash),
			).to.deep.equal([c, a]); // [c, a]  order because a is expiring, and we want to prioritize c

			await delay(routeMaxRetentionPeriod + 1000);

			expect(
				routes.routes
					.get(me)!
					.get(b)!
					.list.map((x) => x.hash),
			).to.deep.equal([c]);
		});

		it("add old will expire old", async () => {
			const now = +new Date();
			let routeMaxRetentionPeriod = 1000;
			const routes = new Routes(me, { routeMaxRetentionPeriod });
			routes.add(me, a, b, 0, now + 1, now + 1);
			expect(
				routes.routes
					.get(me)!
					.get(b)!
					.list.map((x) => x.hash),
			).to.deep.equal([a]);
			routes.add(me, c, b, 0, now, now);
			expect(
				routes.routes
					.get(me)!
					.get(b)!
					.list.map((x) => x.hash),
			).to.deep.equal([a, c]);

			await delay(routeMaxRetentionPeriod + 1000);

			expect(
				routes.routes
					.get(me)!
					.get(b)!
					.list.map((x) => x.hash),
			).to.deep.equal([a]);
		});

		it("neighbours will always take the lowest distance, no matter session", async () => {
			// this because disconnect will make sure the route does not exist
			// and routes to neighbours should always be the lowest distance
			// TODO this is not the case if we for example have a bluetooth connection that is not reliable
			// in that case we should have a higher distance, and a faster way would be to send through an intermediadry

			const now = +new Date();
			const routes = new Routes(me);
			routes.add(me, a, a, -1, now + 1, now + 1);
			expect(
				routes.routes
					.get(me)!
					.get(a)!
					.list.map((x) => x.session),
			).to.deep.equal([now + 1]);
			routes.add(me, a, a, -1, now, now);
			expect(
				routes.routes
					.get(me)!
					.get(a)!
					.list.map((x) => x.session),
			).to.deep.equal([now + 1]);
		});

		it("cleanup removes empty 'from' maps after expiry", async () => {
			const routes = new Routes(me);
			// Add a single route entry
			routes.add(me, a, b, 0, +new Date(), +new Date());
			// Manually expire the only relay entry
			const entry = routes.routes.get(me)!.get(b)!;
			entry.list[0].expireAt = +new Date() - 1;
			// Trigger internal cleanup for the target
			(routes as any)["cleanup"](me, b);
			// Expect parent 'from' map to be removed when it becomes empty
			expect(routes.routes.get(me)).to.equal(undefined);
		});

		it("bounds number of 'from' maps (pins me)", async () => {
			const routes = new Routes(me, { maxFromEntries: 2 });
			routes.add(me, a, a, -1, +new Date(), +new Date());
			routes.add(a, b, c, 0, +new Date(), +new Date());
			routes.add(b, c, d, 0, +new Date(), +new Date());

			expect(routes.routes.has(me)).to.equal(true);
			expect(routes.routes.has(b)).to.equal(true);
			expect(routes.routes.has(a)).to.equal(false);
		});

		it("bounds number of targets per 'from'", async () => {
			const routes = new Routes(me, { maxTargetsPerFrom: 2 });
			const now = +new Date();
			routes.add(me, a, a, -1, now, now);
			routes.add(me, b, b, -1, now + 1, now + 1);
			routes.add(me, c, c, -1, now + 2, now + 2);

			const map = routes.routes.get(me)!;
			expect(map.has(a)).to.equal(false);
			expect(map.has(b)).to.equal(true);
			expect(map.has(c)).to.equal(true);
		});

		it("bounds relay list per target", async () => {
			const routes = new Routes(me, { maxRelaysPerTarget: 2 });
			const now = +new Date();
			routes.add(me, b, b, -1, now, now);
			routes.add(me, a, b, 0, now + 1, now + 1);
			routes.add(me, c, b, 0, now + 2, now + 2);

			const entry = routes.routes.get(me)!.get(b)!;
			expect(entry.list.length).to.equal(2);
			expect(entry.list.find((x) => x.hash === b)?.distance).to.equal(-1);
		});
	});

	describe("remove", () => {
		it("remote", async () => {
			const routes = new Routes(me);
			routes.add(me, a, b, 0, +new Date(), +new Date());
			expect(routes.remove(b)).to.deep.equal([b]);
			expect(routes.remove(a)).to.be.empty;
		});
		it("neighbour", () => {
			const routes = new Routes(me);
			routes.add(me, a, b, 0, +new Date(), +new Date());
			expect(routes.remove(a)).to.deep.equal([b]);
			expect(routes.remove(a)).to.be.empty;
		});
	});

	describe("getDependent", () => {
		it("neighbour", async () => {
			const routes = new Routes(me);
			routes.add(a, b, c, 0, +new Date(), +new Date());
			expect(routes.getDependent(b)).to.deep.equal([a]);
		});
		it("remote", async () => {
			const routes = new Routes(me);
			routes.add(a, b, c, 0, +new Date(), +new Date());
			expect(routes.getDependent(c)).to.deep.equal([a]);
		});
	});

	describe("getFanout", () => {
		let controller: AbortController;

		beforeEach(() => {
			controller = new AbortController();
		});

		afterEach(() => {
			controller.abort();
		});
		it("me will not send through expired when not relaying", async () => {
			const routes = new Routes(me, { signal: controller.signal });
			let session = 0;
			routes.add(me, b, c, 0, session, 0);
			routes.add(me, d, c, 0, session + 1, 0);

			const fanout = routes.getFanout(me, [c], 2);
			expect(fanout!.size).equal(1);
			expect(fanout!.get(d)!.size).equal(1);
			expect(fanout!.get(d)?.get(c)).to.exist;
		});

		it("another will send through expired when not relaying", async () => {
			const routes = new Routes(me, { signal: controller.signal });
			let session = 0;
			routes.add(a, b, c, 0, session, 0);
			routes.add(a, d, c, 0, session + 1, 0);

			const fanout = routes.getFanout(a, [c], 2);
			expect(fanout!.size).equal(2);
			expect(fanout!.get(b)!.size).equal(1);
			expect(fanout!.get(d)!.size).equal(1);
		});

		it("will send through expired routes directly if not yet have updated info", async () => {
			const routes = new Routes(me, { signal: controller.signal });
			let session = 0;

			routes.add(a, c, c, -1, session, 0); // lower distance but older session
			routes.add(a, d, c, 1, session + 1, 0); // higher distance but newer session

			const fanout = routes.getFanout(a, [c], 2);
			expect(fanout!.size).equal(1); // only c will be used because it is a direct route (no matter if new expire information has been assigned to this route)
			expect(fanout!.get(c)!.size).equal(1);
		});
	});
});
