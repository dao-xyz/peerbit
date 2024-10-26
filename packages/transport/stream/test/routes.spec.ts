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
			).to.deep.equal([a, c]);

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
	});
});
