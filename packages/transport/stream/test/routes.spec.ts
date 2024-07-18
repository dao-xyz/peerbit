import { expect } from "chai";
import { Routes } from "../src/routes.js";

const me = "me";
const a = "a";
const b = "b";
const c = "c";
const d = "d";

describe("routes", () => {
	it("add", async () => {
		const routes = new Routes(me);
		routes.add(me, a, b, 0, +new Date(), +new Date());
		expect(routes.getFanout(me, [b], 1)?.get(a)?.get(b)?.to).equal(b);
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

			const fanout = routes.getFanout(me, [c], 1);
			expect(fanout!.size).equal(1);
			expect(fanout!.get(d)!.size).equal(1);
			expect(fanout!.get(d)?.get(c)).to.exist;
		});

		it("another will not send through expired when not relaying", async () => {
			const routes = new Routes(me, { signal: controller.signal });
			let session = 0;
			routes.add(a, b, c, 0, session, 0);
			routes.add(a, d, c, 0, session + 1, 0);

			const fanout = routes.getFanout(a, [c], 1);
			expect(fanout!.size).equal(2);
			expect(fanout!.get(b)!.size).equal(1);
			expect(fanout!.get(d)!.size).equal(1);
		});
	});
});
