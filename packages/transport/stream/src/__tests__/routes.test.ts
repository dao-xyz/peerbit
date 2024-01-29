import { Ed25519Keypair } from "@peerbit/crypto";
import { Routes } from "../routes";

const me = "me";
const a = "a";
const b = "b";
const c = "c";
const d = "d";

describe("routes", () => {
	it("add", async () => {
		const routes = new Routes(me);
		routes.add(me, a, b, 0, +new Date(), +new Date());
		expect(routes.getFanout(me, [b], 1)?.get(a)?.get(b)?.to).toEqual(b);
	});

	describe("remove", () => {
		it("remote", async () => {
			const routes = new Routes(me);
			routes.add(me, a, b, 0, +new Date(), +new Date());
			expect(routes.remove(b)).toEqual([b]);
			expect(routes.remove(a)).toEqual([]);
		});
		it("neighbour", () => {
			const routes = new Routes(me);
			routes.add(me, a, b, 0, +new Date(), +new Date());
			expect(routes.remove(a)).toEqual([b]);
			expect(routes.remove(a)).toEqual([]);
		});
	});

	describe("getDependent", () => {
		it("neighbour", async () => {
			const routes = new Routes(me);
			routes.add(a, b, c, 0, +new Date(), +new Date());
			expect(routes.getDependent(b)).toEqual([a]);
		});
		it("remote", async () => {
			const routes = new Routes(me);
			routes.add(a, b, c, 0, +new Date(), +new Date());
			expect(routes.getDependent(c)).toEqual([a]);
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
		it("will not send through expired when not relaying", async () => {
			const routes = new Routes(me, { signal: controller.signal });
			let session = 0;
			routes.add(me, b, c, 0, session, 0);
			routes.add(me, d, c, 0, session + 1, 0);

			const fanout = routes.getFanout(me, [c], 1);
			expect(fanout!.size).toEqual(1);
			expect(fanout!.get(d)!.size).toEqual(1);
			expect(fanout!.get(d)?.get(c)).toBeDefined();
		});

		it("will not send through expired when not relaying", async () => {
			const routes = new Routes(me, { signal: controller.signal });
			let session = 0;
			routes.add(a, b, c, 0, session, 0);
			routes.add(a, d, c, 0, session + 1, 0);

			const fanout = routes.getFanout(a, [c], 1);
			expect(fanout!.size).toEqual(2);
			expect(fanout!.get(b)!.size).toEqual(1);
			expect(fanout!.get(d)!.size).toEqual(1);
		});
	});
});
