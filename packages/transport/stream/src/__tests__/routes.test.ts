import { Ed25519Keypair } from "@peerbit/crypto";
import { Routes } from "../routes";

const me = "me";
const a = "a";
const b = "b";
const c = "c";

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
});
