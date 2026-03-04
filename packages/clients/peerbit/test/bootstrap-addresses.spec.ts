import { expect } from "chai";
import { resolveBootstrapAddresses } from "../src/bootstrap.js";

describe("resolveBootstrapAddresses", () => {
	const originalFetch = globalThis.fetch;

	afterEach(() => {
		globalThis.fetch = originalFetch;
	});

	it("defaults to the v5 public bootstrap list", async () => {
		const requested: string[] = [];
		globalThis.fetch = (async (input: string | URL | Request) => {
			requested.push(
				typeof input === "string"
					? input
					: input instanceof URL
						? input.toString()
						: input.url,
			);
			return new Response(
				"/dns4/node-a.peerchecker.com/tcp/4003/wss/p2p/12D3KooA\n\n/dns4/node-b.peerchecker.com/tcp/4003/wss/p2p/12D3KooB\n",
				{ status: 200 },
			);
		}) as typeof fetch;

		const result = await resolveBootstrapAddresses();
		expect(requested).to.deep.equal([
			"https://bootstrap.peerbit.org/bootstrap-5.env",
		]);
		expect(result).to.deep.equal([
			"/dns4/node-a.peerchecker.com/tcp/4003/wss/p2p/12D3KooA",
			"/dns4/node-b.peerchecker.com/tcp/4003/wss/p2p/12D3KooB",
		]);
	});

	it("uses an explicit version override", async () => {
		const requested: string[] = [];
		globalThis.fetch = (async (input: string | URL | Request) => {
			requested.push(
				typeof input === "string"
					? input
					: input instanceof URL
						? input.toString()
						: input.url,
			);
			return new Response("", { status: 200 });
		}) as typeof fetch;

		await resolveBootstrapAddresses("4");
		expect(requested).to.deep.equal([
			"https://bootstrap.peerbit.org/bootstrap-4.env",
		]);
	});
});
