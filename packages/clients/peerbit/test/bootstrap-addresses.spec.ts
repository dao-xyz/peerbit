import { multiaddr } from "@multiformats/multiaddr";
import { expect } from "chai";
import sinon from "sinon";
import {
	getBootstrapPeerId,
	resolveBootstrapAddresses,
} from "../src/bootstrap.js";

const peerIdA = "12D3KooWKj1J1hHxrYyB37qDDGCi9aU2vcHzDZhtMk7te7dEmqqT";
const peerIdB = "12D3KooWAYyiQBc1ti51riCkNX6Nvh33pWWvNfyrcPHrq373qCju";
const addressA = `/dns4/node-a.peerchecker.com/tcp/4003/wss/p2p/${peerIdA}`;
const addressB = `/dns4/node-b.peerchecker.com/tcp/4003/wss/p2p/${peerIdB}`;
const circuitAddress = `/dns4/relay.peerchecker.com/tcp/443/wss/p2p/${peerIdA}/p2p-circuit/p2p/${peerIdB}`;
const webRTCAddress = `/dns4/relay.peerchecker.com/tcp/443/wss/p2p/${peerIdA}/p2p-circuit/webrtc/p2p/${peerIdB}`;

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
			return new Response(`${addressA}\n\n${addressB}\n`, { status: 200 });
		}) as typeof fetch;

		const result = await resolveBootstrapAddresses();
		expect(requested).to.deep.equal([
			"https://bootstrap.peerbit.org/bootstrap-5.env",
		]);
		expect(result).to.deep.equal([addressA, addressB]);
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
			return new Response(`${addressA}\n`, { status: 200 });
		}) as typeof fetch;

		await resolveBootstrapAddresses("4");
		expect(requested).to.deep.equal([
			"https://bootstrap.peerbit.org/bootstrap-4.env",
		]);
	});

	it("falls back to the canonical repository when the public endpoint is unavailable", async () => {
		const requested: string[] = [];
		globalThis.fetch = (async (input: string | URL | Request) => {
			const url =
				typeof input === "string"
					? input
					: input instanceof URL
						? input.toString()
						: input.url;
			requested.push(url);
			if (url.startsWith("https://bootstrap.peerbit.org/")) {
				throw new TypeError("fetch failed");
			}
			return new Response(`${addressA}\n`, { status: 200 });
		}) as typeof fetch;

		const result = await resolveBootstrapAddresses();
		expect(requested).to.deep.equal([
			"https://bootstrap.peerbit.org/bootstrap-5.env",
			"https://raw.githubusercontent.com/dao-xyz/peerbit-bootstrap/master/bootstrap-5.env",
		]);
		expect(result).to.deep.equal([addressA]);
	});

	it("rejects unusable content before accepting a fallback list", async () => {
		let requestCount = 0;
		globalThis.fetch = (async () => {
			requestCount += 1;
			return requestCount === 1
				? new Response("<html>not a bootstrap list</html>", { status: 200 })
				: new Response(`# current relays\n${addressA}\n${addressA}\n`, {
						status: 200,
					});
		}) as typeof fetch;

		expect(await resolveBootstrapAddresses()).to.deep.equal([addressA]);
		expect(requestCount).to.equal(2);
	});

	it("rejects addresses the default transports cannot dial", async () => {
		const unsupportedAddresses = [
			`/p2p/${peerIdA}`,
			"/dns4/bootstrap.example.com",
			"/tcp/4003",
			"/tcp/4003/dns4/bootstrap.example.com",
			`/ws/p2p/${peerIdA}`,
			`/dns4/bootstrap.example.com/p2p/${peerIdA}/tcp/4003`,
			"/p2p-circuit",
			"/webrtc",
			`/p2p/${peerIdA}/p2p-circuit/p2p/${peerIdB}`,
			`/dns4/relay.example.com/tcp/443/wss/p2p-circuit/p2p/${peerIdB}`,
			`/dns4/relay.example.com/tcp/443/wss/p2p-circuit/webrtc/p2p/${peerIdB}`,
			`/dns4/relay.example.com/tcp/443/wss/p2p/${peerIdA}/p2p-circuit`,
			"/ip4/127.0.0.1/udp/4001",
			"/ip4/127.0.0.1/udp/4001/quic-v1",
			"/ip4/127.0.0.1/udp/443/webtransport",
			"/ip4/1.2.3.4/udp/443/wss",
			"/ip4/1.2.3.4/udp/443/quic-v1/wss",
			"/dns4/bootstrap.example.com/udp/4001/tcp/4003/quic-v1",
			"/dns4/bootstrap.example.com/tcp/0",
			"/dns4/bootstrap.example.com/tcp/0/wss",
			"/ip4/0.0.0.0/tcp/4003/wss",
			"/ip6/::/tcp/4003/wss",
		];
		for (const unsupportedAddress of unsupportedAddresses) {
			let requestCount = 0;
			globalThis.fetch = (async () => {
				requestCount += 1;
				return new Response(
					requestCount === 1
						? `${unsupportedAddress}\n`
						: "/dnsaddr/bootstrap.example.com\n",
					{ status: 200 },
				);
			}) as typeof fetch;

			expect(await resolveBootstrapAddresses()).to.deep.equal([
				"/dnsaddr/bootstrap.example.com",
			]);
			expect(requestCount).to.equal(2);
		}

		for (const crossRuntimeAddress of [
			"/dnsaddr/bootstrap.example.com",
			"/dns4/bootstrap.example.com/tcp/4003/wss",
			circuitAddress,
		]) {
			let requestCount = 0;
			globalThis.fetch = (async () => {
				requestCount += 1;
				return new Response(`${crossRuntimeAddress}\n`, { status: 200 });
			}) as typeof fetch;
			expect(await resolveBootstrapAddresses()).to.deep.equal([
				crossRuntimeAddress,
			]);
			expect(requestCount).to.equal(1);
		}
	});

	it("keeps supported alternatives but requires a cross-runtime target", async () => {
		const supplementalAddresses = [
			"/dns4/bootstrap.example.com/tcp/4003",
			"/dns4/bootstrap.example.com/tcp/4003/ws",
			webRTCAddress,
		];

		for (const supplementalAddress of supplementalAddresses) {
			let requestCount = 0;
			globalThis.fetch = (async () => {
				requestCount += 1;
				return requestCount === 1
					? new Response(`${supplementalAddress}\n${addressA}\n`, {
							status: 200,
						})
					: new Response("/dnsaddr/fallback.example.com\n", {
							status: 200,
						});
			}) as typeof fetch;

			expect(await resolveBootstrapAddresses()).to.deep.equal([
				supplementalAddress,
				addressA,
			]);
			expect(requestCount).to.equal(1);

			requestCount = 0;
			globalThis.fetch = (async () => {
				requestCount += 1;
				return new Response(
					requestCount === 1
						? `${supplementalAddress}\n`
						: "/dnsaddr/fallback.example.com\n",
					{ status: 200 },
				);
			}) as typeof fetch;

			expect(await resolveBootstrapAddresses()).to.deep.equal([
				"/dnsaddr/fallback.example.com",
			]);
			expect(requestCount).to.equal(2);
		}
	});

	it("ignores unsupported entries when a source still has a safe target", async () => {
		let requestCount = 0;
		globalThis.fetch = (async () => {
			requestCount += 1;
			return new Response(
				`/tcp/4003/dns4/bootstrap.example.com\n${addressA}\n`,
				{ status: 200 },
			);
		}) as typeof fetch;

		expect(await resolveBootstrapAddresses()).to.deep.equal([addressA]);
		expect(requestCount).to.equal(1);
	});

	it("bounds declared, streamed, and address-list sizes", async () => {
		let declaredBodyCancelled = 0;
		let streamedBodyCancelled = 0;
		const declaredBody = new ReadableStream<Uint8Array>({
			cancel() {
				declaredBodyCancelled += 1;
			},
		});
		const oversizedStream = new ReadableStream<Uint8Array>({
			pull(controller) {
				controller.enqueue(new Uint8Array(40_000));
			},
			cancel() {
				streamedBodyCancelled += 1;
			},
		});
		const oversizedAddresses = `${Array.from(
			{ length: 257 },
			() => addressA,
		).join("\n")}\n`;
		const unusableResponses = [
			new Response(declaredBody, {
				status: 200,
				headers: { "content-length": "65537" },
			}),
			new Response(oversizedStream, { status: 200 }),
			new Response(oversizedAddresses, { status: 200 }),
		];

		for (const unusableResponse of unusableResponses) {
			let requestCount = 0;
			let firstSignal: AbortSignal | null | undefined;
			globalThis.fetch = (async (_input, init) => {
				requestCount += 1;
				if (requestCount === 1) {
					firstSignal = init?.signal;
					return unusableResponse;
				}
				return new Response(`${addressA}\n`, { status: 200 });
			}) as typeof fetch;

			expect(await resolveBootstrapAddresses()).to.deep.equal([addressA]);
			expect(requestCount).to.equal(2);
			expect(firstSignal?.aborted).to.equal(true);
		}
		expect(declaredBodyCancelled).to.equal(1);
		expect(streamedBodyCancelled).to.equal(1);
	});

	it("aborts hanging sources before falling back or aggregating failures", async () => {
		const clock = sinon.useFakeTimers({ shouldClearNativeTimers: true });
		const waitForAbort = (init?: RequestInit): Promise<Response> =>
			new Promise((_resolve, reject) => {
				const signal = init?.signal;
				if (!signal) {
					reject(new Error("Missing bootstrap fetch signal"));
					return;
				}
				const onAbort = () => reject(signal.reason);
				if (signal.aborted) {
					onAbort();
				} else {
					signal.addEventListener("abort", onAbort, { once: true });
				}
			});

		try {
			let requestCount = 0;
			globalThis.fetch = (async (_input, init) => {
				requestCount += 1;
				return requestCount === 1
					? waitForAbort(init)
					: new Response(`${addressA}\n`, { status: 200 });
			}) as typeof fetch;
			const fallbackResult = resolveBootstrapAddresses();
			await clock.tickAsync(10_000);
			expect(await fallbackResult).to.deep.equal([addressA]);
			expect(requestCount).to.equal(2);

			globalThis.fetch = (async (_input, init) =>
				waitForAbort(init)) as typeof fetch;
			const failedResult = resolveBootstrapAddresses();
			await clock.tickAsync(20_000);
			let failure: unknown;
			try {
				await failedResult;
			} catch (error) {
				failure = error;
			}
			expect(failure).to.be.instanceOf(AggregateError);
			expect((failure as AggregateError).errors).to.have.length(2);
			for (const error of (failure as AggregateError).errors) {
				expect((error as Error).message).to.contain("Timed out fetching");
			}
		} finally {
			clock.restore();
		}
	});

	it("reports every source failure instead of returning an empty list", async () => {
		globalThis.fetch = (async (input: string | URL | Request) => {
			const url =
				typeof input === "string"
					? input
					: input instanceof URL
						? input.toString()
						: input.url;
			return url.startsWith("https://bootstrap.peerbit.org/")
				? new Response("unavailable", { status: 503 })
				: new Response("\n# no relays\n", { status: 200 });
		}) as typeof fetch;

		let failure: unknown;
		try {
			await resolveBootstrapAddresses();
		} catch (error) {
			failure = error;
		}
		expect(failure).to.be.instanceOf(AggregateError);
		expect((failure as AggregateError).errors).to.have.length(2);
		expect((failure as Error).message).to.contain(
			"Failed to resolve bootstrap addresses",
		);
	});
});

describe("getBootstrapPeerId", () => {
	it("extracts the p2p component using multiaddr parsing", () => {
		expect(getBootstrapPeerId(addressA)).to.equal(peerIdA);
	});

	it("returns undefined for addresses without a p2p component", () => {
		expect(getBootstrapPeerId("/dns4/node-a.peerchecker.com/tcp/4003/wss")).to
			.be.undefined;
	});

	it("extracts the peer id from a Multiaddr instance", () => {
		expect(getBootstrapPeerId(multiaddr(addressB))).to.equal(peerIdB);
	});

	it("extracts the terminal peer id from a routed address", () => {
		expect(getBootstrapPeerId(circuitAddress)).to.equal(peerIdB);
	});

	it("returns undefined for malformed bootstrap addresses", () => {
		expect(getBootstrapPeerId("definitely-not-a-multiaddr")).to.be.undefined;
	});
});
