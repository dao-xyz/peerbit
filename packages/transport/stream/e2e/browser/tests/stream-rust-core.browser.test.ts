// Browser smoke for rust-core mode: two browser peers run DirectStream with
// the native (wasm) core from @peerbit/network-rust (`?rustCore=1`, see
// browser-node/src/App.tsx) while the node-side relay stays on the plain TS
// core, so the exchange doubles as a mixed-implementation interop check. The
// assertions prove the wasm module loads in a real browser and sits on the
// hot path: both pages report an active rust core, payloads arrive in both
// directions and the always-on wire counters show inbound frames were
// decoded + signature-verified natively.
import { noise } from "@chainsafe/libp2p-noise";
import { yamux } from "@chainsafe/libp2p-yamux";
import { circuitRelayServer } from "@libp2p/circuit-relay-v2";
import { identify } from "@libp2p/identify";
import { webSockets } from "@libp2p/websockets";
import { waitForResolved } from "@peerbit/time";
import { type Page, expect, test } from "@playwright/test";
import { createLibp2p } from "libp2p";
import { TestDirectStream } from "../shared/utils.js";

const buildPageUrl = (relayAddress: string) => {
	const url = new URL("http://localhost:5211/");
	url.searchParams.set("relay", relayAddress);
	url.searchParams.set("autopublish", "0");
	url.searchParams.set("rustCore", "1");
	return url.toString();
};

const hasPeer = async (page: Page, targetPeerId: string) => {
	return await page.evaluate(
		([expectedPeerId]) => {
			const peers = (globalThis as any).streamClient?.services?.stream?.peers;
			if (!peers?.values) return false;
			return Array.from(peers.values()).some(
				(p: any) =>
					typeof p?.peerId?.toString === "function" &&
					p.peerId.toString() === expectedPeerId,
			);
		},
		[targetPeerId],
	);
};

const rustCoreActive = async (page: Page) => {
	return await page.evaluate(() => Boolean((globalThis as any).rustCoreActive));
};

const nativeWireFrames = async (page: Page): Promise<number> => {
	return await page.evaluate(() =>
		Number((globalThis as any).getNativeWireFrames?.() ?? 0),
	);
};

const getReceivedData = async (page: Page): Promise<number> => {
	return await page.evaluate(() => {
		return Number((globalThis as any).getReceivedData?.() ?? 0);
	});
};

test.describe("stream rust-core", () => {
	let relay: Awaited<ReturnType<typeof createLibp2p>>;
	test.beforeEach(async () => {
		relay = await createLibp2p<{
			relay: any;
			identify: any;
			stream: TestDirectStream;
		}>({
			addresses: {
				listen: ["/ip4/127.0.0.1/tcp/0/ws"],
			},
			services: {
				// applyDefaultLimit: false because of https://github.com/libp2p/js-libp2p/issues/2622
				relay: circuitRelayServer({
					reservations: { applyDefaultLimit: false, maxReservations: 1000 },
				}),
				identify: identify(),
				stream: (c) => new TestDirectStream(c),
			},
			transports: [webSockets()],
			streamMuxers: [yamux()],
			connectionEncrypters: [noise()],
		});
	});

	test.afterEach(async () => {
		await relay.stop();
	});

	test("two rust-core browser peers exchange data via a TS relay", async ({
		page,
		browser,
	}) => {
		// WebRTC + relay reservation + circuit addressing can be slow on CI.
		test.setTimeout(120_000);

		const relayAddress = relay.getMultiaddrs()[0].toString();
		const relayPeerId = relayAddress.split("/p2p/").at(-1);
		if (!relayPeerId) {
			throw new Error(`Unable to parse relay peer id from ${relayAddress}`);
		}

		await page.goto(buildPageUrl(relayAddress));
		const peerId = (await page.getByTestId("peer-id").textContent())?.trim();
		if (!peerId) {
			throw new Error("Missing peer id");
		}
		expect(await rustCoreActive(page)).toBe(true);

		const dialableAddress = await waitForResolved(
			async () => {
				const multiaddrs = await page.evaluate(() => {
					return (globalThis as any).streamClient
						?.getMultiaddrs?.()
						.map((a: any) => a.toString());
				});

				const addr = multiaddrs?.find(
					(a: string) =>
						a.includes("/p2p-circuit") &&
						a.includes("/webrtc") &&
						a.includes(relayPeerId) &&
						a.endsWith(`/p2p/${peerId}`),
				);
				if (!addr) {
					throw new Error(
						`No dialable address yet. Known: ${(multiaddrs || []).join(", ")}`,
					);
				}
				return addr;
			},
			{ timeout: 30_000, delayInterval: 200 },
		);

		const anotherPage = await browser.newPage();
		await anotherPage.goto(buildPageUrl(dialableAddress));
		const anotherPeerId = (
			await anotherPage.getByTestId("peer-id").textContent()
		)?.trim();
		if (!anotherPeerId) {
			throw new Error("Missing peer id for other peer");
		}
		expect(await rustCoreActive(anotherPage)).toBe(true);

		await waitForResolved(async () => {
			expect(await hasPeer(page, anotherPeerId)).toBe(true);
		});
		await waitForResolved(async () => {
			expect(await hasPeer(anotherPage, peerId)).toBe(true);
		});

		await Promise.all([
			page.evaluate(() => (globalThis as any).sendTestData?.()),
			anotherPage.evaluate(() => (globalThis as any).sendTestData?.()),
		]);

		await waitForResolved(
			async () => {
				expect(await getReceivedData(page)).toBeGreaterThan(0);
				expect(await getReceivedData(anotherPage)).toBeGreaterThan(0);
			},
			{ timeout: 10_000, delayInterval: 200 },
		);

		// The counters only move when the wasm module decoded + verified
		// inbound frames, so this pins the native path (not a TS fallback).
		expect(await nativeWireFrames(page)).toBeGreaterThan(0);
		expect(await nativeWireFrames(anotherPage)).toBeGreaterThan(0);
	});
});
