import { noise } from "@chainsafe/libp2p-noise";
import { yamux } from "@chainsafe/libp2p-yamux";
import { circuitRelayServer } from "@libp2p/circuit-relay-v2";
import { identify } from "@libp2p/identify";
import { webSockets } from "@libp2p/websockets";
import { waitForResolved } from "@peerbit/time";
import { expect, test } from "@playwright/test";
import { createLibp2p } from "libp2p";
import { TestDirectStream } from "../shared/utils.js";

test.describe("stream", () => {
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

	test("can transmit with webrtc", async ({ page, browser }) => {
		// WebRTC + relay reservation + circuit addressing can be slow/flaky on CI,
		// so give this end-to-end test a larger budget than Playwright's 30s default.
		test.setTimeout(120_000);

		const relayAddres = relay.getMultiaddrs()[0].toString();
		const relayPeerId = relayAddres.split("/p2p/").at(-1);
		if (!relayPeerId) {
			throw new Error(`Unable to parse relay peer id from ${relayAddres}`);
		}
		await page.goto(
			"http://localhost:5211/?relay=" + encodeURIComponent(relayAddres),
		);

		const peerIdLocator = await page.getByTestId("peer-id");
		const peerId = (await peerIdLocator.textContent())?.trim();
		if (!peerId) {
			throw new Error("Missing peer id");
		}

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
		await anotherPage.goto(
			"http://localhost:5211/?relay=" + encodeURIComponent(dialableAddress),
		);

		const anotherPeerIdLocator = await anotherPage.getByTestId("peer-id");
		const anotherPeerId = (await anotherPeerIdLocator.textContent())?.trim();
		if (!anotherPeerId) {
			throw new Error("Missing peer id for other peer");
		}

		await waitForResolved(async () => {
			const hasAnotherPeer = await page.evaluate(
				([targetPeerId]) => {
					const peers = (globalThis as any).streamClient?.services?.stream
						?.peers;
					if (!peers?.values) return false;
					return Array.from(peers.values()).some(
						(p: any) =>
							typeof p?.peerId?.toString === "function" &&
							p.peerId.toString() === targetPeerId,
					);
				},
				[anotherPeerId],
			);
			expect(hasAnotherPeer).toBe(true);
		});

		await waitForResolved(async () => {
			const hasPeer = await anotherPage.evaluate(
				([targetPeerId]) => {
					const peers = (globalThis as any).streamClient?.services?.stream
						?.peers;
					if (!peers?.values) return false;
					return Array.from(peers.values()).some(
						(p: any) =>
							typeof p?.peerId?.toString === "function" &&
							p.peerId.toString() === targetPeerId,
					);
				},
				[peerId],
			);
			expect(hasPeer).toBe(true);
		});

		// manually trigger some traffic from both peers
		await page.evaluate(() => (window as any).sendTestData?.());
		await anotherPage.evaluate(() => (window as any).sendTestData?.());

		// Verify peers remain connected and traffic can flow
		await waitForResolved(
			async () => {
				const isConnectedA = await page.evaluate(
					([targetPeerId]) => {
						const peers = (globalThis as any).streamClient?.services?.stream
							?.peers;
						if (!peers?.values) return false;
						return Array.from(peers.values()).some(
							(p: any) =>
								typeof p?.peerId?.toString === "function" &&
								p.peerId.toString() === targetPeerId,
						);
					},
					[anotherPeerId],
				);
				const isConnectedB = await anotherPage.evaluate(
					([targetPeerId]) => {
						const peers = (globalThis as any).streamClient?.services?.stream
							?.peers;
						if (!peers?.values) return false;
						return Array.from(peers.values()).some(
							(p: any) =>
								typeof p?.peerId?.toString === "function" &&
								p.peerId.toString() === targetPeerId,
						);
					},
					[peerId],
				);
				expect(isConnectedA).toBe(true);
				expect(isConnectedB).toBe(true);
			},
			{ timeout: 10_000, delayInterval: 200 },
		);
	});
});
