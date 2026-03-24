import { noise } from "@chainsafe/libp2p-noise";
import { yamux } from "@chainsafe/libp2p-yamux";
import { circuitRelayServer } from "@libp2p/circuit-relay-v2";
import { identify } from "@libp2p/identify";
import { webSockets } from "@libp2p/websockets";
import { waitForResolved } from "@peerbit/time";
import { expect, test, type Page } from "@playwright/test";
import { createLibp2p } from "libp2p";
import { TestDirectStream } from "../shared/utils.js";

type RTCSnapshot = {
	trackedCount: number;
	totalTransportBytesSent: number;
	totalTransportBytesReceived: number;
	totalDataChannelBytesSent: number;
	totalDataChannelBytesReceived: number;
};

const installRtcStatsProbe = async (page: Page) => {
	await page.addInitScript(() => {
		const root = globalThis as typeof globalThis & Record<string, any>;
		const NativeRTCPeerConnection = root.RTCPeerConnection;
		if (!NativeRTCPeerConnection || root.__peerbitRtcStats) {
			return;
		}

		const tracked: Array<{ id: number; createdAt: number; pc: RTCPeerConnection }> =
			[];

		const summarizePeerConnection = async (record: {
			id: number;
			createdAt: number;
			pc: RTCPeerConnection;
		}) => {
			try {
				const report = await record.pc.getStats();
				const stats = Array.from(report.values());
				const transports = stats.filter((stat: any) => stat.type === "transport");
				const dataChannels = stats.filter(
					(stat: any) => stat.type === "data-channel",
				);

				return {
					id: record.id,
					createdAt: record.createdAt,
					connectionState: record.pc.connectionState,
					iceConnectionState: record.pc.iceConnectionState,
					signalingState: record.pc.signalingState,
					totalTransportBytesSent: transports.reduce(
						(sum: number, stat: any) => sum + (stat.bytesSent ?? 0),
						0,
					),
					totalTransportBytesReceived: transports.reduce(
						(sum: number, stat: any) => sum + (stat.bytesReceived ?? 0),
						0,
					),
					totalDataChannelBytesSent: dataChannels.reduce(
						(sum: number, stat: any) => sum + (stat.bytesSent ?? 0),
						0,
					),
					totalDataChannelBytesReceived: dataChannels.reduce(
						(sum: number, stat: any) => sum + (stat.bytesReceived ?? 0),
						0,
					),
				};
			} catch (error) {
				return {
					id: record.id,
					createdAt: record.createdAt,
					connectionState: record.pc.connectionState,
					iceConnectionState: record.pc.iceConnectionState,
					signalingState: record.pc.signalingState,
					totalTransportBytesSent: 0,
					totalTransportBytesReceived: 0,
					totalDataChannelBytesSent: 0,
					totalDataChannelBytesReceived: 0,
					error: String((error as Error)?.message ?? error),
				};
			}
		};

		const snapshot = async () => {
			const pcs = await Promise.all(tracked.map(summarizePeerConnection));
			return {
				trackedCount: tracked.length,
				totalTransportBytesSent: pcs.reduce(
					(sum, pc) => sum + (pc.totalTransportBytesSent ?? 0),
					0,
				),
				totalTransportBytesReceived: pcs.reduce(
					(sum, pc) => sum + (pc.totalTransportBytesReceived ?? 0),
					0,
				),
				totalDataChannelBytesSent: pcs.reduce(
					(sum, pc) => sum + (pc.totalDataChannelBytesSent ?? 0),
					0,
				),
				totalDataChannelBytesReceived: pcs.reduce(
					(sum, pc) => sum + (pc.totalDataChannelBytesReceived ?? 0),
					0,
				),
				pcs,
			};
		};

		const TrackedRTCPeerConnection = function (...args: any[]) {
			const pc = new NativeRTCPeerConnection(...args);
			tracked.push({ id: tracked.length + 1, createdAt: Date.now(), pc });
			return pc;
		} as unknown as typeof RTCPeerConnection;

		TrackedRTCPeerConnection.prototype = NativeRTCPeerConnection.prototype;
		Object.setPrototypeOf(TrackedRTCPeerConnection, NativeRTCPeerConnection);

		root.RTCPeerConnection = TrackedRTCPeerConnection;
		root.__peerbitRtcStats = { snapshot };
	});
};

const getRtcStats = async (page: Page): Promise<RTCSnapshot> => {
	return await page.evaluate(async () => {
		return await (globalThis as any).__peerbitRtcStats.snapshot();
	});
};

const sendPayload = async (page: Page, totalBytes: number) => {
	await page.evaluate(async ([payloadBytes]) => {
		await (globalThis as any).sendPayload?.(payloadBytes);
	}, [totalBytes]);
};

const getReceivedData = async (page: Page): Promise<number> => {
	return await page.evaluate(() => {
		return Number((globalThis as any).getReceivedData?.() ?? 0);
	});
};

const buildPageUrl = (relayAddress: string) => {
	const url = new URL("http://localhost:5211/");
	url.searchParams.set("relay", relayAddress);
	url.searchParams.set("autopublish", "0");
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

const connectPeers = async ({
	page,
	browser,
	relayAddress,
	relayPeerId,
}: {
	page: Page;
	browser: any;
	relayAddress: string;
	relayPeerId: string;
}) => {
	await installRtcStatsProbe(page);
	await page.goto(buildPageUrl(relayAddress));

	const peerId = (await page.getByTestId("peer-id").textContent())?.trim();
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
	await installRtcStatsProbe(anotherPage);
	await anotherPage.goto(buildPageUrl(dialableAddress));

	const anotherPeerId = (await anotherPage.getByTestId("peer-id").textContent())
		?.trim();
	if (!anotherPeerId) {
		throw new Error("Missing peer id for other peer");
	}

	await waitForResolved(async () => {
		expect(await hasPeer(page, anotherPeerId)).toBe(true);
	});

	await waitForResolved(async () => {
		expect(await hasPeer(anotherPage, peerId)).toBe(true);
	});

	return { anotherPage, peerId, anotherPeerId };
};

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

	test("can transmit payload over webrtc", async ({ page, browser }) => {
		// WebRTC + relay reservation + circuit addressing can be slow/flaky on CI,
		// so give this end-to-end test a larger budget than Playwright's 30s default.
		test.setTimeout(120_000);

		const relayAddres = relay.getMultiaddrs()[0].toString();
		const relayPeerId = relayAddres.split("/p2p/").at(-1);
		if (!relayPeerId) {
			throw new Error(`Unable to parse relay peer id from ${relayAddres}`);
		}
		const { anotherPage, peerId, anotherPeerId } = await connectPeers({
			page,
			browser,
			relayAddress: relayAddres,
			relayPeerId,
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

		const payloadBytes = 512 * 1024;
		const beforeA = await getRtcStats(page);
		const beforeB = await getRtcStats(anotherPage);
		await Promise.all([
			sendPayload(page, payloadBytes),
			sendPayload(anotherPage, payloadBytes),
		]);

		// Verify peers remain connected and traffic can flow
		await waitForResolved(
			async () => {
				expect(await hasPeer(page, anotherPeerId)).toBe(true);
				expect(await hasPeer(anotherPage, peerId)).toBe(true);
			},
			{ timeout: 10_000, delayInterval: 200 },
		);

		await waitForResolved(
			async () => {
				const afterA = await getRtcStats(page);
				const afterB = await getRtcStats(anotherPage);
				expect(afterA.trackedCount).toBeGreaterThan(0);
				expect(afterB.trackedCount).toBeGreaterThan(0);

				expect(
					afterA.totalDataChannelBytesSent - beforeA.totalDataChannelBytesSent,
				).toBeGreaterThan(payloadBytes / 2);
				expect(
					afterA.totalDataChannelBytesReceived -
						beforeA.totalDataChannelBytesReceived,
				).toBeGreaterThan(payloadBytes / 2);
				expect(
					afterB.totalDataChannelBytesSent - beforeB.totalDataChannelBytesSent,
				).toBeGreaterThan(payloadBytes / 2);
				expect(
					afterB.totalDataChannelBytesReceived -
						beforeB.totalDataChannelBytesReceived,
				).toBeGreaterThan(payloadBytes / 2);
			},
			{ timeout: 30_000, delayInterval: 200 },
		);
	});

});
