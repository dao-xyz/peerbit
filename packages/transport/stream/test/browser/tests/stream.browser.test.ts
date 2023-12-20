import { test, expect } from "@playwright/test";
import { createLibp2p } from "libp2p";
import { webSockets } from "@libp2p/websockets";
import { circuitRelayServer } from "@libp2p/circuit-relay-v2";
import { delay, waitForResolved } from "@peerbit/time";
import { noise } from "@dao-xyz/libp2p-noise";
import { yamux } from "@chainsafe/libp2p-yamux";
import { identify } from "@libp2p/identify";
import { all } from "@libp2p/websockets/filters";
import { TestDirectStream } from "../shared/utils";

test.describe("stream", () => {
	let relay: Awaited<ReturnType<typeof createLibp2p>>;
	test.beforeEach(async () => {
		relay = await createLibp2p<{
			relay: any;
			identify: any;
			stream: TestDirectStream;
		}>({
			addresses: {
				listen: ["/ip4/127.0.0.1/tcp/0/ws"]
			},
			services: {
				relay: circuitRelayServer({ reservations: { maxReservations: 1000 } }),
				identify: identify(),
				stream: (c) => new TestDirectStream(c)
			},
			transports: [webSockets({ filter: all })],
			streamMuxers: [yamux()],
			connectionEncryption: [noise()]
		});
	});

	test.afterEach(async () => {
		await relay.stop();
	});

	test("can transmit with webrtc", async ({ page, browser }) => {
		const relayAddres = relay.getMultiaddrs()[0].toString();
		await page.goto(
			"http://localhost:5211/?relay=" + encodeURIComponent(relayAddres)
		);

		const peerIdLocator = await page.getByTestId("peer-id");

		const anotherPage = await browser.newPage();
		await anotherPage.goto(
			"http://localhost:5211/?relay=" +
				encodeURIComponent(
					relayAddres +
						"/p2p-circuit/webrtc/p2p/" +
						(await peerIdLocator.textContent())
				)
		);

		await waitForResolved(async () => {
			const counter = await page.getByTestId("peer-counter");
			await expect(counter).toHaveText(String(2), { timeout: 15e3 });
		});

		await waitForResolved(async () => {
			const counter = await anotherPage.getByTestId("peer-counter");
			await expect(counter).toHaveText(String(2), { timeout: 15e3 });
		});

		// Shut down the relay to make sure traffic works over webrtc

		await relay.stop();

		const byteCounterA = page.getByTestId("received-data");
		const byteCounterB = page.getByTestId("received-data");

		const byteCounterA1 = await byteCounterA.textContent();
		const byteCounterB1 = await byteCounterB.textContent();
		await delay(3000);

		const byteCounterA2 = await byteCounterA.textContent();
		const byteCounterB2 = await byteCounterB.textContent();

		expect(Number(byteCounterA1)).toBeLessThan(Number(byteCounterA2));
		expect(Number(byteCounterB1)).toBeLessThan(Number(byteCounterB2));
	});
});
