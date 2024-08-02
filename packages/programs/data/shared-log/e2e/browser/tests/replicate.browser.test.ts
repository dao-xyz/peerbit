import { delay, waitForResolved } from "@peerbit/time";
import { expect, test } from "@playwright/test";
import { Peerbit } from "peerbit";

test.describe("stream", () => {
	let relay: Peerbit;
	test.beforeEach(async () => {
		relay = await Peerbit.create();
	});

	test.afterEach(async () => {
		await relay.stop();
	});

	test("can transmit with webrtc", async ({ page, browser }) => {
		const relayAddres = relay.getMultiaddrs()[0].toString();

		console.log("RELAY", relay.peerId.toString());
		await page.goto(
			"http://localhost:5213/?relay=" + encodeURIComponent(relayAddres),
		);
		page.addListener("console", (msg: any) => {
			console.log("#1", msg.text());
		});

		const peerIdLocator = await page.getByTestId("peer-id");
		console.log("#1", await peerIdLocator.textContent());

		const anotherPage = await browser.newPage();
		anotherPage.addListener("console", (msg: any) => {
			console.log("#2", msg.text());
		});
		await anotherPage.goto(
			"http://localhost:5213/?relay=" +
				encodeURIComponent(
					relayAddres +
						"/p2p-circuit/webrtc/p2p/" +
						(await peerIdLocator.textContent()),
				),
		);
		const peerId2Locator = await anotherPage.getByTestId("peer-id");

		console.log("#2", await peerId2Locator.textContent());

		await waitForResolved(async () => {
			const counter = await page.getByTestId("replicators");
			await expect(counter).toHaveText(String(2), { timeout: 15e3 });
		});

		await waitForResolved(async () => {
			const counter = await anotherPage.getByTestId("replicators");
			await expect(counter).toHaveText(String(2), { timeout: 15e3 });
		});

		await waitForResolved(async () => {
			const counter = await page.getByTestId("log-length");
			await expect(counter).toHaveText(String(2), { timeout: 15e3 });
		});

		await waitForResolved(async () => {
			const counter = await anotherPage.getByTestId("log-length");
			await expect(counter).toHaveText(String(2), { timeout: 15e3 });
		});

		console.log("DONE!");
	});

	test("can transmit with webrtc after restarting", async ({
		page,
		browser,
	}) => {
		const relayAddres = relay.getMultiaddrs()[0].toString();
		await page.goto(
			"http://localhost:5213/?relay=" + encodeURIComponent(relayAddres),
		);

		const peerIdLocator = await page.getByTestId("peer-id");

		const anotherPage = await browser.newPage();
		await anotherPage.goto(
			"http://localhost:5213/?relay=" +
				encodeURIComponent(
					relayAddres +
						"/p2p-circuit/webrtc/p2p/" +
						(await peerIdLocator.textContent()),
				),
		);

		await waitForResolved(async () => {
			const counter = await page.getByTestId("replicators");
			await expect(counter).toHaveText(String(2), { timeout: 15e3 });
		});

		await waitForResolved(async () => {
			const counter = await anotherPage.getByTestId("replicators");
			await expect(counter).toHaveText(String(2), { timeout: 15e3 });
		});

		await waitForResolved(async () => {
			const counter = await page.getByTestId("log-length");
			await expect(counter).toHaveText(String(2), { timeout: 15e3 });
		});

		await waitForResolved(async () => {
			const counter = await anotherPage.getByTestId("log-length");
			await expect(counter).toHaveText(String(2), { timeout: 15e3 });
		});

		// reload one page to simulate a restart
		await anotherPage.reload();

		await waitForResolved(async () => {
			const counter = await anotherPage.getByTestId("replicators");
			await expect(counter).toHaveText(String(2), { timeout: 15e3 });
		});

		await waitForResolved(async () => {
			const counter = await page.getByTestId("replicators");
			await expect(counter).toHaveText(String(2), { timeout: 15e3 });
		});

		await waitForResolved(async () => {
			const counter = await page.getByTestId("log-length");
			await expect(counter).toHaveText(String(3), { timeout: 15e3 });
		});

		await waitForResolved(async () => {
			const counter = await anotherPage.getByTestId("log-length");
			await expect(counter).toHaveText(String(3), { timeout: 15e3 });
		});
	});
});
