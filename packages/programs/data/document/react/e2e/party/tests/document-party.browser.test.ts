import { expect, test, type BrowserContext, type Page } from "@playwright/test";
import { startReplicator } from "./support/replicator.js";
import { launchBrowserContext, withSearchParams } from "./support/browser.js";
import { Peerbit } from "peerbit";

const PARTICIPANTS = [
	{ label: "replica-a", replicate: true },
	{ label: "observer-b", replicate: false },
	{ label: "replica-c", replicate: true },
];

const getMessages = async (page: Page) => {
	return page.$$eval('[data-testid="messages"] li', (elements) =>
		elements.map((el) => ({
			text: el.textContent?.trim() || "",
			timestamp: Number(el.getAttribute("data-timestamp")),
		}))
	);
};

const attachLogging = (page: Page, label: string) => {
	page.on("console", (msg) => {
		console.log(`[${label}]`, msg.type(), msg.text());
	});
	page.on("pageerror", (err) => {
		console.error(`[${label}] pageerror`, err);
	});
};

test.describe("document react party", () => {
	let bootstrap: string[] = [];
	let stopReplicator: (() => Promise<void>) | undefined;

	test.beforeAll(async () => {
		const replicator = await startReplicator();
		bootstrap = replicator.addresses;
		stopReplicator = replicator.stop;
	});

	test.afterAll(async () => {
		await stopReplicator?.();
	});


	test("it will observe self messages when not replicating", async ({ page }, testInfo) => {
		const baseURL = (testInfo.project.use.baseURL as string | undefined) ??
			"http://localhost:5255";

		attachLogging(page, "observer-self");

		const url = withSearchParams(baseURL, {
			label: "observer-self",
			replicate: "false",
			bootstrap: bootstrap.join(","),
		});
		await page.goto(url);

		const messageInput = page.getByTestId("message-input");
		const sendButton = page.getByTestId("send-button");
		await messageInput.fill("Hello, self!");
		await sendButton.click();

		await expect(page.getByTestId("message-count")).toHaveText("1", {
			timeout: 10_000,
		});

		const messages = await getMessages(page);
		expect(messages).toHaveLength(1);
		expect(messages[0].text).toBe("Hello, self!");

		// reload and ensure message gets queried again
		await page.reload();

		await expect(page.getByTestId("message-count")).toHaveText("1", {
			timeout: 10_000,
		});

		const messagesAfterReload = await getMessages(page);
		expect(messagesAfterReload).toHaveLength(1);
		expect(messagesAfterReload[0].text).toBe("Hello, self!");
	});
	

	test("all peers observe the sorted message log", async ({ page }, testInfo) => {
		const baseURL = (testInfo.project.use.baseURL as string | undefined) ??
			"http://localhost:5255";

		const pages = [page];
		const extraContexts:BrowserContext[] = [];

		const dummyClient = await Peerbit.create();
		for (const addr of bootstrap) {
			await dummyClient.dial(addr);
		}

		for (let i = 0; i < PARTICIPANTS.length; i++) {
			let currentPage = i === 0 ? page : undefined;
			if (!currentPage) {
				const ctx = await launchBrowserContext(testInfo, {});
				extraContexts.push(ctx);
				currentPage = await ctx.newPage();
				pages.push(currentPage);
			}
			attachLogging(currentPage, PARTICIPANTS[i].label);

			const url = withSearchParams(baseURL, {
				label: PARTICIPANTS[i].label,
				replicate: PARTICIPANTS[i].replicate,
				bootstrap: bootstrap.join(","),
			});
			await currentPage.goto(url);
		}

		const expectedCount = PARTICIPANTS.length;
		for (const pg of pages) {
			await expect(pg.getByTestId("message-count")).toHaveText(
				String(expectedCount),
				{ timeout: 60_000 }
			);
		}

		const reference = await getMessages(pages[0]);
		expect(reference).toHaveLength(expectedCount);
		const referenceTexts = reference.map((msg) => msg.text);
		expect([...reference.map((m) => m.timestamp)].sort((a, b) => a - b)).toEqual(
			reference.map((m) => m.timestamp)
		);

		for (const pg of pages) {
			const snapshot = await getMessages(pg);
			expect(snapshot.map((s) => s.text)).toEqual(referenceTexts);
			expect(snapshot.map((s) => s.timestamp)).toEqual(
				[...snapshot.map((s) => s.timestamp)].sort((a, b) => a - b)
			);
		}

		await Promise.all(extraContexts.map((ctx) => ctx.close()));
	});
});
