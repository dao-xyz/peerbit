import { waitForResolved } from "@peerbit/time";
import {
	type BrowserContext,
	type Page,
	type TestInfo,
	expect,
	test,
} from "@playwright/test";
import type { Peerbit } from "peerbit";
import { launchBrowserContext, withSearchParams } from "./support/browser.js";
import { type RunningRelay, startRelay } from "./support/replicator.js";

const attachLogging = (page: Page, label: string) => {
	page.on("console", (msg) => {
		console.log(`[${label}]`, msg.type(), msg.text());
	});
	page.on("pageerror", (err) => {
		console.error(`[${label}] pageerror`, err);
	});
};

const getMessages = async (page: Page) => {
	return page.$$eval('[data-testid="messages"] li', (elements) =>
		elements.map((el) => ({
			text: el.textContent?.trim() || "",
			timestamp: Number(el.getAttribute("data-timestamp")),
		})),
	);
};

const assertMessages = async (page: Page, expectedMessages: string[]) => {
	const messages = await getMessages(page);
	expect(messages.map((m) => m.text)).toEqual(expectedMessages);
};

const waitForMessages = async (
	page: Page,
	expectedMessages: string[],
	options?: { ignoreOrder: boolean },
) => {
	await expect
		.poll(
			async () => {
				const messages = await getMessages(page);
				if (messages.length != expectedMessages.length) {
					return false;
				}
				if (options?.ignoreOrder) {
					const texts = messages.map((m) => m.text);
					for (const expectedMessage of expectedMessages) {
						if (!texts.includes(expectedMessage)) {
							return false;
						}
					}
					return true;
				}

				for (let i = 0; i < expectedMessages.length; i++) {
					if (messages[i]?.text !== expectedMessages[i]) {
						return false;
					}
				}
				return true;
			},
			{
				timeout: 20_000,
			},
		)
		.toBe(true);
};

const assertReplicating = async (page: Page, expected: boolean) => {
	await expect(page.getByTestId("replicate-status")).toHaveText(
		`Replicating: ${expected ? "yes" : "no"}`,
		{
			timeout: 10_000,
		},
	);
};

const sendMessage = async (page: Page, string: string) => {
	const messageInput = page.getByTestId("message-input");
	await messageInput.fill(string);

	const sendButton = page.getByTestId("send-button");
	// wait for send button to be enabled
	await expect(sendButton).toBeEnabled({ timeout: 20_000 });

	await sendButton.click();
};

test.describe("document react party", () => {
	let bootstrap: string[] = [];
	let relay: RunningRelay;

	const resetRelay = async () => {
		if (relay) {
			await relay.peer.stop();
		}
		relay = undefined;
		bootstrap = [];
	};

	const relayAsReplicator = async () => {
		await resetRelay();
		relay = await startRelay({ replicate: true });
	};

	const relayAsObserver = async () => {
		await resetRelay();
		relay = await startRelay({ replicate: false });
	};

	const relayAsBootstrap = async () => {
		await resetRelay();
		relay = await startRelay(false);
	};

	test.beforeAll(async () => {
		relay = await startRelay();

		const addresses = relay.peer
			.getMultiaddrs()
			.map((addr) => addr.toString())
			.filter((addr) => addr.includes("/ws"));

		bootstrap = addresses;
	});

	test.afterAll(async () => {
		await relay?.peer.stop();
	});

	const spawnWithConfig = async (
		testInfo: TestInfo,
		page: Page,
		participants: {
			label: string;
			replicate: boolean;
			push?: boolean;
			write: string[];
		}[],
	) => {
		const baseURL = testInfo.project.use.baseURL;
		const pages: Page[] = [page];
		const contexts: BrowserContext[] = [];
		for (let i = 0; i < participants.length; i++) {
			let currentPage = i === 0 ? page : undefined;
			if (!currentPage) {
				const ctx = await launchBrowserContext(testInfo, {});
				contexts.push(ctx);
				currentPage = await ctx.newPage();
				pages.push(currentPage);
			}
			attachLogging(currentPage, participants[i].label);

			const url = withSearchParams(baseURL, {
				label: participants[i].label,
				replicate: participants[i].replicate,
				bootstrap: bootstrap.join(","),
				write: participants[i].write.join(","),
				push: participants[i].push,
			});
			await currentPage.goto(url);
			await waitForConnected(currentPage);
		}

		return { pages, contexts };
	};

	const waitForConnected = async (
		page: Page,
		expectedAddresses: string[] = bootstrap,
	) => {
		await expect(page.getByTestId("connection-status")).toHaveText(
			"connected",
			{
				timeout: 20_000,
			},
		);

		await expect
			.poll(
				async () => {
					const out = await page.evaluate(() => {
						const peerbit: Peerbit = (window as any).peerbit;
						if (!peerbit) {
							return undefined;
						}
						return peerbit.libp2p
							.getConnections()
							.map((a) => a.remoteAddr.toString())
							.sort();
					});
					const allAddresses = out;
					const compareAddresses = expectedAddresses.sort();
					return (
						JSON.stringify(allAddresses) === JSON.stringify(compareAddresses)
					);
				},
				{
					timeout: 10_000,
				},
			)
			.toBeTruthy();
	};

	const connect = async (page: Page, addresses: string[] = bootstrap) => {
		await page.waitForFunction(() => (window as any).peerbit !== undefined);

		return page.evaluate(async (args) => {
			// access the peerbit client from the window
			const peerbit: Peerbit = (window as any).peerbit;
			if (!peerbit) {
				throw new Error("Peerbit client not found on window");
			}
			// connect to the replicator bootstrap address
			await peerbit.dial(args[0]);
		}, addresses);
	};

	test("observer write and reload", async ({ page }, testInfo) => {
		const baseURL = testInfo.project.use.baseURL;

		const url = withSearchParams(baseURL, {
			label: "observer-self",
			replicate: "false",
			bootstrap: bootstrap.join(","),
		});
		await page.goto(url);

		// expect not replicating
		await assertReplicating(page, false);
		await waitForConnected(page);

		const message = "Hello, self!";
		await sendMessage(page, message);
		await assertMessages(page, [message]);

		// reload and ensure message gets queried again
		await page.reload();
		await waitForMessages(page, [message]);
	});

	test("observer write before connect and reload", async ({
		page,
	}, testInfo) => {
		const baseURL = testInfo.project.use.baseURL;
		attachLogging(page, "observer-self");

		const url = withSearchParams(baseURL, {
			label: "observer-self",
			replicate: "false",
			bootstrap: undefined,
		});
		await page.goto(url);

		// expect not replicating
		await assertReplicating(page, false);

		const message = "Hello, self!";
		await sendMessage(page, message);
		await assertMessages(page, [message]);

		await connect(page, bootstrap);

		await waitForConnected(page, bootstrap);

		// reload and ensure message gets queried again
		await page.reload();
		await connect(page, bootstrap);
		await waitForMessages(page, [message]);
	});

	test("observers write and join", async ({ page }, testInfo) => {
		const { pages } = await spawnWithConfig(testInfo, page, [
			{ label: "observer-a", replicate: false, write: ["a"] },
			{ label: "observer-b", replicate: false, write: ["b"] },
		]);

		await page.waitForTimeout(3e4);
		for (const page of pages) {
			await waitForMessages(page, ["a", "b"], { ignoreOrder: true });
		}
	});

	test.describe("join", () => {
		test("it does not query observers on join", async ({ page }, testInfo) => {
			await relayAsObserver();
			const { pages } = await spawnWithConfig(testInfo, page, [
				{ label: "observer-a", replicate: false, write: ["a"] },
				{ label: "observer-b", replicate: false, write: ["b"] },
			]);

			await page.waitForTimeout(5e3);
			await assertMessages(pages[0], ["a"]);
			await assertMessages(pages[1], ["b"]);
		});
	});

	test.describe("push", () => {
		test("false means no push events", async ({ page }, testInfo) => {
			const { pages } = await spawnWithConfig(testInfo, page, [
				{ label: "observer-a", replicate: false, write: [] },
				{ label: "observer-b", replicate: false, write: [], push: false },
			]);

			await sendMessage(pages[0], "a");

			// wait for 23 seconds
			// assert that the replicator has the message
			await waitForResolved(
				() => expect(relay.store.documents.log.log).toHaveLength(1),
				{
					timeout: 5e3, // should be almost since nodes are already connected
				},
			);

			// now assert that observer-b does not have the message
			await page.waitForTimeout(5e3);

			await assertMessages(pages[1], []);
		});

		test("true means push", async ({ page }, testInfo) => {
			page.setDefaultTimeout(6e4);
			const { pages } = await spawnWithConfig(testInfo, page, [
				{ label: "observer-a", replicate: false, write: [] },
				{ label: "observer-b", replicate: false, write: [], push: true },
			]);

			await page.waitForTimeout(5e3);

			await sendMessage(page, "a");

			// assert that the replicator has the message
			await waitForResolved(
				() => expect(relay.store.documents.log.log).toHaveLength(1),
				{
					timeout: 5e3, // should be almost since nodes are already connected
				},
			);

			// also assert at this moment the replicator also have the block

			const head = await relay.store.documents.log.log.getHeads().all();
			expect(head.length).toBe(1);
			expect(await relay.store.documents.log.log.blocks.has(head[0].hash)).toBe(
				true,
			);

			for (const page of pages) {
				await waitForMessages(page, ["a"]);
			}
		});
	});
});
