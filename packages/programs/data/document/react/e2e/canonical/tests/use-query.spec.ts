import { expect, test } from "@playwright/test";

const transports = [
	{ name: "sharedworker", query: "" },
	{ name: "serviceworker", query: "&transport=serviceworker" },
];

const sessionParam = (value: string) => encodeURIComponent(value);

const getMessages = async (page: { $$eval: any }) => {
	return page.$$eval('[data-testid="messages"] li', (elements: Element[]) =>
		elements.map((el) => el.textContent?.trim() || ""),
	);
};

const sendMessage = async (page: any, text: string) => {
	await page.getByTestId("message-input").fill(text);
	await expect(page.getByTestId("send-button")).toBeEnabled({
		timeout: 20_000,
	});
	await page.getByTestId("send-button").click();
};

for (const transport of transports) {
	test.describe(`document react canonical (${transport.name})`, () => {
		test("useQuery updates across tabs", async ({ page }, testInfo) => {
			const session = sessionParam(
				`documents-react-${transport.name}-${testInfo.title}`,
			);
			await page.goto(`/?label=tab-1&session=${session}${transport.query}`);
			await expect(page.getByTestId("status")).toHaveText("ready", {
				timeout: 20_000,
			});

			const page2 = await page.context().newPage();
			await page2.goto(`/?label=tab-2&session=${session}${transport.query}`);
			await expect(page2.getByTestId("status")).toHaveText("ready", {
				timeout: 20_000,
			});

			const peerId1 = (await page.getByTestId("peer-id").textContent())?.trim();
			const peerId2 = (
				await page2.getByTestId("peer-id").textContent()
			)?.trim();
			expect(peerId1).toBeTruthy();
			expect(peerId1).toEqual(peerId2);

			await sendMessage(page, "hello");

			await expect
				.poll(async () => getMessages(page2 as any), { timeout: 20_000 })
				.toContain("tab-1: hello");

			await sendMessage(page2, "world");

			await expect
				.poll(async () => getMessages(page as any), { timeout: 20_000 })
				.toContain("tab-2: world");

			await page2.evaluate(() => (window as any).__canonicalTest.close());
			await page2.close();

			await page.evaluate(() => (window as any).__canonicalTest.close());
		});
	});
}
