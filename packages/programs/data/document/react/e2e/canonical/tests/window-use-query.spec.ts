import { expect, test } from "@playwright/test";

const sessionParam = (value: string) => encodeURIComponent(value);

const getMessages = async (frame: { locator: any }) => {
	return frame.locator('[data-testid="messages"] li').allTextContents();
};

test.describe("document react canonical window transport", () => {
	test("parent hosts canonical and iframes useQuery", async ({
		page,
	}, testInfo) => {
		const session = sessionParam(`documents-react-window-${testInfo.title}`);
		const channel = sessionParam(`window-channel-${testInfo.title}`);

		await page.goto(
			`/?transport=window&role=parent&channel=${channel}&session=${session}`,
		);
		await expect(page.getByTestId("status")).toHaveText("ready", {
			timeout: 20_000,
		});

		await page.evaluate(
			({ session, channel }) => {
				const makeFrame = (label: string, testId: string) => {
					const iframe = document.createElement("iframe");
					iframe.src = `/?label=${label}&session=${session}&transport=window&role=child&channel=${channel}`;
					iframe.setAttribute("data-testid", testId);
					iframe.setAttribute("name", testId);
					iframe.style.width = "640px";
					iframe.style.height = "480px";
					document.body.appendChild(iframe);
				};
				makeFrame("iframe-1", "iframe-1");
				makeFrame("iframe-2", "iframe-2");
			},
			{ session, channel },
		);

		const frame1Locator = page.frameLocator('iframe[data-testid="iframe-1"]');
		const frame2Locator = page.frameLocator('iframe[data-testid="iframe-2"]');

		await expect(frame1Locator.getByTestId("status")).toHaveText("ready", {
			timeout: 20_000,
		});
		await expect(frame2Locator.getByTestId("status")).toHaveText("ready", {
			timeout: 20_000,
		});

		const peerId1 = (
			await frame1Locator.getByTestId("peer-id").textContent()
		)?.trim();
		const peerId2 = (
			await frame2Locator.getByTestId("peer-id").textContent()
		)?.trim();
		expect(peerId1).toBeTruthy();
		expect(peerId1).toEqual(peerId2);

		await frame1Locator.getByTestId("message-input").fill("hello");
		await frame1Locator.getByTestId("send-button").click();

		await expect
			.poll(async () => getMessages(frame2Locator as any), { timeout: 20_000 })
			.toContain("iframe-1: hello");

		const frame1 = page.frame({ name: "iframe-1" });
		const frame2 = page.frame({ name: "iframe-2" });
		await frame1?.evaluate(() => (window as any).__canonicalTest.close());
		await frame2?.evaluate(() => (window as any).__canonicalTest.close());
		await page.evaluate(() => (window as any).__canonicalTest.close());
	});
});
