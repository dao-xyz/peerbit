import { expect, test } from "@playwright/test";

const sessionParam = (value: string) => encodeURIComponent(value);

test.describe("shared-log events", () => {
	test("propagates host-emitted events to the proxy EventTarget", async ({
		page,
	}, testInfo) => {
		const session = sessionParam(`shared-log-events-${testInfo.title}`);
		await page.goto(`/?scenario=shared-log&label=tab-1&session=${session}`);
		await expect(page.getByTestId("status")).toHaveText("ready", {
			timeout: 20_000,
		});

		const result = await page.evaluate(async () => {
			const api = (window as any).__canonicalTest;
			const wait = api.waitForEvent("replicator:join", 5_000);
			await api.emitHostEvent("replicator:join");
			return await wait;
		});

		expect(result?.type).toBe("replicator:join");
		expect(typeof result?.publicKeyHash).toBe("string");
		expect(result.publicKeyHash.length).toBeGreaterThan(0);
	});
});
