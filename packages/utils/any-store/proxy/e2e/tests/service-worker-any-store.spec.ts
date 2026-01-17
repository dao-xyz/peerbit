import { expect, test } from "@playwright/test";

const sessionParam = (value: string) => encodeURIComponent(value);

test.describe("any-store canonical service worker", () => {
	test("shares AnyStore across tabs", async ({ page }, testInfo) => {
		const session = sessionParam(`any-store-${testInfo.title}`);
		await page.goto(
			`/?scenario=any-store&session=${session}&transport=serviceworker`,
		);
		await expect(page.getByTestId("status")).toHaveText("ready", {
			timeout: 20_000,
		});

		const page2 = await page.context().newPage();
		await page2.goto(
			`/?scenario=any-store&session=${session}&transport=serviceworker`,
		);
		await expect(page2.getByTestId("status")).toHaveText("ready", {
			timeout: 20_000,
		});

		const peerId1 = (await page.getByTestId("peer-id").textContent())?.trim();
		const peerId2 = (await page2.getByTestId("peer-id").textContent())?.trim();
		expect(peerId1).toBeTruthy();
		expect(peerId1).toEqual(peerId2);

		await page.evaluate(() =>
			(window as any).__canonicalTest.put("a", "hello"),
		);

		await expect
			.poll(
				() => page2.evaluate(() => (window as any).__canonicalTest.get("a")),
				{ timeout: 20_000 },
			)
			.toBe("hello");

		await page.evaluate(() =>
			(window as any).__canonicalTest.subPut("sub", "b", "world"),
		);

		await expect
			.poll(
				() =>
					page2.evaluate(() =>
						(window as any).__canonicalTest.subGet("sub", "b"),
					),
				{ timeout: 20_000 },
			)
			.toBe("world");

		const keys = await page2.evaluate(() =>
			(window as any).__canonicalTest.listKeys(),
		);
		expect(keys).toContain("a");

		await page2.close();
	});
});
