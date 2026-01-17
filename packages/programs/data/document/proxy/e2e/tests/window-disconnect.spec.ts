import { expect, test } from "@playwright/test";

const sessionParam = (value: string) => encodeURIComponent(value);

test.describe("canonical window disconnects", () => {
	test("fails RPC calls after host disconnects clients", async ({
		page,
	}, testInfo) => {
		const session = sessionParam(`window-disconnect-${testInfo.title}`);
		const channel = sessionParam(`window-channel-${testInfo.title}`);

		await page.goto(
			`/?scenario=host&transport=window&role=parent&channel=${channel}&session=${session}`,
		);
		await expect(page.getByTestId("status")).toHaveText("ready", {
			timeout: 20_000,
		});

		const popupPromise = page.waitForEvent("popup");
		await page.evaluate(
			({ session, channel }) => {
				window.open(
					`/?scenario=documents&label=popup&session=${session}&transport=window&role=child&channel=${channel}`,
					"canonical-popup",
				);
			},
			{ session, channel },
		);
		const child = await popupPromise;
		await expect(child.getByTestId("status")).toHaveText("ready", {
			timeout: 20_000,
		});

		await page.evaluate(() => (window as any).__canonicalTest.close());
		await child.waitForTimeout(100);

		const result = await child.evaluate(async () => {
			try {
				await (window as any).__canonicalTest.put("after-disconnect");
				return { ok: true, error: "" };
			} catch (e: any) {
				return { ok: false, error: String(e?.message ?? e) };
			}
		});
		expect(result.ok).toBe(false);
		expect(result.error).toMatch(/rpc transport closed|closed/i);

		await child.close();
	});

	test("fails RPC calls after abrupt host close (no goodbyes)", async ({
		page,
	}, testInfo) => {
		const session = sessionParam(`window-abrupt-${testInfo.title}`);
		const channel = sessionParam(`window-abrupt-channel-${testInfo.title}`);

		const host = await page.context().newPage();
		await host.goto(
			`/?scenario=host&transport=window&role=parent&channel=${channel}&session=${session}`,
		);
		await expect(host.getByTestId("status")).toHaveText("ready", {
			timeout: 20_000,
		});

		const popupPromise = host.waitForEvent("popup");
		await host.evaluate(
			({ session, channel }) => {
				window.open(
					`/?scenario=documents&label=popup&session=${session}&transport=window&role=child&channel=${channel}&keepAliveIntervalMs=250&keepAliveTimeoutMs=500&keepAliveFailures=1`,
					"canonical-popup",
				);
			},
			{ session, channel },
		);
		const child = await popupPromise;
		await expect(child.getByTestId("status")).toHaveText("ready", {
			timeout: 20_000,
		});

		await host.close();

		const result = await child.evaluate(async () => {
			try {
				await Promise.race([
					(window as any).__canonicalTest.put("after-host-close"),
					new Promise((_resolve, reject) =>
						setTimeout(() => reject(new Error("Timed out")), 3_000),
					),
				]);
				return { ok: true, error: "" };
			} catch (e: any) {
				return { ok: false, error: String(e?.message ?? e) };
			}
		});
		expect(result.ok).toBe(false);
		expect(result.error).toMatch(/rpc transport closed|timeout|closed/i);

		await child.close();
	});
});
