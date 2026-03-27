import { expect, test } from "@playwright/test";
import { Peerbit } from "peerbit";

let bootstrap: Peerbit;
let bootstrapAddr: string;
let baseURL: string;

test.describe("relay addresses", () => {
	test.beforeEach(async (_args, testInfo) => {
		baseURL =
			testInfo.project.use.baseURL?.toString() ||
			process.env.PLAYWRIGHT_BASE_URL ||
			"http://localhost:4183";

		bootstrapAddr = process.env.PEERBIT_TEST_BOOTSTRAP_ADDR ?? "";
		if (!bootstrapAddr) {
			bootstrap = await Peerbit.create();
			bootstrapAddr = bootstrap.getMultiaddrs()[0]?.toString() ?? "";
			if (!bootstrapAddr) {
				throw new Error("Bootstrap peer has no address");
			}
		}
	});

	test.afterEach(async () => {
		await bootstrap?.stop();
		bootstrap = undefined as never;
	});

	test("node runtime browser client exposes relayed addresses after bootstrap", async ({
		page,
	}) => {
		const target = new URL(
			`/?bootstrap=${encodeURIComponent(bootstrapAddr)}`,
			baseURL,
		).toString();

		await page.goto(target);

		await expect(page.getByTestId("status")).toContainText("connected");
		await expect(page.getByTestId("connections")).toContainText("/ws");

		await expect
			.poll(
				async () => {
					return await page.getByTestId("multiaddrs").textContent();
				},
				{
					timeout: 30_000,
					message:
						"expected browser peer to expose a relayed address after bootstrapping",
				},
			)
			.toContain("/p2p-circuit");
	});
});
