import {
	type BrowserContext,
	type TestInfo,
	chromium,
	expect,
	test,
} from "@playwright/test";

const launchPersistentBrowserContext = async (
	testInfo: TestInfo,
	baseURL: string,
) => {
	const userDataDir = testInfo.outputPath("peerbit-react-sqlite-bench");
	const context = await chromium.launchPersistentContext(userDataDir, {
		headless: !!process.env.CI,
		viewport: { width: 1280, height: 800 },
		args: ["--enable-features=FileSystemAccessAPI"],
	});

	await context.addInitScript(() => {
		Object.defineProperty(navigator, "storage", {
			value: {
				...navigator.storage,
				persist: async () => true,
				persisted: async () => true,
			},
			configurable: true,
		});
	});

	const origin = new URL(baseURL).origin;
	await context.grantPermissions(["storage-access"], { origin });

	return context;
};

test("runs the persisted sqlite worker benchmark for both protocols", async (
	_fixtures,
	testInfo,
) => {
	const baseURL =
		testInfo.project.use.baseURL?.toString() ||
		process.env.PLAYWRIGHT_BASE_URL ||
		"http://localhost:4183";
	let context: BrowserContext | undefined;

	try {
		context = await launchPersistentBrowserContext(testInfo, baseURL);
		const page = await context.newPage();
		await page.goto(
			new URL("/?sqlitebench=1&bytes=32768&count=4", baseURL).toString(),
		);

		await expect(page.getByTestId("sqlite-benchmark-status")).toHaveText(
			"ready",
			{
				timeout: 120_000,
			},
		);

		const resultsText =
			(await page.getByTestId("sqlite-benchmark-results").textContent()) ?? "";
		const results = JSON.parse(resultsText) as Array<{
			protocol: "legacy" | "clone";
			payloadBytes: number;
			count: number;
			insertMs: number;
			selectMs: number;
			profile: {
				totalRequests: number;
				byType: Record<string, { count: number }>;
			};
		}>;

		expect(results).toHaveLength(2);
		for (const result of results) {
			expect(result.payloadBytes).toBe(32768);
			expect(result.count).toBe(4);
			expect(result.insertMs).toBeGreaterThan(0);
			expect(result.selectMs).toBeGreaterThan(0);
			expect(result.profile.totalRequests).toBeGreaterThan(0);
			expect(result.profile.byType["run-statement"]?.count).toBe(4);
			expect(result.profile.byType.get?.count).toBe(4);
		}
	} finally {
		await context?.close();
	}
});
