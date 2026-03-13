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
	const userDataDir = testInfo.outputPath("peerbit-react-doc-bench");
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

test("runs the document write benchmark", async ({ page }) => {
	await page.goto("/?docbench=1&inmemory=1&bytes=32768&count=4");

	await expect(page.getByTestId("document-benchmark-status")).toHaveText(
		"ready",
		{
			timeout: 120_000,
		},
	);

	const resultsText =
		(await page.getByTestId("document-benchmark-results").textContent()) ?? "";
	const result = JSON.parse(resultsText) as {
		payloadBytes: number;
		count: number;
		inMemory: boolean;
		persisted: boolean | undefined;
		serializeMs: number;
		blockPutMs: number;
		documentPutMs: number;
	};

	expect(result.payloadBytes).toBe(32768);
	expect(result.count).toBe(4);
	expect(result.inMemory).toBe(true);
	expect(result.serializeMs).toBeGreaterThanOrEqual(0);
	expect(result.blockPutMs).toBeGreaterThan(0);
	expect(result.documentPutMs).toBeGreaterThan(0);
});

test("runs the document write benchmark with persistence", async (
	{},
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
		await page.goto(new URL("/?docbench=1&bytes=32768&count=4", baseURL).toString());

		await expect(page.getByTestId("document-benchmark-status")).toHaveText(
			"ready",
			{
				timeout: 120_000,
			},
		);

		const resultsText =
			(await page.getByTestId("document-benchmark-results").textContent()) ?? "";
		const result = JSON.parse(resultsText) as {
			payloadBytes: number;
			count: number;
			inMemory: boolean;
			persisted: boolean | undefined;
			serializeMs: number;
			blockPutMs: number;
			documentPutMs: number;
		};

		expect(result.payloadBytes).toBe(32768);
		expect(result.count).toBe(4);
		expect(result.inMemory).toBe(false);
		expect(result.persisted).toBe(true);
		expect(result.serializeMs).toBeGreaterThanOrEqual(0);
		expect(result.blockPutMs).toBeGreaterThan(0);
		expect(result.documentPutMs).toBeGreaterThan(0);
	} finally {
		await context?.close();
	}
});

test("runs the document write benchmark with persistence and clone sqlite protocol", async (
	{},
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
			new URL(
				"/?docbench=1&bytes=32768&count=4&sqliteprotocol=clone",
				baseURL,
			).toString(),
		);

		await expect(page.getByTestId("document-benchmark-status")).toHaveText(
			"ready",
			{
				timeout: 120_000,
			},
		);

		const resultsText =
			(await page.getByTestId("document-benchmark-results").textContent()) ?? "";
		const result = JSON.parse(resultsText) as {
			payloadBytes: number;
			count: number;
			sqliteProtocol?: "legacy" | "clone";
			sqliteProfile?: {
				totalRequests: number;
				byType: Record<string, { count: number }>;
			};
			documentPutMs: number;
		};
		const profiles = (await page.evaluate(
			() => (window as any).__sqliteProfiles || [],
		)) as Array<{ requestType?: string; sql?: string }>;
		const hasCoordinateInsert = profiles.some((sample) => {
			const sql = sample.sql ?? "";
			return sql.includes("__coordinates__v_wrapped");
		});
		const hasLogHeadInsert = profiles.some((sample) => {
			const sql = sample.sql ?? "";
			return sql.includes("__log_heads__v_0") && sql.includes("VALUES");
		});
		const hasExplicitPrimaryKeyIndexCreation = profiles.some((sample) => {
			const sql = (sample.sql ?? "").toLowerCase();
			return (
				sql.includes("create index if not exists") &&
				sql.includes("_index_id on")
			);
		});
		const byIdAllQueryCount = profiles.filter((sample) => {
			const sql = (sample.sql ?? "").toLowerCase();
			return (
				sample.requestType === "all" &&
				sql.includes(" where ") &&
				sql.includes(" = ?") &&
				sql.includes("limit ? offset ?")
			);
		}).length;

		expect(result.payloadBytes).toBe(32768);
		expect(result.count).toBe(4);
		expect(result.sqliteProtocol).toBe("clone");
		expect(result.documentPutMs).toBeGreaterThan(0);
		expect(result.sqliteProfile?.totalRequests ?? 0).toBeGreaterThan(0);
		expect((result.sqliteProfile?.byType.prepare?.count ?? 0) > 0).toBeTruthy();
		expect(hasCoordinateInsert).toBeFalsy();
		expect(hasLogHeadInsert).toBeFalsy();
		expect(hasExplicitPrimaryKeyIndexCreation).toBeFalsy();
		expect(byIdAllQueryCount).toBeLessThanOrEqual(12);
	} finally {
		await context?.close();
	}
});
