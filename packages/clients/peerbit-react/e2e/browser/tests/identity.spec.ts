import {
	type BrowserContext,
	type Page,
	type TestInfo,
	chromium,
	expect,
	test,
} from "@playwright/test";
import { Peerbit } from "peerbit";

const launchPersistentBrowserContext = async (
	testInfo: TestInfo,
	baseURL: string,
	protectedFromEviction = true,
) => {
	const userDataDir = testInfo.outputPath("peerbit-react");
	const context = await chromium.launchPersistentContext(userDataDir, {
		headless: !!process.env.CI,
		viewport: { width: 1280, height: 800 },
		args: ["--enable-features=FileSystemAccessAPI"],
	});

	await context.addInitScript((persisted) => {
		Object.defineProperty(navigator.storage, "persist", {
			value: async () => persisted,
			configurable: true,
		});
		Object.defineProperty(navigator.storage, "persisted", {
			value: async () => persisted,
			configurable: true,
		});
	}, protectedFromEviction);

	const origin = new URL(baseURL).origin;
	await context.grantPermissions(["storage-access"], { origin });

	return context;
};

const waitForPeerHash = (page: Page) =>
	page.waitForFunction(
		() => {
			const text =
				(window as any).__peerInfo?.peerHash ??
				document
					.querySelector("[data-testid='peer-hash']")
					?.textContent?.trim();
			return text && text !== "no-peer" ? text : null;
		},
		{ timeout: 20_000 },
	);

const getStorageState = (page: Page) =>
	page.evaluate(async () => {
		const root = await navigator.storage.getDirectory();
		let opfsEntryCount = 0;
		for await (const _entry of root.entries()) {
			opfsEntryCount += 1;
		}
		const estimate = await navigator.storage.estimate();
		return {
			evictionProtected: await navigator.storage.persisted(),
			reactPersisted: (window as any).__peerInfo?.persisted ?? null,
			opfsEntryCount,
			usage: estimate.usage ?? 0,
		};
	});

let bootstrap: Peerbit;
let bootstrapAddr: string;
let persistentContext: BrowserContext | undefined;
let baseURL: string;

test.describe("identity", () => {
	test.beforeEach(async ({ browserName: _browserName }, testInfo) => {
		baseURL =
			testInfo.project.use.baseURL?.toString() ||
			process.env.PLAYWRIGHT_BASE_URL ||
			"http://localhost:4173";

		bootstrap = await Peerbit.create();
		bootstrapAddr = bootstrap.getMultiaddrs()[0]?.toString() ?? "";
		if (!bootstrapAddr) {
			throw new Error("Bootstrap peer has no address");
		}
	});

	test.afterEach(async () => {
		await persistentContext?.close();
		await bootstrap?.stop();
	});

	test("reuses identity across reload", async ({ browserName: _browserName }, testInfo) => {
		persistentContext = await launchPersistentBrowserContext(testInfo, baseURL);
		const page = await persistentContext!.newPage();
		const target = new URL(
			`/?bootstrap=${encodeURIComponent(bootstrapAddr)}`,
			baseURL,
		).toString();

		await page.goto(target);

		const first = await waitForPeerHash(page);
		const firstValue = (await first.jsonValue()) as string | null;

		expect(firstValue && firstValue !== "no-peer").toBeTruthy();

		await page.reload();

		const second = await waitForPeerHash(page);

		const secondValue = (await second.jsonValue()) as string | null;
		expect(secondValue).toBe(firstValue);
	});

	test("uses OPFS and reuses identity without eviction protection", async (
		{ browserName: _browserName },
		testInfo,
	) => {
		persistentContext = await launchPersistentBrowserContext(
			testInfo,
			baseURL,
			false,
		);
		const page = await persistentContext.newPage();
		const target = new URL(
			`/?bootstrap=${encodeURIComponent(bootstrapAddr)}`,
			baseURL,
		).toString();

		await page.goto(target);

		const first = await waitForPeerHash(page);
		const firstValue = (await first.jsonValue()) as string | null;
		expect(firstValue && firstValue !== "no-peer").toBeTruthy();
		const firstStorage = await getStorageState(page);
		expect(firstStorage.evictionProtected).toBe(false);
		expect(firstStorage.reactPersisted).toBe(false);
		expect(firstStorage.opfsEntryCount).toBeGreaterThan(0);
		expect(firstStorage.usage).toBeGreaterThan(0);

		await page.reload();

		const second = await waitForPeerHash(page);
		const secondValue = (await second.jsonValue()) as string | null;
		expect(secondValue).toBe(firstValue);
		const secondStorage = await getStorageState(page);
		expect(secondStorage.evictionProtected).toBe(false);
		expect(secondStorage.reactPersisted).toBe(false);
		expect(secondStorage.opfsEntryCount).toBeGreaterThan(0);
		expect(secondStorage.usage).toBeGreaterThan(0);
	});

	test("creates new identity when storage is cleared", async ({ page }) => {
		const target = new URL(
			`/?bootstrap=${encodeURIComponent(bootstrapAddr)}`,
			baseURL,
		).toString();

		await page.goto(target);

		const first = await waitForPeerHash(page);
		const firstValue = (await first.jsonValue()) as string | null;
		expect(firstValue && firstValue !== "no-peer").toBeTruthy();

		// Clear storage to simulate non-persistence and force a new identity on reload.
		await page.evaluate(async () => {
			localStorage.clear();
			sessionStorage.clear();
			if (indexedDB.databases) {
				const dbs = await indexedDB.databases();
				await Promise.all(
					dbs.map((db) => db.name && indexedDB.deleteDatabase(db.name)),
				);
			}
		});

		await page.reload();

		const second = await waitForPeerHash(page);
		const secondValue = (await second.jsonValue()) as string | null;

		expect(secondValue).not.toBe(firstValue);
	});
});
