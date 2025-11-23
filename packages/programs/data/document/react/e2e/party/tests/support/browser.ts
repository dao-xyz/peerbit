import { type BrowserContext, type TestInfo, chromium } from "@playwright/test";
import inspector from "inspector";

export type LaunchOptions = {
	headless?: boolean;
	viewport?: { width: number; height: number };
};

export const isDebugging = () => inspector.url() !== undefined;

export async function launchBrowserContext(
	testInfo: TestInfo,
	options: LaunchOptions = {},
): Promise<BrowserContext> {
	const { headless = !isDebugging(), viewport = { width: 1280, height: 800 } } =
		options;
	const browser = await chromium.launch({ headless });
	const context = await browser.newContext({ viewport });
	context.once("close", () => browser.close().catch(() => {}));
	return context;
}

export function withSearchParams(
	url: string,
	params: Record<string, string | number | boolean | undefined>,
) {
	const [head, hash = ""] = url.split("#");
	const u = new URL(head);
	for (const [k, v] of Object.entries(params)) {
		if (v === undefined) continue;
		u.searchParams.set(k, String(v));
	}
	return u.toString() + (hash ? `#${hash}` : "");
}
