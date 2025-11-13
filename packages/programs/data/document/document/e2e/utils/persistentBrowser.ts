import { chromium, type BrowserContext, type TestInfo } from "@playwright/test";
import inspector from "inspector";

export type PersistentContextOptions = {
    /** Subdirectory name under the test output path. */
    scope: string;
    /** Absolute base URL for the app (used to grant permissions). */
    baseURL: string;
    headless?: boolean;
    viewport?: { width: number; height: number };
};

export function isDebugging() {
    return inspector.url() !== undefined;
}
/**
 * Launch a persistent Chromium context mirroring the behaviour from
 * `fixtures/persistentContext`, so tests can spin up additional users without
 * duplicating the boilerplate.
 */
export async function launchPersistentBrowserContext(
    testInfo: TestInfo,
    options: PersistentContextOptions
): Promise<BrowserContext> {
    const {
        scope,
        baseURL,
        headless = !isDebugging(),
        viewport = { width: 1280, height: 800 },
    } = options;

    const userDataDir = testInfo.outputPath(scope);
    const context = await chromium.launchPersistentContext(userDataDir, {
        headless,
        viewport,
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
}
