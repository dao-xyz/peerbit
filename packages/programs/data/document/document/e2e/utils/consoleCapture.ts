import { Page, TestInfo } from "@playwright/test";

export type ConsoleCaptureOptions = {
    printAll?: boolean; // echo everything to test runner
    failOnError?: boolean; // throw when a console error happens
    ignorePatterns?: (string | RegExp)[]; // do not fail/print when matching
    capturePageErrors?: boolean; // also capture window "Uncaught" exceptions
    captureWebErrors?: boolean; // BrowserContext 'weberror' (if supported by PW version)
};

export function setupConsoleCapture(
    page: Page,
    testInfo: TestInfo,
    opts: ConsoleCaptureOptions = {
        printAll: true,
        failOnError: false,
        capturePageErrors: true,
        captureWebErrors: true,
    }
) {
    const ignore = opts.ignorePatterns || [];
    const shouldIgnore = (text: string) =>
        ignore.some((p) =>
            typeof p === "string" ? text.includes(p) : p.test(text)
        );

    page.on("console", (msg) => {
        const text = msg.text();
        const type = msg.type();

        if (opts.printAll) {
            testInfo
                .attach(`console:${type}`, {
                    body: Buffer.from(text, "utf8"),
                    contentType: "text/plain",
                })
                .catch(() => {});
            // Also echo to stdout for quick diagnosis
            // eslint-disable-next-line no-console
            console.log(`Page console ${type}: ${text}`);
        }

        if (type === "error" && !shouldIgnore(text)) {
            if (opts.failOnError) {
                throw new Error(`Page console error: ${text}`);
            }
        }
    });

    if (opts.capturePageErrors) {
        page.on("pageerror", (err) => {
            const text = (err && (err.stack || err.message)) || String(err);
            testInfo
                .attach(`pageerror`, {
                    body: Buffer.from(text, "utf8"),
                    contentType: "text/plain",
                })
                .catch(() => {});
            // eslint-disable-next-line no-console
            console.error(`Page error: ${text}`);
            if (opts.failOnError && !shouldIgnore(text)) {
                throw new Error(`Page error: ${text}`);
            }
        });
    }

    if (opts.captureWebErrors) {
        const ctx: any = page.context?.();
        try {
            ctx?.on?.("weberror", (event: any) => {
                // Shape is PW-version dependent; do best-effort extraction
                const err = (event && (event.error || event)) as any;
                const text =
                    (err && (err.stack || err.message)) ||
                    JSON.stringify(event);
                testInfo
                    .attach("weberror", {
                        body: Buffer.from(String(text), "utf8"),
                        contentType: "text/plain",
                    })
                    .catch(() => {});
                // eslint-disable-next-line no-console
                console.error(`Web error: ${text}`);
                if (opts.failOnError && !shouldIgnore(String(text))) {
                    throw new Error(`Web error: ${text}`);
                }
            });
        } catch {
            // ignore if not supported
        }
    }
}
