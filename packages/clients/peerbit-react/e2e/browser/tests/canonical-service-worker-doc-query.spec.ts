import { expect, test } from "@playwright/test";

test("runs a document query via canonical service worker (prod build)", async ({
	page,
}) => {
	await page.goto("/?doc=1&canonical=1");

	await expect(page.getByTestId("doc-query-status")).toHaveText("ready", {
		timeout: 40_000,
	});

	await expect(page.getByTestId("doc-query-results")).toContainText("hello");
	await expect(page.getByTestId("doc-query-results")).not.toContainText("bye");
});
