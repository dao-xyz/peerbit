import { create } from "@peerbit/indexer-sqlite3";

const status = document.querySelector<HTMLElement>(
	'[data-testid="sqlite-status"]',
);

try {
	const direct = await create();
	await direct.stop();
	if (!new URLSearchParams(globalThis.location.search).has("strict-csp")) {
		const persistent = await create("peerbit-sqlite-vite-dev");
		await persistent.drop();
	}
	if (status) status.textContent = "ready";
} catch (error) {
	console.error(error);
	if (status) status.textContent = "error";
}
