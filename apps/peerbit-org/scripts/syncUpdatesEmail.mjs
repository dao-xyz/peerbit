import fs from "node:fs";
import path from "node:path";

function required(name, value) {
	if (!value) {
		throw new Error(`Missing required env var: ${name}`);
	}
	return value;
}

function resolveSyncUrl() {
	return (
		process.env.UPDATES_SYNC_URL ||
		process.env.SUPABASE_UPDATES_SYNC_URL ||
		(process.env.SUPABASE_URL
			? `${process.env.SUPABASE_URL.replace(/\/$/, "")}/functions/v1/updates-sync`
			: "")
	);
}

function resolveSyncSecret() {
	return process.env.UPDATES_SYNC_SECRET || process.env.SUPABASE_UPDATES_SYNC_SECRET;
}

async function main() {
	const updatesSyncUrl = required("UPDATES_SYNC_URL (or SUPABASE_UPDATES_SYNC_URL)", resolveSyncUrl());
	const updatesSyncSecret = required(
		"UPDATES_SYNC_SECRET (or SUPABASE_UPDATES_SYNC_SECRET)",
		resolveSyncSecret(),
	);

	const indexPath = path.resolve(
		process.cwd(),
		"apps/peerbit-org/dist/content/docs/updates/index.json",
	);
	if (!fs.existsSync(indexPath)) {
		throw new Error(
			`Missing ${path.relative(process.cwd(), indexPath)}. Run \`pnpm site:build\` first.`,
		);
	}

	const body = fs.readFileSync(indexPath);
	const res = await fetch(updatesSyncUrl, {
		method: "POST",
		headers: {
			Authorization: `Bearer ${updatesSyncSecret}`,
			"Content-Type": "application/json",
		},
		body,
	});

	if (!res.ok) {
		throw new Error(`updates-sync failed (${res.status}): ${await res.text()}`);
	}

	process.stdout.write(`updates-sync OK (${res.status})\n`);
}

try {
	await main();
} catch (error) {
	process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
	process.exitCode = 1;
}

