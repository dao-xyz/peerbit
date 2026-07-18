import assert from "node:assert/strict";
import test from "node:test";
import { withDeadline } from "./templates/promise-deadline.mjs";

test("withDeadline preserves a promise that settles before its deadline", async () => {
	assert.equal(
		await withDeadline(Promise.resolve("ready"), 1_000, "late"),
		"ready",
	);
});

test("withDeadline rejects a promise that remains pending", async () => {
	const startedAt = performance.now();
	await assert.rejects(
		withDeadline(new Promise(() => {}), 20, "reader listing expired"),
		/reader listing expired/,
	);
	const elapsedMs = performance.now() - startedAt;
	assert.ok(elapsedMs >= 10, `deadline fired too early after ${elapsedMs}ms`);
	assert.ok(
		elapsedMs < 1_000,
		`deadline failed to bound the wait: ${elapsedMs}ms`,
	);
});

test("withDeadline validates its deadline contract", async () => {
	await assert.rejects(
		withDeadline(Promise.resolve(), 0, "expired"),
		/timeoutMs must be a positive safe integer/,
	);
	await assert.rejects(
		withDeadline(Promise.resolve(), 1, ""),
		/deadline message must be a non-empty string/,
	);
});
