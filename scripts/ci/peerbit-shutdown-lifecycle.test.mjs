import assert from "node:assert/strict";
import test from "node:test";
import { withPeerShutdown } from "./peerbit-shutdown-lifecycle.mjs";

test("stops peers created during the body and waits for every stop", async () => {
	const peers = [];
	let release;
	const gate = new Promise((resolve) => {
		release = resolve;
	});
	let settled = false;
	const stopped = [];
	const work = withPeerShutdown(peers, async () => {
		peers.push({
			stop: async () => {
				await gate;
				stopped.push(1);
			},
		});
		peers.push({
			stop: async () => {
				stopped.push(2);
			},
		});
	}).then(() => {
		settled = true;
	});
	try {
		await Promise.resolve();
		await Promise.resolve();
		assert.deepEqual(stopped, [2]);
		assert.equal(settled, false);
	} finally {
		release();
		await work;
	}
	assert.deepEqual(stopped, [2, 1]);
});

test("preserves an undefined body failure after successful cleanup", async () => {
	let stopped = false;
	let rejected = false;
	await withPeerShutdown(
		[
			{
				stop: () => {
					stopped = true;
				},
			},
		],
		() => {
			throw undefined;
		},
	).catch((error) => {
		rejected = true;
		assert.equal(error, undefined);
	});
	assert.equal(rejected, true);
	assert.equal(stopped, true);
});

test("preserves the body failure and every stop failure without skipping peers", async () => {
	const bodyError = new Error("convergence failed");
	const stopError = new Error("stop failed");
	let lastStopped = false;
	await assert.rejects(
		withPeerShutdown(
			[
				{
					stop: () => {
						throw stopError;
					},
				},
				{
					stop: async () => {
						throw undefined;
					},
				},
				{
					stop: () => {
						lastStopped = true;
					},
				},
			],
			() => {
				throw bodyError;
			},
		),
		(error) => {
			assert.ok(error instanceof AggregateError);
			assert.deepEqual(error.errors, [bodyError, stopError, undefined]);
			return true;
		},
	);
	assert.equal(lastStopped, true);
});

test("preserves a sole cleanup failure", async () => {
	const error = new Error("stop failed");
	await assert.rejects(
		withPeerShutdown(
			[
				{
					stop: async () => {
						throw error;
					},
				},
			],
			async () => {},
		),
		(actual) => actual === error,
	);
});
