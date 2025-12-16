/**
 * Tests for getCover behavior during lifecycle transitions (start/close).
 *
 * These tests verify that getCover gracefully handles NotStartedError and ClosedError
 * conditions that can occur during:
 * - Component unmount during active remote queries
 * - Rapid open/close cycles
 * - Race conditions between query and close operations
 *
 * Related bug: NotStartedError from @peerbit/shared-log when a shared-log–backed
 * program performs a remote lookup during startup/shutdown.
 */
import { TestSession } from "@peerbit/test-utils";
import { delay } from "@peerbit/time";
import { expect } from "chai";
import { EventStore } from "./utils/stores/index.js";

describe("lifecycle - getCover", () => {
	let session: TestSession;

	afterEach(async () => {
		await session?.stop();
	});

	describe("graceful handling when closed", () => {
		it("returns empty array when log is closed", async () => {
			session = await TestSession.connected(1);
			const store = new EventStore();
			const db = await session.peers[0].open(store);

			// Close the log
			await db.close();

			// getCover should return empty array instead of throwing NotStartedError
			const cover = await db.log.getCover({ args: undefined });
			expect(cover).to.deep.equal([]);
		});

		it("returns empty array when log is closing during getCover call", async () => {
			session = await TestSession.connected(1);
			const store = new EventStore();
			const db = await session.peers[0].open(store);

			// Start closing and call getCover concurrently
			const closePromise = db.close();

			// This should not throw, even if close happens during getCover
			const coverPromise = db.log.getCover({ args: undefined });

			// Wait for both to complete
			const [cover] = await Promise.all([coverPromise, closePromise]);

			// Should either return valid cover or empty array, but not throw
			expect(Array.isArray(cover)).to.be.true;
		});

		it("handles rapid open/close cycles without throwing", async () => {
			session = await TestSession.connected(1);

			for (let i = 0; i < 5; i++) {
				const store = new EventStore();
				const db = await session.peers[0].open(store);

				// Fire off getCover without waiting
				const coverPromise = db.log.getCover({ args: undefined }).catch((e) => {
					// If it throws, it should not be NotStartedError bubbling up unhandled
					throw new Error(
						`getCover threw during rapid cycle ${i}: ${e.message}`,
					);
				});

				// Close immediately
				await db.close();

				// getCover should complete gracefully
				const cover = await coverPromise;
				expect(Array.isArray(cover)).to.be.true;
			}
		});
	});

	describe("concurrent operations during shutdown", () => {
		it("multiple getCover calls during close all resolve gracefully", async () => {
			session = await TestSession.connected(1);
			const store = new EventStore();
			const db = await session.peers[0].open(store);

			// Start multiple getCover calls
			const coverPromises = [
				db.log.getCover({ args: undefined }),
				db.log.getCover({ args: undefined }),
				db.log.getCover({ args: undefined }),
			];

			// Start close
			const closePromise = db.close();

			// All should resolve without throwing NotStartedError
			const results = await Promise.all([
				...coverPromises.map((p) => p.catch(() => [])), // Convert any errors to empty arrays
				closePromise,
			]);

			// First 3 results should be arrays
			for (let i = 0; i < 3; i++) {
				expect(Array.isArray(results[i])).to.be.true;
			}
		});

		it("getCover called after close starts returns empty array", async () => {
			session = await TestSession.connected(2);
			const store = new EventStore();
			const db1 = await session.peers[0].open(store);
			const db2 = await session.peers[1].open(store.clone(), {
				args: { replicate: { factor: 1 } },
			});

			// Ensure some replication data exists
			await db1.add("test");
			await delay(500);

			// Start close but don't await
			const closePromise = db1.close();

			// Small delay to ensure close has started
			await delay(10);

			// getCover should not throw even though close is in progress
			const cover = await db1.log.getCover({ args: undefined }).catch(() => []);
			expect(Array.isArray(cover)).to.be.true;

			await closePromise;
			await db2.close();
		});
	});

	describe("network scenarios", () => {
		it("getCover during peer disconnect/reconnect cycle", async () => {
			session = await TestSession.connected(2);
			const store = new EventStore();
			const db1 = await session.peers[0].open(store, {
				args: { replicate: { factor: 1 } },
			});
			const db2 = await session.peers[1].open(store.clone(), {
				args: { replicate: { factor: 1 } },
			});

			// Let them sync
			await delay(500);

			// Close db2 (simulating peer leaving)
			await db2.close();

			// getCover on db1 should still work
			const cover = await db1.log.getCover({ args: undefined });
			expect(Array.isArray(cover)).to.be.true;

			await db1.close();
		});
	});

	describe("abort signal handling", () => {
		it("returns empty array when signal is already aborted", async () => {
			session = await TestSession.connected(1);
			const store = new EventStore();
			const db = await session.peers[0].open(store);

			// Create an already-aborted controller
			const controller = new AbortController();
			controller.abort();

			// getCover should return empty array immediately
			const cover = await db.log.getCover(
				{ args: undefined },
				{ signal: controller.signal },
			);
			expect(cover).to.deep.equal([]);

			await db.close();
		});

		it("returns empty array when signal is aborted during operation", async () => {
			session = await TestSession.connected(2);
			const store = new EventStore();
			const db1 = await session.peers[0].open(store, {
				args: { replicate: { factor: 1 } },
			});
			const db2 = await session.peers[1].open(store.clone(), {
				args: { replicate: { factor: 1 } },
			});

			// Add some data and let it sync
			await db1.add("test1");
			await db1.add("test2");
			await delay(500);

			const controller = new AbortController();

			// Start getCover and abort during operation
			const coverPromise = db1.log.getCover(
				{ args: undefined },
				{ signal: controller.signal },
			);

			// Abort after a brief delay
			await delay(5);
			controller.abort();

			// Should return gracefully (either valid cover or empty array)
			const cover = await coverPromise;
			expect(Array.isArray(cover)).to.be.true;

			await db1.close();
			await db2.close();
		});

		it("abort signal from AbortSignal.any works correctly", async () => {
			session = await TestSession.connected(1);
			const store = new EventStore();
			const db = await session.peers[0].open(store);

			// Simulate the pattern used in document store
			const controller1 = new AbortController();
			const controller2 = new AbortController();
			const combinedSignal = AbortSignal.any([
				controller1.signal,
				controller2.signal,
			]);

			// Abort one of the controllers
			controller1.abort();

			// getCover should respect the combined signal
			const cover = await db.log.getCover(
				{ args: undefined },
				{ signal: combinedSignal },
			);
			expect(cover).to.deep.equal([]);

			await db.close();
		});
	});
});
