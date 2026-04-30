import { AbortError, TimeoutError } from "@peerbit/time";
import { expect } from "chai";
import { runInNewContext } from "node:vm";
import sinon from "sinon";
import { normalizeUint8Array } from "../src/bytes.js";
import { toUint8Array, waitForSubscribers } from "../src/index.js";

describe("pubsub", function () {
	describe("byte normalization", () => {
		it("materializes list-like byte sources without relying on Uint8ArrayList identity", () => {
			const payload = new Uint8Array([1, 2, 3]);
			const listLike = {
				byteLength: payload.byteLength,
				subarray: () => payload,
			};

			expect(toUint8Array(listLike)).to.equal(payload);
		});

		it("normalizes cross-realm and view-backed bytes to local Uint8Array", () => {
			const foreignBytes = runInNewContext("new Uint8Array([4, 5, 6])");
			const normalizedForeign = normalizeUint8Array(foreignBytes);

			expect(normalizedForeign).to.be.instanceOf(Uint8Array);
			expect([...normalizedForeign!]).to.deep.equal([4, 5, 6]);

			const backing = new Uint8Array([0, 7, 8, 0]);
			const view = new DataView(backing.buffer, 1, 2);
			const normalizedView = normalizeUint8Array(view);

			expect(normalizedView).to.be.instanceOf(Uint8Array);
			expect([...normalizedView!]).to.deep.equal([7, 8]);
		});
	});

	describe("waitForSubscribers", () => {
		let clock: ReturnType<typeof sinon.useFakeTimers>;

		beforeEach(() => {
			clock = sinon.useFakeTimers();
		});

		afterEach(() => {
			clock.restore();
		});

		it("rejects immediately when signal already aborted", async () => {
			const getSubscribers = sinon.stub().resolves([]);
			const libp2p = { services: { pubsub: { getSubscribers } } } as any;

			const controller = new AbortController();
			controller.abort();

			let err: unknown;
			try {
				await waitForSubscribers(libp2p, "peer", "topic", {
					signal: controller.signal,
					timeout: 1000,
				});
			} catch (e) {
				err = e;
			}

			expect(err).to.be.instanceOf(AbortError);
			expect(getSubscribers.callCount).to.equal(0);
		});

		it("stops polling on abort", async () => {
			const getSubscribers = sinon.stub().resolves([]);
			const addEventListener = sinon.stub();
			const removeEventListener = sinon.stub();
			const libp2p = {
				services: { pubsub: { getSubscribers, addEventListener, removeEventListener } },
			} as any;

			const controller = new AbortController();
			const promise = waitForSubscribers(libp2p, "peer", "topic", {
				signal: controller.signal,
				timeout: 10_000,
			});

			await Promise.resolve();
			expect(getSubscribers.callCount).to.equal(1);
			const before = getSubscribers.callCount;

			controller.abort();

			let err: unknown;
			try {
				await promise;
			} catch (e) {
				err = e;
			}
			expect(err).to.be.instanceOf(AbortError);

			clock.tick(5_000);
			await Promise.resolve();
			expect(getSubscribers.callCount).to.equal(before);
		});

		it("stops polling on timeout", async () => {
			const getSubscribers = sinon.stub().resolves([]);
			const addEventListener = sinon.stub();
			const removeEventListener = sinon.stub();
			const libp2p = {
				services: { pubsub: { getSubscribers, addEventListener, removeEventListener } },
			} as any;

			const promise = waitForSubscribers(libp2p, "peer", "topic", {
				timeout: 1000,
			});

			clock.tick(1001);
			await Promise.resolve();

			let err: unknown;
			try {
				await promise;
			} catch (e) {
				err = e;
			}
			expect(err).to.be.instanceOf(TimeoutError);

			const before = getSubscribers.callCount;
			clock.tick(5_000);
			await Promise.resolve();
			expect(getSubscribers.callCount).to.equal(before);
		});
	});
});
