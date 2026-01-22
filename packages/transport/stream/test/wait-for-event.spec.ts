import { AbortError } from "@peerbit/time";
import { expect } from "chai";
import { waitForEvent } from "../src/wait-for-event.js";

describe("waitForEvent", () => {
	it("cleans up listeners after resolve", async () => {
		const emitter = new EventTarget();

		let calls = 0;
		const promise = waitForEvent(
			emitter as any,
			["foo"] as any,
			(deferred) => {
				calls++;
				if (calls === 2) {
					deferred.resolve();
				}
			},
			{ timeout: 1000 },
		);

		emitter.dispatchEvent(new Event("foo"));
		await promise;

		emitter.dispatchEvent(new Event("foo"));
		expect(calls).to.equal(2);
	});

	it("rejects immediately when signal already aborted", async () => {
		const emitter = new EventTarget();
		const controller = new AbortController();
		controller.abort(new AbortError("aborted"));

		let err: unknown;
		try {
			await waitForEvent(emitter as any, ["foo"] as any, () => {}, {
				signals: [controller.signal],
				timeout: 1000,
			});
		} catch (e) {
			err = e;
		}

		expect(err).to.be.instanceOf(AbortError);
	});
});

