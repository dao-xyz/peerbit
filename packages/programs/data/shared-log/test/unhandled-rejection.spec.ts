import { TestSession } from "@peerbit/test-utils";
import { delay } from "@peerbit/time";
import { expect } from "chai";
import { EventStore } from "./utils/stores/index.js";

describe("shared-log unhandled rejections", () => {
	let session: TestSession | undefined;

	afterEach(async () => {
		await session?.stop();
	});

	it("does not emit unhandledRejection when _waitForReplicators check fails", async () => {
		session = await TestSession.connected(1);
		const db = await session.peers[0].open(new EventStore());
		const { entry } = await db.add("hello");

		let resolveUnhandled: ((reason: any) => void) | undefined;
		const unhandledPromise = new Promise<any>((resolve) => {
			resolveUnhandled = resolve;
		});
		const handler = (reason: any) => resolveUnhandled?.(reason);
		process.once("unhandledRejection", handler);

		// Force an invalid entry hash to trigger the internal persistCoordinate path.
		const badEntry = { ...(entry as any), hash: undefined } as any;
		await (db.log as any)._waitForReplicators([0n], badEntry, [], {
			timeout: 25,
			persist: {},
		});

		const reason = await Promise.race([unhandledPromise, delay(50).then(() => null)]);
		process.removeListener("unhandledRejection", handler);

		expect(reason).to.equal(null);
	});
});

