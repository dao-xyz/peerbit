// B12 rejection contract: the `compatibility` open option was removed and the
// pre-v10 replication-info network compatibility modes are retired. ANY
// explicitly-defined runtime value — including 10, which used to behave like
// the default — must reject at open() BEFORE any open-time side effect runs
// (rpc.open, index/native setup, domain resolution, synchronizer creation,
// subscription setup). The option no longer exists on the TYPE, so every leg
// here deliberately casts past the type system: removing the field from the
// types must never silently change runtime semantics for untyped JS callers.
// An explicitly-present `undefined` stays accepted.
import { TestSession } from "@peerbit/test-utils";
import { expect } from "chai";
import sinon from "sinon";
import { CompatibilityModeRetiredError, SharedLog } from "../src/index.js";

describe("network compatibility retirement", () => {
	let session: TestSession | undefined;

	beforeEach(async () => {
		session = await TestSession.disconnected(1);
	});

	afterEach(async () => {
		sinon.restore();
		await session?.stop();
		session = undefined;
	});

	for (const value of [0, 8, 9, 10, 1234] as const) {
		it(`rejects explicit compatibility ${String(value)} before any open side effect`, async () => {
			const log = new SharedLog<any, any>();
			const rpcOpen = sinon.spy(log.rpc, "open");
			const pubsub = session!.peers[0].services.pubsub as any;
			const addListener = sinon.spy(pubsub, "addEventListener");

			let rejection: unknown;
			try {
				await session!.peers[0].open(log as any, {
					// Untyped-JS leg: the option is gone from SharedLogOptions, so
					// the cast is the only way to express what a plain-JS caller can
					// still write at runtime.
					args: { replicate: false, compatibility: value } as any,
				});
			} catch (error) {
				rejection = error;
			}

			expect(rejection, "open must reject").to.exist;
			expect(rejection).to.be.instanceOf(CompatibilityModeRetiredError);
			const message = (rejection as Error).message;
			// The message names the removed option, states the retirement and
			// hints at the document-level 6/7 mapping.
			expect(message).to.contain('"compatibility"');
			expect(message).to.contain(
				"replication-info network compatibility modes are retired",
			);
			expect(message).to.contain(`${String(value)}`);
			expect(message).to.contain("compatibility 6/7");

			// No partial open state: the rejection fired before every open-time
			// side effect.
			expect(rpcOpen.notCalled, "rpc.open must never run").to.be.true;
			expect(
				addListener
					.getCalls()
					.filter(
						(call: any) =>
							call.args[0] === "subscribe" || call.args[0] === "unsubscribe",
					),
				"subscription setup must never run",
			).to.have.length(0);
			expect((log as any).domain, "domain resolution must never run").to.be
				.undefined;
			expect((log as any).syncronizer, "synchronizer must never be created").to
				.be.undefined;
			expect(
				(log as any)._entryCoordinatesIndex,
				"entry coordinate index setup must never run",
			).to.be.undefined;
			expect(
				(log as any)._replicationRangeIndex,
				"replication range index setup must never run",
			).to.be.undefined;
			expect(
				(log as any)._nativeBackbone,
				"native backbone setup must never run",
			).to.be.undefined;
		});
	}

	it("accepts an explicitly-present undefined compatibility value", async () => {
		const log = new SharedLog<any, any>();
		await session!.peers[0].open(log as any, {
			// The key is present but the value is undefined: only DEFINED values
			// reject.
			args: { replicate: false, compatibility: undefined } as any,
		});
		try {
			expect((log as any).syncronizer).to.exist;
			expect((log as any).domain).to.exist;
			expect((log as any).domain.resolution).to.equal("u64");
		} finally {
			await log.close();
		}
	});
});
