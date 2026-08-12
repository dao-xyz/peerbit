// B12 rejection contract at the DOCUMENT layer: the document `compatibility`
// open option (6 | 7) was removed. Any explicitly-defined runtime value must
// reject with the document-named error BEFORE the historical 6 -> log v8 /
// 7 -> log v9 mapping could reach shared-log semantics — an untyped 6 must
// never surface as a confusing shared-log error, and never silently slip
// through. The option no longer exists on the TYPE, so every leg casts past
// the type system: removing the field from the types must never silently
// change runtime semantics for untyped JS callers. An explicitly-present
// `undefined` stays accepted. Persisted tag-0/tag-2 data written by old
// compatibility-6 stores remains decodable (see operation-tombstone.spec.ts).
import { TestSession } from "@peerbit/test-utils";
import { expect } from "chai";
import { DocumentCompatibilityRetiredError, Documents } from "../src/index.js";
import { TestStore } from "./data.js";

describe("document compatibility retirement", () => {
	let session: TestSession | undefined;

	beforeEach(async () => {
		session = await TestSession.disconnected(1);
	});

	afterEach(async () => {
		await session?.stop();
		session = undefined;
	});

	for (const value of [6, 7, 9] as const) {
		it(`rejects explicit document compatibility ${String(value)} with the document-named error`, async () => {
			const store = new TestStore({ docs: new Documents() });

			let rejection: unknown;
			try {
				await session!.peers[0].open(store, {
					// Untyped-JS leg: the option is gone from SetupOptions, so the
					// cast is the only way to express what a plain-JS caller can
					// still write at runtime.
					args: { replicate: false, compatibility: value } as any,
				});
			} catch (error) {
				rejection = error;
			}

			expect(rejection, "open must reject").to.exist;
			expect(rejection).to.be.instanceOf(DocumentCompatibilityRetiredError);
			expect((rejection as Error).name).to.equal(
				"DocumentCompatibilityRetiredError",
			);
			const message = (rejection as Error).message;
			// The DOCUMENT-named contract: the error names the document option
			// and its 6 | 7 domain — it must never surface as a shared-log
			// compatibility error.
			expect(message).to.contain("document");
			expect(message).to.contain("6 | 7");
			expect(message).to.contain(`${String(value)}`);
			expect(message).to.not.contain("replication-info");

			// The value never reached shared-log semantics: the underlying log
			// was never opened.
			expect((store.docs.log as any).syncronizer).to.be.undefined;
			expect((store.docs.log as any).domain).to.be.undefined;
			// B12 stage 5: the residual compatibility field is deleted outright.
			expect((store.docs as any).compatibility).to.be.undefined;
		});
	}

	it("accepts an explicitly-present undefined compatibility value", async () => {
		const store = new TestStore({ docs: new Documents() });
		await session!.peers[0].open(store, {
			// The key is present but the value is undefined: only DEFINED values
			// reject.
			args: { replicate: false, compatibility: undefined } as any,
		});
		try {
			expect((store.docs as any).compatibility).to.be.undefined;
			expect((store.docs.log as any).syncronizer).to.exist;
		} finally {
			await store.close();
		}
	});
});
