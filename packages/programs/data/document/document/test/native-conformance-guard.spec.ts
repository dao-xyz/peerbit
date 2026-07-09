import { TestSession } from "@peerbit/test-utils";
import { expect } from "chai";
import { Documents } from "../src/index.js";
import { Document, TestStore } from "./data.js";

/**
 * Hard guard for the opt-in `[default, native]` document conformance leg
 * (`PEERBIT_SHARED_LOG_RUST_CORE=1`, wired as `test:document-rust-core`).
 *
 * The switch (packages/clients/test-utils/src/session.ts) attaches the native
 * Rust data plane to every TestSession peer, so a Documents store opened in the
 * DEFAULT `mode:"auto"` (plain args, no `mode:"native"`) builds its generic
 * index on `@peerbit/indexer-rust` — i.e. `docs.index.index` is a `RustIndex`
 * on the switch peer where it is a `SQLiteIndex` on the default backend.
 *
 * This test asserts that fact so the leg can never false-green by silently
 * running the allowlist on the JS (SQLite) backend when the native build is
 * missing or the switch failed to engage — the document analog of the
 * shared-log leg's hard native-present guard. When the env is unset (default /
 * JS baseline run) the same assertion confirms the backend is SQLite, so the
 * guard is a no-op on the baseline leg.
 */
describe("native conformance guard", () => {
	let session: TestSession;

	afterEach(async () => {
		await session?.stop();
	});

	it("document generic index is the expected backend for this leg", async () => {
		session = await TestSession.connected(1);
		const store = new TestStore({ docs: new Documents<Document>() });
		await session.peers[0].open(store);

		const inner = (store.docs.index as unknown as { index?: unknown }).index;
		const backend = (inner as { constructor?: { name?: string } })?.constructor
			?.name;

		const nativeLeg =
			process.env.PEERBIT_SHARED_LOG_RUST_CORE === "1" ||
			process.env.PEERBIT_SHARED_LOG_RUST_CORE === "true";

		if (nativeLeg) {
			// If this fails, the leg is running the allowlist against the JS
			// (SQLite) backend — a false-green. Refuse it loudly.
			expect(
				backend,
				`Expected docs.index.index to be a RustIndex under the native ` +
					`conformance leg but it was "${backend}". The native Rust data ` +
					`plane did not engage — refusing to false-green on the JS backend.`,
			).to.eq("RustIndex");
		} else {
			// Baseline (env unset): confirm we are on the JS backend.
			expect(backend).to.eq("SQLiteIndex");
		}
	});
});
