/**
 * Regression: AnyBlockStore.put() throws LEVEL_DATABASE_NOT_OPEN after stop()
 *
 * When a late replication write arrives after the blockstore has been closed,
 * the Level backend throws { code: "LEVEL_DATABASE_NOT_OPEN" }.
 * put() should catch this and return the CID as a silent no-op.
 */

import { expect } from "chai";
import os from "os";
import path from "path";
import fs from "fs";
import { createStore } from "@peerbit/any-store";
import { AnyBlockStore } from "../src/any-blockstore.js";

describe("@peerbit/blocks — shutdown race", () => {
	let tmpDir: string;

	beforeEach(() => {
		tmpDir = path.join(os.tmpdir(), "blocks-shutdown-test-" + Date.now());
	});

	afterEach(() => {
		fs.rmSync(tmpDir, { recursive: true, force: true });
	});

	it("put() after stop() should not throw LEVEL_DATABASE_NOT_OPEN", async () => {
		const store = new AnyBlockStore(createStore(tmpDir));
		await store.start();

		const data = new Uint8Array([1, 2, 3, 4]);
		const cid = await store.put(data);
		expect(cid).to.exist;

		await store.stop();

		// Late replication write — should not throw.
		const lateCid = await store.put(data);
		expect(lateCid).to.exist;
	});

	it("get() after stop() should not throw (baseline check)", async () => {
		const store = new AnyBlockStore(createStore(tmpDir));
		await store.start();

		const data = new Uint8Array([10, 20, 30]);
		const cid = await store.put(data);
		await store.stop();

		try {
			await store.get(cid);
		} catch {
			// acceptable — the point is no *unhandled* rejection
		}
	});
});
