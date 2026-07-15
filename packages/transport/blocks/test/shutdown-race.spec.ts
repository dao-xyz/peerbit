/**
 * Regression: AnyBlockStore.put() throws LEVEL_DATABASE_NOT_OPEN after stop()
 *
 * When a late replication write arrives after the blockstore has been closed,
 * the Level backend throws { code: "LEVEL_DATABASE_NOT_OPEN" }.
 * put() should catch this and return the CID as a silent no-op.
 */
import { createStore } from "@peerbit/any-store";
import type { AnyStore } from "@peerbit/any-store-interface";
import { Ed25519Keypair } from "@peerbit/crypto";
import { expect } from "chai";
import fs from "fs";
import os from "os";
import path from "path";
import sinon from "sinon";
import { AnyBlockStore } from "../src/any-blockstore.js";
import { RemoteBlocks } from "../src/remote.js";

describe("@peerbit/blocks — shutdown race", () => {
	let tmpDir: string;
	let stores: AnyBlockStore[];
	let remotes: RemoteBlocks[];
	const trackStore = <T extends AnyBlockStore>(store: T): T => {
		stores.push(store);
		return store;
	};
	const trackRemote = <T extends RemoteBlocks>(remote: T): T => {
		remotes.push(remote);
		return remote;
	};

	beforeEach(() => {
		tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "blocks-shutdown-test-"));
		stores = [];
		remotes = [];
	});

	afterEach(async () => {
		// Always close live handles before deleting their directory. This keeps the
		// race tests deterministic on Windows and after an assertion failure.
		for (const remote of remotes.reverse()) {
			if (remote.status !== "closed") {
				await remote.stop().catch((): void => undefined);
			}
		}
		for (const store of stores.reverse()) {
			if (store.status() !== "closed") {
				await store.stop().catch((): void => undefined);
			}
		}
		await fs.promises.rm(tmpDir, { recursive: true, force: true });
	});

	it("put() after stop() should not throw LEVEL_DATABASE_NOT_OPEN", async () => {
		const store = trackStore(new AnyBlockStore(createStore(tmpDir)));
		await store.start();

		const data = new Uint8Array([1, 2, 3, 4]);
		const cid = await store.put(data);
		expect(cid).to.exist;

		await store.stop();

		// Late replication write — should not throw.
		const lateCid = await store.put(data);
		expect(lateCid).to.exist;
	});

	it("putMany() after stop() should not throw LEVEL_DATABASE_NOT_OPEN", async () => {
		const store = trackStore(new AnyBlockStore(createStore(tmpDir)));
		await store.start();

		const data = [new Uint8Array([1, 2, 3]), new Uint8Array([4, 5, 6])];
		const cids = await store.putMany(data);
		expect(cids).to.have.length(2);

		await store.stop();

		const lateCids = await store.putMany(data);
		expect(lateCids).to.deep.equal(cids);
	});

	it("put() before start() should still throw for an unopened store", async () => {
		const unopenedStore: AnyStore = {
			status: () => "closed",
			close: async () => {},
			open: async () => {},
			get: async () => undefined,
			put: async () => {
				const error = new Error("Database is not open");
				(error as any).code = "LEVEL_DATABASE_NOT_OPEN";
				throw error;
			},
			del: async () => {},
			sublevel: async () => unopenedStore,
			iterator: () => ({
				[Symbol.asyncIterator]: async function* () {},
			}),
			clear: async () => {},
			size: async () => 0,
			persisted: async () => true,
		};
		const store = trackStore(new AnyBlockStore(unopenedStore));

		try {
			await store.put(new Uint8Array([5, 6, 7, 8]));
			expect.fail("Expected put() to throw on an unopened store");
		} catch (error: any) {
			expect(error.code).to.equal("LEVEL_DATABASE_NOT_OPEN");
		}
	});

	it("get() after stop() should not throw (baseline check)", async () => {
		const store = trackStore(new AnyBlockStore(createStore(tmpDir)));
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

	it("RemoteBlocks.stop releases later resources and rethrows the first drain error", async () => {
		const local = trackStore(new AnyBlockStore(createStore(tmpDir)));
		const key = await Ed25519Keypair.create();
		const remote = trackRemote(
			new RemoteBlocks({
				local,
				publicKey: key.publicKey,
				publish: async () => undefined,
				waitFor: async () => [],
			}),
		);
		await remote.start();
		const failure = new Error("deferred notification flush failed");
		const flushStub = sinon
			.stub(remote as any, "flushDeferredStoredNotifications")
			.rejects(failure);
		const localStopSpy = sinon.spy(local, "stop");
		(remote as any)._resolvers.set("pending", async (): Promise<void> => {});
		(remote as any)._readFromPeersPromises.set("pending", {
			promise: Promise.resolve(undefined),
			addProviders: (): void => {},
		});

		try {
			const thrown = await remote.stop().then(
				(): undefined => undefined,
				(error: unknown) => error,
			);
			expect(thrown).equal(failure);
			expect(localStopSpy.calledOnce).equal(true);
			expect(local.status()).equal("closed");
			expect(remote.status).equal("closed");
			expect((remote as any)._resolvers.size).equal(0);
			expect((remote as any)._readFromPeersPromises.size).equal(0);
		} finally {
			localStopSpy.restore();
			flushStub.restore();
		}
	});

	it("does not announce columnar puts when the durability barrier fails", async () => {
		const local = trackStore(
			new AnyBlockStore(createStore(tmpDir)),
		) as AnyBlockStore & {
			putKnownManyColumns: (cids: string[], bytes: Uint8Array[]) => string[];
			waitForDurableWrites: () => Promise<void>;
		};
		local.putKnownManyColumns = (cids) => cids;
		let rejectBarrier!: (error: unknown) => void;
		const barrier = new Promise<void>((_resolve, reject) => {
			rejectBarrier = reject;
		});
		local.waitForDurableWrites = () => barrier;
		const key = await Ed25519Keypair.create();
		const onPut = sinon.spy();
		const remote = trackRemote(
			new RemoteBlocks({
				local,
				publicKey: key.publicKey,
				publish: async () => undefined,
				waitFor: async () => [],
				onPut,
			}),
		);
		const cid = "columnar-cid";

		expect(
			remote.putKnownManyColumns?.([cid], [new Uint8Array([1])]),
		).deep.equal([cid]);
		expect(onPut.callCount).equal(0);
		rejectBarrier(new Error("durability failed"));
		await Promise.resolve();
		await Promise.resolve();
		expect(onPut.callCount).equal(0);
	});
});
