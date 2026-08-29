import { field, variant, vec } from "@dao-xyz/borsh";
import {
	type OrderedIndexWriteSession,
	id,
	toId,
} from "@peerbit/indexer-interface";
import { expect } from "chai";
import { SQLiteIndices } from "../src/engine.js";
import { create } from "../src/index.js";
import { setup } from "./utils.js";

@variant("sqlite_ordered_write_document")
class OrderedDocument {
	@id({ type: "string" })
	id: string;

	@field({ type: "string" })
	label: string;

	@field({ type: vec("string") })
	tags: string[];

	constructor(properties: { id: string; label?: string; tags?: string[] }) {
		this.id = properties.id;
		this.label = properties.label ?? "";
		this.tags = properties.tags ?? [];
	}
}

const deferred = () => {
	let resolve!: () => void;
	const promise = new Promise<void>((r) => {
		resolve = r;
	});
	return { promise, resolve };
};

const expectRejected = async (promise: Promise<unknown>) => {
	try {
		await promise;
	} catch (error) {
		return error;
	}
	throw new Error("Expected promise to reject");
};

describe("SQLite ordered write sessions", () => {
	it("commits the successful prefix and removes a failing row's partial state", async () => {
		const { store } = await setup<OrderedDocument>({
			schema: OrderedDocument,
		});
		expect(store.withOrderedWriteSession).to.be.a("function");

		const markers: string[] = [];
		const failure = new Error("transform failed");
		const error = await expectRejected(
			Promise.resolve(
				store.withOrderedWriteSession!(async (session) => {
					markers.push("transform:first");
					await session.put(
						new OrderedDocument({ id: "first", tags: ["persisted"] }),
					);
					expect((await session.get(toId("first")))?.value.tags).to.deep.equal([
						"persisted",
					]);
					markers.push("stored:first");
					throw failure;
					// A sequential caller never starts later transforms after failure.
				}),
			),
		);

		expect(error).to.equal(failure);
		expect(markers).to.deep.equal(["transform:first", "stored:first"]);
		expect((await store.get(toId("first")))?.value.tags).to.deep.equal([
			"persisted",
		]);

		const writeError = await expectRejected(
			Promise.resolve(
				store.withOrderedWriteSession!(async (session) => {
					await session.put(new OrderedDocument({ id: "second" }));
					await session.put({ id: "invalid" } as unknown as OrderedDocument);
				}),
			),
		);
		expect(writeError).to.be.instanceOf(Error);
		expect(await store.get(toId("second"))).not.to.equal(undefined);
		expect(await store.get(toId("invalid"))).to.equal(undefined);
		expect(await store.getSize()).to.equal(2);

		await store.put(new OrderedDocument({ id: "scalar", label: "old" }));
		await store.put(
			new OrderedDocument({ id: "scalar", label: "scalar-new" }),
			undefined,
			{ replace: false },
		);
		await store.put(new OrderedDocument({ id: "session", label: "old" }));
		await store.withOrderedWriteSession!(async (session) => {
			await session.put(
				new OrderedDocument({ id: "session", label: "session-new" }),
				undefined,
				{ replace: false },
			);
		});
		expect((await store.get(toId("scalar")))?.value.label).to.equal(
			"scalar-new",
		);
		expect((await store.get(toId("session")))?.value.label).to.equal(
			"session-new",
		);
	});

	it("keeps reads and writes in other scopes outside uncommitted rows", async () => {
		const indices = (await create()) as SQLiteIndices;
		await indices.start();
		const first = await indices.init<OrderedDocument, never>({
			schema: OrderedDocument,
			indexBy: ["id"],
		});
		const otherScope = await indices.scope("other");
		const second = await otherScope.init<OrderedDocument, never>({
			schema: OrderedDocument,
			indexBy: ["id"],
		});
		const transformGate = deferred();
		const transformStarted = deferred();
		const initialGetFinished = deferred();
		const allowFirstPut = deferred();

		try {
			const ordered = Promise.resolve(
				first.withOrderedWriteSession!(async (session) => {
					await session.get(toId("missing"));
					initialGetFinished.resolve();
					await allowFirstPut.promise;
					await session.put(new OrderedDocument({ id: "uncommitted" }));
					transformStarted.resolve();
					// Models an arbitrary async document transformer between writes.
					await transformGate.promise;
					await session.put(new OrderedDocument({ id: "after-transform" }));
				}),
			);
			await initialGetFinished.promise;
			// A leading read with no uncommitted prefix releases admission just
			// like scalar get(), so an arbitrary first transform cannot hold it.
			await second.put(new OrderedDocument({ id: "before-prefix" }));
			allowFirstPut.resolve();
			await transformStarted.promise;

			let otherWriteSettled = false;
			let otherReadSettled = false;
			const otherWrite = Promise.resolve(
				second.put(new OrderedDocument({ id: "other" })),
			).then(() => {
				otherWriteSettled = true;
			});
			const otherRead = Promise.resolve(first.get(toId("uncommitted"))).then(
				(value) => {
					otherReadSettled = true;
					return value;
				},
			);
			await new Promise((resolve) => setTimeout(resolve, 25));
			expect(otherWriteSettled).to.equal(false);
			expect(otherReadSettled).to.equal(false);

			transformGate.resolve();
			await ordered;
			expect((await otherRead)?.value.id).to.equal("uncommitted");
			await otherWrite;
			expect(await first.getSize()).to.equal(2);
			expect(await second.getSize()).to.equal(2);
		} finally {
			allowFirstPut.resolve();
			transformGate.resolve();
			await indices.stop();
			await indices.drop();
		}
	});

	it("allows awaited scalar work after flush and then resumes the session", async () => {
		const { store } = await setup<OrderedDocument>({
			schema: OrderedDocument,
		});
		await store.withOrderedWriteSession!(async (session) => {
			await session.put(new OrderedDocument({ id: "ordered-before" }));
			await session.flush();
			await store.put(new OrderedDocument({ id: "scalar-between" }));
			await session.put(new OrderedDocument({ id: "ordered-after" }));
		});
		expect(await store.getSize()).to.equal(3);
		expect(await store.get(toId("ordered-before"))).not.to.equal(undefined);
		expect(await store.get(toId("scalar-between"))).not.to.equal(undefined);
		expect(await store.get(toId("ordered-after"))).not.to.equal(undefined);
	});

	it("releases connection admission at the fixed 64-write boundary", async () => {
		const indices = (await create()) as SQLiteIndices;
		await indices.start();
		const first = await indices.init<OrderedDocument, never>({
			schema: OrderedDocument,
			indexBy: ["id"],
		});
		const otherScope = await indices.scope("other");
		const second = await otherScope.init<OrderedDocument, never>({
			schema: OrderedDocument,
			indexBy: ["id"],
		});
		const database = indices.properties.db;
		const originalExec = database.exec.bind(database);
		const firstChunkStarted = deferred();
		const releaseFirstChunk = deferred();
		let paused = false;
		database.exec = async (sql: string) => {
			const result = await originalExec(sql);
			if (!paused && sql.startsWith("SAVEPOINT peerbit_ordered_chunk_")) {
				paused = true;
				firstChunkStarted.resolve();
				await releaseFirstChunk.promise;
			}
			return result;
		};

		try {
			let competitorSettled = false;
			let competitorSeenBeforeWrite65 = false;
			const ordered = Promise.resolve(
				first.withOrderedWriteSession!(async (session) => {
					for (let i = 0; i < 65; i++) {
						await session.get(toId(`ordered-${i}`));
						if (i === 64) {
							competitorSeenBeforeWrite65 = competitorSettled;
						}
						await session.put(new OrderedDocument({ id: `ordered-${i}` }));
					}
				}),
			);
			await firstChunkStarted.promise;
			const competitor = Promise.resolve(
				second.put(new OrderedDocument({ id: "competitor" })),
			).then(() => {
				competitorSettled = true;
			});
			releaseFirstChunk.resolve();

			await Promise.all([ordered, competitor]);
			expect(competitorSeenBeforeWrite65).to.equal(true);
			expect(await first.getSize()).to.equal(65);
			expect(await second.getSize()).to.equal(1);
		} finally {
			releaseFirstChunk.resolve();
			database.exec = originalExec;
			await indices.stop();
			await indices.drop();
		}
	});

	it("rejects use after the callback and after shutdown", async () => {
		const { store } = await setup<OrderedDocument>({
			schema: OrderedDocument,
		});
		let captured: OrderedIndexWriteSession<OrderedDocument> | undefined;
		await store.withOrderedWriteSession!((session) => {
			captured = session;
		});
		await expectRejected(Promise.resolve(captured!.flush()));

		await store.stop();
		await expectRejected(
			Promise.resolve(store.withOrderedWriteSession!(() => Promise.resolve())),
		);
	});

	it("preserves an initial savepoint failure and releases admission", async () => {
		const { indices, store } = await setup<OrderedDocument>({
			schema: OrderedDocument,
		});
		const database = (indices as SQLiteIndices).properties.db;
		const originalExec = database.exec.bind(database);
		const savepointFailure = new Error("injected chunk savepoint failure");
		let failed = false;
		database.exec = (sql: string) => {
			if (!failed && sql.startsWith("SAVEPOINT peerbit_ordered_chunk_")) {
				failed = true;
				throw savepointFailure;
			}
			return originalExec(sql);
		};

		try {
			const error = await expectRejected(
				Promise.resolve(
					store.withOrderedWriteSession!((session) =>
						session.put(new OrderedDocument({ id: "never-written" })),
					),
				),
			);
			expect(error).to.equal(savepointFailure);
			expect(await store.get(toId("never-written"))).to.equal(undefined);
			await store.put(new OrderedDocument({ id: "writer-still-works" }));
			expect(await store.getSize()).to.equal(1);
		} finally {
			database.exec = originalExec;
		}
	});
});
