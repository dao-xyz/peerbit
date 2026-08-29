import { field, variant, vec } from "@dao-xyz/borsh";
import { StringMatch, id, toId } from "@peerbit/indexer-interface";
import { expect } from "chai";
import { SQLiteIndices } from "../src/engine.js";
import { create } from "../src/index.js";
import type { Database } from "../src/types.js";
import { setup } from "./utils.js";

@variant("sqlite_put_batch_document")
class BatchDocument {
	@id({ type: "string" })
	id: string;

	@field({ type: Uint8Array })
	bytes: Uint8Array;

	@field({ type: "string" })
	label: string;

	@field({ type: vec("string") })
	tags: string[];

	@field({ type: vec("u32") })
	values: number[];

	constructor(properties: {
		id: string;
		bytes?: Uint8Array;
		label?: string;
		tags?: string[];
		values?: number[];
	}) {
		this.id = properties.id;
		this.bytes = properties.bytes ?? new Uint8Array();
		this.label = properties.label ?? "";
		this.tags = properties.tags ?? [];
		this.values = properties.values ?? [];
	}
}

const expectRejected = async (promise: Promise<unknown>) => {
	try {
		await promise;
	} catch (error) {
		return error;
	}
	throw new Error("Expected promise to reject");
};

const pauseFirstSavepoint = (database: Database) => {
	const originalExec = database.exec.bind(database);
	let release!: () => void;
	const gate = new Promise<void>((resolve) => {
		release = resolve;
	});
	let markStarted!: () => void;
	const started = new Promise<void>((resolve) => {
		markStarted = resolve;
	});
	let paused = false;
	database.exec = async (sql: string) => {
		const result = await originalExec(sql);
		if (!paused && sql.startsWith("SAVEPOINT peerbit_put_batch_")) {
			paused = true;
			markStarted();
			await gate;
		}
		return result;
	};
	return {
		release,
		started,
		restore: () => {
			database.exec = originalExec;
		},
	};
};

describe("SQLite putBatch", () => {
	it("writes nested rows in order and keeps the last duplicate id", async () => {
		const { store } = await setup<BatchDocument>({ schema: BatchDocument });
		expect(store.putBatch).to.be.a("function");

		await store.putBatch!([
			new BatchDocument({
				id: "a",
				bytes: new Uint8Array([1]),
				tags: ["old"],
				values: [1],
			}),
			new BatchDocument({
				id: "b",
				bytes: new Uint8Array([2]),
				tags: ["second"],
				values: [2, 3],
			}),
			new BatchDocument({
				id: "a",
				bytes: new Uint8Array([3, 4]),
				tags: ["new", "last"],
				values: [4, 5, 6],
			}),
		]);

		expect(await store.getSize()).to.equal(2);
		const replaced = await store.get(toId("a"));
		expect(replaced?.value.bytes).to.deep.equal(new Uint8Array([3, 4]));
		expect(replaced?.value.tags).to.deep.equal(["new", "last"]);
		expect(replaced?.value.values).to.deep.equal([4, 5, 6]);
	});

	it("accepts an empty batch", async () => {
		const { store } = await setup<BatchDocument>({ schema: BatchDocument });
		await store.putBatch!([]);
		expect(await store.getSize()).to.equal(0);
	});

	it("rolls back the current chunk and retains earlier bounded chunks", async () => {
		const { store } = await setup<BatchDocument>({ schema: BatchDocument });
		const firstChunk = Array.from(
			{ length: 64 },
			(_, index) => new BatchDocument({ id: `valid-${index}` }),
		);
		const invalid = { id: "invalid" } as unknown as BatchDocument;

		await expectRejected(
			Promise.resolve(store.putBatch!([firstChunk[0]!, invalid])),
		);
		expect(await store.getSize()).to.equal(0);

		await expectRejected(
			Promise.resolve(store.putBatch!([...firstChunk, invalid])),
		);
		expect(await store.getSize()).to.equal(64);
		await store.put(new BatchDocument({ id: "after-failure" }));
		expect(await store.getSize()).to.equal(65);
	});

	it("keeps reads and lazy planner DDL outside a rollback savepoint", async () => {
		const { indices, store } = await setup<BatchDocument>({
			schema: BatchDocument,
		});
		const database = (indices as SQLiteIndices).properties.db;
		const pause = pauseFirstSavepoint(database);

		try {
			const batch = expectRejected(
				Promise.resolve(
					store.putBatch!([
						new BatchDocument({ id: "rolled-back", label: "query-me" }),
						{ id: "invalid" } as unknown as BatchDocument,
					]),
				),
			);
			await pause.started;

			let readSettled = false;
			const read = Promise.resolve(store.get(toId("rolled-back"))).then(
				(value) => {
					readSettled = true;
					return value;
				},
			);
			let querySettled = false;
			const query = Promise.resolve(
				store
					.iterate({
						query: [new StringMatch({ key: "label", value: "query-me" })],
					})
					.all(),
			).then((results) => {
				querySettled = true;
				return results;
			});

			await new Promise((resolve) => setTimeout(resolve, 20));
			expect(readSettled).to.equal(false);
			expect(querySettled).to.equal(false);

			pause.release();
			await batch;
			expect(await read).to.equal(undefined);
			expect(await query).to.deep.equal([]);
			expect(
				await store
					.iterate({
						query: [new StringMatch({ key: "label", value: "query-me" })],
					})
					.all(),
			).to.deep.equal([]);
		} finally {
			pause.release();
			pause.restore();
		}
	});

	it("keeps another scope's table creation outside a rollback savepoint", async () => {
		const indices = (await create()) as SQLiteIndices;
		await indices.start();
		const first = await indices.init<BatchDocument, never>({
			schema: BatchDocument,
			indexBy: ["id"],
		});
		const otherScope = await indices.scope("other");
		const pause = pauseFirstSavepoint(indices.properties.db);

		try {
			const batch = expectRejected(
				Promise.resolve(
					first.putBatch!([
						new BatchDocument({ id: "rolled-back" }),
						{ id: "invalid" } as unknown as BatchDocument,
					]),
				),
			);
			await pause.started;

			let initSettled = false;
			const secondPromise = Promise.resolve(
				otherScope.init<BatchDocument, never>({
					schema: BatchDocument,
					indexBy: ["id"],
				}),
			).then((index) => {
				initSettled = true;
				return index;
			});
			await new Promise((resolve) => setTimeout(resolve, 20));
			expect(initSettled).to.equal(false);

			pause.release();
			await batch;
			const second = await secondPromise;
			await second.put(new BatchDocument({ id: "other" }));
			expect(await first.getSize()).to.equal(0);
			expect(await second.getSize()).to.equal(1);
		} finally {
			pause.release();
			pause.restore();
			await indices.stop();
			await indices.drop();
		}
	});

	it("serializes batches with writes from another scope on the same database", async () => {
		const indices = (await create()) as SQLiteIndices;
		await indices.start();
		const first = await indices.init<BatchDocument, never>({
			schema: BatchDocument,
			indexBy: ["id"],
		});
		const otherScope = await indices.scope("other");
		const second = await otherScope.init<BatchDocument, never>({
			schema: BatchDocument,
			indexBy: ["id"],
		});

		const database = indices.properties.db;
		const originalExec = database.exec.bind(database);
		let releaseSavepoint!: () => void;
		const savepointGate = new Promise<void>((resolve) => {
			releaseSavepoint = resolve;
		});
		let savepointStarted!: () => void;
		const savepointSeen = new Promise<void>((resolve) => {
			savepointStarted = resolve;
		});
		let blocked = false;
		database.exec = async (sql: string) => {
			if (!blocked && sql.startsWith("SAVEPOINT peerbit_put_batch_")) {
				blocked = true;
				savepointStarted();
				await savepointGate;
			}
			return originalExec(sql);
		};

		try {
			const batch = Promise.resolve(
				first.putBatch!([new BatchDocument({ id: "batch" })]),
			);
			await savepointSeen;

			let otherWriteSettled = false;
			const otherWrite = Promise.resolve(
				second.put(new BatchDocument({ id: "other" })),
			).then(() => {
				otherWriteSettled = true;
			});
			await new Promise((resolve) => setTimeout(resolve, 20));
			expect(otherWriteSettled).to.equal(false);

			releaseSavepoint();
			await Promise.all([batch, otherWrite]);
			expect(await first.getSize()).to.equal(1);
			expect(await second.getSize()).to.equal(1);
		} finally {
			releaseSavepoint();
			database.exec = originalExec;
			await indices.stop();
			await indices.drop();
		}
	});
});
