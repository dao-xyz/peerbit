import { expect } from "chai";
import sinon from "sinon";
import { create } from "../src/sqlite3.wasm.js";

const isNode = typeof process !== "undefined" && !!process.versions?.node;
// Exercise OO1 itself in both browser and worker runners, not the Node adapter.
const describeWasm = isNode ? describe.skip : describe;
const thrown = (operation: () => unknown): unknown => {
	try {
		operation();
	} catch (error) {
		return error;
	}
	throw new Error("Expected the statement operation to throw");
};

describeWasm("WASM statement error lifecycle", () => {
	let db: Awaited<ReturnType<typeof create>> | undefined;
	beforeEach(async () => {
		db = await create();
		await db.open();
	});
	afterEach(async () => {
		sinon.restore();
		await db?.close();
		db = undefined;
	});

	for (const method of ["all", "get"] as const) {
		it(`preserves a real ${method} error and reuses the reset statement`, async () => {
			db!.exec("CREATE TABLE rows (id INTEGER)");
			const sql = "SELECT id FROM rows";
			const statement = await db!.prepare(sql, "read");
			db!.exec("DROP TABLE rows");
			let originalError: unknown;
			const step = statement.statement.step.bind(statement.statement);
			const probe = sinon.stub(statement.statement, "step").callsFake(() => {
				try {
					return step();
				} catch (error) {
					originalError = error;
					throw error;
				}
			});
			const error = thrown(() => statement[method]([]));
			expect(error).to.equal(originalError).and.be.instanceOf(Error);
			expect((error as Error).message).to.include("no such table");
			probe.restore();
			db!.exec("CREATE TABLE rows (id INTEGER); INSERT INTO rows VALUES (7)");
			expect(await db!.prepare(sql, "read")).to.equal(statement);
			expect(statement[method]([])).to.deep.equal(
				method === "all" ? [{ id: 7 }] : { id: 7 },
			);
			await db!.close();
			expect(db!.status()).to.equal("closed");
			expect(db!.statements.size).to.equal(0);
		});
	}

	it("preserves a real constraint error and reuses the reset write", async () => {
		db!.exec("CREATE TABLE rows (id INTEGER PRIMARY KEY)");
		const sql = "INSERT INTO rows VALUES (?)";
		const statement = await db!.prepare(sql, "write");
		statement.run([1]);
		const error = thrown(() => statement.run([1]));
		expect(error).to.be.instanceOf(Error);
		expect((error as Error).message).to.include("UNIQUE constraint failed");
		expect(await db!.prepare(sql, "write")).to.equal(statement);
		statement.run([2]);
		const read = await db!.prepare("SELECT id FROM rows ORDER BY id", "read");
		expect(read.all([])).to.deep.equal([{ id: 1 }, { id: 2 }]);
		await db!.close();
		expect(db!.status()).to.equal("closed");
	});

	it("resets after an ordinary error and preserves its identity", async () => {
		const statement = await db!.prepare("SELECT 7 AS value", "read");
		const primary = new Error("row read failed");
		const get = sinon.stub(statement.statement, "get").throws(primary);
		const reset = sinon.spy(statement.statement, "reset");
		expect(thrown(() => statement.get())).to.equal(primary);
		expect(reset.callCount).to.equal(1);
		get.restore();
		expect(statement.get()).to.deep.equal({ value: 7 });
	});

	it("retains distinct execution and reset errors even with matching code properties", async () => {
		const statement = await db!.prepare("SELECT 7 AS value", "read");
		const primary = Object.assign(new Error("row read failed"), {
			resultCode: 1,
		});
		const cleanup = Object.assign(new Error("reset failed"), { resultCode: 1 });
		const get = sinon.stub(statement.statement, "get").throws(primary);
		const reset = statement.statement.reset.bind(statement.statement);
		const resetProbe = sinon
			.stub(statement.statement, "reset")
			.callsFake(() => {
				reset();
				throw cleanup;
			});
		const error = thrown(() => statement.get());
		expect(error).to.be.instanceOf(AggregateError);
		expect((error as AggregateError).errors).to.deep.equal([primary, cleanup]);
		get.restore();
		resetProbe.restore();
		expect(statement.get()).to.deep.equal({ value: 7 });
	});

	it("propagates a reset failure after a successful read", async () => {
		const statement = await db!.prepare("SELECT 7 AS value", "read");
		const cleanup = new Error("reset failed");
		const reset = statement.statement.reset.bind(statement.statement);
		const probe = sinon.stub(statement.statement, "reset").callsFake(() => {
			reset();
			throw cleanup;
		});
		expect(thrown(() => statement.all([]))).to.equal(cleanup);
		probe.restore();
		expect(statement.all([])).to.deep.equal([{ value: 7 }]);
	});
});
