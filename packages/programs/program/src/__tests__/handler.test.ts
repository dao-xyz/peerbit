import { field, variant } from "@dao-xyz/borsh";
import { ProgramHandler, ProgramClient, Program } from "../index.js";
import { createPeer } from "./utils.js";

@variant("test-shared_nested")
class TestNestedProgram extends Program {
	openInvoked = false;
	async open(): Promise<void> {
		this.openInvoked = true;
		return;
	}
}

@variant("test-shared")
class TestProgram extends Program {
	@field({ type: "u32" })
	id: number;

	@field({ type: TestNestedProgram })
	nested: TestNestedProgram;

	constructor(
		id: number = 0,
		nested: TestNestedProgram = new TestNestedProgram()
	) {
		super();
		this.id = id;
		this.nested = nested;
	}

	async open(): Promise<void> {
		return this.nested.open();
	}
}

describe(`shared`, () => {
	let client: ProgramClient;

	beforeEach(async () => {
		client = await createPeer();
	});

	afterEach(async () => {
		await client.stop();
	});

	it("open same store twice will share instance", async () => {
		const db1 = await client.open(new TestProgram());
		await expect(await client.open(db1)).toEqual(db1);
	});

	it("can open different dbs concurrently", async () => {
		let timeout = 3000;
		let t0 = +new Date();
		const nonExisting = client.open(
			"zb2rhXREnAbm5Twtm2ahJM7QKT6FoQGNksWv5jp7o5W6BQ7ax",
			{ timeout }
		);
		expect(await client.open(new TestProgram())).toBeDefined();
		let t1 = +new Date();

		expect(t1 - t0).toBeLessThan(timeout); // Because db1 will be opened concurrently
		await expect(nonExisting).rejects.toThrowError(
			"Failed to resolve program with address: zb2rhXREnAbm5Twtm2ahJM7QKT6FoQGNksWv5jp7o5W6BQ7ax"
		);
	});

	it("open same store twice by address will throw error", async () => {
		const db1 = await client.open(new TestProgram());
		await expect(() => client.open(db1.address)).rejects.toThrowError(
			`Program at ${db1.address} is already open`
		);
	});

	it("rejects duplicate concurrently", async () => {
		const p1 = new TestProgram();
		const p2 = p1.clone();
		const db1Promise = client.open(p1);
		const db2Promise = client.open(p2);

		await db1Promise;
		//await db2Promise;
		await expect(db2Promise).rejects.toThrowError(
			`Program at ${p1.address} is already open`
		);
		expect(p1.nested.openInvoked).toBeTruthy();
		expect(p2.nested.openInvoked).toBeFalsy();
	});

	it("rejects duplicate sequentally", async () => {
		const p1 = new TestProgram();
		const p2 = p1.clone();
		const db1Promise = client.open(p1);

		await db1Promise;

		const db2Promise = client.open(p2);

		//await db2Promise;
		await expect(db2Promise).rejects.toThrowError(
			`Program at ${p1.address} is already open`
		);
		expect(p1.nested.openInvoked).toBeTruthy();
		expect(p2.nested.openInvoked).toBeFalsy();
	});

	it("replaces duplicate concurrently", async () => {
		const p1 = new TestProgram();
		const p2 = p1.clone();
		const db1Promise = client.open(p1);
		const db2Promise = client.open(p2, { existing: "replace" });

		await db1Promise;
		await db2Promise;

		expect(p1.nested.openInvoked).toBeTruthy();
		expect(p2.nested.openInvoked).toBeTruthy();
	});

	it("replace duplicate sequentially", async () => {
		const p1 = new TestProgram();
		const p2 = p1.clone();
		const db1Promise = client.open(p1);
		await db1Promise;
		const db2Promise = client.open(p2, { existing: "replace" });
		await db2Promise;

		expect(p1.nested.openInvoked).toBeTruthy();
		expect(p2.nested.openInvoked).toBeTruthy();
	});

	it("reuse duplicate concurrently", async () => {
		const p1 = new TestProgram();
		const p2 = p1.clone();
		const db1Promise = client.open(p1);
		const db2Promise = client.open(p2, { existing: "reuse" });

		await db1Promise;
		const db2Open = await db2Promise;
		expect(db2Open == p1).toBeTrue();
		expect(p1.nested.openInvoked).toBeTruthy();
		expect(p2.nested.openInvoked).toBeFalsy();
	});

	it("reuse duplicate sequentially", async () => {
		const p1 = new TestProgram();
		const p2 = p1.clone();
		const db1Promise = client.open(p1);
		await db1Promise;
		const db2Open = await client.open(p2, { existing: "reuse" });
		expect(db2Open == p1).toBeTrue();
		expect(p1.nested.openInvoked).toBeTruthy();
		expect(p2.nested.openInvoked).toBeFalsy();
	});

	// TODO add tests and define behaviour for cross topic programs
	// TODO add tests for shared subprogams
	// TODO add tests for subprograms that is also open as root program
});
