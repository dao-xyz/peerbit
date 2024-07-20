import { field, variant } from "@dao-xyz/borsh";
import { Program } from "@peerbit/program";
import { expect } from "chai";
import { Peerbit } from "../src/peer.js";

@variant("test-shared_nested")
class TestNestedProgram extends Program {
	openInvoked = false;
	async open(): Promise<void> {
		this.openInvoked = true;
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
		nested: TestNestedProgram = new TestNestedProgram(),
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
	let client: Peerbit;

	beforeEach(async () => {
		client = await Peerbit.create();
	});

	afterEach(async () => {
		await client.stop();
	});

	it("open same store twice will share instance", async () => {
		const db1 = await client.open(new TestProgram());
		await expect(await client.open(db1)).equal(db1);
	});

	it("open same store twice by address will throw error", async () => {
		const db1 = await client.open(new TestProgram());
		await expect(client.open(db1.address)).rejectedWith(
			`Program at ${db1.address} is already open`,
		);
	});

	it("rejects duplicate concurrently", async () => {
		const p1 = new TestProgram();
		const p2 = p1.clone();
		const db1Promise = client.open(p1);
		const db2Promise = client.open(p2);

		await db1Promise;
		//await db2Promise;
		await expect(db2Promise).rejectedWith(
			`Program at ${p1.address} is already open`,
		);
		expect(p1.nested.openInvoked).to.be.true;
		expect(p2.nested.openInvoked).to.not.be.true;
	});

	it("rejects duplicate sequentally", async () => {
		const p1 = new TestProgram();
		const p2 = p1.clone();
		const db1Promise = client.open(p1);

		await db1Promise;

		const db2Promise = client.open(p2);

		//await db2Promise;
		await expect(db2Promise).rejectedWith(
			`Program at ${p1.address} is already open`,
		);
		expect(p1.nested.openInvoked).to.be.true;
		expect(p2.nested.openInvoked).to.not.be.true;
	});

	it("replaces duplicate concurrently", async () => {
		const p1 = new TestProgram();
		const p2 = p1.clone();
		const db1Promise = client.open(p1);
		const db2Promise = client.open(p2, { existing: "replace" });

		await db1Promise;
		await db2Promise;

		expect(p1.nested.openInvoked).to.be.true;
		expect(p2.nested.openInvoked).to.be.true;
	});

	it("replace duplicate sequentially", async () => {
		const p1 = new TestProgram();
		const p2 = p1.clone();
		const db1Promise = client.open(p1);
		await db1Promise;
		const db2Promise = client.open(p2, { existing: "replace" });
		await db2Promise;

		expect(p1.nested.openInvoked).to.be.true;
		expect(p2.nested.openInvoked).to.be.true;
	});

	it("reuse duplicate concurrently", async () => {
		const p1 = new TestProgram();
		const p2 = p1.clone();
		const db1Promise = client.open(p1);
		const db2Promise = client.open(p2, { existing: "reuse" });

		await db1Promise;
		const db2Open = await db2Promise;
		expect(db2Open === p1).to.be.true;
		expect(p1.nested.openInvoked).to.be.true;
		expect(p2.nested.openInvoked).to.be.not.be.true;
	});

	it("reuse duplicate sequentially", async () => {
		const p1 = new TestProgram();
		const p2 = p1.clone();
		const db1Promise = client.open(p1);
		await db1Promise;
		const db2Open = await client.open(p2, { existing: "reuse" });
		expect(db2Open === p1).to.be.true;
		expect(p1.nested.openInvoked).to.be.true;
		expect(p2.nested.openInvoked).to.be.not.be.true;
	});

	// TODO add tests and define behaviour for cross topic programs
	// TODO add tests for shared subprogams
	// TODO add tests for subprograms that is also open as root program
});
