import { expect, use } from "chai";
import chaiAsPromised from "chai-as-promised";
import { type ProgramClient } from "../src/index.js";
import { TestProgram } from "./samples.js";
import { createPeer } from "./utils.js";

use(chaiAsPromised);

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
		await expect(await client.open(db1)).equal(db1);
	});

	it("can open different dbs concurrently", async () => {
		let timeout = 3000;
		let t0 = +new Date();
		const nonExisting = client.open(
			"zb2rhXREnAbm5Twtm2ahJM7QKT6FoQGNksWv5jp7o5W6BQ7ax",
			{ timeout },
		);
		expect(await client.open(new TestProgram())).to.exist;
		let t1 = +new Date();

		expect(t1 - t0).lessThan(timeout); // Because db1 will be opened concurrently
		await expect(nonExisting).rejectedWith(
			"Failed to resolve program with address: zb2rhXREnAbm5Twtm2ahJM7QKT6FoQGNksWv5jp7o5W6BQ7ax",
		);
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
		expect(p2.nested.openInvoked).to.not.be.true;
	});

	it("reuse duplicate sequentially", async () => {
		const p1 = new TestProgram();
		const p2 = p1.clone();
		const db1Promise = client.open(p1);
		await db1Promise;
		const db2Open = await client.open(p2, { existing: "reuse" });
		expect(db2Open === p1).to.be.true;
		expect(p1.nested.openInvoked).to.be.true;
		expect(p2.nested.openInvoked).to.not.be.true;
	});

	it("rejects", async () => {
		const someParent = new TestProgram();
		await expect(client.open(someParent, { parent: someParent })).rejectedWith(
			"Parent program can not be equal to the program",
		);
	});

	it("opens when existing is not in items", async () => {
		const someParent = new TestProgram();
		await client.open(someParent);

		const p1 = new TestProgram(1);
		await client.open(p1, { parent: someParent });

		let didReopen = false;
		const pOpen = p1.open.bind(p1);
		p1.open = () => {
			didReopen = true;
			return pOpen();
		};
		const db2Open = await client.open(p1);
		expect(db2Open).equal(p1);
		expect(didReopen).equal(false);
		expect(p1.closed).to.be.false;
		await client.stop();
		expect(p1.closed).to.be.true;
	});

	// TODO add tests and define behaviour for cross topic programs
	// TODO add tests for shared subprogams
	// TODO add tests for subprograms that is also open as root program
});
