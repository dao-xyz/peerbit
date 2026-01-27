import { delay } from "@peerbit/time";
import { expect, use } from "chai";
import chaiAsPromised from "chai-as-promised";
import { type ProgramClient } from "../src/index.js";
import { TestParenteRefernceProgram, TestProgram } from "./samples.js";
import { creatMockPeer } from "./utils.js";

use(chaiAsPromised);

describe(`shared`, () => {
	let client: ProgramClient;

	beforeEach(async () => {
		client = await creatMockPeer();
	});

	afterEach(async () => {
		await client.stop();
	});

	it("open same store twice will share instance", async () => {
		const db1 = await client.open(new TestProgram());
		expect(await client.open(db1)).equal(db1);
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

	it("reuse open by address returns existing instance", async () => {
		const db1 = await client.open(new TestProgram());
		const db2 = await client.open(db1.address, { existing: "reuse" });
		expect(db2).equal(db1);
	});

	it("replace open by address closes old and reopens", async () => {
		const db1 = await client.open(new TestProgram());
		const address = db1.address;

		const db2 = await client.open(address, { existing: "replace" });

		expect(db1.closed).to.be.true;
		expect(db2.closed).to.be.false;
		expect(db2.address).to.equal(address);
		expect(db2).to.not.equal(db1);
	});

	it("is open on open", async () => {
		const instance = new TestProgram();
		const openFn = instance.open.bind(instance);
		let openInvoked = false;
		instance.open = async () => {
			expect(instance.closed).to.be.false;
			await openFn();
			openInvoked = true;
		};

		await client.open(instance);
		expect(openInvoked).to.be.true;
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

	it("reuse clone multiple times and close", async () => {
		const p1 = new TestProgram();
		const db1Promise = client.open(p1);
		await db1Promise;
		const p2 = await client.open(p1.clone(), { existing: "reuse" });
		const p3 = await client.open(p1.clone(), { existing: "reuse" });
		expect(p2 === p1).to.be.true;
		expect(p3 === p1).to.be.true;
		await p2.close();
		expect(p1.closed).to.be.true;
	});

	it("reuse multiple times and close", async () => {
		const p1 = new TestProgram();
		const db1Promise = client.open(p1);
		await db1Promise;
		const p2 = await client.open(p1, { existing: "reuse" });
		const p3 = await client.open(p1, { existing: "reuse" });
		expect(p2 === p1).to.be.true;
		expect(p3 === p1).to.be.true;
		await p2.close();
		expect(p1.closed).to.be.true;
	});

	it("will not resave if address already exists", async () => {
		const p1 = new TestProgram();
		await client.open(p1);
		const p1Clone = p1.clone();
		let saveCalled = false;
		const put = client.services.blocks.put.bind(client.services.blocks);
		client.services.blocks.put = async (o) => {
			saveCalled = true;
			return put(o);
		};
		await client.open(p1Clone, { existing: "reuse" });
		expect(saveCalled).to.be.false;
	});

	it("will not resave if opened with address", async () => {
		const p1 = new TestProgram();
		await client.open(p1);
		await p1.close();

		const put = client.services.blocks.put.bind(client.services.blocks);
		let saveCalled = false;
		client.services.blocks.put = async (block) => {
			saveCalled = true;
			return put(block);
		};
		const openedAgain = await client.open(p1.address, { existing: "reuse" });
		expect(openedAgain.closed).to.be.false;
		expect(openedAgain.address).to.exist;
		expect(saveCalled).to.be.false;
	});

	it("save reset", async () => {
		const p1 = new TestProgram();
		await client.open(p1);
		expect(p1.address).to.exist;
		const address = p1.address;

		expect(client.services.blocks.has(address)).to.be.true;

		p1.id = 333;
		try {
			await p1.save(client.services.blocks);
			throw new Error("Expected error to throw");
		} catch (error: any) {
			expect(error.message).to.equal(
				"Program properties has been changed after constructor so that the hash has changed. Make sure that the 'setup(...)' function does not modify any properties that are to be serialized",
			);
		}

		await p1.save(client.services.blocks, { reset: true });
		expect(client.services.blocks.has(address)).to.be.false;
		expect(p1.address).to.exist;
		expect(client.services.blocks.has(p1.address)).to.be.true;
	});

	it("will address children when opening with parent reference", async () => {
		const p1 = new TestParenteRefernceProgram();
		await client.open(p1);

		expect(p1.nested.closed).to.be.false;
		expect(p1.nested.address).to.exist;

		await p1.close();
		expect(p1.nested.closed).to.be.true;
		expect(p1.closed).to.be.true;
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

	it("throws whe open an already open program with another client", async () => {
		const p1 = new TestProgram();
		await client.open(p1);
		await expect(
			creatMockPeer().then((c) => c.open(p1)),
		).eventually.rejectedWith(
			`Program at ${p1.address} is already opened with a different client`,
		);
	});

	it("can reopen", async () => {
		const put = client.services.blocks.put.bind(client.services.blocks);
		let putCalls = 0;
		client.services.blocks.put = async (block) => {
			putCalls++;
			return put(block);
		};

		const p1 = new TestProgram();
		await client.open(p1);

		expect(p1.nested.closed).to.be.false;
		expect(p1.nested.address).to.exist;

		await p1.close();
		expect(p1.nested.closed).to.be.true;
		expect(p1.closed).to.be.true;

		const p1Again = await client.open(p1.clone(), { existing: "reuse" });

		expect(p1Again.nested.closed).to.be.false;
		expect(p1Again.nested.address).to.exist;

		expect(putCalls).to.equal(1);

		expect(await client.services.blocks.has(p1Again.nested.address)).to.be
			.false;
	});

	it("can reopen weak references", async () => {
		const put = client.services.blocks.put.bind(client.services.blocks);
		let putCalls = 0;
		client.services.blocks.put = async (block) => {
			putCalls++;
			return put(block);
		};

		const p1 = new TestParenteRefernceProgram();
		await client.open(p1);

		expect(p1.nested.closed).to.be.false;
		expect(p1.nested.address).to.exist;

		await p1.close();
		expect(p1.nested.closed).to.be.true;
		expect(p1.closed).to.be.true;
		expect(await client.services.blocks.has(p1.nested.address)).to.be.true;

		const p1Again = await client.open(p1.clone(), { existing: "reuse" });

		expect(p1Again.nested.closed).to.be.false;
		expect(p1Again.nested.address).to.exist;

		expect(putCalls).to.equal(2);

		expect(await client.services.blocks.has(p1Again.nested.address)).to.be.true;
		await p1Again.drop();
		expect(await client.services.blocks.has(p1Again.nested.address)).to.be
			.false;
	});

	// TODO add tests and define behaviour for cross topic programs
	// TODO add tests for shared subprogams
	// TODO add tests for subprograms that is also open as root program

	describe("parent option", () => {
		it("second open should wait for first open to complete", async () => {
			const parent = new TestProgram(999);
			await client.open(parent);

			const child = new TestProgram(1);

			let childOpenStarted = false;
			let childOpenCompleted = false;
			const originalOpen = child.open.bind(child);

			child.open = async (args) => {
				childOpenStarted = true;
				await delay(150);
				await originalOpen(args);
				childOpenCompleted = true;
			};

			const openPromise1 = client.open(child, {
				parent: parent,
				existing: "reuse",
			});

			while (!childOpenStarted) {
				await delay(5);
			}
			expect(childOpenCompleted).to.be.false;

			const openPromise2 = client.open(child, {
				parent: parent,
				existing: "reuse",
			});
			const result2 = await openPromise2;

			expect(childOpenCompleted).to.be.true;

			const result1 = await openPromise1;
			expect(result1).to.equal(child);
			expect(result2).to.equal(child);
		});
	});

	describe("stop", () => {
		it("waits for in-progress parent opens to complete", async () => {
			const parent = new TestProgram(999);
			await client.open(parent);

			const child = new TestProgram(1);

			let childOpenStarted = false;
			let childOpenCompleted = false;
			const originalOpen = child.open.bind(child);

			child.open = async (args) => {
				childOpenStarted = true;
				await delay(100);
				await originalOpen(args);
				childOpenCompleted = true;
			};

			// Start opening but don't await
			const openPromise = client.open(child, { parent: parent });

			// Wait for open to start
			while (!childOpenStarted) {
				await delay(5);
			}
			expect(childOpenCompleted).to.be.false;

			// Stop should wait for the in-progress open to complete
			await client.stop();

			// After stop, the open should have completed
			expect(childOpenCompleted).to.be.true;

			// The promise should resolve (not reject)
			const result = await openPromise;
			expect(result).to.equal(child);
		});

		it("cleans up state so restart works correctly", async () => {
			const p1 = new TestProgram(1);

			let openStarted = false;
			const originalOpen = p1.open.bind(p1);

			p1.open = async (args) => {
				openStarted = true;
				await delay(50);
				await originalOpen(args);
			};

			// Start opening
			const openPromise = client.open(p1);

			// Wait for open to start
			while (!openStarted) {
				await delay(5);
			}

			// Stop while open is in progress
			await client.stop();
			await openPromise;

			// Create a new client (simulating restart)
			client = await creatMockPeer();

			// Should be able to open a new program with same structure
			const p2 = new TestProgram(2);
			const result = await client.open(p2);
			expect(result).to.equal(p2);
			expect(result.closed).to.be.false;
		});

		it("waits for multiple in-progress opens", async () => {
			const parent = new TestProgram(999);
			await client.open(parent);

			const children = [
				new TestProgram(1),
				new TestProgram(2),
				new TestProgram(3),
			];

			const openStarted = [false, false, false];
			const openCompleted = [false, false, false];

			children.forEach((child, i) => {
				const originalOpen = child.open.bind(child);
				child.open = async (args) => {
					openStarted[i] = true;
					await delay(50 + i * 30); // Staggered delays
					await originalOpen(args);
					openCompleted[i] = true;
				};
			});

			// Start all opens
			const promises = children.map((child) =>
				client.open(child, { parent: parent }),
			);

			// Wait for all to start
			while (!openStarted.every(Boolean)) {
				await delay(5);
			}

			// None should be completed yet
			expect(openCompleted.some(Boolean)).to.be.false;

			// Stop should wait for all
			await client.stop();

			// All should be completed after stop
			expect(openCompleted.every(Boolean)).to.be.true;

			// All promises should resolve
			await Promise.all(promises);
		});

		it("handles failing opens gracefully", async () => {
			const parent = new TestProgram(999);
			await client.open(parent);

			const child = new TestProgram(1);

			let openStarted = false;
			child.open = async () => {
				openStarted = true;
				await delay(50);
				throw new Error("Simulated open failure");
			};

			// Start opening (will fail)
			const openPromise = client.open(child, { parent: parent });

			// Wait for open to start
			while (!openStarted) {
				await delay(5);
			}

			// Stop should not throw even though open will fail
			await client.stop();

			// The open promise should reject
			await expect(openPromise).to.be.rejectedWith("Simulated open failure");
		});
	});
});
