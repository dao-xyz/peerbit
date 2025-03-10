import { TestSession } from "@peerbit/test-utils";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import { waitForConverged } from "./utils.js";
import { EventStore } from "./utils/stores/event-store.js";

describe(`countAssignedHeads`, function () {
	let session: TestSession;

	beforeEach(async () => {
		session = await TestSession.connected(2);
	});

	afterEach(async () => {
		await session.stop();
	});

	it("all", async () => {
		let db1 = await session.peers[0].open(new EventStore<string, any>(), {
			args: {
				replicate: {
					factor: 1,
				},
			},
		});

		await db1.add("hello");
		expect(await db1.log.countAssignedHeads()).to.eq(1);
	});

	it("partial", async () => {
		let db1 = await session.peers[0].open(new EventStore<string, any>(), {
			args: {
				replicate: {
					offset: 0,
					factor: 0.5,
				},
				replicas: {
					min: 1,
				},
			},
		});

		let db2 = await session.peers[1].open(db1.clone(), {
			args: {
				replicate: {
					offset: 0.5,
					factor: 0.5,
				},
				replicas: {
					min: 1,
				},
			},
		});

		let count = 1000;

		for (let i = 0; i < count; i++) {
			await db1.add(i.toString(), { meta: { next: [] } });
		}

		await waitForResolved(() =>
			expect(db2.log.log.length).to.be.greaterThan(0),
		);
		await waitForConverged(() => db1.log.log.length);
		await waitForConverged(() => db2.log.log.length);

		const owned1 = await db1.log.countAssignedHeads();
		const owned2 = await db2.log.countAssignedHeads();

		expect(owned1).to.be.within(count * 0.4, count * 0.6);
		expect(owned2).to.be.within(count * 0.4, count * 0.6);
	});
});

describe(`countHeads`, function () {
	describe("approximative", () => {
		let session: TestSession;

		beforeEach(async () => {
			session = await TestSession.connected(2);
		});

		afterEach(async () => {
			await session.stop();
		});

		it("throws when not replicating", async () => {
			let db1 = await session.peers[0].open(new EventStore<string, any>(), {
				args: {
					replicate: {
						factor: 1,
					},
				},
			});

			await db1.add("hello");
			expect(db1.log.countHeads({ approximate: true })).eventually.to.throw(
				"Not implemented for non-replicators",
			);
		});

		it("partial", async () => {
			let db1 = await session.peers[0].open(new EventStore<string, any>(), {
				args: {
					replicate: {
						offset: 0,
						factor: 0.5,
					},
				},
			});

			let db2 = await session.peers[1].open(db1.clone(), {
				args: {
					replicate: {
						offset: 0.5,
						factor: 0.5,
					},
				},
			});

			let count = 1000;

			for (let i = 0; i < count; i++) {
				await db1.add(i.toString(), { meta: { next: [] } });
			}

			await waitForResolved(() =>
				expect(db2.log.log.length).to.be.greaterThan(0),
			);
			await waitForConverged(() => db1.log.log.length);
			await waitForConverged(() => db2.log.log.length);

			const total1 = await db1.log.countHeads({ approximate: true });
			const total2 = await db2.log.countHeads({ approximate: true });

			expect(total1).to.be.within(count * 0.8, count * 1.2);
			expect(total2).to.be.within(count * 0.8, count * 1.2);
		});
	});
});
