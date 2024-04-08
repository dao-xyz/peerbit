import { TestSession } from "@peerbit/test-utils";
import { EventStore } from "./utils/stores/index.js";
import { expect } from "chai";

describe("recover", () => {
	let session: TestSession;
	let db1: EventStore<string>;

	beforeEach(async () => {
		session = await TestSession.connected(1, {
			directory: "./tmp/shared-log/recover/" + new Date()
		});

		db1 = await session.peers[0].open(new EventStore<string>());
	});

	afterEach(async () => {
		if (db1) await db1.drop();
		await session.stop();
	});

	it("can recover from too strict access log", async () => {
		const a = await db1.add("a");
		const b = await db1.add("b");
		const c = await db1.add("c");
		expect(db1.log.log.length).equal(3);
		await db1.close();
		db1 = await session.peers[0].open(new EventStore<string>({ id: db1.id }), {
			args: {
				canAppend: (entry) => {
					if ([a, b, c].map((x) => x.entry.hash).includes(entry.hash)) {
						return false;
					}
					return true;
				}
			}
		});

		await db1.add("d");
		expect(db1.log.log.length).equal(1);
		await db1.log.log.recover();
		expect(db1.log.log.length).equal(1);

		await db1.close();

		// remove ACL
		db1 = await session.peers[0].open(new EventStore<string>({ id: db1.id }));

		expect(db1.log.log.length).equal(1); // because heads from last session was only one
		await db1.log.log.recover(); // will load previously non-allowed heads (3)
		expect(db1.log.log.length).equal(4); // 3 + 1
	});
});
