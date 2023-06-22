import { LSession } from "@peerbit/test-utils";
import { Program } from "@peerbit/program";
import { field, variant } from "@dao-xyz/borsh";

@variant("test-shared_nested")
class TestNestedProgram extends Program {
	async setup(): Promise<void> {
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

	async setup(): Promise<void> {
		return;
	}
}

describe(`shared`, () => {
	let session: LSession;

	beforeEach(async () => {
		session = await LSession.connected(2);
	});

	afterEach(async () => {
		await session.stop();
	});

	/* it("open same store twice will share instance", async () => {
		const db1 = await session.peers[0].open(new TestProgram());
		const sameDb = await session.peers[0].open(new TestProgram());
		expect(db1 === sameDb);
	});

	it("can share nested stores", async () => {
		const db1 = await client1.open(new TestProgram(0, new TestNestedProgram()));
		const db2 = await client1.open(new TestProgram(1, new TestNestedProgram()));
		expect(db1 !== db2);
		expect(db1.store === db2.store); 
	});*/

	// TODO add tests and define behaviour for cross topic programs
	// TODO add tests for shared subprogams
	// TODO add tests for subprograms that is also open as root program
});
