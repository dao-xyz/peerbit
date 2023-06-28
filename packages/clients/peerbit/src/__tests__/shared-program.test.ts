import { Program, ProgramClient } from "@peerbit/program";
import { field, variant } from "@dao-xyz/borsh";
import { Peerbit } from "../peer.js";

@variant("test-shared_nested")
class TestNestedProgram extends Program {
	async open(): Promise<void> {
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
		return;
	}
}

describe(`shared`, () => {
	let client: ProgramClient;

	beforeEach(async () => {
		client = await Peerbit.create();
	});

	afterEach(async () => {
		await client.stop();
	});

	it("open same store twice will share instance", async () => {
		const db1 = await client.open(new TestProgram());
		await expect(await client.open(db1)).toEqual(db1);
	});

	it("open same store twice by address will throw error", async () => {
		const db1 = await client.open(new TestProgram());
		await expect(() => client.open(db1.address)).rejects.toThrowError(
			`Program at ${db1.address} is already open`
		);
	});

	// TODO add tests and define behaviour for cross topic programs
	// TODO add tests for shared subprogams
	// TODO add tests for subprograms that is also open as root program
});
