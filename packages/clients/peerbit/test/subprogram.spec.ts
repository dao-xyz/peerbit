import { field, variant } from "@dao-xyz/borsh";
import { Program } from "@peerbit/program";
import { Peerbit } from "../src/peer.js";

@variant("child")
class SubProgram extends Program {
	constructor() {
		super();
	}
	async open() {}
}

@variant("parent")
class ParentProgram extends Program {
	@field({ type: SubProgram })
	subprogram: SubProgram;

	constructor() {
		super();
		this.subprogram = new SubProgram();
	}
	async open(): Promise<void> {
		await this.node.open(this.subprogram, { parent: this });
	}
}

describe("subprogram", () => {
	let client: Peerbit;
	beforeEach(async () => {
		client = await Peerbit.create();
	});

	afterEach(async () => {
		await client.stop();
	});

	it("subprogram can open on parent open", async () => {
		// if this never resolved then we have a deadlock
		await client.open(new ParentProgram());
	});
});
