import { field, fixedArray, variant } from "@dao-xyz/borsh";
import { Program } from "@peerbit/program";

export class CounterProgram extends Program<{ start?: bigint | number }> {
	id: Uint8Array;

	private value = 0n;

	constructor(properties?: { id?: Uint8Array }) {
		super();
		this.id = properties?.id ?? new Uint8Array(32);
	}

	async open(args?: { start?: bigint | number }): Promise<void> {
		if (args?.start != null && this.value === 0n) {
			this.value = BigInt(args.start);
		}
	}

	async get(): Promise<bigint> {
		return this.value;
	}

	async increment(amount: bigint | number = 1n): Promise<bigint> {
		this.value += BigInt(amount);
		return this.value;
	}
}

variant("counter_program")(CounterProgram);
field({ type: fixedArray("u8", 32) })(CounterProgram.prototype, "id");
