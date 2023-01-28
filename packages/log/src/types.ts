import { field, vec } from "@dao-xyz/borsh";

export class BigIntObject {
	@field({ type: "u64" })
	size: bigint;

	constructor(properties?: { size: bigint }) {
		if (properties) {
			this.size = properties.size;
		}
	}
}

export class StringArray {
	@field({ type: vec("string") })
	arr: string[];

	constructor(properties?: { arr: string[] }) {
		if (properties) {
			this.arr = properties.arr;
		}
	}
}
