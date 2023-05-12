import { field, vec } from "@dao-xyz/borsh";

export class StringArray {
	@field({ type: vec("string") })
	arr: string[];

	constructor(properties?: { arr: string[] }) {
		if (properties) {
			this.arr = properties.arr;
		}
	}
}
