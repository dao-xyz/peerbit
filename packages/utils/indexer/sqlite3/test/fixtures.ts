import { field, variant } from "@dao-xyz/borsh";

export class DocumentNoVariant {
	@field({ type: "u8" })
	id: number;

	constructor(obj: any) {
		this.id = obj.id;
	}
}

@variant(0)
export class DocumentWithVariant {
	@field({ type: "u8" })
	id: number;

	constructor(obj: any) {
		this.id = obj.id;
	}
}
