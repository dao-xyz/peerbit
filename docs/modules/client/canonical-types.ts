import { field, variant } from "@dao-xyz/borsh";

@variant("my_doc")
export class MyDoc {
	@field({ type: "string" })
	id: string;

	@field({ type: "string" })
	text: string;

	constructor(properties?: { id?: string; text?: string }) {
		this.id = properties?.id ?? "";
		this.text = properties?.text ?? "";
	}
}
