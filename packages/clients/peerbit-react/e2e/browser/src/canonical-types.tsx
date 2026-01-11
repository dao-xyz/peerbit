import { field, variant } from "@dao-xyz/borsh";

@variant("canonical_post")
export class CanonicalPost {
	@field({ type: "string" })
	id!: string;

	@field({ type: "string" })
	message!: string;

	constructor(properties?: { id?: string; message?: string }) {
		if (!properties) return; // borsh
		this.id = properties.id ?? `${Date.now()}-${Math.random()}`;
		this.message = properties.message ?? "";
	}
}
