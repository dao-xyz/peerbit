import { field, option, variant, vec } from "@dao-xyz/borsh";
import { randomBytes } from "@peerbit/crypto";
import { Program } from "@peerbit/program";
import { Documents, type SetupOptions } from "../src/index.js";

@variant(0)
export class Document {
	@field({ type: "string" })
	id: string;

	@field({ type: option("string") })
	name?: string;

	@field({ type: option("u64") })
	number?: bigint;

	@field({ type: vec("string") })
	tags: string[];

	@field({ type: option("bool") })
	bool?: boolean;

	@field({ type: option(Uint8Array) })
	data?: Uint8Array;

	constructor(opts: Document) {
		this.id = opts.id;
		this.name = opts.name;
		this.number = opts.number;
		this.tags = opts.tags || [];
		this.bool = opts.bool;
		this.data = opts.data;
	}
}

@variant("test_documents")
export class TestStore extends Program<Partial<SetupOptions<Document>>> {
	@field({ type: Uint8Array })
	id: Uint8Array;

	@field({ type: Documents })
	docs: Documents<Document>;

	constructor(properties: { docs: Documents<Document> }) {
		super();
		this.id = randomBytes(32);
		this.docs = properties.docs;
	}

	async open(options?: Partial<SetupOptions<Document>>): Promise<void> {
		await this.docs.open({
			...options,
			type: Document,
			index: { ...options?.index, idProperty: "id" },
		});
	}
}
