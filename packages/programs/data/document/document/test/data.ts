import { field, option, variant, vec } from "@dao-xyz/borsh";
import { randomBytes } from "@peerbit/crypto";
import { Program } from "@peerbit/program";
import {
	type ReplicationDomain,
	type ReplicationDomainHash,
} from "@peerbit/shared-log";
import { v4 as uuid } from "uuid";
import { Documents, type Operation, type SetupOptions } from "../src/index.js";

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

	constructor(opts: Partial<Document>) {
		this.id = opts.id || uuid();
		this.name = opts.name;
		this.number = opts.number;
		this.tags = opts.tags || [];
		this.bool = opts.bool;
		this.data = opts.data;
	}
}

@variant("test_documents")
export class TestStore<
	D extends ReplicationDomain<any, Operation> = ReplicationDomainHash,
> extends Program<Partial<SetupOptions<Document, Document, D>>> {
	@field({ type: Uint8Array })
	id: Uint8Array;

	@field({ type: Documents })
	docs: Documents<Document, Document, D>;

	constructor(properties: { docs: Documents<Document, Document, D> }) {
		super();
		this.id = randomBytes(32);
		this.docs = properties.docs;
	}

	async open(
		options?: Partial<SetupOptions<Document, Document, D>>,
	): Promise<void> {
		await this.docs.open({
			...options,
			type: Document,
			domain: options?.domain,
			index: { ...options?.index, idProperty: "id" },
		});
	}
}
