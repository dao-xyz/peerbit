import { field, option, variant, vec } from "@dao-xyz/borsh";
import { randomBytes } from "@peerbit/crypto";
import { ShallowEntry } from "@peerbit/log";
import { Program } from "@peerbit/program";
import {
	type ReplicationDomain,
	type ReplicationDomainHash,
} from "@peerbit/shared-log";
import { v4 as uuid } from "uuid";
import {
	Documents,
	type Operation,
	type SetupOptions,
	isPutOperation,
} from "../src/index.js";

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

type RangeArgs = { from: number; to: number };
type CustomDomain = ReplicationDomain<RangeArgs, Operation>;
const createDomain = (
	db: Documents<Document, Document, CustomDomain>,
): CustomDomain => {
	return {
		type: "custom",
		fromArgs(args, log) {
			if (!args) {
				return { offset: log.node.identity.publicKey };
			}
			return {
				offset: args.from,
				length: args.to - args.from,
			};
		},
		fromEntry: async (entry) => {
			const item = await (
				entry instanceof ShallowEntry ? await db.log.log.get(entry.hash) : entry
			)?.getPayloadValue();
			if (!item) {
				// @eslint-ignore no-console
				console.error("Item not found");
				// max u32 (pu tit at the end)
				return 0xffffffff;
			}

			if (isPutOperation(item)) {
				const document = db.index.valueEncoding.decoder(item.data);
				return document.number ? Number(document.number) : 0;
			}

			// else max u32 (pu tit at the end)
			return 0xffffffff;
		},
	};
};

@variant("StoreWithCustomDomain")
export class StoreWithCustomDomain extends Program {
	@field({ type: Documents })
	docs: Documents<Document, Document, CustomDomain>;

	constructor(properties?: {
		docs?: Documents<Document, Document, CustomDomain>;
	}) {
		super();
		this.docs =
			properties?.docs || new Documents<Document, Document, CustomDomain>();
	}

	async open(
		args?: Partial<SetupOptions<Document, Document, CustomDomain>>,
	): Promise<void> {
		return this.docs.open({
			...(args || {}),
			domain: createDomain(this.docs),
			type: Document,
		});
	}
}
