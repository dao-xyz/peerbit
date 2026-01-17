import { type AbstractType, field, fixedArray, variant } from "@dao-xyz/borsh";
import { Documents, type DocumentsLike } from "@peerbit/document";
import { Program } from "@peerbit/program";
import type { TestMessage } from "./types.js";

export class MixedProgram extends Program<{ type: AbstractType<TestMessage> }> {
	id: Uint8Array;

	docs!: DocumentsLike<TestMessage, TestMessage>;

	constructor(properties?: { id?: Uint8Array }) {
		super();
		this.id = properties?.id ?? new Uint8Array(32);
	}

	async open(args?: { type: AbstractType<TestMessage> }): Promise<void> {
		if (!args?.type) {
			throw new Error("MixedProgram requires args.type");
		}

		this.docs = await this.node.open(
			new Documents<TestMessage>({ id: this.id }),
			{
				args: { type: args.type },
				parent: this,
				existing: "reuse",
			},
		);
	}

	async put(doc: TestMessage): Promise<void> {
		await this.docs.put(doc);
	}

	async get(id: string): Promise<TestMessage | undefined> {
		return this.docs.get(id);
	}
}

variant("mixed_program")(MixedProgram);
field({ type: fixedArray("u8", 32) })(MixedProgram.prototype, "id");
