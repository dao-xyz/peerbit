import "reflect-metadata";
import { field, variant } from "@dao-xyz/borsh";
import { sha256Sync } from "@peerbit/crypto";
import { Documents } from "@peerbit/document";
import { Program } from "@peerbit/program";

@variant(0)
export class PartyMessage {
	@field({ type: "string" })
	id!: string;

	@field({ type: "string" })
	author!: string;

	@field({ type: "string" })
	content!: string;

	@field({ type: "u64" })
	timestamp!: bigint;

	constructor(props?: {
		id?: string;
		author: string;
		content: string;
		timestamp: bigint;
	}) {
		if (!props) return;
		this.id = props.id ?? `${props.author}-${Number(props.timestamp)}`;
		this.author = props.author;
		this.content = props.content;
		this.timestamp = props.timestamp;
	}
}

@variant(0)
export class PartyMessageIndex {
	@field({ type: "string" })
	id!: string;

	@field({ type: "string" })
	author!: string;

	@field({ type: "u64" })
	timestamp!: bigint;

	constructor(props?: PartyMessage) {
		if (!props) return;
		this.id = props.id;
		this.author = props.author;
		this.timestamp = props.timestamp;
	}
}

@variant("document-react-party-store")
export class PartyDocumentStore extends Program<{ replicate?: boolean }> {
	@field({ type: Documents })
	documents: Documents<PartyMessage, PartyMessageIndex>;

	constructor() {
		super();
		this.documents = new Documents<PartyMessage, PartyMessageIndex>();
	}

	async open(args?: { replicate?: boolean }): Promise<void> {
		await this.documents.open({
			type: PartyMessage,
			index: { type: PartyMessageIndex },
			replicate: args?.replicate ? { factor: 1 } : false,
		});
	}

	static createFixed() {
		const deterministicId = new Uint8Array(
			sha256Sync(new TextEncoder().encode("document-react-party")),
		);
		const store = new PartyDocumentStore();
		store.documents = new Documents<PartyMessage, PartyMessageIndex>({
			id: deterministicId,
		});
		return store;
	}
}
