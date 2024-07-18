import { field, variant } from "@dao-xyz/borsh";
import { PublicSignKey } from "@peerbit/crypto";
import { Documents } from "@peerbit/document";
import { Program } from "@peerbit/program";
import { decodeReplicas } from "@peerbit/shared-log";
import { Peerbit } from "peerbit";
import { v4 as uuid } from "uuid";

class Text {
	@field({ type: "string" })
	id: string;

	@field({ type: "string" })
	text: string;

	constructor(text: string) {
		this.id = uuid();
		this.text = text;
	}
}

type Args = { replicas?: { min?: number; max?: number } };

@variant("replicated-text-documents")
export class TextDocuments extends Program<Args> {
	@field({ type: Documents })
	documents: Documents<Text>;

	constructor() {
		super();
		this.documents = new Documents();
	}
	async open(args?: Args): Promise<void> {
		await this.documents.open({
			type: Text,
			replicas: {
				min: args?.replicas?.min ?? 2,
				max: args?.replicas?.max,
			},
			canReplicate: (publicKey: PublicSignKey) => {
				return true; // Assume anyone can be a replicator
			},

			// Don't allow operations to be appended if the have too high replication degree
			canPerform: (properties) => {
				const replicationConfig = decodeReplicas(properties.entry);
				if (replicationConfig.getValue(db.documents.log) > 10) {
					return false;
				}
				return true;
			},
		});
	}
}

const peer = await Peerbit.create();

const db = await peer.open(new TextDocuments(), {
	args: {
		// Assume the default replication degree should at least be 1 (if omitted 2 will be used)
		replicas: {
			min: 1,
			max: undefined, // If provided you can set an upper bound
		},
	},
});

// will replicate at least the amount of time provided in the open arguments
// Assuming every peer opens the DB in the same way
await db.documents.put(new Text("hello world"));

// Override replication degree on a specific document
await db.documents.put(new Text("this is very important"), { replicas: 3 });

await peer.stop();
