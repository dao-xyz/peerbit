// this benchmark will test how much memory is used to store 1m documents
import { field, option, variant } from "@dao-xyz/borsh";
import { Program, type ProgramClient } from "@peerbit/program";
import { TestSession } from "@peerbit/test-utils";
import crypto from "crypto";
import path from "path";
import { v4 as uuid } from "uuid";
import { Documents, type SetupOptions } from "../../src/program.js";
import type { Message } from "./utils.js";

// Run with "node --loader ts-node/esm ./benchmark/memory/index.ts"

// handle io from process and parent

@variant("document")
class Document {
	@field({ type: "string" })
	id: string;

	@field({ type: option("string") })
	name?: string;

	@field({ type: option("u64") })
	number?: bigint;

	@field({ type: Uint8Array })
	bytes: Uint8Array;

	constructor(opts: Document) {
		if (opts) {
			this.id = opts.id;
			this.name = opts.name;
			this.number = opts.number;
			this.bytes = opts.bytes;
		}
	}
}

@variant("test_documents")
class TestStore extends Program<Partial<SetupOptions<Document>>> {
	@field({ type: Documents })
	docs: Documents<Document>;

	constructor(properties?: { docs: Documents<Document> }) {
		super();
		if (properties) {
			this.docs = properties.docs;
		}
	}
	async open(options?: Partial<SetupOptions<Document>>): Promise<void> {
		await this.docs.open({ ...options, type: Document });
	}
}

const peersCount = 1;

const sendReady = () => process.send!(JSON.stringify({ type: "ready" }));

let session: Awaited<ReturnType<typeof TestSession.connected>>;
try {
	let store: TestStore | undefined = undefined;

	process.on("message", async (message: Message) => {
		if (message.type === "init") {
			session = await TestSession.connected(peersCount, {
				directory:
					message.storage === "in-memory"
						? undefined
						: path.join("./tmp", uuid()),
			});

			store = new TestStore({
				docs: new Documents<Document>(),
			});

			const client: ProgramClient = session.peers[0];
			await client.open(store, {
				args: {
					replicate: {
						factor: 1,
					},
				},
			});
		} else if (message.type === "insert") {
			for (let i = 0; i < message.docs; i++) {
				const doc = new Document({
					id: uuid(),
					name: "hello",
					number: 1n,
					bytes: crypto.randomBytes(message.size ?? 1024),
				});
				await store!.docs.put(doc, { unique: true });
			}
		} else if (message.type === "done") {
			process.exit(0);
		}

		sendReady();
	});

	sendReady();

	// suspend the process until we receive a 'done' from parent
	await new Promise<void>((resolve) => {
		let listener = (message: Message) => {
			if (message.type === "done") {
				process.off("message", listener);
				resolve();
			}
		};
		process.on("message", listener);
	});
} catch (error: any) {
	throw new Error("Failed to insert: " + error?.message);
} finally {
	await session!.stop();
}
