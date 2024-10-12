// this benchmark will test how much memory is used to store 1m documents
import { field, option, variant } from "@dao-xyz/borsh";
import { BORSH_ENCODING } from "@peerbit/log";
import { Program, type ProgramClient } from "@peerbit/program";
import { TestSession } from "@peerbit/test-utils";
import { waitFor } from "@peerbit/time";
import crypto from "crypto";
import path from "path";
import { v4 as uuid } from "uuid";
import { type Args, SharedLog } from "../../src/index.js";
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
		this.id = opts.id;
		this.name = opts.name;
		this.number = opts.number;
		this.bytes = opts.bytes;
	}
}

@variant("test_documents")
class TestStore extends Program<Partial<Args<Document>>> {
	@field({ type: SharedLog })
	docs: SharedLog<Document>;

	constructor(properties: { docs: SharedLog<Document> }) {
		super();
		this.docs = properties.docs;
	}
	async open(options?: Partial<Args<Document>>): Promise<void> {
		await this.docs.open({ ...options, encoding: BORSH_ENCODING(Document) });
	}
}

const sendReady = () => process.send!(JSON.stringify({ type: "ready" }));

let session: Awaited<ReturnType<typeof TestSession.connected>>;
try {
	let store1: TestStore | undefined = undefined;
	let store2: TestStore | undefined = undefined;

	let totalInsertCount = 0;

	process.on("message", async (message: Message) => {
		if (message.type === "init") {
			session = await TestSession.connected(2, [
				{
					directory:
						message.storage === "in-memory"
							? undefined
							: path.join("./tmp", uuid()),
				},
				{
					directory:
						message.storage === "in-memory"
							? undefined
							: path.join("./tmp", uuid()),
				},
			]);

			store1 = new TestStore({
				docs: new SharedLog<Document>(),
			});

			const client: ProgramClient = session.peers[0];
			store1 = await client.open(store1, {
				args: {
					replicate: {
						factor: 1,
					},
				},
			});
			store2 = await session.peers[1].open(store1.clone(), {
				args: {
					replicate: {
						factor: 1,
					},
				},
			});
		} else if (message.type === "insert") {
			for (let i = 0; i < message.docs; i++) {
				totalInsertCount++;
				const doc = new Document({
					id: uuid(),
					name: "hello",
					number: 1n,
					bytes: crypto.randomBytes(message.size ?? 1024),
				});
				await store1!.docs.append(doc, { meta: { next: [] }, target: "none" });
				await store1?.docs.rebalanceAll({ clearCache: true }); // force replication to happen instead of sending the appending entry directly to the remote peer
				// this will trigger the most memory intesive operation
			}
			await waitFor(() => store2!.docs.log.length === totalInsertCount);
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
