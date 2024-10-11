// this benchmark will test how much memory is used to store 1m documents
import { createStore } from "@peerbit/any-store";
import { AnyBlockStore } from "@peerbit/blocks";
import { Ed25519Keypair, randomBytes } from "@peerbit/crypto";
import { create } from "@peerbit/indexer-sqlite3";
import { TestSession } from "@peerbit/test-utils";
import path from "path";
import { v4 as uuid } from "uuid";
import { Log } from "../../src/log.js";
import type { Message } from "./utils.js";

const key = await Ed25519Keypair.create();

// Run with "node --loader ts-node/esm ./benchmark/memory/index.ts"

const sendReady = () => process.send!(JSON.stringify({ type: "ready" }));

let session: Awaited<ReturnType<typeof TestSession.connected>>;
try {
	let store: AnyBlockStore | undefined = undefined;
	let log: Log<Uint8Array> | undefined = undefined;
	process.on("message", async (message: Message) => {
		if (message.type === "init") {
			log = new Log<Uint8Array>();
			store = new AnyBlockStore(
				await createStore(
					message.storage === "in-memory"
						? undefined
						: path.join("./tmp/blocks", uuid()),
				),
			);
			await log.open(store!, key, {
				indexer: await create(
					message.storage === "in-memory"
						? undefined
						: path.join("./tmp/index", uuid()),
				),
			});
		} else if (message.type === "insert") {
			for (let i = 0; i < message.docs; i++) {
				await log!.append(randomBytes(1024), { meta: { next: [] } });
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
