import path from "node:path";
import { pathToFileURL } from "node:url";

const directory = process.argv[2];
if (!directory) throw new Error("Missing checkpoint worker directory");

const moduleUrl = pathToFileURL(
	path.join(process.cwd(), "dist", "src", "index.js"),
).href;
const { createStore } = await import(moduleUrl);
const store = createStore(directory, {
	durability: "strict",
	compactOnClose: false,
	compactMaxJournalBytes: 1,
});
await store.open();
await store.put("deleted", new Uint8Array([1, 2, 3]));
await store.put("survivor", new Uint8Array([7, 8, 9]));
await store.del("deleted");

// Every mutation above crossed strict WAL fsync. The one-byte floor forces a
// checkpoint, and the combined suffix crosses its adaptive allowance again so
// the final acknowledged state is a checkpoint with no trailing mutation. The
// parent verifies that boundary, then kills this process without close().
process.stdout.write("CHECKPOINT_ACK\n");
setInterval(() => {}, 1_000);
