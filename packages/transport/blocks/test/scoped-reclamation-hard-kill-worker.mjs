const { createStore } = await import("@peerbit/any-store");
const { createBuiltInScopedReclamationBlockStore } = await import(
	new URL("../dist/src/scoped-reclamation.js", import.meta.url).href
);

const [directory, mode, expectedCid] = process.argv.slice(2);
if (!directory || (mode !== "retain" && mode !== "release")) {
	throw new Error("Expected a directory and retain/release mode");
}

const store = createBuiltInScopedReclamationBlockStore(createStore(directory));
await store.start();
const scopeKey = new Uint8Array(32);
scopeKey[31] = 1;
const scope = store.localReclamation.openScope(scopeKey);
const bytes = new Uint8Array([71, 72, 73, 74]);
let cid;
if (mode === "retain") {
	cid = await scope.put(bytes);
} else {
	cid = expectedCid;
	if (!cid) throw new Error("Expected the retained CID in release mode");
	const result = await scope.release(cid);
	if (result !== "reclaimed") {
		throw new Error(`Expected reclaimed, received ${result}`);
	}
}

process.stdout.write(`${JSON.stringify({ event: "ack", mode, cid })}\n`);
setInterval(() => undefined, 1_000);
