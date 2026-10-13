import { serialize } from "@dao-xyz/borsh";
import { EntryType } from "@peerbit/log";
import { Peerbit } from "peerbit";

const distModule = (relativePath) =>
	new URL(`../dist/${relativePath}`, import.meta.url).href;
const { Documents } = await import(distModule("src/program.js"));
const { Document } = await import(distModule("test/data.js"));
const [directory, native] = process.argv.slice(2);
if (!directory || !process.send) throw new Error("Expected directory and IPC");
const peer = await Peerbit.create({ directory });
const docs = await peer.open(
	new Documents({
		format: {
			version: "owner-proofs-v1",
			owner: peer.identity.publicKey,
			keyType: "string",
		},
	}),
	{
		args: {
			type: Document,
			nativeGraph: native === "true",
			timeUntilRoleMaturity: 0,
		},
	},
);
const first = await docs.put(new Document({ id: "victim", name: "first" }));
const second = await docs.put(new Document({ id: "victim", name: "second" }));
await docs.put(new Document({ id: "survivor", name: "unchanged" }));

// This is the precise crash window: the marker's block and lower row have
// crossed durability barriers, but none of its exact target retirement ran.
docs.log.log.entryIndex.completeExactDelete = async () => {
	const markers = (await docs.log.log.getHeads(true).all()).filter(
		(entry) => entry.meta.type === EntryType.CUT,
	);
	if (markers.length !== 1) throw new Error("Expected one durable marker");
	process.send({
		manifest: Buffer.from(serialize(docs)).toString("base64"),
		marker: markers[0].hash,
		targets: [first.entry.hash, second.entry.hash],
	});
	await new Promise(() => {});
};
await docs.del("victim");
throw new Error("Crash fence unexpectedly resumed");
