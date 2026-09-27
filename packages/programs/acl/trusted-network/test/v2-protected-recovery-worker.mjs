// Fresh-process offline recovery: no parent-process resolver or anchor can survive.
import { deserialize } from "@dao-xyz/borsh";
import { calculateRawCid } from "@peerbit/blocks-interface";
import { readFile } from "node:fs/promises";

// Load precisely the built code under test, without a TS loader in the child.
const { NetworkDescriptorV2 } = await import(
	new URL("../dist/src/v2.js", import.meta.url).href
);
const { openRecoveryReplica } = await import(
	new URL("../dist/test/utils/v2-protected-recovery.js", import.meta.url).href
);

const configuration = JSON.parse(await readFile(process.argv[2], "utf8"));
for (const directory of configuration.directories) {
	const descriptor = deserialize(
		Buffer.from(configuration.descriptor, "base64"),
		NetworkDescriptorV2,
	);
	const replica = await openRecoveryReplica(directory, descriptor);
	try {
		let bytesVerified = 0;
		for (const cid of configuration.retainedCids) {
			const bytes = replica.projection.get(cid);
			if (!bytes || (await calculateRawCid(bytes)).cid !== cid)
				throw new Error(`Missing or changed retained bytes: ${cid}`);
			bytesVerified++;
		}
		const result = await replica.projection.withDocuments(
			configuration.fenceCid,
			/** @param {import('../src/v2-resource-document-projection.js').ProtectedDocumentViewV2} view */
			(view) => view,
		);
		console.log(
			"RECOVERED " +
				JSON.stringify(
					{
						connections: replica.node.getConnections().length,
						result,
						retainedCids: [...replica.projection.entries()].sort(),
						bytesVerified,
					},
					(_key, value) =>
						typeof value === "bigint"
							? value.toString()
							: value instanceof Uint8Array
								? [...value]
								: value,
				),
		);
	} finally {
		await replica.close();
	}
}
// Natural process exit is part of the test; no process.exit or live keepalive.
