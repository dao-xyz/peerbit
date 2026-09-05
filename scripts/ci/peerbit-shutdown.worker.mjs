import assert from "node:assert/strict";
import { join } from "node:path";
import { Peerbit } from "../../packages/clients/peerbit/dist/src/peer.js";
import { Documents } from "../../packages/programs/data/document/document/dist/src/index.js";
import {
	Document,
	TestStore,
} from "../../packages/programs/data/document/document/dist/test/data.js";
import { randomBytes } from "../../packages/utils/crypto/dist/src/index.js";

const mode = process.argv[2];
assert.ok(mode === "memory" || mode === "disk");
const directory = process.argv[3];
assert.ok(directory);
const started = performance.now();
const mark = (phase, extra = {}) =>
	console.log(
		JSON.stringify({ phase, elapsedMs: performance.now() - started, ...extra }),
	);
process.once("beforeExit", () => {
	// Deliberately payload-free: never emit process.report environment variables.
	mark("beforeExit", { resources: process.getActiveResourcesInfo() });
});
process.once("exit", (code) => mark("exit-event", { code }));
mark("start", { node: process.version, mode });

const peers = [];
try {
	for (let i = 0; i < 3; i++) {
		peers.push(
			await Peerbit.create(
				mode === "disk" ? { directory: join(directory, String(i)) } : {},
			),
		);
	}
	for (let i = 0; i < peers.length; i++) {
		for (let j = i + 1; j < peers.length; j++) await peers[i].dial(peers[j]);
	}
	mark("connected");
	const store = new TestStore({
		docs: new Documents({ id: randomBytes(32) }),
	});
	const programs = await Promise.all(
		peers.map((peer, i) =>
			peer.open(i === 0 ? store : store.clone(), {
				args: { replicate: { factor: 1 } },
			}),
		),
	);
	await Promise.all(
		programs.flatMap((program, i) =>
			peers
				.filter((_, j) => i !== j)
				.map((remote) =>
					program.docs.log.waitForReplicator(remote.identity.publicKey, {
						timeout: 30_000,
					}),
				),
		),
	);
	mark("ready");
	await Promise.all(
		programs.map((program, i) =>
			program.docs.put(
				new Document({ id: `writer-${i}`, data: new Uint8Array([i, 3, 7]) }),
			),
		),
	);
	const deadline = performance.now() + 30_000;
	while (true) {
		let converged = true;
		for (const program of programs) {
			for (let i = 0; i < peers.length; i++) {
				const row = await program.docs.index.get(`writer-${i}`, {
					local: true,
					remote: false,
				});
				if (!row) {
					converged = false;
				} else {
					assert.deepEqual(row.data, new Uint8Array([i, 3, 7]));
				}
			}
		}
		if (converged) break;
		assert.ok(performance.now() < deadline, "exact convergence timed out");
		await new Promise((resolve) => setTimeout(resolve, 25));
	}
	mark("converged");
} finally {
	const results = await Promise.allSettled(peers.map((peer) => peer.stop()));
	for (const result of results) {
		if (result.status === "rejected") throw result.reason;
	}
	mark("stopped", { resources: process.getActiveResourcesInfo() });
}
