import { deserialize } from "@dao-xyz/borsh";
import { toId } from "@peerbit/indexer-interface";
import { expect } from "chai";
import { type ChildProcess, fork } from "node:child_process";
import { once } from "node:events";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Peerbit } from "peerbit";
import { Documents } from "../src/program.js";
import { Document } from "./data.js";

describe("owner-proofs Documents interrupted retirement", function () {
	this.timeout(30_000);
	for (const nativeGraph of [false, true]) {
		it(`replays durable marker cleanup after SIGKILL (${nativeGraph ? "native" : "JS"})`, async () => {
			const directory = await mkdtemp(join(tmpdir(), "peerbit-proof-kill-"));
			let child: ChildProcess | undefined;
			let peer: Peerbit | undefined;
			let timer: ReturnType<typeof setTimeout> | undefined;
			try {
				child = fork(
					new URL(
						"../../test/proof-documents-hard-kill-worker.mjs",
						import.meta.url,
					),
					[directory, String(nativeGraph)],
					{
						execArgv: [],
						stdio: ["ignore", "pipe", "pipe", "ipc"],
					},
				);
				let stderr = "";
				child.stderr!.on("data", (chunk) => {
					stderr += chunk.toString();
				});
				const exited = once(child, "exit");
				const captured = await new Promise<{
					manifest: string;
					marker: string;
					targets: string[];
				}>((resolve, reject) => {
					child!.once("message", (message) => resolve(message as any));
					child!.once("error", reject);
					child!.once("exit", (code, signal) =>
						reject(
							new Error(
								`Worker exited before crash fence: ${code}/${signal}: ${stderr}`,
							),
						),
					);
					timer = setTimeout(
						() => reject(new Error(`Crash fence timed out: ${stderr}`)),
						15_000,
					);
				});
				clearTimeout(timer);
				expect(child.kill("SIGKILL")).true;
				await exited;
				peer = await Peerbit.create({ directory });
				const docs = await peer.open(
					deserialize(
						Buffer.from(captured.manifest, "base64"),
						Documents,
					) as unknown as Documents<Document>,
					{
						args: { type: Document, nativeGraph, timeUntilRoleMaturity: 0 },
					},
				);
				expect(Boolean(docs.log.log.entryIndex.properties.nativeGraph)).equal(
					nativeGraph,
				);
				expect(peer.libp2p.getConnections()).to.have.length(0);
				expect(await docs.get("victim", { local: true, remote: false }))
					.undefined;
				expect(
					(await docs.get("survivor", { local: true, remote: false }))?.name,
				).equal("unchanged");
				expect(await docs.count()).equal(1);
				expect(await docs.log.log.has(captured.marker)).true;
				expect(await docs.log.entryCoordinatesIndex.get(toId(captured.marker)))
					.to.exist;
				for (const target of captured.targets) {
					expect(await docs.log.log.has(target)).false;
					expect(await docs.log.entryCoordinatesIndex.get(toId(target)))
						.undefined;
				}
				await docs.put(new Document({ id: "after-recovery" }));
				expect(await docs.count()).equal(2);
			} finally {
				if (timer) clearTimeout(timer);
				if (child && child.exitCode === null && child.signalCode === null) {
					const exited = once(child, "exit");
					child.kill("SIGKILL");
					await exited;
				}
				await peer?.stop();
				await rm(directory, { recursive: true, force: true });
			}
		});
	}
});
