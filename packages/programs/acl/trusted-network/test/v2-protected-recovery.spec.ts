import { serialize } from "@dao-xyz/borsh";
import { Ed25519Keypair } from "@peerbit/crypto";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import { execFile } from "node:child_process";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { promisify } from "node:util";
import {
	createRecoveryHistory,
	openRecoveryReplica,
} from "./utils/v2-protected-recovery.js";

type Replica = Awaited<ReturnType<typeof openRecoveryReplica>>;
type Record = Awaited<
	ReturnType<typeof createRecoveryHistory>
>["policies"][number];

const transfer = async (
	from: Replica,
	to: Replica,
	record: Record,
	kind: "policy" | "fence" | "operation",
) => {
	// Do not let an accidentally shared store turn a network assertion into a
	// local cache hit. Only fetched bytes, never the fixture's copy, enter to.
	expect(
		await to.peer.services.blocks.get(record.cid, { remote: false }),
	).to.equal(undefined);
	const bytes = await to.peer.services.blocks.get(record.cid, {
		remote: {
			from: [from.peer.identity.publicKey.hashcode()],
			timeout: 10_000,
		},
	});
	expect(bytes).to.deep.equal(record.bytes);
	await to.remember(bytes!, kind);
	return bytes!;
};

const json = (value: unknown) =>
	JSON.parse(
		JSON.stringify(value, (_key, entry) =>
			typeof entry === "bigint"
				? entry.toString()
				: entry instanceof Uint8Array
					? [...entry]
					: entry,
		),
	);

describe("TrustedNetwork v2 independent-peer protected recovery", function () {
	this.timeout(90_000);
	this.retries(0);

	it("reconciles an offline writer and cold peer, then recreates every anchor offline in a fresh process", async () => {
		const directory = await mkdtemp(join(tmpdir(), "peerbit-v2-recovery-"));
		const replicas: Replica[] = [];
		try {
			const history = await createRecoveryHistory(
				await Ed25519Keypair.create(),
				await Ed25519Keypair.create(),
			);
			const { policies, fences, operations } = history;
			const { before, concurrent, after, regranted } = operations;
			const directories = ["owner", "writer", "cold"].map((name) =>
				join(directory, name),
			);
			for (const path of directories)
				replicas.push(await openRecoveryReplica(path, history.descriptor));
			const owner = replicas[0]!;
			let writer = replicas[1]!;
			const cold = replicas[2]!;
			await writer.peer.dial(owner.peer.getMultiaddrs());
			await owner.remember(policies[0]!.bytes, "policy");
			await owner.remember(fences[0]!.bytes, "fence");
			expect((await owner.policy.ingest(policies[0]!.bytes)).status).to.equal(
				"accepted",
			);
			expect((await owner.fence.ingest(fences[0]!.bytes)).status).to.equal(
				"accepted",
			);
			await writer.policy.ingest(
				await transfer(owner, writer, policies[0]!, "policy"),
			);
			await writer.fence.ingest(
				await transfer(owner, writer, fences[0]!, "fence"),
			);
			for (const entry of [before, concurrent]) {
				await writer.remember(entry.bytes, "operation");
				expect((await writer.projection.retain(entry.bytes)).status).to.equal(
					"retained",
				);
			}
			await owner.projection.retain(
				await transfer(writer, owner, before, "operation"),
			);
			const provisional = await writer.projection.withDocuments(
				fences[0]!.cid,
				(view) => view,
			);
			expect(provisional.status).to.equal("completed");
			if (provisional.status !== "completed")
				throw new Error(provisional.status);
			expect(
				provisional.value.documents.map((row) => row.entryCid),
			).to.have.members([before.cid, concurrent.cid]);

			// A hangUp alone permits automatic redial. Stop the actual writer and
			// all of its anchors to establish a deterministic offline interval.
			await writer.close();
			expect(writer.node.status).to.equal("stopped");
			await waitForResolved(() => {
				expect(owner.node.getConnections()).to.have.length(0);
			});
			await owner.remember(policies[1]!.bytes, "policy");
			await owner.remember(fences[1]!.bytes, "fence");
			expect((await owner.policy.ingest(policies[1]!.bytes)).status).to.equal(
				"accepted",
			);
			expect((await owner.fence.ingest(fences[1]!.bytes)).status).to.equal(
				"accepted",
			);

			// The writer's omitted branch remains locally recoverable. Reconnecting
			// and later regrant must never launder it into the closed interval.
			writer = await openRecoveryReplica(directories[1]!, history.descriptor);
			replicas[1] = writer;
			await writer.peer.dial(owner.peer.getMultiaddrs());
			await writer.policy.ingest(
				await transfer(owner, writer, policies[1]!, "policy"),
			);
			await writer.fence.ingest(
				await transfer(owner, writer, fences[1]!, "fence"),
			);
			await writer.remember(after.bytes, "operation");
			await writer.projection.retain(after.bytes);
			for (const entry of [concurrent, after])
				await owner.projection.retain(
					await transfer(writer, owner, entry, "operation"),
				);
			await owner.remember(policies[2]!.bytes, "policy");
			await owner.policy.ingest(policies[2]!.bytes);
			await writer.policy.ingest(
				await transfer(owner, writer, policies[2]!, "policy"),
			);
			await owner.remember(fences[2]!.bytes, "fence");
			await owner.fence.ingest(fences[2]!.bytes);
			await writer.fence.ingest(
				await transfer(owner, writer, fences[2]!, "fence"),
			);
			await writer.remember(regranted.bytes, "operation");
			await writer.projection.retain(regranted.bytes);
			await owner.projection.retain(
				await transfer(writer, owner, regranted, "operation"),
			);

			// Test-owned delivery deliberately puts operations before policy/fence
			// context. This is not an automatic inbound acceptance/frontier API.
			await cold.peer.dial(writer.peer.getMultiaddrs());
			await cold.peer.dial(owner.peer.getMultiaddrs());
			for (const entry of [regranted, after, concurrent, before]) {
				expect(
					(
						await cold.projection.retain(
							await transfer(writer, cold, entry, "operation"),
						)
					).status,
				).to.equal("retained");
			}
			let prematureReads = 0;
			const readUnavailable = async () => {
				expect(
					(
						await cold.projection.withDocuments(
							fences[2]!.cid,
							() => prematureReads++,
						)
					).status,
				).to.equal("unavailable");
				expect(prematureReads).to.equal(0);
			};
			await readUnavailable();
			for (const entry of [...policies].reverse())
				await cold.policy.ingest(await transfer(owner, cold, entry, "policy"));
			for (const entry of [fences[2]!, fences[1]!])
				await cold.fence.ingest(await transfer(owner, cold, entry, "fence"));
			await readUnavailable();
			await cold.fence.ingest(await transfer(owner, cold, fences[0]!, "fence"));
			const expected = await owner.projection.withDocuments(
				fences[2]!.cid,
				(view) => view,
			);
			expect(expected.status).to.equal("completed");
			if (expected.status !== "completed") throw new Error(expected.status);
			expect(expected.value.acceptedPolicyHead.digest).to.deep.equal(
				policies[2]!.digest,
			);
			expect(expected.value.acceptedFenceHead.entryCid).to.equal(
				fences[2]!.cid,
			);
			expect(
				expected.value.documents.map((row) => [
					row.entryCid,
					row.status,
					[...row.value.value],
				]),
			).to.have.deep.members([
				[before.cid, "policy-final", [1]],
				[regranted.cid, "provisional", [4]],
			]);
			expect(
				await cold.projection.withDocuments(fences[2]!.cid, (view) => view),
			).to.deep.equal(expected);
			// A saved successful view must not hide missing causal dependencies.
			await cold.peer.services.blocks.rm(fences[0]!.cid);
			await readUnavailable();
			await transfer(owner, cold, fences[0]!, "fence");
			expect(
				await cold.projection.withDocuments(fences[2]!.cid, (view) => view),
			).to.deep.equal(expected);

			for (const replica of replicas) {
				for (const entry of Object.values(operations))
					expect(replica.projection.get(entry.cid)).to.deep.equal(entry.bytes);
				await replica.close();
			}
			replicas.length = 0;
			// Do not fresh-read the writer before stopping: its document checkpoint
			// is still the old two-row provisional view, unlike its newer anchors.
			const configuration = join(directory, "reopen.json");
			await writeFile(
				configuration,
				JSON.stringify({
					descriptor: Buffer.from(serialize(history.descriptor)).toString(
						"base64",
					),
					directories,
					fenceCid: fences[2]!.cid,
					retainedCids: Object.values(operations)
						.map((entry) => entry.cid)
						.sort(),
				}),
			);
			const { stdout } = await promisify(execFile)(
				process.execPath,
				[
					join(process.cwd(), "test/v2-protected-recovery-worker.mjs"),
					configuration,
				],
				{ timeout: 30_000, killSignal: "SIGKILL", maxBuffer: 1024 * 1024 },
			);
			const reports = stdout
				.split("\n")
				.filter((line) => line.startsWith("RECOVERED "))
				.map((line) => JSON.parse(line.slice("RECOVERED ".length)));
			expect(reports).to.have.length(3);
			for (const report of reports) {
				expect(report.connections).to.equal(0);
				expect(report.result).to.deep.equal(json(expected));
				expect(report.retainedCids).to.deep.equal(
					Object.values(operations)
						.map((entry) => entry.cid)
						.sort(),
				);
				expect(report.bytesVerified).to.equal(4);
			}
		} finally {
			await Promise.all(replicas.map((replica) => replica.close()));
			await rm(directory, { recursive: true, force: true });
		}
	});
});
