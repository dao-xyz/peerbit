/* eslint-disable no-console */
import { field, variant, vec } from "@dao-xyz/borsh";
import { type Index, id } from "@peerbit/indexer-interface";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { performance } from "node:perf_hooks";
import { create } from "../src/index.js";

@variant("sqlite_put_batch_benchmark")
class BenchmarkDocument {
	@id({ type: "string" })
	id: string;

	@field({ type: Uint8Array })
	bytes: Uint8Array;

	@field({ type: vec("string") })
	chunkRefs: string[];

	@field({ type: vec("string") })
	causalRefs: string[];

	constructor(index: number) {
		this.id = `document-${index}`;
		this.bytes = new Uint8Array(1024).fill(index % 251);
		this.chunkRefs = Array.from(
			{ length: 4 },
			(_, chunk) => `chunk-${index}-${chunk}`,
		);
		this.causalRefs = Array.from(
			{ length: 2 },
			(_, parent) => `parent-${index}-${parent}`,
		);
	}
}

const readArgument = (name: string, fallback: string): string => {
	const prefix = `--${name}=`;
	return (
		process.argv
			.find((argument) => argument.startsWith(prefix))
			?.slice(prefix.length) ?? fallback
	);
};

const mode = readArgument("mode", "sequential");
if (mode !== "sequential" && mode !== "batch") {
	throw new Error(`Unsupported mode: ${mode}`);
}

const count = Number(readArgument("count", "6200"));
const rounds = Number(readArgument("rounds", "3"));
if (!Number.isSafeInteger(count) || count <= 0) {
	throw new Error(`Invalid count: ${count}`);
}
if (!Number.isSafeInteger(rounds) || rounds <= 0) {
	throw new Error(`Invalid rounds: ${rounds}`);
}

const documents = Array.from(
	{ length: count },
	(_, index) => new BenchmarkDocument(index),
);
const samples: number[] = [];

for (let round = 0; round < rounds; round++) {
	const directory = await mkdtemp(join(tmpdir(), "peerbit-sqlite-put-batch-"));
	try {
		const indices = await create(directory);
		await indices.start();
		const index = (await indices.init({
			schema: BenchmarkDocument,
			indexBy: ["id"],
		})) as Index<BenchmarkDocument>;

		const startedAt = performance.now();
		if (mode === "batch") {
			if (!index.putBatch) {
				throw new Error("SQLite index does not implement putBatch");
			}
			await index.putBatch(documents);
		} else {
			for (const document of documents) {
				await index.put(document);
			}
		}
		const elapsedMs = performance.now() - startedAt;
		if ((await index.getSize()) !== count) {
			throw new Error("Indexed row count did not match the input count");
		}
		await indices.stop();

		const reopened = await create(directory);
		await reopened.start();
		const reopenedIndex = await reopened.init<BenchmarkDocument, never>({
			schema: BenchmarkDocument,
			indexBy: ["id"],
		});
		if ((await reopenedIndex.getSize()) !== count) {
			throw new Error("Reopened row count did not match the input count");
		}
		await reopened.stop();

		samples.push(elapsedMs);
		console.log(
			JSON.stringify({
				mode,
				count,
				round: round + 1,
				elapsedMs,
				documentsPerSecond: (count * 1000) / elapsedMs,
			}),
		);
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
}

const sorted = [...samples].sort((left, right) => left - right);
const medianMs = sorted[Math.floor(sorted.length / 2)]!;
console.log(
	JSON.stringify({
		mode,
		count,
		rounds,
		medianMs,
		medianDocumentsPerSecond: (count * 1000) / medianMs,
	}),
);
