/* eslint-disable no-console */
import { deserialize, field, variant } from "@dao-xyz/borsh";
import { Entry } from "@peerbit/log";
import { Program } from "@peerbit/program";
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { createHash, randomBytes } from "node:crypto";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { arch, cpus, platform, tmpdir } from "node:os";
import { join } from "node:path";
import { performance } from "node:perf_hooks";
import { setImmediate } from "node:timers/promises";
import { Peerbit } from "peerbit";
import { Documents } from "../src/index.js";

// Build, then run in separate processes for each matched cohort:
// node --expose-gc dist/benchmark/document-put-durable.js
// DOC_DURABLE_SINGLES=60 DOC_DURABLE_ROUNDS=30
// Ordinary disk-backed Peerbit, JS authorization, default targeting, FULL/sync
// durability, no retries. Four concurrent puts are NOT an atomic putMany batch.
// Raw samples exclude input construction and reopen verification. This is a
// graceful-stop/reopen check, not crash-durability or remote-receipt evidence.

const positiveInteger = (name: string, fallback: number): number => {
	const value = Number(process.env[name] ?? fallback);
	assert(Number.isSafeInteger(value) && value > 0, `${name} must be positive`);
	return value;
};
const singles = positiveInteger("DOC_DURABLE_SINGLES", 60);
const rounds = positiveInteger("DOC_DURABLE_ROUNDS", 30);
const digest = (bytes: Uint8Array): string =>
	createHash("sha256").update(bytes).digest("hex");

@variant("durable_put_benchmark_document")
class BlobDocument {
	@field({ type: "string" })
	id: string;

	@field({ type: Uint8Array })
	bytes: Uint8Array;

	constructor(sequence: number, size: number) {
		this.id = `document-${sequence}`;
		this.bytes = randomBytes(size);
	}
}

@variant("durable_put_benchmark_projection")
class BlobProjection {
	@field({ type: "string" })
	id: string;

	@field({ type: "u32" })
	size: number;

	constructor(document: BlobDocument) {
		this.id = document.id;
		this.size = document.bytes.byteLength;
	}
}

@variant("durable_put_benchmark_store")
class Store extends Program {
	@field({ type: Documents })
	docs: Documents<BlobDocument, BlobProjection>;

	constructor() {
		super();
		this.docs = new Documents();
	}

	async open(): Promise<void> {
		await this.docs.open({
			type: BlobDocument,
			replicate: { factor: 1 },
			canPerform: () => true,
			index: { type: BlobProjection, cache: { resolver: 0 } },
		});
	}
}

const percentile = (values: number[], fraction: number): number =>
	[...values].sort((a, b) => a - b)[Math.ceil(values.length * fraction) - 1]!;

const measure = async (bytes: number, concurrency: number, samples: number) => {
	const warmup = concurrency === 1 ? 5 : 2;
	const directory = await mkdtemp(join(tmpdir(), "peerbit-durable-put-"));
	console.log(
		JSON.stringify({ event: "start", directory, bytes, concurrency }),
	);
	let peer: Peerbit | undefined;
	try {
		peer = await Peerbit.create({ directory });
		const store = await peer.open(new Store());
		assert.equal(peer.libp2p.getConnections().length, 0);
		const address = store.address;
		const identity = peer.identity.publicKey.hashcode();
		const expected: { id: string; digest: string; hash: string }[] = [];
		const wallMs: number[] = [];
		const cpuMs: number[] = [];
		let sequence = 0;
		for (let round = -warmup; round < samples; round++) {
			const inputs = Array.from({ length: concurrency }, () => {
				const index = sequence++;
				const document = new BlobDocument(index, bytes);
				return { document, id: document.id, digest: digest(document.bytes) };
			});
			await setImmediate();
			if (round === 0) globalThis.gc?.();
			const cpu = process.cpuUsage();
			const start = performance.now();
			const writes = inputs.map(({ document }) =>
				store.docs.put(document, { unique: true }),
			);
			const committed = await Promise.all(writes).catch(async (error) => {
				await Promise.allSettled(writes);
				throw error;
			});
			const elapsed = performance.now() - start;
			const used = process.cpuUsage(cpu);
			if (round >= 0) {
				wallMs.push(elapsed);
				cpuMs.push((used.user + used.system) / 1000);
			}
			for (let index = 0; index < inputs.length; index++) {
				expected.push({
					id: inputs[index]!.id,
					digest: inputs[index]!.digest,
					hash: committed[index]!.entry.hash,
				});
			}
		}
		assert.equal(peer.libp2p.getConnections().length, 0);
		await peer.stop();
		peer = undefined;
		peer = await Peerbit.create({ directory });
		assert.equal(peer.identity.publicKey.hashcode(), identity);
		const reopened = await peer.open<Store>(address);
		assert.equal(reopened.docs.log.log.length, expected.length);
		assert.equal(await reopened.docs.index.getSize(), expected.length);
		for (const item of expected) {
			const actual = await reopened.docs.index.get(item.id, {
				local: true,
				remote: false,
			});
			assert(actual);
			assert.equal(actual.id, item.id);
			assert.equal(actual.bytes.length, bytes);
			assert.equal(digest(actual.bytes), item.digest);
			assert.equal(actual.__context.head, item.hash);
			const stored = await reopened.docs.log.log.blocks.get(item.hash, {
				remote: false,
			});
			assert(stored);
			const entry = deserialize(stored, Entry);
			assert.deepEqual(
				new Uint8Array(entry.getStorageBytes()),
				new Uint8Array(stored),
			);
			assert.equal(await Entry.prepareMultihash(entry), item.hash);
			entry.init(reopened.docs.log.log);
			assert.equal(await entry.verifySignatures(), true);
			const projected = await reopened.docs.index.get(item.id, {
				local: true,
				remote: false,
				resolve: false,
			});
			assert.equal(projected?.size, bytes);
		}
		assert.equal(peer.libp2p.getConnections().length, 0);
		await peer.stop();
		peer = undefined;
		console.log(
			JSON.stringify({
				event: "result",
				bytes,
				concurrency,
				samples,
				warmup,
				wallMs,
				cpuMs,
				p50Ms: percentile(wallMs, 0.5),
				p95Ms: percentile(wallMs, 0.95),
				offlineVerifiedEntries: expected.length,
			}),
		);
		// Only remove the directory created by this invocation, after verification.
		await rm(directory, { recursive: true });
	} catch (error) {
		console.error(`Preserving failed benchmark data: ${directory}`);
		throw error;
	} finally {
		await peer?.stop();
	}
};

console.log(
	JSON.stringify({
		event: "environment",
		head: execFileSync("git", ["rev-parse", "HEAD"], {
			encoding: "utf8",
		}).trim(),
		programSha256: createHash("sha256")
			.update(await readFile(new URL("../src/program.js", import.meta.url)))
			.digest("hex"),
		node: process.version,
		platform: platform(),
		arch: arch(),
		cpu: cpus()[0]?.model,
		gc: !!globalThis.gc,
		configuration: {
			peers: 1,
			replicate: { factor: 1 },
			replicas: "default (min 2)",
			target: "default",
			durability: "default (SQLite FULL / Level sync)",
			canPerform: "JavaScript allow-all callback",
			resolverCache: 0,
			unique: true,
		},
	}),
);
for (const [bytes, concurrency] of [
	[4096, 1],
	[524288, 1],
	[262144, 4],
	[524288, 4],
]) {
	await measure(bytes!, concurrency!, concurrency === 1 ? singles : rounds);
}
