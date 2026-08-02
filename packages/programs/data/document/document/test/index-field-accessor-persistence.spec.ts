import { field, variant } from "@dao-xyz/borsh";
import { expect } from "chai";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { Peerbit } from "peerbit";
import { createRustPeerbitOptions } from "peerbit/rust";
import { Documents, registerIndexFieldAccessor } from "../src/index.js";

const EMPTY = new Uint8Array();
const decodedContentLengths: number[] = [];

@variant("index_field_accessor_document")
class Chunk {
	@field({ type: "string" })
	id: string;

	@field({ type: Uint8Array })
	content: Uint8Array;

	constructor(id: string, content: Uint8Array) {
		this.id = id;
		this.content = content;
	}
}

@variant("index_field_accessor_index")
class LegacyChunkIndex {
	@field({ type: "string" })
	id: string;

	@field({ type: Uint8Array })
	content: Uint8Array;

	constructor(chunk: Chunk) {
		this.id = chunk.id;
		this.content = chunk.content;
	}
}

@variant("index_field_accessor_index")
class CompactChunkIndex {
	@field({ type: "string" })
	id: string;

	get content(): Uint8Array {
		return EMPTY;
	}

	set content(content: Uint8Array) {
		decodedContentLengths.push(content.byteLength);
	}

	constructor(chunk: Chunk) {
		this.id = chunk.id;
	}
}
registerIndexFieldAccessor(CompactChunkIndex, "content", {
	type: Uint8Array,
});

const storeId = Uint8Array.from(
	{ length: 32 },
	(_, index) => (index * 13 + 7) & 0xff,
);
const payload = Uint8Array.from(
	{ length: 256 * 1024 },
	(_, index) => (index * 31 + 17) & 0xff,
);

const openLegacy = (peer: Peerbit, docs: Documents<Chunk, LegacyChunkIndex>) =>
	peer.open(docs, {
		args: {
			type: Chunk,
			replicate: false,
			index: {
				idProperty: "id",
				type: LegacyChunkIndex,
				transform: (chunk) => new LegacyChunkIndex(chunk),
			},
		},
	});

const openCompact = (
	peer: Peerbit,
	docs: Documents<Chunk, CompactChunkIndex>,
) =>
	peer.open(docs, {
		args: {
			type: Chunk,
			replicate: false,
			index: {
				idProperty: "id",
				type: CompactChunkIndex,
				transform: (chunk) => new CompactChunkIndex(chunk),
			},
		},
	});

const expectCompactIndex = async (
	docs: Documents<Chunk, CompactChunkIndex>,
) => {
	const indexed = await docs.index.get("chunk", {
		resolve: false,
		remote: false,
	});
	expect(indexed).to.exist;
	expect(indexed!.content).to.equal(EMPTY);
	expect(Object.hasOwn(indexed!, "content")).to.equal(false);

	const resolved = await docs.index.get("chunk", { remote: false });
	expect(resolved?.content).to.deep.equal(payload);
};

describe("index field accessor persistence", () => {
	for (const backend of [
		{ label: "SQLite", options: () => ({}) },
		{
			label: "Rust",
			options: () =>
				createRustPeerbitOptions({
					network: false,
					indexer: { persistence: { compactAfterOperations: 1 } },
				}),
		},
	]) {
		it(`migrates legacy payload index rows with ${backend.label}`, async function () {
			this.timeout(180_000);
			const directory = await fs.mkdtemp(
				path.join(os.tmpdir(), "peerbit-index-field-accessor-"),
			);
			let peer: Peerbit | undefined;

			try {
				peer = await Peerbit.create({ directory, ...backend.options() });
				const legacy = await openLegacy(
					peer,
					new Documents<Chunk, LegacyChunkIndex>({ id: storeId }),
				);
				await legacy.put(new Chunk("chunk", payload), {
					replicate: false,
					target: "none",
				});
				expect(
					(await legacy.index.get("chunk", { resolve: false }))?.content,
				).to.deep.equal(payload);
				const firstClone = legacy.clone() as Documents<
					Chunk,
					CompactChunkIndex
				>;
				await peer.stop();

				decodedContentLengths.length = 0;
				peer = await Peerbit.create({ directory, ...backend.options() });
				const migrated = await openCompact(peer, firstClone);
				await expectCompactIndex(migrated);
				expect(decodedContentLengths).to.include(payload.byteLength);

				await migrated.recover();
				await expectCompactIndex(migrated);
				const secondClone = migrated.clone() as Documents<
					Chunk,
					CompactChunkIndex
				>;
				await peer.stop();

				decodedContentLengths.length = 0;
				peer = await Peerbit.create({ directory, ...backend.options() });
				const reopened = await openCompact(peer, secondClone);
				await expectCompactIndex(reopened);
				expect(decodedContentLengths).to.not.be.empty;
				expect(decodedContentLengths.every((length) => length === 0)).to.equal(
					true,
				);
			} finally {
				await peer?.stop().catch(() => undefined);
				await fs.rm(directory, { recursive: true, force: true });
			}
		});
	}
});
