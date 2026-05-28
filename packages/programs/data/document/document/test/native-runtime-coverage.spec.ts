import {
	NativeBackboneCoordinatePersistence,
	type NativeBackboneCoordinatePersistenceStore,
} from "@peerbit/native-backbone";
import { TestSession } from "@peerbit/test-utils";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import { createRustPeerbitOptions } from "peerbit/rust";
import sinon from "sinon";
import { policy, transform } from "../src/index.js";
import { Documents } from "../src/program.js";
import { Document, TestStore } from "./data.js";

const concatBytes = (chunks: Uint8Array[]): Uint8Array => {
	const total = chunks.reduce((sum, chunk) => sum + chunk.byteLength, 0);
	const out = new Uint8Array(total);
	let offset = 0;
	for (const chunk of chunks) {
		out.set(chunk, offset);
		offset += chunk.byteLength;
	}
	return out;
};

class MemoryCoordinatePersistenceStore
	implements NativeBackboneCoordinatePersistenceStore
{
	readonly files = new Map<string, Uint8Array>();

	async read(name: string): Promise<Uint8Array | undefined> {
		return this.files.get(name);
	}

	async write(name: string, bytes: Uint8Array): Promise<void> {
		this.files.set(name, bytes.slice());
	}

	async append(name: string, bytes: Uint8Array): Promise<void> {
		const existing = this.files.get(name);
		this.files.set(
			name,
			existing ? concatBytes([existing, bytes]) : bytes.slice(),
		);
	}

	async remove(name: string): Promise<void> {
		this.files.delete(name);
	}
}

const nativeBackboneDocumentIndexOptions = () => ({
	optional: false,
	documentIndex: true,
	coordinatePersistence: new NativeBackboneCoordinatePersistence(
		new MemoryCoordinatePersistenceStore(),
		{ flushOnAppend: false },
	),
});

describe("strict native runtime coverage", () => {
	let session: TestSession;

	beforeEach(async () => {
		session = await TestSession.connected(2, createRustPeerbitOptions());
	});

	afterEach(async () => {
		await session.stop();
	});

	it("syncs a larger cold batch through native receive without generic document indexing", async function () {
		this.timeout(60_000);

		const source = new TestStore({
			docs: new Documents<Document>(),
		});
		const target = source.clone();
		const openArgs = () => ({
			mode: "native" as const,
			replicate: { factor: 1 },
			nativeGraph: true,
			nativeBackbone: nativeBackboneDocumentIndexOptions(),
			canPerform: policy.allowAll<Document>(),
			index: {
				type: Document,
				transform: transform.identity<Document>(),
			},
		});

		await session.peers[0].open(source, { args: openArgs() });
		const documents = Array.from(
			{ length: 128 },
			(_, index) =>
				new Document({
					id: `native-cold-sync-${index}`,
					name: `native-cold-sync-name-${index}`,
					tags: [`batch-${Math.floor(index / 16)}`],
				}),
		);
		await source.docs.putMany(documents, { unique: true });

		await session.peers[1].open(target, { args: openArgs() });
		const documentPutSpy = sinon.spy(target.docs.index, "put");
		const documentPreparedNativeStoredPutSpy = sinon.spy(
			target.docs.index,
			"_putPreparedNativeBackboneDocumentIndexStoredWithContext",
		);
		const decoderSpy = sinon.spy(target.docs.index.valueEncoding, "decoder");
		try {
			await waitForResolved(
				async () => expect(await target.docs.index.getSize()).equal(128),
				{
					timeout: 30_000,
					timeoutMessage: "strict native cold batch sync",
				},
			);

			expect(decoderSpy.callCount).equal(0);
			expect(documentPutSpy.callCount).equal(0);
			expect(documentPreparedNativeStoredPutSpy.callCount).greaterThan(0);

			const targetBackbone = (target.docs.log as any)._nativeBackbone;
			expect(targetBackbone.documentValueLength).equal(128);
			expect(
				Array.from(
					targetBackbone.documentKeysExist([
						"string:native-cold-sync-0",
						"string:native-cold-sync-63",
						"string:native-cold-sync-127",
					]),
				),
			).to.deep.equal([1, 1, 1]);

			const first = await target.docs.get("native-cold-sync-0", {
				local: true,
				remote: false,
			});
			const middle = await target.docs.get("native-cold-sync-63", {
				local: true,
				remote: false,
			});
			const last = await target.docs.get("native-cold-sync-127", {
				local: true,
				remote: false,
			});
			expect(first?.name).equal("native-cold-sync-name-0");
			expect(middle?.name).equal("native-cold-sync-name-63");
			expect(last?.name).equal("native-cold-sync-name-127");
		} finally {
			decoderSpy.restore();
			documentPreparedNativeStoredPutSpy.restore();
			documentPutSpy.restore();
			await target.close();
			await source.close();
		}
	});
});
