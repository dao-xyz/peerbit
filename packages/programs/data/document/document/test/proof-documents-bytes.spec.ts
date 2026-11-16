import { deserialize, field, serialize, variant } from "@dao-xyz/borsh";
import { StringKey, toId } from "@peerbit/indexer-interface";
import { expect } from "chai";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Peerbit } from "peerbit";
import sinon from "sinon";
import { Documents } from "../src/program.js";
import {
	deriveProofDeleteScope,
	getProofDeleteContract,
} from "../src/proof-delete-manifest.js";
import { ProofPutOperation } from "../src/proof-operations.js";

@variant(0)
class ByteDocument {
	@field({ type: Uint8Array })
	id: Uint8Array;

	@field({ type: Uint8Array })
	data: Uint8Array;

	constructor(id: Uint8Array, data: Uint8Array) {
		this.id = id;
		this.data = data;
	}
}

describe("owner-proofs Documents byte keys", () => {
	for (const nativeGraph of [false, true]) {
		describe(nativeGraph ? "native graph" : "JS graph", () => {
			let directory: string;
			let peer: Peerbit;
			let docs: Documents<ByteDocument>;
			const args = {
				type: ByteDocument,
				nativeGraph,
				timeUntilRoleMaturity: 0,
			};
			beforeEach(async () => {
				directory = await mkdtemp(join(tmpdir(), "peerbit-proof-byte-keys-"));
				peer = await Peerbit.create({ directory });
				docs = await peer.open(
					new Documents<ByteDocument>({
						format: {
							version: "owner-proofs-v1",
							owner: peer.identity.publicKey,
							keyType: "bytes",
						},
					}),
					{ args },
				);
			});
			afterEach(async () => {
				sinon.restore();
				await peer?.stop();
				await rm(directory, { recursive: true, force: true });
			});

			it("snapshots caller arrays, updates and deletes exact binary history, and reopens offline", async () => {
				const manifest = serialize(docs);
				const key = Uint8Array.of(1, 2, 3);
				const expectedKey = Uint8Array.from(key);
				const data = Uint8Array.of(4, 5, 6);
				const writing = docs.put(new ByteDocument(key, data));
				key.fill(0);
				data.fill(0);
				const first = await writing;
				const read = await docs.get(expectedKey, {
					local: true,
					remote: false,
				});
				expect(read?.id).deep.equal(expectedKey);
				expect(read?.data).deep.equal(Uint8Array.of(4, 5, 6));
				expect(await docs.get(key, { local: true, remote: false })).equal(
					undefined,
				);
				const second = await docs.put(
					new ByteDocument(expectedKey, Uint8Array.of(7)),
				);
				expect(
					(await docs.get(expectedKey, { local: true, remote: false }))?.data,
				).deep.equal(Uint8Array.of(7));
				const survivorKey = Uint8Array.of(9, 9);
				await docs.put(new ByteDocument(survivorKey, Uint8Array.of(8)));
				const deleteKey = Uint8Array.from(expectedKey);
				const deleting = docs.del(deleteKey);
				deleteKey.fill(0);
				const removed = await deleting;
				expect(removed.entry.meta.next).to.have.members([
					first.entry.hash,
					second.entry.hash,
				]);
				for (const entry of [first.entry, second.entry]) {
					expect(await docs.log.log.has(entry.hash)).equal(false);
					expect(
						await docs.log.entryCoordinatesIndex.get(toId(entry.hash)),
					).equal(undefined);
				}
				expect(
					await docs.get(expectedKey, { local: true, remote: false }),
				).equal(undefined);
				await peer.stop();
				peer = await Peerbit.create({ directory });
				docs = await peer.open(
					deserialize(
						manifest,
						Documents,
					) as unknown as Documents<ByteDocument>,
					{ args },
				);
				expect(peer.libp2p.getConnections()).to.have.length(0);
				expect(
					await docs.get(expectedKey, { local: true, remote: false }),
				).equal(undefined);
				expect(
					(await docs.get(survivorKey, { local: true, remote: false }))?.data,
				).deep.equal(Uint8Array.of(8));
				expect(await docs.log.log.has(removed.entry.hash)).equal(true);
				await docs.put(new ByteDocument(expectedKey, Uint8Array.of(10)));
				expect(
					(await docs.get(expectedKey, { local: true, remote: false }))?.data,
				).deep.equal(Uint8Array.of(10));
			});

			it("rejects a string key before any block or index write and remains usable", async () => {
				const writes = sinon.spy(
					docs.log.log.entryIndex.properties.store,
					"put",
				);
				await expect(docs.del("wrong-kind")).rejected;
				await expect(
					docs.log.append(
						new ProofPutOperation({
							scope: deriveProofDeleteScope(getProofDeleteContract(docs.log)!),
							key: new StringKey("wrong-kind"),
							data: serialize(
								new ByteDocument(Uint8Array.of(1), Uint8Array.of(2)),
							),
						}),
						{ meta: { next: [] } },
					),
				).rejected;
				expect(writes.called).equal(false);
				expect(docs.log.log.length).equal(0);
				expect(await docs.count()).equal(0);
				await docs.put(new ByteDocument(Uint8Array.of(1), Uint8Array.of(2)));
				expect(await docs.count()).equal(1);
			});
		});
	}
});
