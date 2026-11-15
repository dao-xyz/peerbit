import { deserialize, serialize } from "@dao-xyz/borsh";
import { AnyBlockStore } from "@peerbit/blocks";
import { Ed25519Keypair } from "@peerbit/crypto";
import { HashmapIndices } from "@peerbit/indexer-simple";
import { expect } from "chai";
import sinon from "sinon";
import { NO_ENCODING } from "../src/encoding.js";
import { Entry } from "../src/entry.js";
import { Log } from "../src/log.js";

describe("native append result materialization", () => {
	for (const nativeStorage of [true, false]) {
		for (const method of ["append", "appendMany"] as const) {
			for (const afterClose of [false, true]) {
				it(`${method} returns usable signed entries with ${nativeStorage ? "native" : "JS"} storage ${afterClose ? "after block removal and close" : "before any readback"}`, async () => {
					const store = nativeStorage
						? await (
								await import("@peerbit/log-rust")
							).createNativeLogBlockStore()
						: new AnyBlockStore();
					await store.start();
					const identity = await Ed25519Keypair.create();
					const log = new Log<Uint8Array>();
					let closed = false;
					await log.open(store, identity, {
						appendDurability: "strict",
						indexer: new HashmapIndices(),
						nativeGraph: true,
					});
					const graph = log.entryIndex.properties.nativeGraph!.graph;
					const prepareSpy = sinon.spy(
						graph,
						method === "append"
							? "prepareEntryV0PlainEntryCommit"
							: "prepareEntryV0PlainChainCommit",
					);
					try {
						const payloads =
							method === "append"
								? [Uint8Array.of(1, 2)]
								: [
										Uint8Array.of(1, 2),
										Uint8Array.of(3),
										Uint8Array.of(4, 5, 6),
									];
						const entries =
							method === "append"
								? [
										(await log.append(payloads[0], { meta: { next: [] } }))
											.entry,
									]
								: (await log.appendMany(payloads, { meta: { next: [] } }))
										.entries;
						expect(prepareSpy.callCount).equal(1);
						expect(entries).to.have.length(payloads.length);

						// Read the committed bytes independently, without resolving the returned
						// entries through Log.get (which can repair a hollow cache entry).
						const storedBytes = await Promise.all(
							entries.map((entry) => store.get(entry.hash)),
						);
						for (const bytes of storedBytes) {
							expect(bytes).to.be.instanceOf(Uint8Array);
						}
						if (afterClose) {
							// Do not touch returned payloads, signatures or serializers until their
							// source blocks and the native graph are no longer available.
							for (const entry of entries) {
								await store.rm(entry.hash);
								expect(await store.has(entry.hash)).equal(false);
							}
							await log.close();
							await store.stop();
							closed = true;
						}

						for (const [index, entry] of entries.entries()) {
							const stored = deserialize(storedBytes[index]!, Entry).init({
								encoding: NO_ENCODING,
							});
							// Direct serialization must work too, not only an explicit
							// toMaterialized() call by a consumer who knows this is native.
							const roundtrip = deserialize(serialize(entry), Entry).init({
								encoding: NO_ENCODING,
							});
							expect(roundtrip.getSignableBytes()).to.deep.equal(
								stored.getSignableBytes(),
							);
							expect(await roundtrip.verifySignatures()).equal(true);
							expect(await entry.verifySignatures()).equal(true);
							expect(entry.signatures).to.have.length(1);
							expect(entry.signatures[0].signature).to.deep.equal(
								stored.signatures[0].signature,
							);
							expect(entry.publicKeys[0].equals(identity.publicKey)).equal(
								true,
							);
							expect(entry.payload.getValue()).to.deep.equal(payloads[index]);
							expect(await entry.getPayloadValue()).to.deep.equal(
								payloads[index],
							);
							expect(entry.meta.next).to.deep.equal(
								index === 0 ? [] : [entries[index - 1].hash],
							);
							expect(entry.size).equal(storedBytes[index]!.length);
							expect(entry.createdLocally).equal(true);
							const storageRoundtrip = deserialize(
								entry.getStorageBytes(),
								Entry,
							);
							expect(storageRoundtrip.getSignableBytes()).to.deep.equal(
								stored.getSignableBytes(),
							);
							expect(await storageRoundtrip.verifySignatures()).equal(true);
							expect(entry.toMaterialized().getSignableBytes()).to.deep.equal(
								stored.getSignableBytes(),
							);

							// Knowing the local signer is not a substitute for verifying the
							// returned signature. Mutating it must invalidate verification.
							entry.signatures[0].signature.fill(0);
							expect(await entry.verifySignatures()).equal(false);
							expect(await stored.verifySignatures()).equal(true);
						}
					} finally {
						prepareSpy.restore();
						if (!closed) {
							await log.close();
							await store.stop();
						}
					}
				});
			}
		}
	}
});
