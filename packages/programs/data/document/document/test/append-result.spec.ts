import { deserialize, serialize } from "@dao-xyz/borsh";
import { Entry } from "@peerbit/log";
import { TestSession } from "@peerbit/test-utils";
import { expect } from "chai";
import { type Operation, PutOperation } from "../src/operation.js";
import { Documents } from "../src/program.js";
import { Document } from "./data.js";

describe("document append results", () => {
	let session: TestSession;
	let source: Documents<Document>;
	const args = { type: Document, replicate: false as const };
	const documents = () => [
		new Document({ id: "first", name: "one" }),
		new Document({ id: "second", name: "two" }),
	];

	beforeEach(async () => {
		session = await TestSession.disconnected(2);
		source = await session.peers[0].open(new Documents<Document>(), { args });
	});

	afterEach(async () => {
		await session.stop();
	});

	const assertReadable = async (entries: Entry<Operation>[]) => {
		expect(entries).to.have.length(2);
		for (const [index, entry] of entries.entries()) {
			expect(await entry.verifySignatures()).to.equal(true);
			const operation = await entry.getPayloadValue();
			expect(operation).to.be.instanceOf(PutOperation);
			expect(
				deserialize((operation as PutOperation).data, Document),
			).to.deep.equal(documents()[index]);
		}
	};

	for (const batch of [false, true]) {
		const write = async () => {
			if (batch)
				return (await source.putMany(documents(), { unique: true })).entries;
			const entries: Entry<Operation>[] = [];
			for (const document of documents())
				entries.push((await source.put(document)).entry);
			return entries;
		};
		const label = batch ? "putMany" : "put";

		it(`returns verifiable payloads from ${label}`, async () => {
			await assertReadable(await write());
		});

		it(`rejects a mutated ${label} signature without changing stored bytes`, async () => {
			const entries = await write();
			entries[0].signatures[0].signature.fill(0);
			expect(await entries[0].verifySignatures()).to.equal(false);
			expect(await entries[1].verifySignatures()).to.equal(true);
			const stored = await Entry.fromMultihash<Operation>(
				source.log.log.blocks,
				entries[0].hash,
				{ remote: false },
			);
			expect(await stored.verifySignatures()).to.equal(true);
		});

		it(`returns serializable entries from ${label}`, async () => {
			for (const entry of await write()) {
				const stored = (
					await Entry.fromMultihash<Operation>(
						source.log.log.blocks,
						entry.hash,
						{ remote: false },
					)
				).init(source.log.log);
				expect(entry.getStorageBytes()).to.deep.equal(stored.getStorageBytes());
				expect(serialize(entry.toMaterialized())).to.deep.equal(
					serialize(stored),
				);
			}
		});

		it(`joins ${label} results with signature verification`, async () => {
			const target = await session.peers[1].open(source.clone(), { args });
			const entries = await write();
			await target.log.join(entries, { verifySignatures: true });
			expect(target.log.log.length).to.equal(2);
			for (const document of documents()) {
				const actual = await target.index.get(document.id, { remote: false });
				expect(actual).to.exist;
				expect(serialize(actual!)).to.deep.equal(serialize(document));
			}
		});

		it(`keeps ${label} entries readable after their blocks and writer are gone`, async () => {
			const entries = await write();
			// Do not read payloads/signatures first: retaining only a live-store
			// lookup would otherwise appear to satisfy the returned-entry contract.
			for (const entry of entries) await source.log.log.blocks.rm(entry.hash);
			await session.peers[0].stop();
			await assertReadable(entries);
			for (const entry of entries) expect(entry.getStorageBytes()).to.exist;
		});
	}
});
