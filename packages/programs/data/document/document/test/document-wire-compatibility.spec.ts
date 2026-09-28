import { deserialize, serialize } from "@dao-xyz/borsh";
import { EntryType } from "@peerbit/log";
import { Program } from "@peerbit/program";
import { expect } from "chai";
import { Peerbit } from "peerbit";
import {
	BORSH_ENCODING_OPERATION,
	DeleteOperation,
	Documents,
	Operation,
	PutOperation,
	toId,
} from "../src/index.js";
import { Document } from "./data.js";

// Captured from c2e919d053a4247b6f6e6490558d13b52b766b60, before any
// checkpoint resource integration. These are persisted wire fixtures, not values
// recomputed by the implementation under test. The log ID is bytes 0 through 31.
const DESCRIPTOR_HEX =
	"0009000000646f63756d656e7473000a0000007368617265645f6c6f6700000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f000300000072706300000f000000646f63756d656e74735f696e6465780003000000727063";
const DESCRIPTOR_ADDRESS = "zb2rhkGvKPM7JYRq8dGeu3YDJB4NUU1awJx9g4MMhmfU5LvEN";
const DOCUMENT_HEX =
	"0008000000776972652d6b6579010a000000776972652d76616c756500000000000000";
const PUT_HEX =
	"0003230000000008000000776972652d6b6579010a000000776972652d76616c756500000000000000";
const DELETE_HEX = "00040008000000776972652d6b6579";

const bytes = (value: string) => new Uint8Array(Buffer.from(value, "hex"));
const hex = (value: Uint8Array) => Buffer.from(value).toString("hex");
const fixedId = () => Uint8Array.from({ length: 32 }, (_, index) => index);
const value = () => new Document({ id: "wire-key", name: "wire-value" });
const openArgs = () => ({
	type: Document,
	mode: "compat" as const,
	replicate: false as const,
	keep: "self" as const,
	nativeGraph: false as const,
	nativeBackbone: false as const,
	nativeRangePlanner: false as const,
});

describe("Documents persisted wire compatibility", function () {
	this.timeout(30_000);

	it("preserves the literal legacy descriptor, address and Program discriminator", async () => {
		const docs = new Documents<Document>({ id: fixedId() });
		expect(hex(serialize(docs))).equal(DESCRIPTOR_HEX);
		expect((await docs.calculateAddress()).address).equal(DESCRIPTOR_ADDRESS);

		for (const decoded of [
			deserialize(bytes(DESCRIPTOR_HEX), Documents),
			deserialize(bytes(DESCRIPTOR_HEX), Program),
		]) {
			expect(decoded).instanceOf(Documents);
			const documentStore = decoded as Documents<Document>;
			expect(documentStore.immutable).equal(false);
			expect(documentStore.log.log.id).deep.equal(fixedId());
			expect(hex(serialize(documentStore))).equal(DESCRIPTOR_HEX);
			expect((await documentStore.calculateAddress()).address).equal(
				DESCRIPTOR_ADDRESS,
			);
		}
	});

	it("preserves ordinary PUT and DELETE wire discriminators and payloads", () => {
		expect(hex(serialize(value()))).equal(DOCUMENT_HEX);
		const put = new PutOperation({ data: bytes(DOCUMENT_HEX) });
		const del = new DeleteOperation({ key: toId("wire-key") });
		expect(hex(serialize(put))).equal(PUT_HEX);
		expect(hex(BORSH_ENCODING_OPERATION.encoder(put))).equal(PUT_HEX);
		expect(hex(serialize(del))).equal(DELETE_HEX);
		expect(hex(BORSH_ENCODING_OPERATION.encoder(del))).equal(DELETE_HEX);

		const decodedPut = deserialize(bytes(PUT_HEX), Operation);
		expect(decodedPut).instanceOf(PutOperation);
		expect(hex((decodedPut as PutOperation).data)).equal(DOCUMENT_HEX);
		const decodedDelete = BORSH_ENCODING_OPERATION.decoder(bytes(DELETE_HEX));
		expect(decodedDelete).instanceOf(DeleteOperation);
		expect((decodedDelete as DeleteOperation).key.primitive).equal("wire-key");
	});

	it("opens the persisted descriptor and reloads its address through Peerbit", async () => {
		const peer = await Peerbit.create();
		try {
			const storedAddress = await peer.services.blocks.put(
				bytes(DESCRIPTOR_HEX),
			);
			expect(storedAddress).equal(DESCRIPTOR_ADDRESS);
			const loaded = await Program.load<Documents<Document>>(
				DESCRIPTOR_ADDRESS,
				peer.services.blocks,
			);
			expect(loaded).instanceOf(Documents);
			if (!loaded) throw new Error("Golden Documents descriptor did not load");
			const docs = await peer.open(loaded, { args: openArgs() });
			expect(docs.address).equal(DESCRIPTOR_ADDRESS);
			const put = await docs.put(value(), { target: "none" });
			await put.entry.getPayloadValue();
			expect(put.entry.meta.type).equal(EntryType.APPEND);
			expect(hex(put.entry.payload.data)).equal(PUT_HEX);
			expect((await docs.get("wire-key", { remote: false }))?.name).equal(
				"wire-value",
			);
			expect(hex(serialize(docs))).equal(DESCRIPTOR_HEX);
			await docs.close();

			const reopened = await peer.open<Documents<Document>>(
				DESCRIPTOR_ADDRESS,
				{
					args: openArgs(),
				},
			);
			expect(reopened).instanceOf(Documents);
			expect(reopened.address).equal(DESCRIPTOR_ADDRESS);
			expect((await reopened.get("wire-key", { remote: false }))?.name).equal(
				"wire-value",
			);
			const deleted = await reopened.del("wire-key", { target: "none" });
			await deleted.entry.getPayloadValue();
			expect(deleted.entry.meta.type).equal(EntryType.CUT);
			expect(hex(deleted.entry.payload.data)).equal(DELETE_HEX);
			expect(await reopened.get("wire-key", { remote: false })).equal(
				undefined,
			);
			expect(hex(serialize(reopened))).equal(DESCRIPTOR_HEX);
		} finally {
			await peer.stop();
		}
	});
});
