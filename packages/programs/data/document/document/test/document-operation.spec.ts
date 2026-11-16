import { deserialize, serialize } from "@dao-xyz/borsh";
import type { IdKey } from "@peerbit/indexer-interface";
import type { Entry } from "@peerbit/log";
import type { SharedLog } from "@peerbit/shared-log";
import { expect } from "chai";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Peerbit } from "peerbit";
import {
	BORSH_ENCODING_OPERATION,
	DeleteOperation,
	type DocumentOperation,
	Documents,
	Operation,
	PutOperation,
	inspectDocumentOperation,
	isDeleteOperation,
	isPutOperation,
	toId,
} from "../src/index.js";
import {
	BORSH_ENCODING_PROOF_OPERATION,
	ProofDeleteOperation,
	ProofPutOperation,
} from "../src/proof-operations.js";
import { Document } from "./data.js";

// Compiled, not executed: keep existing result/log assignments compatible while
// exposing enough information to inspect proof payloads without private imports.
const publicTypeCompatibility = async (documents: Documents<Document>) => {
	const oldLog: SharedLog<Operation> = documents.log;
	const result = await documents.put(new Document({ id: "typed" }));
	const oldEntry: Entry<Operation> = result.entry;
	const neutralEntry: Entry<DocumentOperation> = oldEntry;
	const view = inspectDocumentOperation(await neutralEntry.getPayloadValue());
	if (view?.format === "owner-proofs-v1") {
		const key: IdKey = view.operation.key;
		if (view.type === "put") {
			const bytes: Uint8Array = view.operation.data;
			void bytes;
		} else {
			const target: string = view.operation.target;
			void target;
		}
		void key;
	} else if (view?.type === "put") {
		const bytes: Uint8Array = view.operation.data;
		void bytes;
	}
	return { oldLog, oldEntry };
};
void publicTypeCompatibility;

describe("document operation inspection", () => {
	const scope = new Uint8Array(32);
	const fixtures = [
		{ operation: new PutOperation({ data: Uint8Array.of(1) }), type: "put" },
		{ operation: new DeleteOperation({ key: toId("a") }), type: "delete" },
		{
			// Decode the existing tag-0 tombstone fixture; never create new legacy data.
			operation: deserialize(
				Uint8Array.from(
					Buffer.from(
						"00000d000000746f6d6273746f6e652d646f630400000001020304",
						"hex",
					),
				),
				Operation,
			),
			type: "put",
		},
		{
			operation: deserialize(
				Uint8Array.from(
					Buffer.from("00020d000000746f6d6273746f6e652d646f63", "hex"),
				),
				Operation,
			),
			type: "delete",
		},
		{
			operation: new ProofPutOperation({
				scope,
				key: toId("a"),
				data: Uint8Array.of(3),
			}),
			type: "put",
		},
		{
			operation: new ProofDeleteOperation({
				scope,
				key: toId("a"),
				target: "inspection-does-not-authenticate-this-target",
				witnesses: [Uint8Array.of(4)],
			}),
			type: "delete",
		},
	] as const;
	for (const { operation, type } of fixtures) {
		it(`inspects ${operation.constructor.name} without normalizing its bytes`, () => {
			const legacy = operation instanceof Operation;
			const encoding = legacy
				? BORSH_ENCODING_OPERATION
				: BORSH_ENCODING_PROOF_OPERATION;
			const bytes = encoding.encoder(operation);
			const decoded = encoding.decoder(bytes);
			const view = inspectDocumentOperation(decoded);
			expect(view?.format).equal(legacy ? "legacy" : "owner-proofs-v1");
			expect(view?.type).equal(type);
			expect(view?.operation).equal(decoded);
			expect(encoding.encoder(decoded)).deep.equal(bytes);
			expect(isPutOperation(decoded)).equal(legacy && type === "put");
			expect(isDeleteOperation(decoded)).equal(legacy && type === "delete");
			if (!legacy)
				expect(() => BORSH_ENCODING_OPERATION.decoder(bytes)).to.throw();
		});
	}

	it("does not classify unknown or structural lookalike payloads", () => {
		for (const value of [
			undefined,
			null,
			1,
			"put",
			{},
			{ scope, key: toId("a"), data: new Uint8Array() },
		])
			expect(inspectDocumentOperation(value)).equal(undefined);
	});

	for (const proof of [false, true]) {
		it(`decodes real ${proof ? "owner-proofs" : "legacy"} put/delete results with their store codec`, async () => {
			const directory = await mkdtemp(join(tmpdir(), "peerbit-operation-api-"));
			let peer: Peerbit | undefined;
			try {
				peer = await Peerbit.create({ directory });
				const documents = await peer.open(
					new Documents<Document>({
						format: proof
							? {
									version: "owner-proofs-v1",
									owner: peer.identity.publicKey,
									keyType: "string",
								}
							: undefined,
					}),
					{
						args: {
							type: Document,
							nativeGraph: false,
							timeUntilRoleMaturity: 0,
						},
					},
				);
				const put = await documents.put(
					new Document({ id: "a", name: "value" }),
				);
				const del = await documents.del("a");
				for (const [type, result] of [
					["put", put],
					["delete", del],
				] as const) {
					const payload: DocumentOperation =
						await result.entry.getPayloadValue();
					const bytes = serialize(payload);
					const decoded = documents.log.log.encoding.decoder(bytes);
					const view = inspectDocumentOperation(decoded);
					expect(view?.format).equal(proof ? "owner-proofs-v1" : "legacy");
					expect(view?.type).equal(type);
					expect(documents.log.log.encoding.encoder(decoded)).deep.equal(bytes);
				}
			} finally {
				await peer?.stop();
				await rm(directory, { recursive: true, force: true });
			}
		});
	}
});
