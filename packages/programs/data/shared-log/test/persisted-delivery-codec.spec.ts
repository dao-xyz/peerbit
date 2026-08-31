import { deserialize, serialize } from "@dao-xyz/borsh";
import { expect } from "chai";
import { SYNC_CAPABILITY_PERSISTED_ENTRY_RECEIPTS } from "../src/exchange-heads.js";
import { TransportMessage } from "../src/message.js";
import { RequestPersistedEntriesV1 } from "../src/sync/simple.js";

describe("append delivery options — persisted receipt codec", () => {
	it("pins the capability bit and request variant [0,13]", () => {
		expect(SYNC_CAPABILITY_PERSISTED_ENTRY_RECEIPTS).to.equal(1 << 5);

		const request = new RequestPersistedEntriesV1({
			expectedReceiverSession: 0x0102030405060708n,
			hashes: ["a", "bc"],
		});
		const bytes = serialize(request);
		expect(Buffer.from(bytes).toString("hex")).to.equal(
			"00000d0807060504030201020000000100000061020000006263",
		);

		const decoded = deserialize(bytes, TransportMessage);
		expect(decoded).to.be.instanceOf(RequestPersistedEntriesV1);
		expect(
			(decoded as RequestPersistedEntriesV1).expectedReceiverSession,
		).to.equal(0x0102030405060708n);
		expect((decoded as RequestPersistedEntriesV1).hashes).to.deep.equal([
			"a",
			"bc",
		]);
	});
});
