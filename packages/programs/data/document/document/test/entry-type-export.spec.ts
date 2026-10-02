import { EntryType } from "@peerbit/document";
import { EntryType as LogEntryType } from "@peerbit/log";
import { expect } from "chai";

describe("document entry type export", () => {
	it("re-exports the existing log enum for public put metadata", () => {
		const type: EntryType = LogEntryType.CUT;
		expect(EntryType).to.equal(LogEntryType);
		expect(EntryType.APPEND).to.equal(0);
		expect(type).to.equal(1);
	});
});
