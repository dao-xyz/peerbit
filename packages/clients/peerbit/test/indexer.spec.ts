import { HashmapIndices, create } from "@peerbit/indexer-simple";
import { SQLiteIndices } from "@peerbit/indexer-sqlite3";
import { expect } from "chai";
import { Peerbit } from "../src";

describe("indexer", () => {
	let client: Peerbit;
	afterEach(async () => {
		await client?.stop();
	});

	it("sqlite indexer by default", async () => {
		client = await Peerbit.create();
		expect(client.indexer).to.be.instanceOf(SQLiteIndices);
	});

	it("can provide custom indexer", async () => {
		client = await Peerbit.create({ indexer: create });
		expect(client.indexer).to.be.instanceOf(HashmapIndices);
	});
});
