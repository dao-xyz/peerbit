import fs from "fs/promises";
import os from "os";
import path from "path";
import { HashmapIndices, create } from "@peerbit/indexer-simple";
import { SQLiteIndices } from "@peerbit/indexer-sqlite3";
import { expect } from "chai";
import { Peerbit } from "../src/index.js";

describe("indexer", () => {
	let client: Peerbit;
	let directory: string | undefined;
	afterEach(async () => {
		await client?.stop();
		if (directory) {
			await fs.rm(directory, { recursive: true, force: true });
			directory = undefined;
		}
	});

	it("sqlite indexer by default", async () => {
		client = await Peerbit.create();
		expect(client.indexer).to.be.instanceOf(SQLiteIndices);
		expect(await client.indexer.persisted()).to.be.false;
	});

	it("sqlite indexer is persistent when opened with a directory", async () => {
		directory = await fs.mkdtemp(path.join(os.tmpdir(), "peerbit-indexer-"));
		client = await Peerbit.create({ directory });
		expect(client.indexer).to.be.instanceOf(SQLiteIndices);
		expect(await client.indexer.persisted()).to.be.true;
	});

	it("can provide custom indexer", async () => {
		client = await Peerbit.create({ indexer: create });
		expect(client.indexer).to.be.instanceOf(HashmapIndices);
		expect(await client.indexer.persisted()).to.be.false;
	});
});
