import { HashmapIndices, create } from "@peerbit/indexer-simple";
import { SQLiteIndices } from "@peerbit/indexer-sqlite3";
import { expect } from "chai";
import { Peerbit } from "../src/index.js";

const isNode = typeof process !== "undefined" && process.versions?.node != null;

describe("indexer", () => {
	let client: Peerbit;
	let cleanupDirectory: (() => Promise<void>) | undefined;
	afterEach(async () => {
		await client?.stop();
		await cleanupDirectory?.();
		cleanupDirectory = undefined;
	});

	it("sqlite indexer by default", async () => {
		client = await Peerbit.create();
		expect(client.indexer).to.be.instanceOf(SQLiteIndices);
		expect(await client.indexer.persisted()).to.be.false;
	});

	(isNode ? it : it.skip)(
		"sqlite indexer is persistent when opened with a directory",
		async () => {
			// Keep Node built-ins behind non-literal dynamic imports. Aegir bundles
			// this spec for browsers too, where static Node imports cannot resolve.
			const fsModule = "node:fs/promises";
			const osModule = "node:os";
			const pathModule = "node:path";
			const [fs, os, path] = await Promise.all([
				import(fsModule),
				import(osModule),
				import(pathModule),
			]);
			const directory = await fs.mkdtemp(
				path.join(os.tmpdir(), "peerbit-indexer-"),
			);
			cleanupDirectory = () =>
				fs.rm(directory, { recursive: true, force: true });
			client = await Peerbit.create({ directory });
			expect(client.indexer).to.be.instanceOf(SQLiteIndices);
			expect(await client.indexer.persisted()).to.be.true;
		},
	);

	it("can provide custom indexer", async () => {
		client = await Peerbit.create({ indexer: create });
		expect(client.indexer).to.be.instanceOf(HashmapIndices);
		expect(await client.indexer.persisted()).to.be.false;
	});
});
