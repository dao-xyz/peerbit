import { expect } from "chai";

const describeNode =
	typeof process !== "undefined" && process.versions?.node
		? describe
		: describe.skip;

describeNode("SQLite browser worker bundle", () => {
	it("publishes the direct init module without the upstream worker-promiser loader", async () => {
		const fs = await import(["node", "fs"].join(":"));
		const path = await import(["node", "path"].join(":"));
		const packageRoot = process.cwd();
		const workerPath = path.resolve(
			packageRoot,
			"dist/assets/sqlite3/sqlite3.worker.min.js",
		);
		const modulePath = path.resolve(
			packageRoot,
			"dist/assets/sqlite3/sqlite3.mjs",
		);
		const packageJson = JSON.parse(
			fs.readFileSync(path.resolve(packageRoot, "package.json"), "utf8"),
		);
		const worker = fs.readFileSync(workerPath, "utf8");

		expect(fs.existsSync(modulePath)).to.be.true;
		expect(packageJson.files).not.to.include(
			"!dist/assets/sqlite3/sqlite3.mjs",
		);
		expect(worker).to.include("/peerbit/sqlite3/sqlite3.mjs");
		expect(worker).not.to.include("sqlite3-worker1.js");
	});
});
