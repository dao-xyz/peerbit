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
		const browserEntryPath = path.resolve(
			packageRoot,
			"dist/src/sqlite3.wasm.js",
		);
		const browserProxyPath = path.resolve(
			packageRoot,
			"dist/src/sqlite3.browser.js",
		);
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
		const browserEntry = fs.readFileSync(browserEntryPath, "utf8");
		const browserProxy = fs.readFileSync(browserProxyPath, "utf8");
		const worker = fs.readFileSync(workerPath, "utf8");

		expect(fs.existsSync(modulePath)).to.be.true;
		expect(packageJson.files).not.to.include(
			"!dist/assets/sqlite3/sqlite3.mjs",
		);
		expect(worker).to.include("/peerbit/sqlite3/sqlite3.mjs");
		expect(worker).not.to.include("sqlite3-worker1.js");
		expect(worker).not.to.include("__vite__injectQuery");
		expect(browserEntry).to.include("import(");
		expect(browserEntry).to.include(
			"new URL(SQLITE3_MODULE_PATH, globalThis.location.href)",
		);
		expect(browserEntry).not.to.include('document.createElement("script")');
		expect(browserProxy).to.include(
			"new URL(SQLITE3_WORKER_PATH, globalThis.location.href)",
		);
		expect(browserProxy).to.include(
			'new Worker(workerUrl, { type: "module" })',
		);
		expect(browserProxy).not.to.include("new Worker(new URL(");
	});
});
