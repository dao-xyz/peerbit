import { expect } from "chai";
import { getPackageName } from "../src/config.js";

describe("tgz", () => {
	it("can get package.json name", async () => {
		const pathLib = await import("path");
		const urlLib = await import("url");
		const filename = urlLib.fileURLToPath(import.meta.url);
		const dirname = pathLib.dirname(filename);

		expect(await getPackageName(pathLib.join(dirname, "/test.tgz"))).equal(
			"@peerbit/test-lib",
		);
	});
});
