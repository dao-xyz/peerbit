import { expect } from "chai";
import { getPackageName } from "../src/config.js";

describe("tgz", () => {
	it("can get package.json name", async () => {
		const urlLib = await import("url");
		const tgzPath = urlLib.fileURLToPath(
			new URL("../../test/test.tgz", import.meta.url),
		);

		expect(await getPackageName(tgzPath)).equal(
			"@peerbit/test-lib",
		);
	});
});
