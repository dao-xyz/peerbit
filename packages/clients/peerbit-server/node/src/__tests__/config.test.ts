import { getPackageName } from "../config";

describe("tgz", () => {
	it("can get package.json name", async () => {
		const pathLib = await import("path");
		const urlLib = await import("url");
		const __filename = urlLib.fileURLToPath(import.meta.url);
		const __dirname = pathLib.dirname(__filename);

		expect(await getPackageName(pathLib.join(__dirname, "/test.tgz"))).toEqual(
			"@peerbit/test-lib"
		);
	});
});
