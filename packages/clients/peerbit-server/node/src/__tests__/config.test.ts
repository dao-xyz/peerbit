import { getPackageName } from "../config";

describe("tgz", () => {
	it("can get package.json name", async () => {
		const fs = await import("fs");
		const pathLib = await import("path");
		const tar = await import("tar-stream");
		const zlib = await import("zlib");
		const urlLib = await import("url");

		const __filename = urlLib.fileURLToPath(import.meta.url);
		const __dirname = pathLib.dirname(__filename);

		expect(await getPackageName(pathLib.join(__dirname, "/test.tgz"))).toEqual(
			"@peerbit/server"
		);
	});
});
