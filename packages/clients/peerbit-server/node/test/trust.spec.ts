import { expect } from "chai";
import fs from "fs";
import os from "os";
import path from "path";
import { Trust } from "../src/trust.js";

describe("trust", () => {
	let directory: string;

	beforeEach(() => {
		directory = fs.mkdtempSync(path.join(os.tmpdir(), "peerbit-trust-"));
	});

	afterEach(() => {
		fs.rmSync(directory, { recursive: true, force: true });
	});

	it("persists removals across reloads", () => {
		const trustPath = path.join(directory, "trust.json");
		const trust = new Trust(trustPath);
		trust.add("revoked-admin");
		trust.add("remaining-admin");

		expect(trust.remove("revoked-admin")).to.equal(true);
		expect(trust.remove("missing-admin")).to.equal(false);
		expect(JSON.parse(fs.readFileSync(trustPath, "utf8"))).to.deep.equal([
			"remaining-admin",
		]);

		const reloaded = new Trust(trustPath);
		expect(reloaded.isTrusted("revoked-admin")).to.equal(false);
		expect(reloaded.isTrusted("remaining-admin")).to.equal(true);
	});
});
