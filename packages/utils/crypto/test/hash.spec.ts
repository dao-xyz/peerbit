import { expect } from "chai";
import sodium from "libsodium-wrappers";
import { equals } from "uint8arrays";
import {
	sha256,
	sha256Base64,
	sha256Base64 as sha256Base64Browser,
	sha256Base64Sync,
	sha256Base64Sync as sha256Base64SyncBrowser,
	sha256 as sha256Browser,
	sha256Sync,
	sha256Sync as sha256SyncBrowser,
} from "../src/hash.js";

describe("hash", () => {
	before(async () => {
		await sodium.ready;
	});
	it("sha256", async () => {
		const data = new Uint8Array([1, 2, 3]);
		expect(equals(await sha256(data), await sha256Browser(data))).to.be.true;
	});

	it("sha256Sync", async () => {
		const data = new Uint8Array([1, 2, 3]);
		expect(equals(sha256Sync(data), sha256SyncBrowser(data))).to.be.true;
	});

	it("sha256Base64", async () => {
		const data = new Uint8Array([1, 2, 3]);
		expect(await sha256Base64(data)).equal(await sha256Base64Browser(data));
	});

	it("sha256Base64Sync", async () => {
		const data = new Uint8Array([1, 2, 3]);
		expect(sha256Base64Sync(data)).equal(sha256Base64SyncBrowser(data));
		expect(sha256Base64Sync(data)).equal(await sha256Base64Browser(data));
	});
});
