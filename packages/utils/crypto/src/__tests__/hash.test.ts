import { sha256, sha256Base64, sha256Base64Sync } from "../hash.js";
import {
	sha256 as sha256Browser,
	sha256Base64 as sha256Base64Browser,
	sha256Base64Sync as sha256Base64SyncBrowser,
} from "../hash-browser.js";

import { equals } from "uint8arrays";
import { webcrypto } from "crypto";
globalThis.crypto = webcrypto as any;
it("sha256", async () => {
	const data = new Uint8Array([1, 2, 3]);
	expect(equals(await sha256(data), await sha256Browser(data))).toBeTrue();
});

it("sha256Base64", async () => {
	const data = new Uint8Array([1, 2, 3]);
	expect(await sha256Base64(data)).toEqual(await sha256Base64Browser(data));
});

it("sha256", async () => {
	const data = new Uint8Array([1, 2, 3]);
	expect(sha256Base64Sync(data)).toEqual(sha256Base64SyncBrowser(data));
	expect(sha256Base64Sync(data)).toEqual(await sha256Base64Browser(data));
});
