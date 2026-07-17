import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import test from "node:test";
import {
	OPFS_READBACK_CHUNK_BYTES,
	sha256AndCrc32OpfsSavedViaPicker,
} from "./templates/opfs-readback.mjs";

const createNodeSha256 = () => {
	const hash = createHash("sha256");
	return {
		update(bytes) {
			hash.update(bytes);
			return this;
		},
		digest() {
			return new Uint8Array(hash.digest());
		},
	};
};

const sha256Base64 = (bytes) =>
	createHash("sha256").update(bytes).digest("base64");

const createFakeOpfsPage = ({ bytes, truncateOffset = null }) => {
	const readSlices = [];
	return {
		readSlices,
		async evaluate(_callback, argument) {
			if (typeof argument === "string") {
				return { storageName: "fixture.opfs", sizeBytes: bytes.byteLength };
			}
			assert.equal(argument.storageName, "fixture.opfs");
			assert.equal(argument.expectedSize, bytes.byteLength);
			readSlices.push([argument.offset, argument.end]);
			const chunk = bytes.slice(argument.offset, argument.end);
			return argument.offset === truncateOffset ? chunk.subarray(0, -1) : chunk;
		},
	};
};

const readback = (page, bytes) =>
	sha256AndCrc32OpfsSavedViaPicker(
		page,
		"fixture.bin",
		bytes.byteLength,
		createNodeSha256,
	);

test("bounded OPFS readback computes standard SHA-256 and CRC-32", async () => {
	const bytes = new TextEncoder().encode("123456789");
	const page = createFakeOpfsPage({ bytes });
	assert.deepEqual(await readback(page, bytes), {
		sizeBytes: bytes.byteLength,
		sha256Base64: sha256Base64(bytes),
		crc32Hex: "cbf43926",
	});
	assert.deepEqual(page.readSlices, [[0, bytes.byteLength]]);
});

test("bounded OPFS readback detects same-size persisted-byte tampering", async () => {
	const bytes = new Uint8Array(OPFS_READBACK_CHUNK_BYTES * 2 + 17);
	for (let index = 0; index < bytes.length; index++) {
		bytes[index] = (index * 31 + 7) & 0xff;
	}
	const originalPage = createFakeOpfsPage({ bytes });
	const original = await readback(originalPage, bytes);
	assert.equal(original.sha256Base64, sha256Base64(bytes));
	assert.deepEqual(originalPage.readSlices, [
		[0, OPFS_READBACK_CHUNK_BYTES],
		[OPFS_READBACK_CHUNK_BYTES, OPFS_READBACK_CHUNK_BYTES * 2],
		[OPFS_READBACK_CHUNK_BYTES * 2, bytes.byteLength],
	]);

	const tamperedBytes = bytes.slice();
	tamperedBytes[OPFS_READBACK_CHUNK_BYTES + 123] ^= 0xff;
	assert.equal(tamperedBytes.byteLength, bytes.byteLength);
	const tampered = await readback(
		createFakeOpfsPage({ bytes: tamperedBytes }),
		tamperedBytes,
	);
	assert.equal(tampered.sha256Base64, sha256Base64(tamperedBytes));
	assert.notEqual(tampered.sha256Base64, original.sha256Base64);
	assert.notEqual(tampered.crc32Hex, original.crc32Hex);
});

test("bounded OPFS readback rejects a truncated persisted slice", async () => {
	const bytes = new Uint8Array(OPFS_READBACK_CHUNK_BYTES + 1);
	const page = createFakeOpfsPage({
		bytes,
		truncateOffset: OPFS_READBACK_CHUNK_BYTES,
	});
	await assert.rejects(readback(page, bytes), /truncated chunk/);
});
