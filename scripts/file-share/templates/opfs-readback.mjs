export const OPFS_READBACK_CHUNK_BYTES = 4 * 1024 * 1024;

const CRC32_INITIAL_STATE = 0xffffffff;
const CRC32_TABLE = (() => {
	const table = new Uint32Array(256);
	for (let index = 0; index < table.length; index++) {
		let value = index;
		for (let bit = 0; bit < 8; bit++) {
			value = value & 1 ? 0xedb88320 ^ (value >>> 1) : value >>> 1;
		}
		table[index] = value >>> 0;
	}
	return table;
})();

const updateCrc32State = (state, bytes) => {
	let next = state >>> 0;
	for (const byte of bytes) {
		next = CRC32_TABLE[(next ^ byte) & 0xff] ^ (next >>> 8);
	}
	return next >>> 0;
};

const formatCrc32State = (state) =>
	((state ^ CRC32_INITIAL_STATE) >>> 0).toString(16).padStart(8, "0");

const toBase64 = (bytes) => btoa(String.fromCharCode(...bytes));

// OPFS is an explicit persistence-diagnostic cohort. Its bounded cryptographic
// readback runs after the measured sink has closed, so it cannot extend the
// download timing or materialize the whole benchmark file in renderer memory.
export const sha256AndCrc32OpfsSavedViaPicker = async (
	page,
	fileName,
	expectedSizeBytes,
	createSha256,
) => {
	if (typeof createSha256 !== "function") {
		throw new TypeError(
			"OPFS readback requires an incremental SHA-256 factory",
		);
	}
	const saved = await page.evaluate(async (expectedName) => {
		const files = window.__mockSavedFiles ?? [];
		const match = [...files]
			.reverse()
			.find((file) => file.name === expectedName && file.sink === "opfs");
		if (!match) {
			throw new Error(`No completed OPFS sink named "${expectedName}"`);
		}
		const root = await navigator.storage.getDirectory();
		const handle = await root.getFileHandle(match.storageName);
		const file = await handle.getFile();
		return { storageName: match.storageName, sizeBytes: file.size };
	}, fileName);
	if (saved.sizeBytes !== expectedSizeBytes) {
		throw new Error("Persisted OPFS readback size does not match fixture");
	}

	const sha256 = createSha256();
	let crc32State = CRC32_INITIAL_STATE;
	let totalBytes = 0;
	for (
		let offset = 0;
		offset < expectedSizeBytes;
		offset += OPFS_READBACK_CHUNK_BYTES
	) {
		const end = Math.min(expectedSizeBytes, offset + OPFS_READBACK_CHUNK_BYTES);
		const chunk = await page.evaluate(
			async ({ storageName, offset: start, end: finish, expectedSize }) => {
				const root = await navigator.storage.getDirectory();
				const handle = await root.getFileHandle(storageName);
				const file = await handle.getFile();
				if (file.size !== expectedSize) {
					throw new Error("Persisted OPFS file changed during readback");
				}
				return new Uint8Array(await file.slice(start, finish).arrayBuffer());
			},
			{
				storageName: saved.storageName,
				offset,
				end,
				expectedSize: expectedSizeBytes,
			},
		);
		if (!(chunk instanceof Uint8Array)) {
			throw new Error("Persisted OPFS readback returned invalid bytes");
		}
		if (chunk.byteLength !== end - offset) {
			throw new Error("Persisted OPFS readback returned a truncated chunk");
		}
		sha256.update(chunk);
		crc32State = updateCrc32State(crc32State, chunk);
		totalBytes += chunk.byteLength;
	}
	if (totalBytes !== expectedSizeBytes) {
		throw new Error(
			"Persisted OPFS readback byte count does not match fixture",
		);
	}
	const digest = sha256.digest();
	if (!(digest instanceof Uint8Array) || digest.byteLength !== 32) {
		throw new Error("Incremental OPFS SHA-256 returned an invalid digest");
	}
	return {
		sizeBytes: totalBytes,
		sha256Base64: toBase64(digest),
		crc32Hex: formatCrc32State(crc32State),
	};
};
